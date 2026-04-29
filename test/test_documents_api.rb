# frozen_string_literal: true

# Unit tests for GoldLapel::DocumentsAPI — the nested `gl.documents.<verb>`
# namespace introduced in Phase 4 of schema-to-core.
#
# Asserts:
#   - gl.documents is a DocumentsAPI bound to the parent client (back-ref,
#     no state duplication)
#   - Each verb fetches DDL patterns via DocumentsAPI#_patterns then
#     dispatches to GoldLapel.doc_*
#   - The unlogged kwarg flows through to the DDL options
#   - $lookup.from collections in aggregate are resolved via the proxy too
#   - Hard cut — flat `gl.doc_*` methods are gone

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/streams"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel"

class DocsApiMockResult
  attr_reader :values, :fields

  def initialize(rows = [], fields = [])
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end

  def ntuples; @rows.length; end
  def cmd_tuples; @rows.length; end
  def [](i); @rows[i]; end
  def map(&b); @rows.map(&b); end
  def each(&b); @rows.each(&b); end
end

class DocsApiMockConn
  attr_reader :calls

  def initialize
    @calls = []
    @insert_row = DocsApiMockResult.new(
      [{ "_id" => "00000000-0000-0000-0000-000000000000",
         "data" => "{}",
         "created_at" => "2026-04-28" }],
      ["_id", "data", "created_at"]
    )
    @empty = DocsApiMockResult.new
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql }
    @empty.tap { |r| block&.call(r) }
  end

  def exec_params(sql, params = [], _fmt = 0, &block)
    @calls << { method: :exec_params, sql: sql, params: params }
    r = sql.include?("RETURNING") || sql.include?("SELECT") ? @insert_row : @empty
    block&.call(r)
    r
  end

  def close; end
  def finished?; false; end
end

# Build an Instance + DocumentsAPI without spawning the binary. The
# DocumentsAPI's _patterns is replaced with a tracked stub so we can assert
# the patterns came from the proxy round-trip.
def make_docs_api_inst
  conn = DocsApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  documents = GoldLapel::DocumentsAPI.new(inst)
  inst.instance_variable_set(:@documents, documents)
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  fetches = []
  documents.define_singleton_method(:_patterns) do |collection, **opts|
    fetches << { collection: collection, opts: opts }
    {
      tables: { "main" => "_goldlapel.doc_#{collection}" },
      query_patterns: {},
    }
  end
  [inst, conn, fetches]
end

class TestDocumentsAPINamespaceShape < Minitest::Test
  def test_documents_is_a_DocumentsAPI
    inst, _conn, _fetches = make_docs_api_inst
    assert_kind_of GoldLapel::DocumentsAPI, inst.documents
  end

  def test_documents_holds_back_reference_to_parent
    inst, _conn, _fetches = make_docs_api_inst
    assert_same inst, inst.documents.instance_variable_get(:@gl)
  end

  def test_no_legacy_flat_doc_methods_on_instance
    inst, _conn, _fetches = make_docs_api_inst
    %i[doc_insert doc_find doc_update doc_delete doc_count
       doc_create_collection doc_aggregate].each do |legacy|
      refute inst.respond_to?(legacy),
        "Legacy flat method #{legacy} should have been removed; use gl.documents.#{legacy.to_s.sub('doc_', '')}."
    end
  end
end

class TestDocumentsAPIVerbDispatch < Minitest::Test
  def test_insert_dispatches_through_doc_table
    inst, conn, fetches = make_docs_api_inst
    inst.documents.insert("users", { name: "alice" })

    insert_call = conn.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "INSERT INTO _goldlapel.doc_users"
    assert_equal 1, fetches.length
    assert_equal "users", fetches[0][:collection]
  end

  def test_find_passes_filter_and_kwargs
    inst, conn, _fetches = make_docs_api_inst
    inst.documents.find("users", filter: { a: 1 }, sort: { b: 1 }, limit: 5, skip: 2)

    select_call = conn.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil select_call
    assert_includes select_call[:sql], "FROM _goldlapel.doc_users"
    assert_includes select_call[:sql], "ORDER BY"
    assert_includes select_call[:sql], "LIMIT"
    assert_includes select_call[:sql], "OFFSET"
  end

  def test_update_one_passes_filter_and_update
    inst, conn, _fetches = make_docs_api_inst
    inst.documents.update_one("users", { "id" => 1 }, { "$set" => { "name" => "x" } })
    upd = conn.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil upd
    assert_includes upd[:sql], "_goldlapel.doc_users"
  end

  def test_count_passes_filter
    inst, conn, _fetches = make_docs_api_inst
    inst.documents.count("users", filter: { active: true })
    sel = conn.calls.find { |c| c[:sql].include?("SELECT COUNT(*)") }
    refute_nil sel
    assert_includes sel[:sql], "FROM _goldlapel.doc_users"
  end

  def test_create_collection_just_fetches_patterns
    inst, conn, fetches = make_docs_api_inst
    inst.documents.create_collection("users")
    assert_equal 1, fetches.length
    assert_equal "users", fetches[0][:collection]
    assert_equal false, fetches[0][:opts][:unlogged]
    assert_empty conn.calls,
                 "create_collection must not issue any SQL — proxy owns DDL"
  end

  def test_create_collection_unlogged_passes_through
    inst, _conn, fetches = make_docs_api_inst
    inst.documents.create_collection("sessions", unlogged: true)
    assert_equal({ unlogged: true }, fetches[0][:opts])
  end
end

class TestDocumentsAPIAggregateLookupResolution < Minitest::Test
  def test_aggregate_resolves_lookup_from_collections
    # Two distinct fetches expected: one for the source ("users"), one per
    # unique $lookup.from collection ("orders").
    inst, conn, fetches = make_docs_api_inst

    inst.documents.aggregate("users", [
      { "$match" => { "active" => true } },
      { "$lookup" => {
        "from" => "orders",
        "localField" => "id",
        "foreignField" => "userId",
        "as" => "user_orders",
      }},
    ])

    fetched_collections = fetches.map { |f| f[:collection] }
    assert_includes fetched_collections, "users"
    assert_includes fetched_collections, "orders"

    sql = conn.calls.find { |c| c[:method] == :exec_params }[:sql]
    assert_includes sql, "FROM _goldlapel.doc_users"
    assert_includes sql, "FROM _goldlapel.doc_orders AS orders"
  end

  def test_aggregate_caches_repeated_lookup_from
    # Two $lookup stages with the same `from` should result in only one
    # additional pattern fetch (the source counts as one, dedup on `orders`).
    inst, _conn, fetches = make_docs_api_inst

    inst.documents.aggregate("users", [
      { "$lookup" => {
        "from" => "orders",
        "localField" => "id",
        "foreignField" => "userId",
        "as" => "first_orders",
      }},
      { "$lookup" => {
        "from" => "orders",
        "localField" => "id",
        "foreignField" => "userId",
        "as" => "second_orders",
      }},
    ])

    counts = fetches.group_by { |f| f[:collection] }.transform_values(&:size)
    assert_equal 1, counts["users"]
    assert_equal 1, counts["orders"], "duplicate $lookup.from should hit the per-pipeline dedup"
  end
end
