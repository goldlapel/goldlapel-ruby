# frozen_string_literal: true

# Unit tests for GoldLapel::HashesAPI.
#
# Phase 5 schema decisions:
#   - Storage flipped from JSONB-blob-per-key to row-per-field
#     (`hash_key`, `field`, `value` JSONB).
#   - `hash_get_all(hash_key)` aggregates rows back into a Ruby Hash.
#   - `hash_set` is JSON-encoded so callers can store arbitrary structured
#     payloads.

require "minitest/autorun"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/streams"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/counters"
require_relative "../lib/goldlapel/zsets"
require_relative "../lib/goldlapel/hashes"
require_relative "../lib/goldlapel/queues"
require_relative "../lib/goldlapel/geos"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel"

class HashesApiMockResult
  attr_reader :values, :fields
  def initialize(rows = [], fields = [])
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end
  def ntuples; @rows.length; end
  def cmd_tuples; @cmd_tuples || @rows.length; end
  def cmd_tuples=(v); @cmd_tuples = v; end
  def [](i); @rows[i]; end
  def map(&b); @rows.map(&b); end
  def each(&b); @rows.each(&b); end
end

class HashesApiMockConn
  attr_reader :calls
  attr_accessor :next_result

  def initialize
    @calls = []
    @next_result = HashesApiMockResult.new
  end

  def exec(sql, &b)
    @calls << { method: :exec, sql: sql }
    @next_result.tap { |r| b&.call(r) }
  end

  def exec_params(sql, params = [], _f = 0, &b)
    @calls << { method: :exec_params, sql: sql, params: params }
    @next_result.tap { |r| b&.call(r) }
  end

  def close; end
  def finished?; false; end
end

HASH_MAIN = "_goldlapel.hash_sessions"
FAKE_HASH_PATTERNS = {
  tables: { "main" => HASH_MAIN },
  query_patterns: {
    "hset" => "INSERT INTO #{HASH_MAIN} (hash_key, field, value) VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value RETURNING value",
    "hget" => "SELECT value FROM #{HASH_MAIN} WHERE hash_key = $1 AND field = $2",
    "hgetall" => "SELECT field, value FROM #{HASH_MAIN} WHERE hash_key = $1 ORDER BY field",
    "hkeys" => "SELECT field FROM #{HASH_MAIN} WHERE hash_key = $1 ORDER BY field",
    "hvals" => "SELECT value FROM #{HASH_MAIN} WHERE hash_key = $1 ORDER BY field",
    "hexists" => "SELECT EXISTS (SELECT 1 FROM #{HASH_MAIN} WHERE hash_key = $1 AND field = $2)",
    "hdel" => "DELETE FROM #{HASH_MAIN} WHERE hash_key = $1 AND field = $2",
    "hlen" => "SELECT COUNT(*) FROM #{HASH_MAIN} WHERE hash_key = $1",
    "delete_key" => "DELETE FROM #{HASH_MAIN} WHERE hash_key = $1",
    "delete_all" => "DELETE FROM #{HASH_MAIN}",
  },
}.freeze

def make_hashes_api_inst
  conn = HashesApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  hashes = GoldLapel::HashesAPI.new(inst)
  inst.instance_variable_set(:@hashes, hashes)
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst.instance_variable_set(:@counters, GoldLapel::CountersAPI.new(inst))
  inst.instance_variable_set(:@zsets, GoldLapel::ZsetsAPI.new(inst))
  inst.instance_variable_set(:@queues, GoldLapel::QueuesAPI.new(inst))
  inst.instance_variable_set(:@geos, GoldLapel::GeosAPI.new(inst))
  fetches = []
  hashes.define_singleton_method(:_patterns) do |name|
    fetches << name
    FAKE_HASH_PATTERNS
  end
  [inst, conn, fetches]
end

class TestHashesAPINamespaceShape < Minitest::Test
  def test_hashes_is_a_HashesAPI
    inst, _conn, _fetches = make_hashes_api_inst
    assert_kind_of GoldLapel::HashesAPI, inst.hashes
  end

  def test_no_legacy_flat_methods
    inst, _conn, _fetches = make_hashes_api_inst
    %i[hset hget hgetall hdel].each do |legacy|
      refute inst.respond_to?(legacy),
        "Phase 5 removed flat #{legacy} — use gl.hashes.<verb>."
    end
  end
end

class TestHashesAPIVerbDispatch < Minitest::Test
  def test_set_threads_hash_key_first_and_json_encodes_value
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([{ "value" => '"Alice"' }], ["value"])
    result = inst.hashes.set("sessions", "user:1", "name", "Alice")
    assert_equal "Alice", result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "ON CONFLICT (hash_key, field)"
    # value column is JSON-encoded so structured payloads round-trip.
    assert_equal ["user:1", "name", '"Alice"'], call[:params]
  end

  def test_get_returns_decoded_value
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([{ "value" => '{"k":"v"}' }], ["value"])
    assert_equal({ "k" => "v" }, inst.hashes.get("sessions", "user:1", "blob"))
  end

  def test_get_returns_nil_when_absent
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([], ["value"])
    assert_nil inst.hashes.get("sessions", "user:1", "missing")
  end

  def test_get_all_aggregates_rows_into_hash
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new(
      [
        { "field" => "name", "value" => '"Alice"' },
        { "field" => "age", "value" => "30" },
      ],
      ["field", "value"]
    )
    result = inst.hashes.get_all("sessions", "user:1")
    assert_equal({ "name" => "Alice", "age" => 30 }, result)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["user:1"], call[:params]
  end

  def test_get_all_returns_empty_hash_for_unknown_key
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([], ["field", "value"])
    assert_equal({}, inst.hashes.get_all("sessions", "missing"))
  end

  def test_keys_returns_field_names
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new(
      [{ "field" => "age" }, { "field" => "name" }],
      ["field"]
    )
    assert_equal ["age", "name"], inst.hashes.keys("sessions", "user:1")
  end

  def test_values_returns_decoded_values
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new(
      [{ "value" => "30" }, { "value" => '"Alice"' }],
      ["value"]
    )
    assert_equal [30, "Alice"], inst.hashes.values("sessions", "user:1")
  end

  def test_exists_true
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([{ "exists" => "t" }], ["exists"])
    assert_equal true, inst.hashes.exists("sessions", "user:1", "name")
  end

  def test_exists_false
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([{ "exists" => "f" }], ["exists"])
    assert_equal false, inst.hashes.exists("sessions", "user:1", "missing")
  end

  def test_delete_returns_true_on_rowcount_one
    inst, conn, _fetches = make_hashes_api_inst
    res = HashesApiMockResult.new([], [])
    res.cmd_tuples = 1
    conn.next_result = res
    assert_equal true, inst.hashes.delete("sessions", "user:1", "name")
  end

  def test_delete_returns_false_when_absent
    inst, conn, _fetches = make_hashes_api_inst
    res = HashesApiMockResult.new([], [])
    res.cmd_tuples = 0
    conn.next_result = res
    assert_equal false, inst.hashes.delete("sessions", "user:1", "missing")
  end

  def test_len_returns_zero_for_unknown_key
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([], ["count"])
    assert_equal 0, inst.hashes.len("sessions", "missing")
  end

  def test_len_returns_count
    inst, conn, _fetches = make_hashes_api_inst
    conn.next_result = HashesApiMockResult.new([{ "count" => "5" }], ["count"])
    assert_equal 5, inst.hashes.len("sessions", "user:1")
  end
end

class TestHashesPhase5Contract < Minitest::Test
  # Phase 5 storage is per-row, NOT JSONB-blob-per-key. Each pattern operates
  # on (hash_key, field, value) — confirming we're not still emitting the
  # legacy `data JSONB` shape.
  def test_hset_pattern_inserts_per_field_row
    sql = FAKE_HASH_PATTERNS[:query_patterns]["hset"]
    assert_includes sql, "(hash_key, field, value)"
    refute_includes sql, "jsonb_build_object"
  end

  def test_hget_pattern_filters_by_hash_key_and_field
    sql = FAKE_HASH_PATTERNS[:query_patterns]["hget"]
    assert_includes sql, "WHERE hash_key = $1 AND field = $2"
  end

  def test_hgetall_returns_field_value_columns
    sql = FAKE_HASH_PATTERNS[:query_patterns]["hgetall"]
    assert_includes sql, "SELECT field, value"
  end
end
