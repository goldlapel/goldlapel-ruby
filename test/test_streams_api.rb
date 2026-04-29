# frozen_string_literal: true

# Unit tests for GoldLapel::StreamsAPI — the nested `gl.streams.<verb>`
# namespace introduced alongside Phase 4 of schema-to-core.
#
# (Streams DDL ownership shipped earlier — Phase 1+2; the namespace nesting
# restructure is the new piece here.)

require "minitest/autorun"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/streams"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel"

class StreamsApiMockResult
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

class StreamsApiMockConn
  attr_reader :calls
  def initialize
    @calls = []
    @insert_row = StreamsApiMockResult.new(
      [{ "id" => "1", "created_at" => "2026-04-28" }], ["id", "created_at"]
    )
  end
  def transaction
    @calls << { method: :begin }
    yield self
    @calls << { method: :commit }
  end
  def in_transaction?; true; end
  def exec(sql, &b)
    @calls << { method: :exec, sql: sql }
    StreamsApiMockResult.new.tap { |r| b&.call(r) }
  end
  def exec_params(sql, params = [], _f = 0, &b)
    @calls << { method: :exec_params, sql: sql, params: params }
    @insert_row.tap { |r| b&.call(r) }
  end
  def close; end
  def finished?; false; end
end

def make_streams_api_inst
  conn = StreamsApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  streams = GoldLapel::StreamsAPI.new(inst)
  inst.instance_variable_set(:@streams, streams)
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  fetches = []
  streams.define_singleton_method(:_patterns) do |stream|
    fetches << stream
    {
      tables: { "main" => "_goldlapel.stream_#{stream}" },
      query_patterns: {
        "insert" => "INSERT INTO _goldlapel.stream_#{stream} (payload) VALUES ($1) RETURNING id, created_at",
        "create_group" => "INSERT INTO _goldlapel.stream_#{stream}_groups (group_name) VALUES ($1) ON CONFLICT DO NOTHING",
        "ack" => "DELETE FROM _goldlapel.stream_#{stream}_pending WHERE group_name = $1 AND message_id = $2",
      },
    }
  end
  [inst, conn, fetches]
end

class TestStreamsAPINamespaceShape < Minitest::Test
  def test_streams_is_a_StreamsAPI
    inst, _conn, _fetches = make_streams_api_inst
    assert_kind_of GoldLapel::StreamsAPI, inst.streams
  end

  def test_streams_holds_back_reference_to_parent
    inst, _conn, _fetches = make_streams_api_inst
    assert_same inst, inst.streams.instance_variable_get(:@gl)
  end

  def test_no_legacy_flat_stream_methods_on_instance
    inst, _conn, _fetches = make_streams_api_inst
    %i[stream_add stream_create_group stream_read stream_ack stream_claim].each do |legacy|
      refute inst.respond_to?(legacy),
        "Legacy flat method #{legacy} should have been removed; use gl.streams.<verb>."
    end
  end
end

class TestStreamsAPIVerbDispatch < Minitest::Test
  def test_add_dispatches_to_stream_add
    inst, conn, fetches = make_streams_api_inst
    inst.streams.add("events", { type: "click" })
    insert = conn.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert
    assert_includes insert[:sql], "_goldlapel.stream_events"
    assert_equal ["events"], fetches
  end

  def test_create_group_passes_group
    inst, conn, _fetches = make_streams_api_inst
    inst.streams.create_group("events", "workers")
    insert = conn.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert
    assert_equal ["workers"], insert[:params]
  end

  def test_ack_passes_message_id
    inst, conn, _fetches = make_streams_api_inst
    inst.streams.ack("events", "workers", 42)
    del = conn.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DELETE") }
    refute_nil del
    assert_equal ["workers", 42], del[:params]
  end
end
