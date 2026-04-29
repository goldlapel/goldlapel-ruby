# frozen_string_literal: true

# Unit tests for GoldLapel::CountersAPI — the nested gl.counters namespace
# introduced in Phase 5 of schema-to-core (counter / zset / hash / queue / geo).
#
# Tests cover:
#   - gl.counters is a CountersAPI bound to the parent client.
#   - Each verb fetches DDL patterns from the proxy and dispatches to the
#     `GoldLapel.counter_*` helper with the right args.
#   - SQL builders execute the proxy's canonical query patterns verbatim.
#   - Phase-5 counter `updated_at` parity: the canonical patterns reference
#     `NOW()` on every UPDATE — wrappers don't paper over this.

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

class CountersApiMockResult
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

class CountersApiMockConn
  attr_reader :calls
  attr_accessor :next_result

  def initialize
    @calls = []
    @next_result = CountersApiMockResult.new
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

FAKE_COUNTER_PATTERNS = {
  tables: { "main" => "_goldlapel.counter_pageviews" },
  query_patterns: {
    "incr" => "INSERT INTO _goldlapel.counter_pageviews (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = _goldlapel.counter_pageviews.value + EXCLUDED.value, updated_at = NOW() RETURNING value",
    "set" => "INSERT INTO _goldlapel.counter_pageviews (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value",
    "get" => "SELECT value FROM _goldlapel.counter_pageviews WHERE key = $1",
    "delete" => "DELETE FROM _goldlapel.counter_pageviews WHERE key = $1",
    "delete_all" => "DELETE FROM _goldlapel.counter_pageviews",
    "count_keys" => "SELECT COUNT(*) FROM _goldlapel.counter_pageviews",
  },
}.freeze

def make_counters_api_inst
  conn = CountersApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  counters = GoldLapel::CountersAPI.new(inst)
  inst.instance_variable_set(:@counters, counters)
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst.instance_variable_set(:@zsets, GoldLapel::ZsetsAPI.new(inst))
  inst.instance_variable_set(:@hashes, GoldLapel::HashesAPI.new(inst))
  inst.instance_variable_set(:@queues, GoldLapel::QueuesAPI.new(inst))
  inst.instance_variable_set(:@geos, GoldLapel::GeosAPI.new(inst))
  fetches = []
  counters.define_singleton_method(:_patterns) do |name|
    fetches << name
    FAKE_COUNTER_PATTERNS
  end
  [inst, conn, fetches]
end

class TestCountersAPINamespaceShape < Minitest::Test
  def test_counters_is_a_CountersAPI
    inst, _conn, _fetches = make_counters_api_inst
    assert_kind_of GoldLapel::CountersAPI, inst.counters
  end

  def test_counters_holds_back_reference_to_parent
    inst, _conn, _fetches = make_counters_api_inst
    assert_same inst, inst.counters.instance_variable_get(:@gl)
  end

  def test_no_legacy_flat_methods_on_instance
    inst, _conn, _fetches = make_counters_api_inst
    %i[incr get_counter].each do |legacy|
      refute inst.respond_to?(legacy),
        "Phase 5 removed flat #{legacy} — use gl.counters.<verb>."
    end
  end
end

class TestCountersAPIVerbDispatch < Minitest::Test
  def test_incr_uses_incr_pattern_and_passes_key_amount
    inst, conn, fetches = make_counters_api_inst
    conn.next_result = CountersApiMockResult.new([{ "value" => "7" }], ["value"])
    result = inst.counters.incr("pageviews", "home", 5)
    assert_equal 7, result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "INSERT INTO _goldlapel.counter_pageviews"
    assert_equal ["home", 5], call[:params]
    assert_equal ["pageviews"], fetches
  end

  def test_decr_passes_negative_amount
    inst, conn, _fetches = make_counters_api_inst
    conn.next_result = CountersApiMockResult.new([{ "value" => "-3" }], ["value"])
    inst.counters.decr("pageviews", "home", 3)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["home", -3], call[:params]
  end

  def test_set_passes_value
    inst, conn, _fetches = make_counters_api_inst
    conn.next_result = CountersApiMockResult.new([{ "value" => "100" }], ["value"])
    inst.counters.set("pageviews", "home", 100)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
    assert_equal ["home", 100], call[:params]
  end

  def test_get_returns_zero_for_unknown_key
    inst, conn, _fetches = make_counters_api_inst
    conn.next_result = CountersApiMockResult.new([], ["value"])
    assert_equal 0, inst.counters.get("pageviews", "missing")
  end

  def test_get_returns_value_for_known_key
    inst, conn, _fetches = make_counters_api_inst
    conn.next_result = CountersApiMockResult.new([{ "value" => "42" }], ["value"])
    assert_equal 42, inst.counters.get("pageviews", "home")
  end

  def test_delete_returns_true_on_rowcount_one
    inst, conn, _fetches = make_counters_api_inst
    res = CountersApiMockResult.new([], [])
    res.cmd_tuples = 1
    conn.next_result = res
    assert_equal true, inst.counters.delete("pageviews", "home")
  end

  def test_delete_returns_false_when_absent
    inst, conn, _fetches = make_counters_api_inst
    res = CountersApiMockResult.new([], [])
    res.cmd_tuples = 0
    conn.next_result = res
    assert_equal false, inst.counters.delete("pageviews", "missing")
  end

  def test_count_keys_no_args_after_name
    inst, conn, _fetches = make_counters_api_inst
    conn.next_result = CountersApiMockResult.new([{ "count" => "5" }], ["count"])
    assert_equal 5, inst.counters.count_keys("pageviews")
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [], call[:params]
  end

  def test_create_just_fetches_patterns
    inst, conn, fetches = make_counters_api_inst
    inst.counters.create("pageviews")
    assert_equal ["pageviews"], fetches
    assert_empty conn.calls
  end
end

class TestCountersPhase5Contract < Minitest::Test
  # The proxy's canonical incr/set patterns must stamp `updated_at = NOW()`
  # — wrappers must not paper over that (it's a behavioral contract).
  def test_incr_pattern_stamps_updated_at
    sql = FAKE_COUNTER_PATTERNS[:query_patterns]["incr"]
    assert_includes sql, "updated_at = NOW()"
  end

  def test_set_pattern_stamps_updated_at
    sql = FAKE_COUNTER_PATTERNS[:query_patterns]["set"]
    assert_includes sql, "updated_at = NOW()"
  end
end

class TestCounterUtilsRequirePatterns < Minitest::Test
  def test_counter_incr_raises_when_patterns_nil
    conn = CountersApiMockConn.new
    assert_raises(RuntimeError) { GoldLapel.counter_incr(conn, "x", "y", 1, patterns: nil) }
  end
end
