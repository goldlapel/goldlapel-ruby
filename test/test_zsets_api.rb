# frozen_string_literal: true

# Unit tests for GoldLapel::ZsetsAPI.
#
# Phase 5 introduced a `zset_key` column in the canonical schema so a single
# namespace table holds many sorted sets. These tests verify:
#   - `zset_key` threads through every method as the first positional arg
#     after the namespace `name` (matching Redis ZADD semantics).
#   - Pattern selection picks `zrange_asc` vs `zrange_desc` based on `desc`.
#   - Range/limit translation is Redis-inclusive (start..stop inclusive).
#   - SQL builders bind in `(zset_key, member, score)` order matching the
#     proxy's `$1, $2, $3` template.

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

class ZsetsApiMockResult
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

class ZsetsApiMockConn
  attr_reader :calls
  attr_accessor :next_result

  def initialize
    @calls = []
    @next_result = ZsetsApiMockResult.new
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

ZSET_MAIN = "_goldlapel.zset_leaderboard"
FAKE_ZSET_PATTERNS = {
  tables: { "main" => ZSET_MAIN },
  query_patterns: {
    "zadd" => "INSERT INTO #{ZSET_MAIN} (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = EXCLUDED.score RETURNING score",
    "zincrby" => "INSERT INTO #{ZSET_MAIN} (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = #{ZSET_MAIN}.score + EXCLUDED.score RETURNING score",
    "zscore" => "SELECT score FROM #{ZSET_MAIN} WHERE zset_key = $1 AND member = $2",
    "zrem" => "DELETE FROM #{ZSET_MAIN} WHERE zset_key = $1 AND member = $2",
    "zrange_asc" => "SELECT member, score FROM #{ZSET_MAIN} WHERE zset_key = $1 ORDER BY score ASC, member ASC LIMIT $2 OFFSET $3",
    "zrange_desc" => "SELECT member, score FROM #{ZSET_MAIN} WHERE zset_key = $1 ORDER BY score DESC, member DESC LIMIT $2 OFFSET $3",
    "zrangebyscore" => "SELECT member, score FROM #{ZSET_MAIN} WHERE zset_key = $1 AND score >= $2 AND score <= $3 ORDER BY score ASC, member ASC LIMIT $4 OFFSET $5",
    "zrank_asc" => "SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rank FROM #{ZSET_MAIN} WHERE zset_key = $1 ) ranked WHERE member = $2",
    "zrank_desc" => "SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score DESC, member DESC) - 1 AS rank FROM #{ZSET_MAIN} WHERE zset_key = $1 ) ranked WHERE member = $2",
    "zcard" => "SELECT COUNT(*) FROM #{ZSET_MAIN} WHERE zset_key = $1",
    "delete_key" => "DELETE FROM #{ZSET_MAIN} WHERE zset_key = $1",
    "delete_all" => "DELETE FROM #{ZSET_MAIN}",
  },
}.freeze

def make_zsets_api_inst
  conn = ZsetsApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  zsets = GoldLapel::ZsetsAPI.new(inst)
  inst.instance_variable_set(:@zsets, zsets)
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst.instance_variable_set(:@counters, GoldLapel::CountersAPI.new(inst))
  inst.instance_variable_set(:@hashes, GoldLapel::HashesAPI.new(inst))
  inst.instance_variable_set(:@queues, GoldLapel::QueuesAPI.new(inst))
  inst.instance_variable_set(:@geos, GoldLapel::GeosAPI.new(inst))
  fetches = []
  zsets.define_singleton_method(:_patterns) do |name|
    fetches << name
    FAKE_ZSET_PATTERNS
  end
  [inst, conn, fetches]
end

class TestZsetsAPINamespaceShape < Minitest::Test
  def test_zsets_is_a_ZsetsAPI
    inst, _conn, _fetches = make_zsets_api_inst
    assert_kind_of GoldLapel::ZsetsAPI, inst.zsets
  end

  def test_no_legacy_flat_methods
    inst, _conn, _fetches = make_zsets_api_inst
    %i[zadd zincrby zrange zrank zscore zrem].each do |legacy|
      refute inst.respond_to?(legacy),
        "Phase 5 removed flat #{legacy} — use gl.zsets.<verb>."
    end
  end
end

class TestZsetsAPIVerbDispatch < Minitest::Test
  def test_add_threads_zset_key_first
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([{ "score" => "100.0" }], ["score"])
    result = inst.zsets.add("leaderboard", "global", "alice", 100)
    assert_equal 100.0, result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    # zset_key, member, score order matches $1, $2, $3 in the canonical pattern.
    assert_equal ["global", "alice", 100.0], call[:params]
    assert_includes call[:sql], "ON CONFLICT (zset_key, member)"
  end

  def test_incr_by_passes_delta
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([{ "score" => "110.0" }], ["score"])
    inst.zsets.incr_by("leaderboard", "global", "alice", 10)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["global", "alice", 10.0], call[:params]
    assert_includes call[:sql], ".score + EXCLUDED.score"
  end

  def test_score_returns_value
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([{ "score" => "100.0" }], ["score"])
    assert_equal 100.0, inst.zsets.score("leaderboard", "global", "alice")
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["global", "alice"], call[:params]
  end

  def test_score_returns_nil_when_absent
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([], ["score"])
    assert_nil inst.zsets.score("leaderboard", "global", "missing")
  end

  def test_rank_picks_desc_pattern_by_default
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([{ "rank" => "0" }], ["rank"])
    inst.zsets.rank("leaderboard", "global", "alice")
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "ORDER BY score DESC"
    assert_equal ["global", "alice"], call[:params]
  end

  def test_rank_picks_asc_when_desc_false
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([{ "rank" => "0" }], ["rank"])
    inst.zsets.rank("leaderboard", "global", "alice", desc: false)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "ORDER BY score ASC"
  end

  def test_range_picks_desc_pattern_by_default
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new(
      [{ "member" => "alice", "score" => "100.0" }, { "member" => "bob", "score" => "90.0" }],
      ["member", "score"]
    )
    result = inst.zsets.range("leaderboard", "global", start: 0, stop: 1)
    assert_equal [["alice", 100.0], ["bob", 90.0]], result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "ORDER BY score DESC"
  end

  def test_range_translates_inclusive_stop_to_limit
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([], ["member", "score"])
    inst.zsets.range("leaderboard", "global", start: 0, stop: 9)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    # 0..9 inclusive == 10 rows; OFFSET = 0.
    assert_equal ["global", 10, 0], call[:params]
  end

  def test_range_stop_minus_one_means_to_end
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([], ["member", "score"])
    inst.zsets.range("leaderboard", "global", start: 0, stop: -1)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    # -1 sentinel maps to a large limit (proxy patterns are LIMIT/OFFSET-based).
    assert_equal "global", call[:params][0]
    assert call[:params][1] >= 100, "expected a generous limit, got #{call[:params][1]}"
  end

  def test_range_by_score_inclusive_bounds
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([], ["member", "score"])
    inst.zsets.range_by_score("leaderboard", "global", 50, 200, limit: 10, offset: 2)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["global", 50.0, 200.0, 10, 2], call[:params]
  end

  def test_remove_returns_true_on_rowcount_one
    inst, conn, _fetches = make_zsets_api_inst
    res = ZsetsApiMockResult.new([], [])
    res.cmd_tuples = 1
    conn.next_result = res
    assert_equal true, inst.zsets.remove("leaderboard", "global", "alice")
  end

  def test_remove_returns_false_when_absent
    inst, conn, _fetches = make_zsets_api_inst
    res = ZsetsApiMockResult.new([], [])
    res.cmd_tuples = 0
    conn.next_result = res
    assert_equal false, inst.zsets.remove("leaderboard", "global", "missing")
  end

  def test_card_returns_zero_for_unknown_key
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([], ["count"])
    assert_equal 0, inst.zsets.card("leaderboard", "missing")
  end

  def test_card_passes_zset_key
    inst, conn, _fetches = make_zsets_api_inst
    conn.next_result = ZsetsApiMockResult.new([{ "count" => "3" }], ["count"])
    assert_equal 3, inst.zsets.card("leaderboard", "global")
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["global"], call[:params]
  end
end

class TestZsetsPhase5Contract < Minitest::Test
  # Phase 5 introduced `zset_key` — every pattern's first WHERE is on that
  # column, so a single namespace table can hold many sorted sets.
  def test_zadd_pattern_includes_zset_key_in_pk
    sql = FAKE_ZSET_PATTERNS[:query_patterns]["zadd"]
    assert_includes sql, "ON CONFLICT (zset_key, member)"
  end

  def test_zscore_pattern_filters_by_zset_key
    sql = FAKE_ZSET_PATTERNS[:query_patterns]["zscore"]
    assert_includes sql, "WHERE zset_key = $1"
  end
end
