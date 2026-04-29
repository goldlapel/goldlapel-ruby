# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

# Regression: Redis-compat helpers must reject injection-shaped identifier args.
# See v0.2 security review finding C1.
#
# Phase 5 of schema-to-core moved the Redis-compat families behind namespace
# APIs (gl.counters / gl.zsets / gl.hashes / gl.queues / gl.geos). Identifier
# validation now happens both in the namespace `_patterns` lookup and in the
# underlying GoldLapel.<family>_* helpers — these tests pin the helper-level
# guard, since that's the SQL-injection chokepoint.

class RedisCompatValidationMockResult
  def initialize(rows = [], fields = [])
    @rows = rows
    @fields = fields
  end

  def ntuples; @rows.length; end
  def cmd_tuples; @rows.length; end
  def [](i); @rows[i]; end
  def map(&b); @rows.map(&b); end
  def each(&b); @rows.each(&b); end
  def values; @rows.map(&:values); end
end

class RedisCompatValidationMockConn
  def exec(*); RedisCompatValidationMockResult.new; end
  def exec_params(*); RedisCompatValidationMockResult.new; end
  def close; end
  def finished?; false; end
end

class TestRedisCompatIdentifierValidation < Minitest::Test
  BAD = "foo; DROP TABLE users--"

  def setup
    @conn = RedisCompatValidationMockConn.new
    # Patterns aren't reached when identifier validation fires first; pass an
    # empty hash so the helpers don't NPE before validating.
    @patterns = { tables: { "main" => "ignored" }, query_patterns: {} }
  end

  # -- Pub/sub --

  def test_publish_rejects_bad_channel
    assert_raises(ArgumentError) { GoldLapel.publish(@conn, BAD, "m") }
  end

  # -- Misc --

  def test_count_distinct_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.count_distinct(@conn, BAD, "col") }
  end

  def test_count_distinct_rejects_bad_column
    assert_raises(ArgumentError) { GoldLapel.count_distinct(@conn, "tbl", BAD) }
  end

  # -- Counter family --

  def test_counter_incr_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.counter_incr(@conn, BAD, "k", 1, patterns: @patterns) }
  end

  def test_counter_set_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.counter_set(@conn, BAD, "k", 1, patterns: @patterns) }
  end

  def test_counter_get_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.counter_get(@conn, BAD, "k", patterns: @patterns) }
  end

  def test_counter_delete_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.counter_delete(@conn, BAD, "k", patterns: @patterns) }
  end

  def test_counter_count_keys_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.counter_count_keys(@conn, BAD, patterns: @patterns) }
  end

  # -- Zset family --

  def test_zset_add_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_add(@conn, BAD, "k", "m", 1, patterns: @patterns) }
  end

  def test_zset_incr_by_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_incr_by(@conn, BAD, "k", "m", 1, patterns: @patterns) }
  end

  def test_zset_score_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_score(@conn, BAD, "k", "m", patterns: @patterns) }
  end

  def test_zset_remove_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_remove(@conn, BAD, "k", "m", patterns: @patterns) }
  end

  def test_zset_range_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_range(@conn, BAD, "k", patterns: @patterns) }
  end

  def test_zset_rank_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_rank(@conn, BAD, "k", "m", patterns: @patterns) }
  end

  def test_zset_card_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.zset_card(@conn, BAD, "k", patterns: @patterns) }
  end

  # -- Hash family --

  def test_hash_set_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.hash_set(@conn, BAD, "hk", "f", "v", patterns: @patterns) }
  end

  def test_hash_get_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.hash_get(@conn, BAD, "hk", "f", patterns: @patterns) }
  end

  def test_hash_get_all_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.hash_get_all(@conn, BAD, "hk", patterns: @patterns) }
  end

  def test_hash_delete_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.hash_delete(@conn, BAD, "hk", "f", patterns: @patterns) }
  end

  # -- Queue family --

  def test_queue_enqueue_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.queue_enqueue(@conn, BAD, {}, patterns: @patterns) }
  end

  def test_queue_claim_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.queue_claim(@conn, BAD, patterns: @patterns) }
  end

  def test_queue_ack_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.queue_ack(@conn, BAD, 1, patterns: @patterns) }
  end

  def test_queue_peek_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.queue_peek(@conn, BAD, patterns: @patterns) }
  end

  # -- Geo family --

  def test_geo_add_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.geo_add(@conn, BAD, "alice", 0, 0, patterns: @patterns) }
  end

  def test_geo_pos_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.geo_pos(@conn, BAD, "alice", patterns: @patterns) }
  end

  def test_geo_dist_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.geo_dist(@conn, BAD, "a", "b", patterns: @patterns) }
  end

  def test_geo_radius_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.geo_radius(@conn, BAD, 0, 0, 100, patterns: @patterns) }
  end

  def test_geo_remove_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.geo_remove(@conn, BAD, "alice", patterns: @patterns) }
  end

  def test_geo_count_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel.geo_count(@conn, BAD, patterns: @patterns) }
  end

  # -- Stream family --

  def test_stream_add_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel.stream_add(@conn, BAD, {}) }
  end

  def test_stream_create_group_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel.stream_create_group(@conn, BAD, "g") }
  end

  def test_stream_read_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel.stream_read(@conn, BAD, "g", "c") }
  end

  def test_stream_ack_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel.stream_ack(@conn, BAD, "g", 1) }
  end

  def test_stream_claim_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel.stream_claim(@conn, BAD, "g", "c") }
  end
end
