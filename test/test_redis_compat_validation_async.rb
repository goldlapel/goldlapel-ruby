# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/async/utils"

# Regression: async Redis-compat helpers must reject injection-shaped identifier
# args, matching the sync path. The sync fix (commit 0c6cd01) missed
# lib/goldlapel/async/utils.rb originally; this test guards the fix across both
# paths. See v0.2 security review finding C1.
#
# Phase 5 of schema-to-core moved the Redis-compat families behind namespace
# APIs. Identifier validation now happens in the underlying
# `GoldLapel::Async::Utils.<family>_*` helpers — these tests pin that
# helper-level guard.

class RedisCompatValidationAsyncMockResult
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

class RedisCompatValidationAsyncMockConn
  def async_exec(*); RedisCompatValidationAsyncMockResult.new; end
  def async_exec_params(*); RedisCompatValidationAsyncMockResult.new; end
  def exec(*); RedisCompatValidationAsyncMockResult.new; end
  def exec_params(*); RedisCompatValidationAsyncMockResult.new; end
  def close; end
  def finished?; false; end
end

class TestRedisCompatIdentifierValidationAsync < Minitest::Test
  BAD = "foo; DROP TABLE users--"

  def setup
    @conn = RedisCompatValidationAsyncMockConn.new
    @patterns = { tables: { "main" => "ignored" }, query_patterns: {} }
  end

  def test_publish_rejects_bad_channel
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.publish(@conn, BAD, "m") }
  end

  def test_subscribe_rejects_bad_channel
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.subscribe(@conn, BAD) { |_ch, _p| } }
  end

  def test_count_distinct_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.count_distinct(@conn, BAD, "col") }
  end

  def test_count_distinct_rejects_bad_column
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.count_distinct(@conn, "tbl", BAD) }
  end

  # -- Counter family --

  def test_counter_incr_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.counter_incr(@conn, BAD, "k", 1, patterns: @patterns) }
  end

  def test_counter_set_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.counter_set(@conn, BAD, "k", 1, patterns: @patterns) }
  end

  def test_counter_get_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.counter_get(@conn, BAD, "k", patterns: @patterns) }
  end

  def test_counter_count_keys_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.counter_count_keys(@conn, BAD, patterns: @patterns) }
  end

  # -- Zset family --

  def test_zset_add_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zset_add(@conn, BAD, "k", "m", 1, patterns: @patterns) }
  end

  def test_zset_score_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zset_score(@conn, BAD, "k", "m", patterns: @patterns) }
  end

  def test_zset_range_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zset_range(@conn, BAD, "k", patterns: @patterns) }
  end

  # -- Hash family --

  def test_hash_set_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hash_set(@conn, BAD, "hk", "f", "v", patterns: @patterns) }
  end

  def test_hash_get_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hash_get(@conn, BAD, "hk", "f", patterns: @patterns) }
  end

  def test_hash_get_all_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hash_get_all(@conn, BAD, "hk", patterns: @patterns) }
  end

  # -- Queue family --

  def test_queue_enqueue_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.queue_enqueue(@conn, BAD, {}, patterns: @patterns) }
  end

  def test_queue_claim_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.queue_claim(@conn, BAD, patterns: @patterns) }
  end

  def test_queue_ack_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.queue_ack(@conn, BAD, 1, patterns: @patterns) }
  end

  # -- Geo family --

  def test_geo_add_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geo_add(@conn, BAD, "alice", 0, 0, patterns: @patterns) }
  end

  def test_geo_dist_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geo_dist(@conn, BAD, "a", "b", patterns: @patterns) }
  end

  def test_geo_radius_rejects_bad_name
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geo_radius(@conn, BAD, 0, 0, 100, patterns: @patterns) }
  end

  # -- Stream family --

  def test_stream_add_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.stream_add(@conn, BAD, {}) }
  end

  def test_stream_create_group_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.stream_create_group(@conn, BAD, "g") }
  end

  def test_stream_read_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.stream_read(@conn, BAD, "g", "c") }
  end

  def test_stream_ack_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.stream_ack(@conn, BAD, "g", 1) }
  end

  def test_stream_claim_rejects_bad_stream
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.stream_claim(@conn, BAD, "g", "c") }
  end
end
