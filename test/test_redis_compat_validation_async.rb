# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/async/utils"

# Regression: async Redis-compat helpers must reject injection-shaped identifier
# args, matching the sync path. The sync fix (commit 0c6cd01) missed
# lib/goldlapel/async/utils.rb; this test guards the fix across both paths.
# See v0.2 security review finding C1.

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
  # Some async helpers also call sync exec via _raw_conn in edge cases; keep
  # a lenient mock so validation is exercised before any exec is attempted.
  def exec(*); RedisCompatValidationAsyncMockResult.new; end
  def exec_params(*); RedisCompatValidationAsyncMockResult.new; end
  def close; end
  def finished?; false; end
end

class TestRedisCompatIdentifierValidationAsync < Minitest::Test
  BAD = "foo; DROP TABLE users--"

  def setup
    @conn = RedisCompatValidationAsyncMockConn.new
  end

  def test_publish_rejects_bad_channel
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.publish(@conn, BAD, "m") }
  end

  def test_subscribe_rejects_bad_channel
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.subscribe(@conn, BAD) { |_ch, _p| } }
  end

  def test_enqueue_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.enqueue(@conn, BAD, {}) }
  end

  def test_dequeue_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.dequeue(@conn, BAD) }
  end

  def test_incr_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.incr(@conn, BAD, "k") }
  end

  def test_get_counter_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.get_counter(@conn, BAD, "k") }
  end

  def test_count_distinct_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.count_distinct(@conn, BAD, "col") }
  end

  def test_count_distinct_rejects_bad_column
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.count_distinct(@conn, "tbl", BAD) }
  end

  def test_zadd_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zadd(@conn, BAD, "m", 1) }
  end

  def test_zincrby_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zincrby(@conn, BAD, "m") }
  end

  def test_zrange_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zrange(@conn, BAD) }
  end

  def test_zrank_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zrank(@conn, BAD, "m") }
  end

  def test_zscore_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zscore(@conn, BAD, "m") }
  end

  def test_zrem_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.zrem(@conn, BAD, "m") }
  end

  def test_geoadd_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geoadd(@conn, BAD, "name", "geom", "x", 0, 0) }
  end

  def test_geoadd_rejects_bad_name_column
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geoadd(@conn, "tbl", BAD, "geom", "x", 0, 0) }
  end

  def test_geoadd_rejects_bad_geom_column
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geoadd(@conn, "tbl", "name", BAD, "x", 0, 0) }
  end

  def test_georadius_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.georadius(@conn, BAD, "geom", 0, 0, 100) }
  end

  def test_georadius_rejects_bad_geom_column
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.georadius(@conn, "tbl", BAD, 0, 0, 100) }
  end

  def test_geodist_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.geodist(@conn, BAD, "geom", "name", "a", "b") }
  end

  def test_hset_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hset(@conn, BAD, "k", "f", "v") }
  end

  def test_hget_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hget(@conn, BAD, "k", "f") }
  end

  def test_hgetall_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hgetall(@conn, BAD, "k") }
  end

  def test_hdel_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel::Async::Utils.hdel(@conn, BAD, "k", "f") }
  end

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
