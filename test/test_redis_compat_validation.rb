# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

# Regression: Redis-compat helpers must reject injection-shaped identifier args.
# See v0.2 security review finding C1.

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
  end

  def test_publish_rejects_bad_channel
    assert_raises(ArgumentError) { GoldLapel.publish(@conn, BAD, "m") }
  end

  def test_enqueue_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.enqueue(@conn, BAD, {}) }
  end

  def test_dequeue_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.dequeue(@conn, BAD) }
  end

  def test_incr_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.incr(@conn, BAD, "k") }
  end

  def test_get_counter_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.get_counter(@conn, BAD, "k") }
  end

  def test_count_distinct_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.count_distinct(@conn, BAD, "col") }
  end

  def test_count_distinct_rejects_bad_column
    assert_raises(ArgumentError) { GoldLapel.count_distinct(@conn, "tbl", BAD) }
  end

  def test_zadd_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.zadd(@conn, BAD, "m", 1) }
  end

  def test_zincrby_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.zincrby(@conn, BAD, "m") }
  end

  def test_zrange_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.zrange(@conn, BAD) }
  end

  def test_zrank_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.zrank(@conn, BAD, "m") }
  end

  def test_zscore_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.zscore(@conn, BAD, "m") }
  end

  def test_zrem_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.zrem(@conn, BAD, "m") }
  end

  def test_geoadd_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.geoadd(@conn, BAD, "name", "geom", "x", 0, 0) }
  end

  def test_geoadd_rejects_bad_name_column
    assert_raises(ArgumentError) { GoldLapel.geoadd(@conn, "tbl", BAD, "geom", "x", 0, 0) }
  end

  def test_geoadd_rejects_bad_geom_column
    assert_raises(ArgumentError) { GoldLapel.geoadd(@conn, "tbl", "name", BAD, "x", 0, 0) }
  end

  def test_georadius_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.georadius(@conn, BAD, "geom", 0, 0, 100) }
  end

  def test_georadius_rejects_bad_geom_column
    assert_raises(ArgumentError) { GoldLapel.georadius(@conn, "tbl", BAD, 0, 0, 100) }
  end

  def test_geodist_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.geodist(@conn, BAD, "geom", "name", "a", "b") }
  end

  def test_hset_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.hset(@conn, BAD, "k", "f", "v") }
  end

  def test_hget_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.hget(@conn, BAD, "k", "f") }
  end

  def test_hgetall_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.hgetall(@conn, BAD, "k") }
  end

  def test_hdel_rejects_bad_table
    assert_raises(ArgumentError) { GoldLapel.hdel(@conn, BAD, "k", "f") }
  end

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
