# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/async/utils"

# Regression: doc_create_ttl_index takes `field` as a JSONB key (interpolated
# into `data->>'#{field}'`), not as a Postgres identifier. The v0.2 security
# review applied a 63-char NAMEDATALEN cap to the identifier validator, which
# accidentally rejected legitimate long JSONB field keys. The fix uses the
# unbounded FIELD_PART_PATTERN for this arg instead.

class TtlJsonbFieldResult
  def ntuples; 0; end
  def cmd_tuples; 0; end
  def [](_); nil; end
  def map(&_b); []; end
  def each; end
  def values; []; end
end

class TtlJsonbSyncMockConn
  attr_reader :sqls

  def initialize
    @sqls = []
  end

  def exec(sql, *_rest)
    @sqls << sql
    TtlJsonbFieldResult.new
  end

  def exec_params(sql, *_rest)
    @sqls << sql
    TtlJsonbFieldResult.new
  end
end

class TtlJsonbAsyncMockConn
  attr_reader :sqls

  def initialize
    @sqls = []
  end

  def async_exec(sql, *_rest)
    @sqls << sql
    TtlJsonbFieldResult.new
  end

  def async_exec_params(sql, *_rest)
    @sqls << sql
    TtlJsonbFieldResult.new
  end

  def exec(sql, *_rest)
    @sqls << sql
    TtlJsonbFieldResult.new
  end
end

class TestDocCreateTtlIndexLongJsonbFieldSync < Minitest::Test
  def test_accepts_64_char_jsonb_field_key
    # A 64+ char JSON key must NOT be rejected as "too long" — it's a JSONB
    # key, not a Postgres identifier.
    conn = TtlJsonbSyncMockConn.new
    long_field = "a" * 100
    GoldLapel.doc_create_ttl_index(conn, "sessions", long_field, expire_after_seconds: 60)
    assert conn.sqls.any? { |s| s.include?("data->>'#{long_field}'") },
           "expected DELETE SQL to interpolate the long field as a JSONB key"
  end

  def test_rejects_sql_injection_in_field_key
    conn = TtlJsonbSyncMockConn.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_create_ttl_index(conn, "sessions", "bad'; DROP", expire_after_seconds: 60)
    end
  end
end

class TestDocCreateTtlIndexLongJsonbFieldAsync < Minitest::Test
  def test_accepts_64_char_jsonb_field_key
    conn = TtlJsonbAsyncMockConn.new
    long_field = "b" * 100
    GoldLapel::Async::Utils.doc_create_ttl_index(conn, "sessions", long_field, expire_after_seconds: 60)
    assert conn.sqls.any? { |s| s.include?("data->>'#{long_field}'") },
           "expected DELETE SQL to interpolate the long field as a JSONB key"
  end

  def test_rejects_sql_injection_in_field_key
    conn = TtlJsonbAsyncMockConn.new
    assert_raises(ArgumentError) do
      GoldLapel::Async::Utils.doc_create_ttl_index(conn, "sessions", "bad'; DROP", expire_after_seconds: 60)
    end
  end
end
