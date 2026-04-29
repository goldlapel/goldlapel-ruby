# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "_doc_patterns_helper"

class OpMockResult
  attr_reader :values, :fields

  def initialize(rows, fields)
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end

  def ntuples; @rows.length; end
  def cmd_tuples; @rows.length; end

  def [](index)
    @rows[index]
  end

  def map(&block)
    @rows.map(&block)
  end

  def each(&block)
    @rows.each(&block)
  end
end

class OpMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = OpMockResult.new([], [])
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql }
    key = @results.keys.find { |k| sql.include?(k) }
    r = key ? @results[key] : @default
    block&.call(r)
    r
  end

  def exec_params(sql, params = [], result_format = 0, &block)
    @calls << { method: :exec_params, sql: sql, params: params }
    key = @results.keys.find { |k| sql.include?(k) }
    r = key ? @results[key] : @default
    block&.call(r)
    r
  end

  def conninfo_hash
    { host: "localhost", port: 5432, dbname: "test" }
  end

  def close; end
  def finished?; false; end
end

# Mimics pg 1.6's dense conninfo_hash, which returns every known key with
# unset entries as `nil` (e.g. :service => nil). See regression tests below.
class OpMockConnectionWithNilConninfo < OpMockConnection
  def conninfo_hash
    {
      host: "localhost",
      port: 5432,
      dbname: "test",
      user: "steve",
      password: nil,
      service: nil,
      options: nil,
      sslmode: nil,
      connect_timeout: nil
    }
  end
end

def suppress_warnings
  original = $VERBOSE
  $VERBOSE = nil
  yield
ensure
  $VERBOSE = original
end

# --- doc_watch ---

class TestDocWatch < Minitest::Test
  def test_creates_trigger_function_and_trigger
    mock = OpMockConnection.new
    # doc_watch enters an infinite LISTEN loop, so we test setup only
    # by mocking PG.connect to raise, which exits before the loop
    pg_mock = Module.new do
      def self.connect(_hash)
        raise StopIteration, "stop"
      end
    end

    original_pg = Object.const_defined?(:PG) ? Object.const_get(:PG) : nil
    suppress_warnings do
      Object.const_set(:PG, pg_mock)
    end

    begin
      GoldLapel.doc_watch(mock, "orders") { |e| }
    rescue StopIteration
      # expected -- we intercepted PG.connect
    ensure
      suppress_warnings do
        Object.send(:remove_const, :PG)
        Object.const_set(:PG, original_pg) if original_pg
      end
    end

    fn_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }
    assert_equal 1, fn_calls.length
    assert_includes fn_calls[0][:sql], "_gl_notify_orders"
    assert_includes fn_calls[0][:sql], "pg_notify"
    assert_includes fn_calls[0][:sql], "_gl_watch_orders"

    # Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
    # Avoids the race where a DROP + CREATE pair could have two concurrent
    # doc_watch calls replace each other's triggers mid-flight.
    trg_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE TRIGGER") }
    assert_equal 1, trg_calls.length
    assert_includes trg_calls[0][:sql], "_gl_notify_orders_trg"
    assert_includes trg_calls[0][:sql], "AFTER INSERT OR UPDATE OR DELETE"
    assert_includes trg_calls[0][:sql], "FOR EACH ROW"

    # Guard against the racy DROP + CREATE pair regressing.
    racy_drops = mock.calls.select do |c|
      c[:sql].include?("DROP TRIGGER IF EXISTS _gl_notify_orders_trg")
    end
    assert_empty racy_drops,
                 "doc_watch should not emit DROP TRIGGER IF EXISTS (racy); use CREATE OR REPLACE TRIGGER"
  end

  def test_watch_rejects_invalid_collection
    mock = OpMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_watch(mock, "drop table;") { |e| }
    end
  end
end

# --- doc_unwatch ---

class TestDocUnwatch < Minitest::Test
  def test_drops_trigger_and_function
    mock = OpMockConnection.new
    GoldLapel.doc_unwatch(mock, "orders")

    sqls = mock.calls.map { |c| c[:sql] }
    drop_trigger = sqls.find { |s| s.include?("DROP TRIGGER") }
    drop_function = sqls.find { |s| s.include?("DROP FUNCTION") }
    refute_nil drop_trigger
    refute_nil drop_function
    assert_includes drop_trigger, "_gl_notify_orders_trg"
    assert_includes drop_function, "_gl_notify_orders"
  end

  def test_unwatch_rejects_invalid_collection
    mock = OpMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_unwatch(mock, "1invalid")
    end
  end
end

# --- doc_create_ttl_index ---

class TestDocCreateTtlIndex < Minitest::Test
  def test_creates_ttl_trigger_function_and_trigger
    mock = OpMockConnection.new
    GoldLapel.doc_create_ttl_index(mock, "sessions", "expires_at", expire_after_seconds: 3600)

    fn_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }
    assert_equal 1, fn_calls.length
    assert_includes fn_calls[0][:sql], "_gl_ttl_sessions"
    assert_includes fn_calls[0][:sql], "expires_at"
    assert_includes fn_calls[0][:sql], "3600 seconds"

    # Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
    trg_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE TRIGGER") }
    assert_equal 1, trg_calls.length
    assert_includes trg_calls[0][:sql], "_gl_ttl_sessions_trg"
    assert_includes trg_calls[0][:sql], "BEFORE INSERT"
    assert_includes trg_calls[0][:sql], "FOR EACH STATEMENT"
  end

  def test_ttl_uses_atomic_create_or_replace
    # Regression guard: doc_create_ttl_index must use atomic
    # CREATE OR REPLACE TRIGGER (PG14+) rather than the racy
    # DROP + CREATE pair. Matches the Go wrapper.
    mock = OpMockConnection.new
    GoldLapel.doc_create_ttl_index(mock, "sessions", "expires_at", expire_after_seconds: 7200)

    sqls = mock.calls.map { |c| c[:sql] }
    assert sqls.any? { |s| s.include?("CREATE OR REPLACE TRIGGER") },
           "expected CREATE OR REPLACE TRIGGER for TTL index"
    refute sqls.any? { |s| s.include?("DROP TRIGGER IF EXISTS _gl_ttl_sessions_trg") },
           "doc_create_ttl_index should not emit DROP TRIGGER IF EXISTS (racy); use CREATE OR REPLACE TRIGGER"
  end

  def test_ttl_accepts_long_jsonb_field_key
    # Regression guard: `field` is a JSONB key, not a Postgres identifier,
    # so the wrapper must NOT apply the 63-char NAMEDATALEN cap to it.
    # (Before the fix, _validate_identifier rejected 64+ char JSON keys.)
    mock = OpMockConnection.new
    long_field = "a" * 100
    GoldLapel.doc_create_ttl_index(mock, "sessions", long_field, expire_after_seconds: 60)

    fn_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }
    assert_equal 1, fn_calls.length
    assert_includes fn_calls[0][:sql], "data->>'#{long_field}'"
  end

  def test_ttl_rejects_sql_injection_in_field_key
    # Field-key validator must still reject non-alphanumeric characters
    # (no 63-cap, but still anchored alphanumeric+underscore).
    mock = OpMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_create_ttl_index(mock, "sessions", "bad'; DROP", expire_after_seconds: 60)
    end
  end
end

# --- doc_remove_ttl_index ---

class TestDocRemoveTtlIndex < Minitest::Test
  def test_drops_ttl_trigger_and_function
    mock = OpMockConnection.new
    GoldLapel.doc_remove_ttl_index(mock, "sessions")

    sqls = mock.calls.map { |c| c[:sql] }
    drop_trigger = sqls.find { |s| s.include?("DROP TRIGGER") }
    drop_function = sqls.find { |s| s.include?("DROP FUNCTION") }
    refute_nil drop_trigger
    refute_nil drop_function
    assert_includes drop_trigger, "_gl_ttl_sessions_trg"
    assert_includes drop_function, "_gl_ttl_sessions"
  end

  def test_remove_ttl_rejects_invalid_collection
    mock = OpMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_remove_ttl_index(mock, "bad table!")
    end
  end
end

# --- doc_create_capped ---

class TestDocCreateCapped < Minitest::Test
  def test_creates_only_cap_trigger
    # Phase 4: proxy owns CREATE TABLE for doc-store. doc_create_capped only
    # adds the cap trigger + function on top of the proxy-managed table.
    mock = OpMockConnection.new
    GoldLapel.doc_create_capped(mock, "logs", max: 1000)

    assert_nil mock.calls.find { |c| c[:sql].include?("CREATE TABLE") },
               "doc_create_capped must not CREATE TABLE — proxy owns doc-store DDL"

    fn_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }
    assert_equal 1, fn_calls.length
    assert_includes fn_calls[0][:sql], "_gl_cap_logs"
    assert_includes fn_calls[0][:sql], "OFFSET 1000"

    # Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
    trg_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE TRIGGER") }
    assert_equal 1, trg_calls.length
    assert_includes trg_calls[0][:sql], "_gl_cap_logs_trg"
    assert_includes trg_calls[0][:sql], "AFTER INSERT"
    assert_includes trg_calls[0][:sql], "FOR EACH STATEMENT"

    # Guard against the racy DROP + CREATE pair regressing.
    racy_drops = mock.calls.select do |c|
      c[:sql].include?("DROP TRIGGER IF EXISTS _gl_cap_logs_trg")
    end
    assert_empty racy_drops,
                 "doc_create_capped should not emit DROP TRIGGER IF EXISTS (racy); use CREATE OR REPLACE TRIGGER"
  end

  def test_cap_function_deletes_oldest_beyond_max
    mock = OpMockConnection.new
    GoldLapel.doc_create_capped(mock, "events", max: 500)

    fn_sql = mock.calls.find { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }[:sql]
    assert_includes fn_sql, "ORDER BY created_at DESC"
    assert_includes fn_sql, "OFFSET 500"
    assert_includes fn_sql, "DELETE FROM events"
  end
end

# --- doc_remove_cap ---

class TestDocRemoveCap < Minitest::Test
  def test_drops_cap_trigger_and_function
    mock = OpMockConnection.new
    GoldLapel.doc_remove_cap(mock, "logs")

    sqls = mock.calls.map { |c| c[:sql] }
    drop_trigger = sqls.find { |s| s.include?("DROP TRIGGER") }
    drop_function = sqls.find { |s| s.include?("DROP FUNCTION") }
    refute_nil drop_trigger
    refute_nil drop_function
    assert_includes drop_trigger, "_gl_cap_logs_trg"
    assert_includes drop_function, "_gl_cap_logs"
  end

  def test_remove_cap_rejects_invalid_collection
    mock = OpMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_remove_cap(mock, "Robert'); DROP TABLE students;--")
    end
  end
end

# --- listener conninfo stripping (regression for pg 1.6) ---
#
# pg 1.6's PG::Connection#conninfo_hash returns every known key, with unset
# values as nil (e.g. :service => nil). Passing the raw hash back into
# PG.connect raises `definition of service "" not found`. subscribe and
# doc_watch must strip nils via _listener_conninfo before reconnecting.

class TestListenerConninfoStripping < Minitest::Test
  def capture_listener_hash
    captured = nil
    pg_mock = Module.new
    pg_mock.define_singleton_method(:connect) do |hash|
      captured = hash
      raise StopIteration, "stop"
    end

    original_pg = Object.const_defined?(:PG) ? Object.const_get(:PG) : nil
    suppress_warnings do
      Object.const_set(:PG, pg_mock)
    end

    begin
      yield
    rescue StopIteration
      # expected -- we intercepted PG.connect
    ensure
      suppress_warnings do
        Object.send(:remove_const, :PG)
        Object.const_set(:PG, original_pg) if original_pg
      end
    end

    captured
  end

  def test_doc_watch_strips_nils_from_conninfo_hash
    mock = OpMockConnectionWithNilConninfo.new
    captured = capture_listener_hash do
      GoldLapel.doc_watch(mock, "orders") { |e| }
    end

    refute_nil captured, "PG.connect should have been called"
    refute captured.key?(:service), "nil :service must be stripped (pg 1.6 rejects empty service)"
    refute captured.key?(:password), "nil :password must be stripped"
    refute captured.key?(:options), "nil :options must be stripped"
    refute captured.key?(:sslmode), "nil :sslmode must be stripped"
    refute captured.key?(:connect_timeout), "nil :connect_timeout must be stripped"
    # Non-nil keys preserved
    assert_equal "localhost", captured[:host]
    assert_equal 5432, captured[:port]
    assert_equal "test", captured[:dbname]
    assert_equal "steve", captured[:user]
    # No nil values remain
    refute captured.any? { |_, v| v.nil? }, "listener conninfo must not contain any nil values"
  end

  def test_subscribe_strips_nils_from_conninfo_hash
    mock = OpMockConnectionWithNilConninfo.new
    captured = capture_listener_hash do
      GoldLapel.subscribe(mock, "events") { |_ch, _payload| }
    end

    refute_nil captured, "PG.connect should have been called"
    refute captured.any? { |_, v| v.nil? }, "listener conninfo must not contain any nil values"
    assert_equal "localhost", captured[:host]
    assert_equal "test", captured[:dbname]
  end

  def test_listener_conninfo_preserves_hash_when_no_nils
    mock = OpMockConnection.new  # returns a hash with no nils
    captured = capture_listener_hash do
      GoldLapel.doc_watch(mock, "orders") { |e| }
    end

    refute_nil captured
    assert_equal({ host: "localhost", port: 5432, dbname: "test" }, captured)
  end
end
