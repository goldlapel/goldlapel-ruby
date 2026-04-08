# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

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

    trg_calls = mock.calls.select { |c| c[:sql].include?("CREATE TRIGGER") }
    assert_equal 1, trg_calls.length
    assert_includes trg_calls[0][:sql], "_gl_notify_orders_trg"
    assert_includes trg_calls[0][:sql], "AFTER INSERT OR UPDATE OR DELETE"
    assert_includes trg_calls[0][:sql], "FOR EACH ROW"
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

    trg_calls = mock.calls.select { |c| c[:sql].include?("CREATE TRIGGER") }
    assert_equal 1, trg_calls.length
    assert_includes trg_calls[0][:sql], "_gl_ttl_sessions_trg"
    assert_includes trg_calls[0][:sql], "BEFORE INSERT"
    assert_includes trg_calls[0][:sql], "FOR EACH STATEMENT"
  end

  def test_ttl_drops_existing_trigger_before_creating
    mock = OpMockConnection.new
    GoldLapel.doc_create_ttl_index(mock, "sessions", "expires_at", expire_after_seconds: 7200)

    sqls = mock.calls.map { |c| c[:sql] }
    drop_idx = sqls.index { |s| s.include?("DROP TRIGGER") }
    create_idx = sqls.index { |s| s.include?("CREATE TRIGGER") }
    refute_nil drop_idx
    refute_nil create_idx
    assert_operator drop_idx, :<, create_idx
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
  def test_creates_table_and_cap_trigger
    mock = OpMockConnection.new
    GoldLapel.doc_create_capped(mock, "logs", max: 1000)

    create_table = mock.calls.find { |c| c[:sql].include?("CREATE TABLE IF NOT EXISTS logs") }
    refute_nil create_table
    assert_includes create_table[:sql], "BIGSERIAL PRIMARY KEY"
    assert_includes create_table[:sql], "JSONB NOT NULL"

    fn_calls = mock.calls.select { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }
    assert_equal 1, fn_calls.length
    assert_includes fn_calls[0][:sql], "_gl_cap_logs"
    assert_includes fn_calls[0][:sql], "OFFSET 1000"

    trg_calls = mock.calls.select { |c| c[:sql].include?("CREATE TRIGGER") }
    assert_equal 1, trg_calls.length
    assert_includes trg_calls[0][:sql], "_gl_cap_logs_trg"
    assert_includes trg_calls[0][:sql], "AFTER INSERT"
    assert_includes trg_calls[0][:sql], "FOR EACH STATEMENT"
  end

  def test_cap_function_deletes_oldest_beyond_max
    mock = OpMockConnection.new
    GoldLapel.doc_create_capped(mock, "events", max: 500)

    fn_sql = mock.calls.find { |c| c[:sql].include?("CREATE OR REPLACE FUNCTION") }[:sql]
    assert_includes fn_sql, "ORDER BY id DESC"
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
