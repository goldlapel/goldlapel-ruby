require "minitest/autorun"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"

class MockConnection
  attr_reader :calls

  def initialize(result = nil)
    @calls = []
    @result = result || MockResult.new([], [])
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql }
    block&.call(@result)
    @result
  end

  def exec_params(sql, params = [], result_format = 0, &block)
    @calls << { method: :exec_params, sql: sql, params: params }
    block&.call(@result)
    @result
  end

  def close; end
  def finished?; false; end
end

class MockResult
  attr_reader :values, :fields

  def initialize(values, fields)
    @values = values
    @fields = fields
  end

  def ntuples; @values.length; end
end

class TestCacheHit < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_hit_skips_real_exec
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1", "widget"]], ["id", "name"])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("SELECT * FROM orders")
    assert_empty mock.calls
  end

  def test_hit_returns_cached_result
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1", "widget"]], ["id", "name"])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    result = conn.exec("SELECT * FROM orders")
    assert_kind_of GoldLapel::CachedResult, result
    assert_equal [["1", "widget"]], result.values
  end

  def test_hit_returns_fields
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1"]], ["id"])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    result = conn.exec("SELECT * FROM orders")
    assert_equal ["id"], result.fields
  end

  def test_hit_returns_ntuples
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1"], ["2"]], ["id"])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    result = conn.exec("SELECT * FROM orders")
    assert_equal 2, result.ntuples
  end
end

class TestCacheMiss < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_miss_calls_real_exec
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("SELECT * FROM orders")
    assert_equal 1, mock.calls.length
  end

  def test_miss_caches_result
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("SELECT * FROM orders")
    entry = @cache.get("SELECT * FROM orders", nil)
    refute_nil entry
    assert_equal [["1"]], entry[:values]
  end

  def test_subsequent_call_is_hit
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("SELECT * FROM orders")
    conn.exec("SELECT * FROM orders")
    assert_equal 1, mock.calls.length # only called once
    assert_equal 1, @cache.stats_hits
  end
end

class TestWrites < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_write_invalidates_table
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("INSERT INTO orders VALUES (2)")
    assert_nil @cache.get("SELECT * FROM orders", nil)
  end

  def test_write_delegates
    mock = MockConnection.new
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("INSERT INTO orders VALUES (2)")
    assert_equal 1, mock.calls.length
  end

  def test_ddl_invalidates_all
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.put("SELECT * FROM users", nil, [["2"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("CREATE TABLE foo (id int)")
    assert_nil @cache.get("SELECT * FROM orders", nil)
    assert_nil @cache.get("SELECT * FROM users", nil)
  end
end

class TestTransactions < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_begin_disables_cache
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("BEGIN")
    conn.exec("SELECT * FROM orders")
    assert(mock.calls.length >= 2) # both went to real
  end

  def test_commit_re_enables_cache
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("BEGIN")
    conn.exec("COMMIT")
    mock.calls.clear
    conn.exec("SELECT * FROM orders")
    assert_empty mock.calls # cache hit
  end

  def test_rollback_re_enables_cache
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("BEGIN")
    conn.exec("ROLLBACK")
    mock.calls.clear
    conn.exec("SELECT * FROM orders")
    assert_empty mock.calls # cache hit
  end

  def test_write_in_transaction_still_invalidates
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("BEGIN")
    conn.exec("INSERT INTO orders VALUES (2)")
    assert_nil @cache.get("SELECT * FROM orders", nil)
  end
end

class TestExecParams < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_exec_params_caches
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec_params("SELECT * FROM users WHERE id = $1", [42])
    conn.exec_params("SELECT * FROM users WHERE id = $1", [42])
    assert_equal 1, mock.calls.length
    assert_equal 1, @cache.stats_hits
  end

  def test_different_params_different_keys
    mock = MockConnection.new(MockResult.new([["1"]], ["id"]))
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec_params("SELECT $1", [1])
    conn.exec_params("SELECT $1", [2])
    assert_equal 2, mock.calls.length
  end
end

class TestMethodMissing < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_forwards_unknown_methods
    mock = MockConnection.new
    def mock.server_version; 150000; end
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    assert_equal 150000, conn.server_version
  end

  def test_respond_to_missing
    mock = MockConnection.new
    def mock.server_version; 150000; end
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    assert conn.respond_to?(:server_version)
    refute conn.respond_to?(:nonexistent_method)
  end
end

class TestEdgeCases < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_query_after_query_resets
    mock = MockConnection.new
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.put("SELECT 2", nil, [["2"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    r1 = conn.exec("SELECT 1")
    assert_equal [["1"]], r1.values
    r2 = conn.exec("SELECT 2")
    assert_equal [["2"]], r2.values
  end

  def test_write_after_hit_clears
    mock = MockConnection.new
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    conn = GoldLapel::CachedConnection.new(mock, @cache)
    conn.exec("SELECT * FROM orders")
    conn.exec("INSERT INTO orders VALUES (2)")
    assert_nil @cache.get("SELECT * FROM orders", nil)
  end
end

# --- GoldLapel.wrap(disable_native_cache:) flag plumbs through to NativeCache ---

class TestWrapDisableNativeCache < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_wrap_default_does_not_disable_native_cache
    mock = MockConnection.new
    # Use a port that won't actually connect — we only want to verify
    # the flag side-effect on the singleton, not the network behavior.
    GoldLapel.wrap(mock, invalidation_port: 1)
    refute GoldLapel::NativeCache.instance.disable_native_cache?
    GoldLapel::NativeCache.instance.stop_invalidation
  end

  def test_wrap_disable_native_cache_true_sets_flag
    mock = MockConnection.new
    GoldLapel.wrap(mock, invalidation_port: 1, disable_native_cache: true)
    assert GoldLapel::NativeCache.instance.disable_native_cache?
    GoldLapel::NativeCache.instance.stop_invalidation
  end

  def test_wrap_disable_native_cache_false_clears_flag
    # Pre-set the flag, then wrap with default false; the wrap call
    # should reset it (explicit configuration wins on every wrap).
    GoldLapel::NativeCache.instance.disable_native_cache = true
    mock = MockConnection.new
    GoldLapel.wrap(mock, invalidation_port: 1, disable_native_cache: false)
    refute GoldLapel::NativeCache.instance.disable_native_cache?
    GoldLapel::NativeCache.instance.stop_invalidation
  end
end
