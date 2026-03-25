require "minitest/autorun"
require "socket"
require_relative "../lib/goldlapel/cache"

class TestDetectWrite < Minitest::Test
  def test_insert
    assert_equal "orders", GoldLapel.detect_write("INSERT INTO orders VALUES (1)")
  end

  def test_insert_schema
    assert_equal "orders", GoldLapel.detect_write("INSERT INTO public.orders VALUES (1)")
  end

  def test_update
    assert_equal "orders", GoldLapel.detect_write("UPDATE orders SET name = 'x'")
  end

  def test_delete
    assert_equal "orders", GoldLapel.detect_write("DELETE FROM orders WHERE id = 1")
  end

  def test_truncate
    assert_equal "orders", GoldLapel.detect_write("TRUNCATE orders")
  end

  def test_truncate_table
    assert_equal "orders", GoldLapel.detect_write("TRUNCATE TABLE orders")
  end

  def test_create_ddl
    assert_equal GoldLapel::DDL_SENTINEL, GoldLapel.detect_write("CREATE TABLE foo (id int)")
  end

  def test_alter_ddl
    assert_equal GoldLapel::DDL_SENTINEL, GoldLapel.detect_write("ALTER TABLE foo ADD COLUMN bar int")
  end

  def test_drop_ddl
    assert_equal GoldLapel::DDL_SENTINEL, GoldLapel.detect_write("DROP TABLE foo")
  end

  def test_select_returns_nil
    assert_nil GoldLapel.detect_write("SELECT * FROM orders")
  end

  def test_case_insensitive
    assert_equal "orders", GoldLapel.detect_write("insert INTO Orders VALUES (1)")
  end

  def test_copy_from
    assert_equal "orders", GoldLapel.detect_write("COPY orders FROM '/tmp/data.csv'")
  end

  def test_copy_to_returns_nil
    assert_nil GoldLapel.detect_write("COPY orders TO '/tmp/data.csv'")
  end

  def test_copy_subquery_returns_nil
    assert_nil GoldLapel.detect_write("COPY (SELECT * FROM orders) TO '/tmp/data.csv'")
  end

  def test_with_cte_insert
    assert_equal GoldLapel::DDL_SENTINEL, GoldLapel.detect_write("WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x")
  end

  def test_with_cte_select
    assert_nil GoldLapel.detect_write("WITH x AS (SELECT 1) SELECT * FROM x")
  end

  def test_empty_returns_nil
    assert_nil GoldLapel.detect_write("")
  end

  def test_whitespace_returns_nil
    assert_nil GoldLapel.detect_write("   ")
  end

  def test_copy_with_columns
    assert_equal "orders", GoldLapel.detect_write("COPY orders(id, name) FROM '/tmp/data.csv'")
  end
end

class TestExtractTables < Minitest::Test
  def test_simple_from
    tables = GoldLapel.extract_tables("SELECT * FROM orders")
    assert_includes tables, "orders"
  end

  def test_join
    tables = GoldLapel.extract_tables("SELECT * FROM orders o JOIN customers c ON o.cid = c.id")
    assert_includes tables, "orders"
    assert_includes tables, "customers"
  end

  def test_schema_qualified
    tables = GoldLapel.extract_tables("SELECT * FROM public.orders")
    assert_includes tables, "orders"
  end

  def test_multiple_joins
    tables = GoldLapel.extract_tables("SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1")
    assert_equal 3, tables.size
  end

  def test_case_insensitive
    tables = GoldLapel.extract_tables("SELECT * FROM ORDERS")
    assert_includes tables, "orders"
  end

  def test_no_tables
    assert_equal 0, GoldLapel.extract_tables("SELECT 1").size
  end

  def test_subquery
    tables = GoldLapel.extract_tables("SELECT * FROM orders WHERE id IN (SELECT oid FROM users)")
    assert_includes tables, "orders"
    assert_includes tables, "users"
  end
end

class TestTransactionDetection < Minitest::Test
  def test_begin
    assert_match GoldLapel::TX_START, "BEGIN"
  end

  def test_start_transaction
    assert_match GoldLapel::TX_START, "START TRANSACTION"
  end

  def test_commit
    assert_match GoldLapel::TX_END, "COMMIT"
  end

  def test_rollback
    assert_match GoldLapel::TX_END, "ROLLBACK"
  end

  def test_end
    assert_match GoldLapel::TX_END, "END"
  end

  def test_savepoint_not_start
    refute_match GoldLapel::TX_START, "SAVEPOINT x"
  end

  def test_set_transaction_not_start
    refute_match GoldLapel::TX_START, "SET TRANSACTION ISOLATION LEVEL"
  end

  def test_select_not_start
    refute_match GoldLapel::TX_START, "SELECT 1"
  end
end

class TestCacheOperations < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_put_and_get
    @cache.put("SELECT * FROM users", nil, [["1", "alice"]], ["id", "name"])
    entry = @cache.get("SELECT * FROM users", nil)
    refute_nil entry
    assert_equal [["1", "alice"]], entry[:values]
  end

  def test_miss_returns_nil
    assert_nil @cache.get("SELECT 1", nil)
  end

  def test_disabled_returns_nil
    @cache.instance_variable_set(:@enabled, false)
    @cache.put("SELECT 1", nil, [["1"]], ["?column?"])
    assert_nil @cache.get("SELECT 1", nil)
  end

  def test_not_connected_returns_nil
    @cache.instance_variable_set(:@invalidation_connected, false)
    @cache.put("SELECT 1", nil, [["1"]], ["?column?"])
    assert_nil @cache.get("SELECT 1", nil)
  end

  def test_params_differentiate_keys
    @cache.put("SELECT $1", [1], [["1"]], ["id"])
    @cache.put("SELECT $1", [2], [["2"]], ["id"])
    assert_equal [["1"]], @cache.get("SELECT $1", [1])[:values]
    assert_equal [["2"]], @cache.get("SELECT $1", [2])[:values]
  end

  def test_stats_tracking
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.get("SELECT 1", nil)
    @cache.get("SELECT 2", nil)
    assert_equal 1, @cache.stats_hits
    assert_equal 1, @cache.stats_misses
  end
end

class TestLRU < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "3"
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    ENV.delete("GOLDLAPEL_NATIVE_CACHE_SIZE")
    GoldLapel::NativeCache.reset!
  end

  def test_eviction_at_capacity
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.put("SELECT 2", nil, [["2"]], [])
    @cache.put("SELECT 3", nil, [["3"]], [])
    @cache.put("SELECT 4", nil, [["4"]], [])
    assert_nil @cache.get("SELECT 1", nil)
    refute_nil @cache.get("SELECT 4", nil)
  end

  def test_access_refreshes_lru
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.put("SELECT 2", nil, [["2"]], [])
    @cache.put("SELECT 3", nil, [["3"]], [])
    @cache.get("SELECT 1", nil) # refresh 1
    @cache.put("SELECT 4", nil, [["4"]], []) # evicts 2
    refute_nil @cache.get("SELECT 1", nil)
    assert_nil @cache.get("SELECT 2", nil)
  end

  def test_eviction_cleans_table_index
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "2"
    GoldLapel::NativeCache.reset!
    cache = GoldLapel::NativeCache.new
    cache.instance_variable_set(:@invalidation_connected, true)
    cache.put("SELECT * FROM orders", nil, [["1"]], [])
    cache.put("SELECT * FROM users", nil, [["2"]], [])
    cache.put("SELECT * FROM products", nil, [["3"]], [])
    # orders was evicted (oldest), so it should not be in the cache
    assert_nil cache.get("SELECT * FROM orders", nil)
  end
end

class TestInvalidation < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_invalidate_table
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.put("SELECT * FROM users", nil, [["2"]], [])
    @cache.invalidate_table("orders")
    assert_nil @cache.get("SELECT * FROM orders", nil)
    refute_nil @cache.get("SELECT * FROM users", nil)
  end

  def test_invalidate_all
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.put("SELECT * FROM users", nil, [["2"]], [])
    @cache.invalidate_all
    assert_nil @cache.get("SELECT * FROM orders", nil)
    assert_nil @cache.get("SELECT * FROM users", nil)
  end

  def test_cross_referenced_cleanup
    @cache.put("SELECT * FROM orders JOIN users ON 1=1", nil, [["1"]], [])
    @cache.invalidate_table("orders")
    assert_nil @cache.get("SELECT * FROM orders JOIN users ON 1=1", nil)
    users_keys = @cache.instance_variable_get(:@table_index)["users"]
    assert(users_keys.nil? || users_keys.empty?)
  end

  def test_invalidation_stats
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.invalidate_table("orders")
    assert_equal 1, @cache.stats_invalidations
  end
end

class TestSignalProcessing < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_table_signal
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.process_signal("I:orders")
    assert_nil @cache.get("SELECT * FROM orders", nil)
  end

  def test_wildcard_signal
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.process_signal("I:*")
    assert_nil @cache.get("SELECT * FROM orders", nil)
  end

  def test_keepalive_preserves_cache
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.process_signal("P:")
    refute_nil @cache.get("SELECT * FROM orders", nil)
  end

  def test_unknown_signal_preserves_cache
    @cache.put("SELECT * FROM orders", nil, [["1"]], [])
    @cache.process_signal("X:something")
    refute_nil @cache.get("SELECT * FROM orders", nil)
  end
end

class TestPushInvalidation < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_remote_signal_clears_cache
    cache = GoldLapel::NativeCache.new
    cache.instance_variable_set(:@invalidation_connected, true)
    cache.put("SELECT * FROM orders", nil, [["1"]], [])

    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]

    cache.instance_variable_set(:@invalidation_connected, false)
    cache.connect_invalidation(port)
    conn = server.accept
    sleep 0.1

    assert cache.connected?
    conn.write("I:orders\n")
    sleep 0.2

    assert_nil cache.get("SELECT * FROM orders", nil)

    conn.close
    server.close
    cache.stop_invalidation
  end

  def test_connection_drop_clears_cache
    cache = GoldLapel::NativeCache.new
    cache.instance_variable_set(:@invalidation_connected, true)
    cache.put("SELECT * FROM orders", nil, [["1"]], [])

    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]

    cache.instance_variable_set(:@invalidation_connected, false)
    cache.connect_invalidation(port)
    conn = server.accept
    sleep 0.1

    assert cache.connected?

    conn.close
    server.close
    sleep 0.5

    refute cache.connected?
    assert_equal 0, cache.size

    cache.stop_invalidation
  end
end

class TestCachedResult < Minitest::Test
  def test_values
    result = GoldLapel::CachedResult.new([["1", "alice"], ["2", "bob"]], ["id", "name"])
    assert_equal [["1", "alice"], ["2", "bob"]], result.values
  end

  def test_fields
    result = GoldLapel::CachedResult.new([["1"]], ["id"])
    assert_equal ["id"], result.fields
  end

  def test_ntuples
    result = GoldLapel::CachedResult.new([["1"], ["2"]], ["id"])
    assert_equal 2, result.ntuples
  end

  def test_each_yields_hashes
    result = GoldLapel::CachedResult.new([["1", "alice"]], ["id", "name"])
    rows = result.to_a
    assert_equal [{ "id" => "1", "name" => "alice" }], rows
  end

  def test_bracket_access
    result = GoldLapel::CachedResult.new([["1", "alice"], ["2", "bob"]], ["id", "name"])
    assert_equal({ "id" => "1", "name" => "alice" }, result[0])
    assert_equal({ "id" => "2", "name" => "bob" }, result[1])
  end

  def test_column_values
    result = GoldLapel::CachedResult.new([["1", "alice"], ["2", "bob"]], ["id", "name"])
    assert_equal ["alice", "bob"], result.column_values(1)
  end

  def test_count_without_block_returns_ntuples
    result = GoldLapel::CachedResult.new([["1"], ["2"], ["3"]], ["id"])
    assert_equal 3, result.count
  end

  def test_count_with_block_filters
    result = GoldLapel::CachedResult.new([["1", "true"], ["2", "false"], ["3", "true"]], ["id", "active"])
    assert_equal 2, result.count { |row| row["active"] == "true" }
  end

  def test_count_with_argument
    result = GoldLapel::CachedResult.new([["1", "alice"], ["2", "alice"], ["3", "bob"]], ["id", "name"])
    target = { "id" => "1", "name" => "alice" }
    assert_equal 1, result.count(target)
  end

  def test_length_returns_ntuples
    result = GoldLapel::CachedResult.new([["1"], ["2"]], ["id"])
    assert_equal 2, result.length
  end

  def test_size_returns_ntuples
    result = GoldLapel::CachedResult.new([["1"], ["2"]], ["id"])
    assert_equal 2, result.size
  end
end
