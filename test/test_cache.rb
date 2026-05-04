require "minitest/autorun"
require "socket"
require "json"
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

class TestConcurrentCache < Minitest::Test
  # Mirrors the concurrent put/get/invalidate coverage that Python, Go, and
  # .NET ship for their native caches. MRI's GIL serializes pure Ruby, but
  # JRuby/TruffleRuby have true parallelism and the wrapper ships Thread-based
  # invalidation listeners — so this exercises `NativeCache`'s Mutex contract
  # under concurrent put/get/invalidate pressure.
  def setup
    GoldLapel::NativeCache.reset!
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "2048"
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    ENV.delete("GOLDLAPEL_NATIVE_CACHE_SIZE")
    GoldLapel::NativeCache.reset!
  end

  def test_concurrent_put_get_invalidate
    writer_threads = 10
    reader_threads = 10
    ops_per_thread = 100
    invalidator_signals = 10
    join_timeout = 30 # seconds

    errors = []
    errors_mutex = Mutex.new
    record_error = lambda do |e|
      errors_mutex.synchronize { errors << e }
    end

    # Seed a couple of keys across the 10 tables so gets have something to
    # hit (and so invalidations have something to evict). Keys collide across
    # writer threads by design — tests both insert and update paths under the
    # mutex.
    threads = []

    writer_threads.times do |tid|
      threads << Thread.new do
        ops_per_thread.times do |i|
          sql = "SELECT * FROM t#{i % 10} WHERE id = #{tid}"
          @cache.put(sql, [tid, i], [[i.to_s, "row#{tid}-#{i}"]], ["id", "name"])
        end
      rescue => e
        record_error.call(e)
      end
    end

    reader_threads.times do |tid|
      threads << Thread.new do
        ops_per_thread.times do |i|
          sql = "SELECT * FROM t#{i % 10} WHERE id = #{tid}"
          @cache.get(sql, [tid, i])
        end
      rescue => e
        record_error.call(e)
      end
    end

    # Fire invalidations concurrently with the above. Use separate threads
    # (one per signal) so they race against puts/gets rather than serializing
    # through a single invalidator loop.
    invalidator_signals.times do |i|
      threads << Thread.new do
        @cache.process_signal("I:t#{i}")
      rescue => e
        record_error.call(e)
      end
    end

    threads.each do |t|
      joined = t.join(join_timeout)
      flunk "thread did not finish within #{join_timeout}s — possible deadlock" if joined.nil?
    end

    assert_empty errors, "threads raised: #{errors.map(&:message).inspect}"

    # Stats counters must stay internally consistent. Hits + misses must
    # equal the total number of gets issued (1 per reader op). Any drift
    # means a racy counter update.
    total_gets = reader_threads * ops_per_thread
    assert_equal total_gets, @cache.stats_hits + @cache.stats_misses,
      "hit/miss counters drifted — expected #{total_gets} gets, got #{@cache.stats_hits} + #{@cache.stats_misses}"

    # Invalidation counter must be non-negative and bounded by total puts
    # (each put can be invalidated at most once). A negative or wildly large
    # value signals a race.
    total_puts = writer_threads * ops_per_thread
    assert @cache.stats_invalidations >= 0, "negative invalidation count"
    assert @cache.stats_invalidations <= total_puts,
      "invalidation count #{@cache.stats_invalidations} exceeds total puts #{total_puts}"

    # Cache must not exceed its configured max size at any steady state.
    assert_operator @cache.size, :<=, 2048, "cache exceeded max_entries"

    # The cache's internal indices must agree with each other — `@cache` and
    # `@access_order` should have identical keysets. Mismatch = split-brain
    # from a racy write.
    cache_map = @cache.instance_variable_get(:@cache)
    access_order = @cache.instance_variable_get(:@access_order)
    assert_equal cache_map.keys.sort, access_order.keys.sort,
      "@cache and @access_order keysets diverged — racy write detected"

    # `@table_index` entries must only reference keys that still exist in
    # `@cache`. Dangling entries = invalidation cleanup raced with a put.
    table_index = @cache.instance_variable_get(:@table_index)
    dangling = table_index.flat_map { |_table, keys| keys.to_a }.reject { |k| cache_map.key?(k) }
    assert_empty dangling, "@table_index references #{dangling.size} keys missing from @cache"
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


# --- L1 telemetry: counters + snapshot shape ---

class TestEvictionsCounter < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "4"
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    ENV.delete("GOLDLAPEL_NATIVE_CACHE_SIZE")
    GoldLapel::NativeCache.reset!
  end

  def test_evictions_counter_starts_zero
    assert_equal 0, @cache.stats_evictions
  end

  def test_evictions_counter_bumps_on_overflow
    8.times do |i|
      @cache.put("SELECT #{i}", nil, [[i.to_s]], [])
    end
    # 8 puts, capacity 4 → 4 evictions.
    assert_equal 4, @cache.stats_evictions
  end

  def test_evictions_counter_no_bump_within_capacity
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "8"
    GoldLapel::NativeCache.reset!
    cache = GoldLapel::NativeCache.new
    cache.instance_variable_set(:@invalidation_connected, true)
    4.times do |i|
      cache.put("SELECT #{i}", nil, [[i.to_s]], [])
    end
    assert_equal 0, cache.stats_evictions
  end
end


class TestSnapshotShape < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "64"
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    ENV.delete("GOLDLAPEL_NATIVE_CACHE_SIZE")
    GoldLapel::NativeCache.reset!
  end

  def test_snapshot_carries_required_fields
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.get("SELECT 1", nil)
    @cache.get("SELECT MISS", nil)
    snap = @cache.send(:build_snapshot)
    assert_equal @cache.wrapper_id, snap["wrapper_id"]
    assert_equal "ruby", snap["lang"]
    assert snap.key?("version")
    assert_equal 1, snap["hits"]
    assert_equal 1, snap["misses"]
    assert_equal 0, snap["evictions"]
    assert_equal 0, snap["invalidations"]
    assert_equal 1, snap["current_size_entries"]
    assert_equal 64, snap["capacity_entries"]
  end

  def test_wrapper_id_is_uuid_v4
    # Format: 8-4-4-4-12 hex chars; version nibble is 4.
    assert_match(/\A\h{8}-\h{4}-4\h{3}-[89ab]\h{3}-\h{12}\z/i, @cache.wrapper_id)
  end

  def test_wrapper_id_stable_across_calls
    a = @cache.send(:build_snapshot)["wrapper_id"]
    b = @cache.send(:build_snapshot)["wrapper_id"]
    assert_equal a, b
  end

  def test_wrapper_lang_is_ruby
    assert_equal "ruby", @cache.wrapper_lang
  end
end


# --- L1 telemetry: state-change emission (unit, no socket) ---

class TestEvictionRateStateChange < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "4"
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    # Capture emissions in lieu of a socket.
    @emissions = emissions = []
    @cache.define_singleton_method(:send_line) { |line| emissions << line }
  end

  def teardown
    ENV.delete("GOLDLAPEL_NATIVE_CACHE_SIZE")
    GoldLapel::NativeCache.reset!
  end

  def test_cache_full_fires_when_evictions_dominate
    # Capacity 4 — every put past the 4th evicts. Window = 200 puts.
    (GoldLapel::EVICT_RATE_WINDOW + 10).times do |i|
      @cache.put("SELECT #{i}", nil, [[i.to_s]], [])
    end
    s_lines = @emissions.select { |e| e.include?("cache_full") }
    refute_empty s_lines, "expected at least one cache_full emission"
    # Latched — second pass should NOT re-emit cache_full.
    before = s_lines.length
    50.times do |i|
      @cache.put("SELECT extra #{i}", nil, [[i.to_s]], [])
    end
    s_lines2 = @emissions.select { |e| e.include?("cache_full") }
    assert_equal before, s_lines2.length, "cache_full re-emitted; latch broken"
  end

  def test_cache_full_does_not_fire_below_window
    # With fewer puts than the window, no state-change fires (warmup gate).
    (GoldLapel::EVICT_RATE_WINDOW - 1).times do |i|
      @cache.put("SELECT #{i}", nil, [[i.to_s]], [])
    end
    refute @emissions.any? { |e| e.include?("cache_full") }
  end

  def test_state_lines_carry_state_field
    (GoldLapel::EVICT_RATE_WINDOW + 5).times do |i|
      @cache.put("SELECT #{i}", nil, [[i.to_s]], [])
    end
    s_line = @emissions.find { |e| e.start_with?("S:") && e.include?("cache_full") }
    refute_nil s_line
    payload = JSON.parse(s_line[2..])
    assert_equal "cache_full", payload["state"]
    assert_equal @cache.wrapper_id, payload["wrapper_id"]
    assert_equal "ruby", payload["lang"]
    assert payload.key?("ts_ms")
  end
end


class TestProcessRequest < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @emissions = emissions = []
    @cache.define_singleton_method(:send_line) { |line| emissions << line }
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_request_snapshot_emits_response
    @cache.send(:process_request, "snapshot")
    r_lines = @emissions.select { |e| e.start_with?("R:") }
    assert_equal 1, r_lines.length
    payload = JSON.parse(r_lines[0][2..])
    assert_equal @cache.wrapper_id, payload["wrapper_id"]
  end

  def test_request_empty_body_treated_as_snapshot
    @cache.send(:process_request, "")
    r_lines = @emissions.select { |e| e.start_with?("R:") }
    assert_equal 1, r_lines.length
  end

  def test_request_unknown_body_silently_dropped
    @cache.send(:process_request, "future_request_type")
    r_lines = @emissions.select { |e| e.start_with?("R:") }
    assert_empty r_lines
  end

  def test_process_signal_question_routes_to_request
    @cache.process_signal("?:snapshot")
    r_lines = @emissions.select { |e| e.start_with?("R:") }
    assert_equal 1, r_lines.length
  end

  def test_process_signal_unknown_silently_ignored
    # Backwards-compat: future proxy prefixes must not crash.
    @cache.process_signal("Z:future-prefix")
    @cache.process_signal("$:bogus")
    @cache.process_signal("")
  end

  def test_emit_wrapper_disconnected_emits_state_event
    @cache.emit_wrapper_disconnected
    s_lines = @emissions.select { |e| e.start_with?("S:") }
    assert_equal 1, s_lines.length
    payload = JSON.parse(s_lines[0][2..])
    assert_equal "wrapper_disconnected", payload["state"]
    assert_equal @cache.wrapper_id, payload["wrapper_id"]
  end

  def test_report_stats_disabled_suppresses_unit_emissions
    ENV["GOLDLAPEL_REPORT_STATS"] = "false"
    begin
      GoldLapel::NativeCache.reset!
      cache = GoldLapel::NativeCache.new
    ensure
      ENV.delete("GOLDLAPEL_REPORT_STATS")
    end
    refute cache.report_stats?
    emissions = []
    cache.define_singleton_method(:send_line) { |line| emissions << line }
    cache.send(:process_request, "snapshot")
    cache.emit_wrapper_disconnected
    cache.send(:emit_state_change, "test")
    assert_empty emissions, "report_stats=false should suppress all emissions"
  end
end


# --- L1 telemetry: state-change emission via real socket ---

class TestStateChangeEmission < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def _spawn_server
    server = TCPServer.new("127.0.0.1", 0)
    [server, server.addr[1]]
  end

  def _wait_for(timeout: 2.0, interval: 0.02)
    deadline = Time.now + timeout
    while Time.now < deadline
      return true if yield
      sleep interval
    end
    false
  end

  def _accept_with_buf(server)
    conn = server.accept
    lines = []
    lines_mutex = Mutex.new
    stop = false
    reader = Thread.new do
      buf = ""
      until stop
        begin
          ready = IO.select([conn], nil, nil, 0.2)
          next unless ready
          chunk = conn.read_nonblock(4096)
          buf += chunk
          while (idx = buf.index("\n"))
            line = buf[0...idx]
            buf = buf[(idx + 1)..]
            lines_mutex.synchronize { lines << line }
          end
        rescue IO::WaitReadable
          next
        rescue EOFError, IOError, Errno::ECONNRESET
          break
        end
      end
    end
    snapshot_lines = -> { lines_mutex.synchronize { lines.dup } }
    stop_fn = lambda do
      stop = true
      conn.close rescue nil
      reader.join(2)
    end
    [conn, snapshot_lines, stop_fn]
  end

  def test_wrapper_connected_emitted_on_socket_connect
    cache = GoldLapel::NativeCache.new
    server, port = _spawn_server
    begin
      cache.connect_invalidation(port)
      conn, snapshot_lines, stop_fn = _accept_with_buf(server)
      begin
        _wait_for { snapshot_lines.call.any? { |l| l.start_with?("S:") } }
        s_lines = snapshot_lines.call.select { |l| l.start_with?("S:") }
        refute_empty s_lines, "expected S: line, got #{snapshot_lines.call.inspect}"
        payload = JSON.parse(s_lines[0][2..])
        assert_equal "wrapper_connected", payload["state"]
        assert_equal cache.wrapper_id, payload["wrapper_id"]
        assert_equal "ruby", payload["lang"]
      ensure
        stop_fn.call
      end
    ensure
      cache.stop_invalidation
      server.close
    end
  end

  def test_snapshot_request_returns_response
    cache = GoldLapel::NativeCache.new
    cache.instance_variable_set(:@invalidation_connected, true)
    cache.put("SELECT 1", nil, [["1"]], [])
    cache.get("SELECT 1", nil)
    # Reset the connected flag so connect_invalidation flips it for real.
    cache.instance_variable_set(:@invalidation_connected, false)
    server, port = _spawn_server
    begin
      cache.connect_invalidation(port)
      conn, snapshot_lines, stop_fn = _accept_with_buf(server)
      begin
        _wait_for { snapshot_lines.call.any? { |l| l.start_with?("S:") } }
        # Send the snapshot request from the "proxy" side.
        conn.write("?:snapshot\n")
        _wait_for { snapshot_lines.call.any? { |l| l.start_with?("R:") } }
        r_lines = snapshot_lines.call.select { |l| l.start_with?("R:") }
        refute_empty r_lines, "expected R: line, got #{snapshot_lines.call.inspect}"
        payload = JSON.parse(r_lines[0][2..])
        assert_equal cache.wrapper_id, payload["wrapper_id"]
        assert_equal 1, payload["hits"]
        assert_equal 1, payload["current_size_entries"]
        # R: lines must NOT carry a state field.
        refute payload.key?("state"), "R: payload must not include state"
      ensure
        stop_fn.call
      end
    ensure
      cache.stop_invalidation
      server.close
    end
  end

  def test_report_stats_disabled_suppresses_emissions
    ENV["GOLDLAPEL_REPORT_STATS"] = "false"
    begin
      GoldLapel::NativeCache.reset!
      cache = GoldLapel::NativeCache.new
    ensure
      ENV.delete("GOLDLAPEL_REPORT_STATS")
    end
    refute cache.report_stats?
    server, port = _spawn_server
    begin
      cache.connect_invalidation(port)
      conn, snapshot_lines, stop_fn = _accept_with_buf(server)
      begin
        sleep 0.2
        conn.write("?:snapshot\n")
        sleep 0.2
        out = snapshot_lines.call.select { |l| l.start_with?("S:") || l.start_with?("R:") }
        assert_empty out, "expected no S/R lines, got #{out.inspect}"
      ensure
        stop_fn.call
      end
    ensure
      cache.stop_invalidation
      server.close
    end
  end

  def test_cache_full_emitted_after_eviction_burst_over_socket
    ENV["GOLDLAPEL_NATIVE_CACHE_SIZE"] = "4"
    begin
      GoldLapel::NativeCache.reset!
      cache = GoldLapel::NativeCache.new
    ensure
      ENV.delete("GOLDLAPEL_NATIVE_CACHE_SIZE")
    end
    server, port = _spawn_server
    begin
      cache.connect_invalidation(port)
      conn, snapshot_lines, stop_fn = _accept_with_buf(server)
      begin
        # Wait for wrapper_connected so the socket is wired.
        _wait_for { snapshot_lines.call.any? { |l| l.start_with?("S:") } }
        # Push more puts than the eviction window so cache_full latches.
        (GoldLapel::EVICT_RATE_WINDOW + 10).times do |i|
          cache.put("SELECT #{i}", nil, [[i.to_s]], [])
        end
        _wait_for(timeout: 3.0) do
          snapshot_lines.call.any? { |l| l.include?("cache_full") }
        end
        full_lines = snapshot_lines.call.select { |l| l.include?("cache_full") }
        refute_empty full_lines, "expected cache_full S: line, got #{snapshot_lines.call.inspect}"
        payload = JSON.parse(full_lines[0][2..])
        assert_equal "cache_full", payload["state"]
        assert_equal cache.wrapper_id, payload["wrapper_id"]
      ensure
        stop_fn.call
      end
    ensure
      cache.stop_invalidation
      server.close
    end
  end
end


# --- Explicit L1 disable: get() always misses, put() is a no-op ---

class TestDisableL1 < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_default_is_false
    refute @cache.disable_l1?
  end

  def test_default_cache_works_as_today
    # Default mode: put + get round-trips, hit counter ticks.
    @cache.put("SELECT * FROM users", nil, [["1", "alice"]], ["id", "name"])
    entry = @cache.get("SELECT * FROM users", nil)
    refute_nil entry
    assert_equal [["1", "alice"]], entry[:values]
    assert_equal 1, @cache.stats_hits
    assert_equal 0, @cache.stats_misses
  end

  def test_disabled_get_always_returns_nil
    # Pre-populate while enabled, then flip the switch.
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.disable_l1 = true
    assert_nil @cache.get("SELECT 1", nil)
    assert_nil @cache.get("SELECT 2", nil)
    assert_nil @cache.get("SELECT 3", nil)
  end

  def test_disabled_put_is_silent_noop
    @cache.disable_l1 = true
    @cache.put("SELECT 1", nil, [["1"]], [])
    @cache.put("SELECT 2", nil, [["2"]], [])
    # Cache must remain empty — put didn't store.
    assert_equal 0, @cache.size
    # Re-enable and confirm get still misses (nothing was actually stored).
    @cache.disable_l1 = false
    assert_nil @cache.get("SELECT 1", nil)
  end

  def test_disabled_misses_tick_hits_stay_zero
    @cache.disable_l1 = true
    5.times { |i| @cache.get("SELECT #{i}", nil) }
    assert_equal 0, @cache.stats_hits
    assert_equal 5, @cache.stats_misses
  end

  def test_disabled_evictions_stay_zero
    # Even under heavy put pressure, disabled put never evicts.
    @cache.disable_l1 = true
    100.times { |i| @cache.put("SELECT #{i}", nil, [[i.to_s]], []) }
    assert_equal 0, @cache.stats_evictions
  end

  def test_snapshot_l1_disabled_field_absent_when_enabled
    snap = @cache.send(:build_snapshot)
    refute snap.key?("l1_disabled"),
      "l1_disabled must not appear when L1 is on (forward-compat: only present when set)"
  end

  def test_snapshot_l1_disabled_field_present_when_disabled
    @cache.disable_l1 = true
    snap = @cache.send(:build_snapshot)
    assert_equal true, snap["l1_disabled"]
  end

  def test_disable_l1_setter_normalizes_truthy
    @cache.disable_l1 = "yes"
    assert_equal true, @cache.disable_l1?
    @cache.disable_l1 = nil
    assert_equal false, @cache.disable_l1?
  end

  def test_invalidation_thread_still_runs_when_disabled
    # Telemetry signal flow must keep working even with L1 off — the
    # proxy/Manor still need to see snapshot replies + state changes.
    @cache.disable_l1 = true
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    @cache.instance_variable_set(:@invalidation_connected, false)
    @cache.connect_invalidation(port)
    conn = server.accept
    sleep 0.1
    assert @cache.connected?, "invalidation thread must still establish a connection"

    # The wrapper_connected snapshot should carry l1_disabled: true.
    buf = ""
    deadline = Time.now + 2.0
    while Time.now < deadline
      ready = IO.select([conn], nil, nil, 0.1)
      next unless ready
      begin
        buf += conn.read_nonblock(4096)
      rescue IO::WaitReadable
        next
      rescue EOFError
        break
      end
      break if buf.include?("\n") && buf.include?("wrapper_connected")
    end

    s_lines = buf.split("\n").select { |l| l.start_with?("S:") && l.include?("wrapper_connected") }
    refute_empty s_lines
    payload = JSON.parse(s_lines[0][2..])
    assert_equal true, payload["l1_disabled"]

    conn.close
    server.close
    @cache.stop_invalidation
  end
end
