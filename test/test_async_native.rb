# frozen_string_literal: true

# Native-async integration tests for `GoldLapel::Async::Instance`.
#
# Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM — the
# standardized integration-test convention shared across all Gold Lapel
# wrappers. See test/_integration_gate.rb. Also requires `async` and `pg`
# gems; if either is unavailable, tests skip.
#
# When available, each test exercises the native-async path end-to-end:
# a real Async reactor, a real PG::Connection under the async utility
# layer, real Postgres round-trips through `async_exec_params` /
# `async_exec` / `wait_for_notify`.
#
# These tests talk directly to Postgres (no proxy subprocess), so they
# verify the wrapper's async utility layer against a real server while
# skipping the proxy/binary concerns covered by the sync/mock test suite.

require "minitest/autorun"
require "json"
require "securerandom"
require_relative "_integration_gate"

class TestAsyncNativeIntegration < Minitest::Test
  # Evaluating the gate at load time surfaces the half-configured CI case
  # (GOLDLAPEL_INTEGRATION=1 set, GOLDLAPEL_TEST_UPSTREAM missing) as a loud
  # raise during test collection — preventing false-green.
  DATABASE_URL = GoldLapelTestGate.integration_upstream

  # Cache the probe across tests — each setup would otherwise pay the full
  # connect timeout to an unreachable host.
  @@probe_done = false
  @@async_available = false
  @@pg_available = false
  @@db_reachable = false

  def self.run_probe
    return if @@probe_done
    @@probe_done = true
    # When integration tests are off (DATABASE_URL is nil), skip the probe
    # entirely — individual tests will skip via skip_unless_ready below.
    return if DATABASE_URL.nil?
    begin
      require "async"
      require "pg"
      require_relative "../lib/goldlapel/async"
      @@async_available = true
      @@pg_available = true
    rescue LoadError
      return
    end
    sep = DATABASE_URL.include?("?") ? "&" : "?"
    probe_url = "#{DATABASE_URL}#{sep}connect_timeout=3"
    begin
      probe = PG.connect(probe_url)
      probe.close
      @@db_reachable = true
    rescue PG::Error, StandardError
      @@db_reachable = false
    end
  end

  def setup
    self.class.run_probe
    @async_available = @@async_available
    @pg_available = @@pg_available
    @db_reachable = @@db_reachable

    @collection = "_gl_async_native_test_#{SecureRandom.hex(4)}"
    @counter_table = "_gl_async_native_counter_#{SecureRandom.hex(4)}"
    @watch_collection = "_gl_async_native_watch_#{SecureRandom.hex(4)}"
  end

  def teardown
    return unless @db_reachable

    begin
      conn = PG.connect(DATABASE_URL)
      [@collection, @counter_table, @watch_collection].compact.each do |tbl|
        conn.exec("DROP TABLE IF EXISTS #{tbl} CASCADE") rescue nil
      end
      fn_name = "_gl_notify_#{@watch_collection}"
      conn.exec("DROP FUNCTION IF EXISTS #{fn_name}() CASCADE") rescue nil
      conn.close
    rescue StandardError
      # cleanup is best-effort
    end
  end

  def skip_unless_ready
    skip GoldLapelTestGate.skip_reason if DATABASE_URL.nil?
    skip "async gem not installed" unless @async_available
    skip "pg gem not installed" unless @pg_available
    skip "GOLDLAPEL_TEST_UPSTREAM not reachable (#{DATABASE_URL})" unless @db_reachable
  end

  # Run block inside an Async reactor with a fresh PG::Connection routed
  # through GoldLapel::Async::Utils. The connection is closed afterwards.
  def inside_reactor
    result = nil
    Kernel.Async do
      conn = PG.connect(DATABASE_URL)
      begin
        result = yield conn
      ensure
        conn.close
      end
    end.wait
    result
  end

  def test_search_under_async_reactor
    skip_unless_ready

    rows = inside_reactor do |conn|
      conn.exec("DROP TABLE IF EXISTS #{@collection} CASCADE")
      conn.exec(<<~SQL)
        CREATE TABLE #{@collection} (
          id SERIAL PRIMARY KEY,
          body TEXT NOT NULL
        )
      SQL
      conn.exec_params(
        "INSERT INTO #{@collection} (body) VALUES ($1), ($2), ($3)",
        ["postgres tuning tips", "redis alternatives", "postgres indexes 101"]
      )
      GoldLapel::Async::Utils.search(conn, @collection, "body", "postgres tuning")
    end

    assert rows.is_a?(Array), "search should return an Array"
    assert rows.length >= 1, "expected at least one match for 'postgres tuning'"
    assert rows.first.key?("_score"), "search results should carry _score"
  end

  def test_doc_insert_and_find_under_async_reactor
    skip_unless_ready

    inserted, found = inside_reactor do |conn|
      ins = GoldLapel::Async::Utils.doc_insert(conn, @collection, { name: "alice", age: 30 })
      fnd = GoldLapel::Async::Utils.doc_find(conn, @collection, filter: { name: "alice" })
      [ins, fnd]
    end

    assert inserted["_id"], "doc_insert should return a row with _id"
    assert_equal "alice", inserted["data"]["name"]
    assert_equal 1, found.length
    assert_equal "alice", found[0]["data"]["name"]
    assert_equal 30, found[0]["data"]["age"]
  end

  def test_doc_find_one_under_async_reactor
    skip_unless_ready

    result = inside_reactor do |conn|
      GoldLapel::Async::Utils.doc_insert(conn, @collection, { key: "v1", n: 1 })
      GoldLapel::Async::Utils.doc_insert(conn, @collection, { key: "v2", n: 2 })
      GoldLapel::Async::Utils.doc_find_one(conn, @collection, filter: { key: "v2" })
    end

    refute_nil result, "doc_find_one should return a row"
    assert_equal "v2", result["data"]["key"]
    assert_equal 2, result["data"]["n"]
  end

  def test_using_scope_under_async_reactor
    skip_unless_ready

    # Simulates `gl.using(conn) do ... end` scoping under an Async task.
    # The fiber-local key must survive async_* calls that yield to the
    # reactor mid-flight without leaking to sibling fibers.
    counts = inside_reactor do |conn|
      # Create the collection up front so both inserts target the same table.
      GoldLapel::Async::Utils.doc_insert(conn, @collection, { setup: true })

      fiber_key = :__gl_native_test_scope_conn
      prev = Fiber[fiber_key]
      begin
        Fiber[fiber_key] = conn
        scoped = Fiber[fiber_key]
        refute_nil scoped, "fiber-local conn should be visible inside Async task"

        # Write through the scoped conn (simulating what Async::Instance#using does)
        GoldLapel::Async::Utils.doc_insert(scoped, @collection, { scoped: true })
      ensure
        Fiber[fiber_key] = prev
      end

      # After the ensure, fiber-local is restored
      assert_nil Fiber[fiber_key], "fiber-local should be unwound after scope exit"

      GoldLapel::Async::Utils.doc_count(conn, @collection)
    end

    assert_equal 2, counts, "both setup and scoped inserts should be visible"
  end

  def test_doc_watch_one_iteration_under_async_reactor
    skip_unless_ready

    # Exercise doc_watch end-to-end for ONE notification, then break out of
    # the infinite loop. Run on a fresh conn because doc_watch blocks on
    # wait_for_notify, and we'll trigger a change from a sibling task.
    events = []

    Kernel.Async do |task|
      conn = PG.connect(DATABASE_URL)
      producer = PG.connect(DATABASE_URL)
      begin
        # Create the table + trigger wiring in a setup task
        GoldLapel::Async::Utils.doc_insert(conn, @watch_collection, { seed: true })

        # Start a watcher task; it'll hang on wait_for_notify inside doc_watch.
        watcher = task.async do
          GoldLapel::Async::Utils.doc_watch(conn, @watch_collection) do |event|
            events << event
          end
        end

        # Give the watcher a tick to register its LISTEN
        task.sleep(0.2)

        # Trigger an insert from the producer conn — this fires the trigger
        # and pg_notify on the channel the watcher is listening to.
        GoldLapel::Async::Utils.doc_insert(producer, @watch_collection, { trigger: "insert" })

        # Wait for the event to arrive, with a timeout
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5.0
        until !events.empty? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          task.sleep(0.05)
        end

        watcher.stop
      ensure
        producer.close
        begin
          GoldLapel::Async::Utils.doc_unwatch(conn, @watch_collection)
        rescue StandardError
          # best-effort
        end
        conn.close
      end
    end.wait

    refute_empty events, "doc_watch should have received at least one event"
    first = events.first
    assert first.is_a?(Hash), "event should be a Hash"
    assert_equal "INSERT", first["op"]
    assert first["data"].is_a?(Hash), "event data should be a Hash (JSON-parsed)"
    assert_equal "insert", first["data"]["trigger"]
  end

  def test_async_instance_uses_async_utils_not_sync_utils
    skip_unless_ready

    # Source-level guard: doc-store / search calls must route through the
    # async Utils layer, never the sync one. Phase 4 split the doc verbs into
    # `lib/goldlapel/async/documents.rb`, so check both files.
    async_src = File.read(File.expand_path("../lib/goldlapel/async.rb", __dir__))
    documents_src = File.read(File.expand_path("../lib/goldlapel/async/documents.rb", __dir__))

    assert_match(/Utils\.doc_insert\b/, documents_src,
      "Async::DocumentsAPI#insert should delegate to Async::Utils.doc_insert")
    assert_match(/Utils\.search\b/, async_src,
      "Async::Instance#search should delegate to Async::Utils.search")

    # No sync GoldLapel.doc_* / GoldLapel.search delegations from the async
    # entry points — that would defeat the cooperative-yield invariant.
    refute_match(/\bGoldLapel\.doc_insert\b/, async_src,
      "Async::Instance should not route through the sync GoldLapel.* delegators")
    refute_match(/\bGoldLapel\.doc_insert\b/, documents_src,
      "Async::DocumentsAPI should not route through the sync GoldLapel.* delegators")
  end
end
