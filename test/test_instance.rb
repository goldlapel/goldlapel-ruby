# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/streams"

class InstanceMockResult
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

class InstanceMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = InstanceMockResult.new([], [])
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

  def close; end
  def finished?; false; end
end

# Helper: build an Instance with a mock conn without spawning the proxy.
# Sub-APIs (documents/streams) are wired up so `inst.documents.<verb>` works
# in tests without going through Instance#initialize (which would try to
# spawn the binary).
def make_test_instance(mock_conn)
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, mock_conn)
  inst.instance_variable_set(:@wrapped_conn, mock_conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst
end

# Helper: build an Instance with no conn (simulates stopped proxy)
def make_stopped_instance
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, nil)
  inst.instance_variable_set(:@wrapped_conn, nil)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst
end

# Stub DocumentsAPI._patterns so doc_* tests don't need a live dashboard.
# Returns a stub keyed off the user collection name (the SQL still reads
# `INSERT INTO users`, matching pre-Phase-4 assertions).
def stub_doc_patterns_on(documents_api)
  documents_api.define_singleton_method(:_patterns) do |collection, **_opts|
    {
      tables: { "main" => collection.to_s },
      query_patterns: {},
    }
  end
end

# --- conn accessor ---

class TestInstanceConn < Minitest::Test
  def test_conn_accessor
    mock = InstanceMockConnection.new
    inst = make_test_instance(mock)
    assert_same mock, inst.conn
  end

  def test_conn_nil_after_stop_raises
    inst = make_stopped_instance
    stub_doc_patterns_on(inst.documents)

    error = assert_raises(RuntimeError) { inst.documents.insert("col", { a: 1 }) }
    assert_match(/Connection not available/, error.message)
  end

  def test_stop_is_idempotent
    # Double-stop is reachable via atexit hooks, signal handlers, try/ensure
    # chains, and test teardown. Guard against regression (NPE on second
    # close / double-unregister / double-kill).
    mock_conn = InstanceMockConnection.new
    stop_calls = 0
    fake_proxy = Struct.new(:upstream, :url, :dashboard_url).new(
      "postgresql://localhost/test", "postgresql://localhost:7932/test", nil
    )
    fake_proxy.define_singleton_method(:stop) { stop_calls += 1 }
    fake_proxy.define_singleton_method(:running?) { false }

    inst = GoldLapel::Instance.allocate
    inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
    inst.instance_variable_set(:@internal_conn, mock_conn)
    inst.instance_variable_set(:@wrapped_conn, mock_conn)
    inst.instance_variable_set(:@proxy, fake_proxy)
    inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")

    # Register so Instance#stop's Proxy.unregister call finds it.
    GoldLapel::Proxy.register(fake_proxy)
    begin
      inst.stop
      inst.stop # must not raise
    ensure
      GoldLapel::Proxy.unregister(fake_proxy)
    end

    # Internal state fully torn down after first stop; second stop is no-op.
    assert_nil inst.instance_variable_get(:@internal_conn)
    assert_nil inst.instance_variable_get(:@wrapped_conn)
    assert_nil inst.instance_variable_get(:@proxy)
    assert_equal 1, stop_calls, "proxy.stop must be called exactly once across two Instance#stop calls"
  end

  def test_stop_is_idempotent_when_never_started
    # An instance that never successfully started has @proxy=nil and
    # @internal_conn=nil. stop() should be a safe no-op both times.
    inst = make_stopped_instance
    inst.stop
    inst.stop # must not raise
    assert_nil inst.instance_variable_get(:@internal_conn)
    assert_nil inst.instance_variable_get(:@proxy)
  end

  def test_all_methods_raise_when_stopped
    inst = make_stopped_instance
    # Stub DDL-fetching sub-APIs so the assertion homes in on _resolve_conn
    # rather than the DDL fetch path (which has its own coverage in test_ddl.rb).
    stub_doc_patterns_on(inst.documents)
    inst.streams.define_singleton_method(:_patterns) do |_stream|
      { query_patterns: {}, tables: { "main" => "_goldlapel.stream_x" } }
    end

    flat_methods = {
      search: ["tbl", "col", "q"],
      incr: ["tbl", "key"],
      get_counter: ["tbl", "key"],
      hset: ["tbl", "k", "f", "v"],
      hget: ["tbl", "k", "f"],
      zadd: ["tbl", "m", 1.0],
      zrange: ["tbl"],
      publish: ["ch", "msg"],
      enqueue: ["q", { a: 1 }],
      dequeue: ["q"],
      count_distinct: ["tbl", "col"],
      percolate_add: ["n", "qid", "q"],
      analyze: ["text"],
    }
    flat_methods.each do |method, args|
      error = assert_raises(RuntimeError, "Expected #{method} to raise") { inst.send(method, *args) }
      assert_match(/Connection not available/, error.message, "#{method} error message mismatch")
    end

    # Sub-API verbs that touch a connection hit the same _resolve_conn guard.
    # `create_collection` is a pure DDL-side fetch — when patterns are stubbed
    # (no live dashboard), it returns nil without consulting the conn, which
    # is the correct contract: an instance whose proxy is stopped can still
    # ask `do you exist?` of a cached pattern. So it's not in this set.
    documents_calls = {
      insert: ["col", { a: 1 }],
      find: ["col"],
      find_one: ["col"],
      count: ["col"],
    }
    documents_calls.each do |verb, args|
      error = assert_raises(RuntimeError, "Expected documents.#{verb} to raise") do
        inst.documents.send(verb, *args)
      end
      assert_match(/Connection not available/, error.message, "documents.#{verb} error mismatch")
    end

    error = assert_raises(RuntimeError, "Expected streams.add to raise") do
      inst.streams.add("s", { a: 1 })
    end
    assert_match(/Connection not available/, error.message)
  end
end

# --- documents.create_collection delegation ---

class TestInstanceDocCreateCollection < Minitest::Test
  def test_no_op_when_patterns_supplied
    # Phase 4: proxy owns DDL. Wrapper-side `documents.create_collection` is
    # purely a pattern-fetch — no SQL flows through the customer's conn.
    mock = InstanceMockConnection.new
    inst = make_test_instance(mock)
    stub_doc_patterns_on(inst.documents)

    inst.documents.create_collection("events")
    assert_empty mock.calls,
                 "documents.create_collection must not issue any SQL on the customer connection"
  end
end

# --- documents.insert delegation ---

class TestInstanceDocInsert < Minitest::Test
  def test_delegates_to_module
    insert_result = InstanceMockResult.new(
      [{ "_id" => "550e8400-e29b-41d4-a716-446655440000", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["_id", "data", "created_at"]
    )
    mock = InstanceMockConnection.new("INSERT" => insert_result)
    inst = make_test_instance(mock)
    stub_doc_patterns_on(inst.documents)

    result = inst.documents.insert("users", { name: "Alice" })
    assert_equal "550e8400-e29b-41d4-a716-446655440000", result["_id"]
    assert_equal({ "name" => "Alice" }, result["data"])

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "INSERT INTO users"
  end
end

# --- documents.find delegation ---

class TestInstanceDocFind < Minitest::Test
  def test_delegates_to_module
    find_result = InstanceMockResult.new(
      [
        { "_id" => "550e8400-e29b-41d4-a716-446655440001", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07" },
        { "_id" => "550e8400-e29b-41d4-a716-446655440002", "data" => '{"name":"Bob"}', "created_at" => "2026-04-07" },
      ],
      ["_id", "data", "created_at"]
    )
    mock = InstanceMockConnection.new("SELECT" => find_result)
    inst = make_test_instance(mock)
    stub_doc_patterns_on(inst.documents)

    results = inst.documents.find("users")
    assert_equal 2, results.length
    assert_equal "Alice", results[0]["data"]["name"]
  end

  def test_passes_filter
    find_result = InstanceMockResult.new(
      [{ "_id" => "550e8400-e29b-41d4-a716-446655440000", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07" }],
      ["_id", "data", "created_at"]
    )
    mock = InstanceMockConnection.new("SELECT" => find_result)
    inst = make_test_instance(mock)
    stub_doc_patterns_on(inst.documents)

    inst.documents.find("users", filter: { name: "Alice" })
    select_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil select_call
    assert_includes select_call[:sql], "@>"
    assert_includes select_call[:params], JSON.generate({ name: "Alice" })
  end
end

# --- search delegation ---

class TestInstanceSearch < Minitest::Test
  def test_delegates_search
    search_result = InstanceMockResult.new(
      [{ "id" => "1", "title" => "Test", "_score" => "0.5" }],
      ["id", "title", "_score"]
    )
    mock = InstanceMockConnection.new("to_tsvector" => search_result)
    inst = make_test_instance(mock)

    results = inst.search("articles", "title", "test")
    assert_equal 1, results.length
    assert_equal "Test", results[0]["title"]
  end
end

# --- incr / get_counter delegation ---

class TestInstanceCounters < Minitest::Test
  def test_incr_delegates
    incr_result = InstanceMockResult.new(
      [{ "value" => "5" }],
      ["value"]
    )
    mock = InstanceMockConnection.new("INSERT" => incr_result)
    inst = make_test_instance(mock)

    val = inst.incr("counters", "page_views")
    assert_equal 5, val
  end

  def test_get_counter_delegates
    counter_result = InstanceMockResult.new(
      [{ "value" => "42" }],
      ["value"]
    )
    mock = InstanceMockConnection.new("SELECT" => counter_result)
    inst = make_test_instance(mock)

    val = inst.get_counter("counters", "page_views")
    assert_equal 42, val
  end
end

# --- hash methods delegation ---

class TestInstanceHash < Minitest::Test
  def test_hset_delegates
    mock = InstanceMockConnection.new
    inst = make_test_instance(mock)

    inst.hset("cache", "user:1", "name", "Alice")
    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "jsonb_build_object"
  end

  def test_hget_delegates
    hget_result = InstanceMockResult.new(
      [{ "?column?" => '"Alice"' }],
      ["?column?"]
    )
    mock = InstanceMockConnection.new("SELECT" => hget_result)
    inst = make_test_instance(mock)

    val = inst.hget("cache", "user:1", "name")
    assert_equal "Alice", val
  end
end

# --- sorted set delegation ---

class TestInstanceSortedSet < Minitest::Test
  def test_zadd_delegates
    mock = InstanceMockConnection.new
    inst = make_test_instance(mock)

    inst.zadd("leaderboard", "player1", 100)
    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:params], "player1"
  end

  def test_zrange_delegates
    zrange_result = InstanceMockResult.new(
      [{ "member" => "player1", "score" => "100.0" }],
      ["member", "score"]
    )
    mock = InstanceMockConnection.new("SELECT" => zrange_result)
    inst = make_test_instance(mock)

    results = inst.zrange("leaderboard")
    assert_equal 1, results.length
    assert_equal "player1", results[0][0]
    assert_equal 100.0, results[0][1]
  end
end

# --- streams.* delegation ---

class TestInstanceStreams < Minitest::Test
  def test_stream_add_delegates
    add_result = InstanceMockResult.new(
      [{ "id" => "1", "created_at" => "2026-04-07" }],
      ["id", "created_at"],
    )
    mock = InstanceMockConnection.new("INSERT" => add_result)
    inst = make_test_instance(mock)
    # Stub _patterns on the streams sub-API so we don't POST to a fake
    # dashboard — the DDL fetch is covered end-to-end in test_ddl.rb /
    # test_streams_integration.rb.
    inst.streams.define_singleton_method(:_patterns) do |_stream|
      {
        query_patterns: {
          "insert" => "INSERT INTO _goldlapel.stream_events (payload) VALUES ($1) RETURNING id, created_at",
        },
        tables: { "main" => "_goldlapel.stream_events" },
      }
    end

    result = inst.streams.add("events", { task: "test" })
    assert_equal 1, result["id"]
    # wrapper hydrates payload from the input hash (proxy pattern returns (id, created_at))
    assert_equal({ task: "test" }, result["payload"])
  end
end

# --- enqueue / dequeue delegation ---

class TestInstanceQueue < Minitest::Test
  def test_enqueue_delegates
    mock = InstanceMockConnection.new
    inst = make_test_instance(mock)

    inst.enqueue("jobs", { task: "email" })
    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
  end

  def test_dequeue_delegates
    dequeue_result = InstanceMockResult.new(
      [{ "payload" => '{"task":"email"}' }],
      ["payload"]
    )
    mock = InstanceMockConnection.new("DELETE" => dequeue_result)
    inst = make_test_instance(mock)

    result = inst.dequeue("jobs")
    assert_equal({ "task" => "email" }, result)
  end
end
