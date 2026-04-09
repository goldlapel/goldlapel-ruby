# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/instance"

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

# Helper: build an Instance with a mock conn without spawning the proxy
def make_test_instance(mock_conn)
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@conn, mock_conn)
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst
end

# --- conn accessor ---

class TestInstanceConn < Minitest::Test
  def test_conn_accessor
    mock = InstanceMockConnection.new
    inst = make_test_instance(mock)
    assert_same mock, inst.conn
  end

  def test_conn_nil_after_stop_raises
    inst = GoldLapel::Instance.allocate
    inst.instance_variable_set(:@conn, nil)
    inst.instance_variable_set(:@upstream, "postgresql://localhost/test")

    error = assert_raises(RuntimeError) { inst.doc_insert("col", { a: 1 }) }
    assert_match(/Connection not available/, error.message)
  end

  def test_all_methods_raise_when_stopped
    inst = GoldLapel::Instance.allocate
    inst.instance_variable_set(:@conn, nil)
    inst.instance_variable_set(:@upstream, "postgresql://localhost/test")

    methods_with_args = {
      doc_insert: ["col", { a: 1 }],
      doc_find: ["col"],
      doc_find_one: ["col"],
      doc_count: ["col"],
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
      stream_add: ["s", { a: 1 }],
      percolate_add: ["n", "qid", "q"],
      analyze: ["text"],
    }

    methods_with_args.each do |method, args|
      error = assert_raises(RuntimeError, "Expected #{method} to raise") { inst.send(method, *args) }
      assert_match(/Connection not available/, error.message, "#{method} error message mismatch")
    end
  end
end

# --- doc_insert delegation ---

class TestInstanceDocInsert < Minitest::Test
  def test_delegates_to_module
    insert_result = InstanceMockResult.new(
      [{ "_id" => "550e8400-e29b-41d4-a716-446655440000", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["_id", "data", "created_at"]
    )
    mock = InstanceMockConnection.new("INSERT" => insert_result)
    inst = make_test_instance(mock)

    result = inst.doc_insert("users", { name: "Alice" })
    assert_equal "550e8400-e29b-41d4-a716-446655440000", result["_id"]
    assert_equal({ "name" => "Alice" }, result["data"])

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "INSERT INTO users"
  end
end

# --- doc_find delegation ---

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

    results = inst.doc_find("users")
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

    inst.doc_find("users", filter: { name: "Alice" })
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

# --- stream delegation ---

class TestInstanceStreams < Minitest::Test
  def test_stream_add_delegates
    add_result = InstanceMockResult.new(
      [{ "id" => "1", "payload" => '{"task":"test"}', "created_at" => "2026-04-07" }],
      ["id", "payload", "created_at"]
    )
    mock = InstanceMockConnection.new("INSERT" => add_result)
    inst = make_test_instance(mock)

    result = inst.stream_add("events", { task: "test" })
    assert_equal 1, result["id"]
    assert_equal({ "task" => "test" }, result["payload"])
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
