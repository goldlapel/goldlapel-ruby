# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class StreamMockResult
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

class StreamMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = StreamMockResult.new([], [])
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql }
    r = @default
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

class TestStreamAdd < Minitest::Test
  def test_creates_table_and_inserts
    insert_result = StreamMockResult.new(
      [{ "id" => "1", "payload" => '{"task":"email"}', "created_at" => "2026-04-02 00:00:00+00" }],
      ["id", "payload", "created_at"]
    )
    mock = StreamMockConnection.new("INSERT" => insert_result)
    result = GoldLapel.stream_add(mock, "events", { task: "email" })

    assert_equal 1, result["id"]
    assert_equal({ "task" => "email" }, result["payload"])
    assert_equal "2026-04-02 00:00:00+00", result["created_at"]

    create_call = mock.calls.find { |c| c[:sql].include?("CREATE TABLE") }
    refute_nil create_call
    assert_includes create_call[:sql], "events"

    insert_call = mock.calls.find { |c| c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_equal [JSON.generate({ task: "email" })], insert_call[:params]
  end
end

class TestStreamCreateGroup < Minitest::Test
  def test_creates_groups_and_pending_tables
    mock = StreamMockConnection.new
    GoldLapel.stream_create_group(mock, "events", "workers")

    sqls = mock.calls.map { |c| c[:sql] }
    groups_ddl = sqls.find { |s| s.include?("events_groups") && s.include?("CREATE TABLE") }
    pending_ddl = sqls.find { |s| s.include?("events_pending") && s.include?("CREATE TABLE") }
    refute_nil groups_ddl
    refute_nil pending_ddl
    assert_includes groups_ddl, "group_name TEXT PRIMARY KEY"
    assert_includes pending_ddl, "message_id BIGINT"

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_equal ["workers"], insert_call[:params]
  end
end

class TestStreamRead < Minitest::Test
  def test_reads_messages_and_tracks_pending
    read_result = StreamMockResult.new(
      [{ "id" => "5", "payload" => '{"x":1}', "created_at" => "2026-04-02 00:00:00+00" }],
      ["id", "payload", "created_at"]
    )
    mock = StreamMockConnection.new("SELECT id, payload, created_at FROM next" => read_result)
    messages = GoldLapel.stream_read(mock, "events", "workers", "c1", count: 1)

    assert_equal 1, messages.length
    assert_equal 5, messages[0]["id"]
    assert_equal({ "x" => 1 }, messages[0]["payload"])

    pending_insert = mock.calls.select { |c| c[:method] == :exec_params && c[:sql].include?("_pending") }
    assert_equal 1, pending_insert.length
    assert_equal ["workers", "c1", 5], pending_insert[0][:params]
  end

  def test_returns_empty_when_no_messages
    mock = StreamMockConnection.new
    messages = GoldLapel.stream_read(mock, "events", "workers", "c1")
    assert_empty messages
  end
end

class TestStreamAck < Minitest::Test
  def test_ack_returns_true_on_delete
    del_result = StreamMockResult.new(
      [{ "dummy" => "1" }],
      ["dummy"]
    )
    mock = StreamMockConnection.new("DELETE" => del_result)
    assert GoldLapel.stream_ack(mock, "events", "workers", 5)
  end

  def test_ack_returns_false_when_not_found
    mock = StreamMockConnection.new
    refute GoldLapel.stream_ack(mock, "events", "workers", 999)
  end
end

class TestStreamClaim < Minitest::Test
  def test_claims_idle_messages
    update_result = StreamMockResult.new(
      [{ "message_id" => "5" }],
      ["message_id"]
    )
    select_result = StreamMockResult.new(
      [{ "id" => "5", "payload" => '{"y":2}', "created_at" => "2026-04-02 00:00:00+00" }],
      ["id", "payload", "created_at"]
    )
    mock = StreamMockConnection.new(
      "UPDATE" => update_result,
      "SELECT" => select_result
    )
    messages = GoldLapel.stream_claim(mock, "events", "workers", "c2", min_idle_ms: 30000)

    assert_equal 1, messages.length
    assert_equal 5, messages[0]["id"]
    assert_equal({ "y" => 2 }, messages[0]["payload"])

    update_call = mock.calls.find { |c| c[:sql].include?("UPDATE") }
    assert_equal ["c2", "workers", "30000"], update_call[:params]
  end

  def test_returns_empty_when_nothing_idle
    mock = StreamMockConnection.new
    messages = GoldLapel.stream_claim(mock, "events", "workers", "c2")
    assert_empty messages
  end
end
