# frozen_string_literal: true
#
# Streams now use proxy-owned DDL: CREATE TABLE never runs in the wrapper.
# The wrapper receives canonical SQL `query_patterns` from the proxy's DDL
# API (see goldlapel/ddl.rb) and executes those verbatim. These tests mock
# both the patterns (fixtures) and the connection.

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

STREAM_PATTERNS = {
  query_patterns: {
    "insert" => "INSERT INTO _goldlapel.stream_events (payload) VALUES ($1) RETURNING id, created_at",
    "read_since" => "SELECT id, payload, created_at FROM _goldlapel.stream_events WHERE id > $1 ORDER BY id LIMIT $2",
    "read_by_id" => "SELECT id, payload, created_at FROM _goldlapel.stream_events WHERE id = $1",
    "group_get_cursor" => "SELECT last_delivered_id FROM _goldlapel.stream_events_groups WHERE group_name = $1 FOR UPDATE",
    "group_advance_cursor" => "UPDATE _goldlapel.stream_events_groups SET last_delivered_id = $1 WHERE group_name = $2",
    "pending_insert" => "INSERT INTO _goldlapel.stream_events_pending (message_id, group_name, consumer) VALUES ($1, $2, $3) ON CONFLICT (group_name, message_id) DO NOTHING",
    "create_group" => "INSERT INTO _goldlapel.stream_events_groups (group_name) VALUES ($1) ON CONFLICT DO NOTHING",
    "ack" => "DELETE FROM _goldlapel.stream_events_pending WHERE group_name = $1 AND message_id = $2",
    "claim" => "UPDATE _goldlapel.stream_events_pending SET consumer = $1, claimed_at = NOW(), delivery_count = delivery_count + 1 WHERE group_name = $2 AND claimed_at < NOW() - INTERVAL '1 millisecond' * $3 RETURNING message_id",
  },
}.freeze

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
  def test_inserts_via_canonical_pattern
    insert_result = StreamMockResult.new(
      [{ "id" => "1", "created_at" => "2026-04-02 00:00:00+00" }],
      ["id", "created_at"],
    )
    mock = StreamMockConnection.new("INSERT" => insert_result)
    result = GoldLapel.stream_add(mock, "events", { task: "email" }, patterns: STREAM_PATTERNS)

    assert_equal 1, result["id"]
    assert_equal({ task: "email" }, result["payload"])
    assert_equal "2026-04-02 00:00:00+00", result["created_at"]

    # No in-wrapper CREATE TABLE — proxy owns DDL.
    assert_nil mock.calls.find { |c| c[:sql].include?("CREATE TABLE") }

    insert_call = mock.calls.find { |c| c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_equal [JSON.generate({ task: "email" })], insert_call[:params]
  end

  def test_raises_without_patterns
    mock = StreamMockConnection.new
    assert_raises(RuntimeError, /requires DDL patterns/) do
      GoldLapel.stream_add(mock, "events", { task: "email" })
    end
  end
end

class TestStreamCreateGroup < Minitest::Test
  def test_issues_create_group_pattern
    mock = StreamMockConnection.new
    GoldLapel.stream_create_group(mock, "events", "workers", patterns: STREAM_PATTERNS)

    # No in-wrapper CREATE TABLE statements.
    assert_nil mock.calls.find { |c| c[:sql].include?("CREATE TABLE") }

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_equal ["workers"], insert_call[:params]
  end
end

class TestStreamRead < Minitest::Test
  def test_reads_messages_and_tracks_pending
    cursor_result = StreamMockResult.new(
      [{ "last_delivered_id" => "0" }], ["last_delivered_id"],
    )
    read_result = StreamMockResult.new(
      [{ "id" => "5", "payload" => '{"x":1}', "created_at" => "2026-04-02 00:00:00+00" }],
      ["id", "payload", "created_at"],
    )
    mock = StreamMockConnection.new(
      "SELECT last_delivered_id" => cursor_result,
      "SELECT id, payload, created_at" => read_result,
    )
    messages = GoldLapel.stream_read(mock, "events", "workers", "c1", count: 1, patterns: STREAM_PATTERNS)

    assert_equal 1, messages.length
    assert_equal 5, messages[0]["id"]
    assert_equal({ "x" => 1 }, messages[0]["payload"])

    pending_insert = mock.calls.select { |c| c[:method] == :exec_params && c[:sql].include?("_pending") }
    assert_equal 1, pending_insert.length
    assert_equal [5, "workers", "c1"], pending_insert[0][:params]
  end

  def test_returns_empty_when_cursor_missing
    mock = StreamMockConnection.new # cursor lookup returns empty default
    messages = GoldLapel.stream_read(mock, "events", "workers", "c1", patterns: STREAM_PATTERNS)
    assert_empty messages
  end
end

class TestStreamAck < Minitest::Test
  def test_ack_returns_true_on_delete
    del_result = StreamMockResult.new(
      [{ "dummy" => "1" }], ["dummy"],
    )
    mock = StreamMockConnection.new("DELETE" => del_result)
    assert GoldLapel.stream_ack(mock, "events", "workers", 5, patterns: STREAM_PATTERNS)
  end

  def test_ack_returns_false_when_not_found
    mock = StreamMockConnection.new
    refute GoldLapel.stream_ack(mock, "events", "workers", 999, patterns: STREAM_PATTERNS)
  end
end

class TestStreamClaim < Minitest::Test
  def test_claims_idle_messages
    update_result = StreamMockResult.new(
      [{ "message_id" => "5" }], ["message_id"],
    )
    select_result = StreamMockResult.new(
      [{ "id" => "5", "payload" => '{"y":2}', "created_at" => "2026-04-02 00:00:00+00" }],
      ["id", "payload", "created_at"],
    )
    mock = StreamMockConnection.new(
      "UPDATE" => update_result,
      "SELECT id, payload" => select_result,
    )
    messages = GoldLapel.stream_claim(mock, "events", "workers", "c2", min_idle_ms: 30000, patterns: STREAM_PATTERNS)

    assert_equal 1, messages.length
    assert_equal 5, messages[0]["id"]
    assert_equal({ "y" => 2 }, messages[0]["payload"])

    update_call = mock.calls.find { |c| c[:sql].include?("UPDATE") }
    assert_equal ["c2", "workers", 30000], update_call[:params]
  end

  def test_returns_empty_when_nothing_idle
    mock = StreamMockConnection.new
    messages = GoldLapel.stream_claim(mock, "events", "workers", "c2", patterns: STREAM_PATTERNS)
    assert_empty messages
  end
end
