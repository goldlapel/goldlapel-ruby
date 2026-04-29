# frozen_string_literal: true

# Unit tests for GoldLapel::QueuesAPI.
#
# Phase 5 introduces at-least-once delivery with visibility timeout. The
# breaking change is `dequeue` (delete-on-fetch) → `claim` (lease + ack).
# These tests verify:
#
#   - `enqueue` returns the assigned id from the proxy's RETURNING clause.
#   - `claim` returns `[id, payload]` or `nil` — explicit tuple shape.
#   - `ack` is a separate call, NOT bundled into claim.
#   - `abandon` releases the claim immediately (NACK).
#   - `extend` pushes the visibility deadline.
#   - No `dequeue` shim exists — that was rejected in the master plan.

require "minitest/autorun"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/streams"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/counters"
require_relative "../lib/goldlapel/zsets"
require_relative "../lib/goldlapel/hashes"
require_relative "../lib/goldlapel/queues"
require_relative "../lib/goldlapel/geos"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel"

class QueuesApiMockResult
  attr_reader :values, :fields
  def initialize(rows = [], fields = [])
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end
  def ntuples; @rows.length; end
  def cmd_tuples; @cmd_tuples || @rows.length; end
  def cmd_tuples=(v); @cmd_tuples = v; end
  def [](i); @rows[i]; end
  def map(&b); @rows.map(&b); end
  def each(&b); @rows.each(&b); end
end

class QueuesApiMockConn
  attr_reader :calls
  attr_accessor :next_result

  def initialize
    @calls = []
    @next_result = QueuesApiMockResult.new
  end

  def exec(sql, &b)
    @calls << { method: :exec, sql: sql }
    @next_result.tap { |r| b&.call(r) }
  end

  def exec_params(sql, params = [], _f = 0, &b)
    @calls << { method: :exec_params, sql: sql, params: params }
    @next_result.tap { |r| b&.call(r) }
  end

  def close; end
  def finished?; false; end
end

QUEUE_MAIN = "_goldlapel.queue_jobs"
FAKE_QUEUE_PATTERNS = {
  tables: { "main" => QUEUE_MAIN },
  query_patterns: {
    "enqueue" => "INSERT INTO #{QUEUE_MAIN} (payload) VALUES ($1::jsonb) RETURNING id, created_at",
    "claim" => "WITH next_msg AS ( SELECT id FROM #{QUEUE_MAIN} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1 ) UPDATE #{QUEUE_MAIN} SET status = 'claimed', visible_at = NOW() + INTERVAL '1 millisecond' * $1 FROM next_msg WHERE #{QUEUE_MAIN}.id = next_msg.id RETURNING #{QUEUE_MAIN}.id, #{QUEUE_MAIN}.payload, #{QUEUE_MAIN}.visible_at, #{QUEUE_MAIN}.created_at",
    "ack" => "DELETE FROM #{QUEUE_MAIN} WHERE id = $1",
    "extend" => "UPDATE #{QUEUE_MAIN} SET visible_at = visible_at + INTERVAL '1 millisecond' * $2 WHERE id = $1 AND status = 'claimed' RETURNING visible_at",
    "nack" => "UPDATE #{QUEUE_MAIN} SET status = 'ready', visible_at = NOW() WHERE id = $1 AND status = 'claimed' RETURNING id",
    "peek" => "SELECT id, payload, visible_at, status, created_at FROM #{QUEUE_MAIN} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id LIMIT 1",
    "count_ready" => "SELECT COUNT(*) FROM #{QUEUE_MAIN} WHERE status = 'ready' AND visible_at <= NOW()",
    "count_claimed" => "SELECT COUNT(*) FROM #{QUEUE_MAIN} WHERE status = 'claimed'",
    "delete_all" => "DELETE FROM #{QUEUE_MAIN}",
  },
}.freeze

def make_queues_api_inst
  conn = QueuesApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  queues = GoldLapel::QueuesAPI.new(inst)
  inst.instance_variable_set(:@queues, queues)
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst.instance_variable_set(:@counters, GoldLapel::CountersAPI.new(inst))
  inst.instance_variable_set(:@zsets, GoldLapel::ZsetsAPI.new(inst))
  inst.instance_variable_set(:@hashes, GoldLapel::HashesAPI.new(inst))
  inst.instance_variable_set(:@geos, GoldLapel::GeosAPI.new(inst))
  fetches = []
  queues.define_singleton_method(:_patterns) do |name|
    fetches << name
    FAKE_QUEUE_PATTERNS
  end
  [inst, conn, fetches]
end

class TestQueuesAPINamespaceShape < Minitest::Test
  def test_queues_is_a_QueuesAPI
    inst, _conn, _fetches = make_queues_api_inst
    assert_kind_of GoldLapel::QueuesAPI, inst.queues
  end

  def test_no_legacy_flat_methods
    # Phase 5 hard cut — `enqueue` / `dequeue` are gone. Use `claim`/`ack`.
    inst, _conn, _fetches = make_queues_api_inst
    %i[enqueue dequeue].each do |legacy|
      refute inst.respond_to?(legacy),
        "Phase 5 removed flat #{legacy} — use gl.queues.<verb>."
    end
  end

  def test_no_dequeue_alias_on_queues_api
    # The dispatcher considered shipping a `dequeue` compat shim that
    # combined claim+ack. The master plan rejected that — there must be
    # NO compat alias here. claim+ack must remain explicit.
    inst, _conn, _fetches = make_queues_api_inst
    refute inst.queues.respond_to?(:dequeue),
      "Phase 5 forbids a dequeue alias — claim+ack is explicit by design."
  end
end

class TestQueuesAPIVerbDispatch < Minitest::Test
  def test_enqueue_returns_id_from_proxy
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new(
      [{ "id" => "99", "created_at" => "2026-04-30" }],
      ["id", "created_at"]
    )
    assert_equal 99, inst.queues.enqueue("jobs", { "x" => 1 })
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ['{"x":1}'], call[:params]
  end

  def test_claim_returns_tuple_id_and_payload
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new(
      [{ "id" => "7", "payload" => '{"x":1}', "visible_at" => "v", "created_at" => "c" }],
      ["id", "payload", "visible_at", "created_at"]
    )
    result = inst.queues.claim("jobs")
    assert_equal [7, { "x" => 1 }], result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [30000], call[:params]
  end

  def test_claim_passes_visibility_timeout
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new([], ["id", "payload", "visible_at", "created_at"])
    inst.queues.claim("jobs", visibility_timeout_ms: 60000)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [60000], call[:params]
  end

  def test_claim_returns_nil_when_empty
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new([], ["id", "payload", "visible_at", "created_at"])
    assert_nil inst.queues.claim("jobs")
  end

  def test_ack_returns_true_when_deleted
    inst, conn, _fetches = make_queues_api_inst
    res = QueuesApiMockResult.new([], [])
    res.cmd_tuples = 1
    conn.next_result = res
    assert_equal true, inst.queues.ack("jobs", 42)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [42], call[:params]
  end

  def test_ack_returns_false_when_id_unknown
    inst, conn, _fetches = make_queues_api_inst
    res = QueuesApiMockResult.new([], [])
    res.cmd_tuples = 0
    conn.next_result = res
    assert_equal false, inst.queues.ack("jobs", 999)
  end

  def test_abandon_uses_nack_pattern
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new([{ "id" => "42" }], ["id"])
    assert_equal true, inst.queues.abandon("jobs", 42)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "status = 'ready'"
    assert_equal [42], call[:params]
  end

  def test_extend_passes_id_then_ms_in_index_order
    # Pattern: $1=id, $2=ms. With native $N binding (pg's exec_params),
    # params are indexed by $N-1 — `[id, ms]` slots into `[$1, $2]`.
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new([{ "visible_at" => "2026-05-01T00:00" }], ["visible_at"])
    result = inst.queues.extend("jobs", 42, 5000)
    assert_equal "2026-05-01T00:00", result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [42, 5000], call[:params]
  end

  def test_peek_returns_dict_or_nil
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new(
      [{ "id" => "42", "payload" => '{"work":"foo"}', "visible_at" => "vat", "status" => "ready", "created_at" => "cat" }],
      ["id", "payload", "visible_at", "status", "created_at"]
    )
    result = inst.queues.peek("jobs")
    assert_equal({
      "id" => 42,
      "payload" => { "work" => "foo" },
      "visible_at" => "vat",
      "status" => "ready",
      "created_at" => "cat",
    }, result)
  end

  def test_count_ready_and_claimed
    inst, conn, _fetches = make_queues_api_inst
    conn.next_result = QueuesApiMockResult.new([{ "count" => "3" }], ["count"])
    assert_equal 3, inst.queues.count_ready("jobs")
    assert_equal 3, inst.queues.count_claimed("jobs")
  end
end

class TestQueuesPhase5Contract < Minitest::Test
  # Phase 5 introduces at-least-once with visibility timeout. The breaking
  # change is `claim` + `ack` instead of `dequeue` (which deleted on fetch).
  def test_claim_pattern_does_not_delete
    sql = FAKE_QUEUE_PATTERNS[:query_patterns]["claim"]
    refute_includes sql.upcase, "DELETE"
    assert_includes sql, "status = 'claimed'"
  end

  def test_ack_pattern_deletes_by_id
    sql = FAKE_QUEUE_PATTERNS[:query_patterns]["ack"]
    assert_includes sql, "DELETE FROM #{QUEUE_MAIN} WHERE id = $1"
  end

  def test_claim_uses_visibility_timeout
    sql = FAKE_QUEUE_PATTERNS[:query_patterns]["claim"]
    assert_includes sql, "INTERVAL '1 millisecond' * $1"
  end
end
