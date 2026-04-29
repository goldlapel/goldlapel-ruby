# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  # Queue namespace API — `gl.queues.<verb>(...)`.
  #
  # Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
  # with visibility-timeout — NOT the legacy fire-and-forget shape. The
  # breaking change:
  #
  #   Before:  payload = gl.dequeue("jobs")        # delete-on-fetch, may lose work
  #   After :  msg = gl.queues.claim("jobs")       # lease the row
  #            id_, payload = msg                  # unpack
  #            # ... handle the work ...
  #            gl.queues.ack("jobs", id_)          # commit; missing ack → redelivery
  #
  # `claim` returns `[id, payload]` or `nil`. The caller MUST `ack(id)` to
  # commit, or `abandon(id)` to release the lease immediately. A consumer
  # that crashes leaves the lease standing; the message becomes ready again
  # after `visibility_timeout_ms` and is redelivered to the next claim.
  class QueuesAPI
    def initialize(gl)
      @gl = gl
    end

    def _patterns(name)
      GoldLapel._validate_identifier(name)
      proxy = @gl.instance_variable_get(:@proxy)
      token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = proxy&.dashboard_port
      GoldLapel::DDL.fetch_patterns(@gl, "queue", name, port, token)
    end

    def create(name)
      _patterns(name)
      nil
    end

    def enqueue(name, payload, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_enqueue(@gl.send(:_resolve_conn, conn), name, payload, patterns: patterns)
    end

    # Claim the next ready message; returns `[id, payload]` or `nil`.
    def claim(name, visibility_timeout_ms: 30000, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_claim(
        @gl.send(:_resolve_conn, conn), name,
        visibility_timeout_ms: visibility_timeout_ms, patterns: patterns,
      )
    end

    def ack(name, message_id, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_ack(@gl.send(:_resolve_conn, conn), name, message_id, patterns: patterns)
    end

    # Release a claim immediately so the message is redelivered without
    # waiting for the visibility timeout. Equivalent to a queue NACK.
    def abandon(name, message_id, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_abandon(@gl.send(:_resolve_conn, conn), name, message_id, patterns: patterns)
    end

    # Push the visibility deadline forward by `additional_ms`.
    def extend(name, message_id, additional_ms, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_extend(@gl.send(:_resolve_conn, conn), name, message_id, additional_ms, patterns: patterns)
    end

    # Look at the next-ready message without claiming.
    def peek(name, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_peek(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
    end

    def count_ready(name, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_count_ready(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
    end

    def count_claimed(name, conn: nil)
      patterns = _patterns(name)
      GoldLapel.queue_count_claimed(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
    end
  end
end
