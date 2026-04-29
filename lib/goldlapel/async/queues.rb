# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  module Async
    # Async sibling of `GoldLapel::QueuesAPI`. At-least-once with visibility
    # timeout — explicit `claim` + `ack` (no `dequeue` shim).
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
        Utils.queue_enqueue(@gl.send(:_resolve_conn, conn), name, payload, patterns: patterns)
      end

      def claim(name, visibility_timeout_ms: 30000, conn: nil)
        patterns = _patterns(name)
        Utils.queue_claim(
          @gl.send(:_resolve_conn, conn), name,
          visibility_timeout_ms: visibility_timeout_ms, patterns: patterns,
        )
      end

      def ack(name, message_id, conn: nil)
        patterns = _patterns(name)
        Utils.queue_ack(@gl.send(:_resolve_conn, conn), name, message_id, patterns: patterns)
      end

      def abandon(name, message_id, conn: nil)
        patterns = _patterns(name)
        Utils.queue_abandon(@gl.send(:_resolve_conn, conn), name, message_id, patterns: patterns)
      end

      def extend(name, message_id, additional_ms, conn: nil)
        patterns = _patterns(name)
        Utils.queue_extend(@gl.send(:_resolve_conn, conn), name, message_id, additional_ms, patterns: patterns)
      end

      def peek(name, conn: nil)
        patterns = _patterns(name)
        Utils.queue_peek(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
      end

      def count_ready(name, conn: nil)
        patterns = _patterns(name)
        Utils.queue_count_ready(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
      end

      def count_claimed(name, conn: nil)
        patterns = _patterns(name)
        Utils.queue_count_claimed(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
      end
    end
  end
end
