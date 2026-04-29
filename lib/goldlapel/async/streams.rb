# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  module Async
    # Async sibling of `GoldLapel::StreamsAPI`. Same shape (verb methods,
    # back-reference to the parent client, shared DDL pattern cache) — every
    # call routes through `GoldLapel::Async::Utils.stream_*` instead of the
    # sync `GoldLapel.stream_*`. See goldlapel/streams.rb for the rationale.
    class StreamsAPI
      def initialize(gl)
        @gl = gl
      end

      def _patterns(stream)
        GoldLapel._validate_identifier(stream)
        proxy = @gl.instance_variable_get(:@proxy)
        token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
        port = proxy&.dashboard_port
        GoldLapel::DDL.fetch_patterns(@gl, "stream", stream, port, token)
      end

      def add(stream, payload, conn: nil)
        patterns = _patterns(stream)
        Utils.stream_add(@gl.send(:_resolve_conn, conn), stream, payload, patterns: patterns)
      end

      def create_group(stream, group, conn: nil)
        patterns = _patterns(stream)
        Utils.stream_create_group(@gl.send(:_resolve_conn, conn), stream, group, patterns: patterns)
      end

      def read(stream, group, consumer, count: 1, conn: nil)
        patterns = _patterns(stream)
        Utils.stream_read(@gl.send(:_resolve_conn, conn), stream, group, consumer, count: count, patterns: patterns)
      end

      def ack(stream, group, message_id, conn: nil)
        patterns = _patterns(stream)
        Utils.stream_ack(@gl.send(:_resolve_conn, conn), stream, group, message_id, patterns: patterns)
      end

      def claim(stream, group, consumer, min_idle_ms: 60000, conn: nil)
        patterns = _patterns(stream)
        Utils.stream_claim(
          @gl.send(:_resolve_conn, conn), stream, group, consumer,
          min_idle_ms: min_idle_ms, patterns: patterns,
        )
      end
    end
  end
end
