# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  # Streams namespace API — `gl.streams.<verb>(...)`.
  #
  # Wraps the wire-level stream methods in a sub-API instance held on the
  # parent GoldLapel client. The instance shares all state (license, dashboard
  # token, http session, conn) by reference back to the parent — no
  # duplication.
  #
  # This is the canonical sub-API shape for the schema-to-core wrapper
  # rollout. Other namespaces (cache, search, queues, counters, hashes,
  # zsets, geo, auth, ...) stay flat for now; they migrate to nested form
  # one-at-a-time as their own schema-to-core phase fires.
  class StreamsAPI
    # Hold a back-reference to the parent client. We never copy lifecycle
    # state (token, port, conn) onto this instance — always read through
    # `@gl` so a config change on the parent (e.g. proxy restart with a new
    # dashboard token) is reflected immediately on the next call.
    def initialize(gl)
      @gl = gl
    end

    # Fetch (and cache) canonical stream DDL + query patterns from the
    # proxy. Cache lives on the parent GoldLapel instance — see ddl.rb.
    def _patterns(stream)
      GoldLapel._validate_identifier(stream)
      proxy = @gl.instance_variable_get(:@proxy)
      token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = proxy&.dashboard_port
      # Cache owner is the parent client so describe-once-per-session works
      # even if the user holds onto a `gl.streams` reference across calls.
      GoldLapel::DDL.fetch_patterns(@gl, "stream", stream, port, token)
    end

    def add(stream, payload, conn: nil)
      patterns = _patterns(stream)
      GoldLapel.stream_add(@gl.send(:_resolve_conn, conn), stream, payload, patterns: patterns)
    end

    def create_group(stream, group, conn: nil)
      patterns = _patterns(stream)
      GoldLapel.stream_create_group(@gl.send(:_resolve_conn, conn), stream, group, patterns: patterns)
    end

    def read(stream, group, consumer, count: 1, conn: nil)
      patterns = _patterns(stream)
      GoldLapel.stream_read(@gl.send(:_resolve_conn, conn), stream, group, consumer, count: count, patterns: patterns)
    end

    def ack(stream, group, message_id, conn: nil)
      patterns = _patterns(stream)
      GoldLapel.stream_ack(@gl.send(:_resolve_conn, conn), stream, group, message_id, patterns: patterns)
    end

    def claim(stream, group, consumer, min_idle_ms: 60000, conn: nil)
      patterns = _patterns(stream)
      GoldLapel.stream_claim(
        @gl.send(:_resolve_conn, conn), stream, group, consumer,
        min_idle_ms: min_idle_ms, patterns: patterns,
      )
    end
  end
end
