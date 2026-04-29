# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  # Counters namespace API — `gl.counters.<verb>(...)`.
  #
  # Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:
  #
  #   1. Calls /api/ddl/counter/create (idempotent) to materialize the
  #      canonical `_goldlapel.counter_<name>` table and pull its query
  #      patterns.
  #   2. Caches `(tables, query_patterns)` on the parent GoldLapel instance
  #      for the session's lifetime (one HTTP round-trip per (family, name)).
  #   3. Hands the patterns off to `GoldLapel.counter_*` helpers, which
  #      execute against the canonical table name.
  #
  # Mirrors `GoldLapel::DocumentsAPI` exactly — the canonical schema-to-core
  # sub-API shape.
  class CountersAPI
    def initialize(gl)
      @gl = gl
    end

    def _patterns(name)
      GoldLapel._validate_identifier(name)
      proxy = @gl.instance_variable_get(:@proxy)
      token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = proxy&.dashboard_port
      GoldLapel::DDL.fetch_patterns(@gl, "counter", name, port, token)
    end

    # -- Lifecycle -----------------------------------------------------------

    # Eagerly materialize the counter table. Other methods will also
    # materialize on first use, so calling this is optional — provided for
    # callers that want explicit setup at startup time.
    def create(name)
      _patterns(name)
      nil
    end

    # -- Per-key ops ---------------------------------------------------------

    def incr(name, key, amount = 1, conn: nil)
      patterns = _patterns(name)
      GoldLapel.counter_incr(@gl.send(:_resolve_conn, conn), name, key, amount, patterns: patterns)
    end

    def decr(name, key, amount = 1, conn: nil)
      patterns = _patterns(name)
      GoldLapel.counter_decr(@gl.send(:_resolve_conn, conn), name, key, amount, patterns: patterns)
    end

    def set(name, key, value, conn: nil)
      patterns = _patterns(name)
      GoldLapel.counter_set(@gl.send(:_resolve_conn, conn), name, key, value, patterns: patterns)
    end

    def get(name, key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.counter_get(@gl.send(:_resolve_conn, conn), name, key, patterns: patterns)
    end

    def delete(name, key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.counter_delete(@gl.send(:_resolve_conn, conn), name, key, patterns: patterns)
    end

    def count_keys(name, conn: nil)
      patterns = _patterns(name)
      GoldLapel.counter_count_keys(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
    end
  end
end
