# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  # Sorted-set (zset) namespace API — `gl.zsets.<verb>(...)`.
  #
  # Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
  # `zset_key` column so a single namespace table holds many sorted sets —
  # matching Redis's mental model. Every method below threads `zset_key` as
  # the first positional arg after the namespace `name`.
  class ZsetsAPI
    def initialize(gl)
      @gl = gl
    end

    def _patterns(name)
      GoldLapel._validate_identifier(name)
      proxy = @gl.instance_variable_get(:@proxy)
      token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = proxy&.dashboard_port
      GoldLapel::DDL.fetch_patterns(@gl, "zset", name, port, token)
    end

    def create(name)
      _patterns(name)
      nil
    end

    def add(name, zset_key, member, score, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_add(@gl.send(:_resolve_conn, conn), name, zset_key, member, score, patterns: patterns)
    end

    def incr_by(name, zset_key, member, delta = 1, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_incr_by(@gl.send(:_resolve_conn, conn), name, zset_key, member, delta, patterns: patterns)
    end

    def score(name, zset_key, member, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_score(@gl.send(:_resolve_conn, conn), name, zset_key, member, patterns: patterns)
    end

    def rank(name, zset_key, member, desc: true, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_rank(@gl.send(:_resolve_conn, conn), name, zset_key, member, desc: desc, patterns: patterns)
    end

    # Members by rank within `zset_key`. Inclusive `start`/`stop` Redis-style;
    # `stop = -1` is a sentinel meaning "to the end" — we map it to a large
    # limit (9999) since the proxy's pattern is LIMIT/OFFSET-based. Callers
    # wanting the entire set should page explicitly via `range_by_score`.
    def range(name, zset_key, start: 0, stop: -1, desc: true, conn: nil)
      stop = 9999 if stop.nil? || stop == -1
      patterns = _patterns(name)
      GoldLapel.zset_range(
        @gl.send(:_resolve_conn, conn), name, zset_key, start, stop, desc,
        patterns: patterns,
      )
    end

    def range_by_score(name, zset_key, min_score, max_score, limit: 100, offset: 0, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_range_by_score(
        @gl.send(:_resolve_conn, conn), name, zset_key, min_score, max_score,
        limit: limit, offset: offset, patterns: patterns,
      )
    end

    def remove(name, zset_key, member, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_remove(@gl.send(:_resolve_conn, conn), name, zset_key, member, patterns: patterns)
    end

    def card(name, zset_key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.zset_card(@gl.send(:_resolve_conn, conn), name, zset_key, patterns: patterns)
    end
  end
end
