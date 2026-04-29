# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  module Async
    # Async sibling of `GoldLapel::ZsetsAPI`. Threads `zset_key` first as the
    # canonical Phase 5 contract.
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
        Utils.zset_add(@gl.send(:_resolve_conn, conn), name, zset_key, member, score, patterns: patterns)
      end

      def incr_by(name, zset_key, member, delta = 1, conn: nil)
        patterns = _patterns(name)
        Utils.zset_incr_by(@gl.send(:_resolve_conn, conn), name, zset_key, member, delta, patterns: patterns)
      end

      def score(name, zset_key, member, conn: nil)
        patterns = _patterns(name)
        Utils.zset_score(@gl.send(:_resolve_conn, conn), name, zset_key, member, patterns: patterns)
      end

      def rank(name, zset_key, member, desc: true, conn: nil)
        patterns = _patterns(name)
        Utils.zset_rank(@gl.send(:_resolve_conn, conn), name, zset_key, member, desc: desc, patterns: patterns)
      end

      def range(name, zset_key, start: 0, stop: -1, desc: true, conn: nil)
        stop = 9999 if stop.nil? || stop == -1
        patterns = _patterns(name)
        Utils.zset_range(
          @gl.send(:_resolve_conn, conn), name, zset_key, start, stop, desc,
          patterns: patterns,
        )
      end

      def range_by_score(name, zset_key, min_score, max_score, limit: 100, offset: 0, conn: nil)
        patterns = _patterns(name)
        Utils.zset_range_by_score(
          @gl.send(:_resolve_conn, conn), name, zset_key, min_score, max_score,
          limit: limit, offset: offset, patterns: patterns,
        )
      end

      def remove(name, zset_key, member, conn: nil)
        patterns = _patterns(name)
        Utils.zset_remove(@gl.send(:_resolve_conn, conn), name, zset_key, member, patterns: patterns)
      end

      def card(name, zset_key, conn: nil)
        patterns = _patterns(name)
        Utils.zset_card(@gl.send(:_resolve_conn, conn), name, zset_key, patterns: patterns)
      end
    end
  end
end
