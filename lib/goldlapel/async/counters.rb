# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  module Async
    # Async sibling of `GoldLapel::CountersAPI`. Same verbs and dispatch shape;
    # routes through `GoldLapel::Async::Utils.counter_*` so the underlying SQL
    # uses pg's `async_exec_params`.
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

      def create(name)
        _patterns(name)
        nil
      end

      def incr(name, key, amount = 1, conn: nil)
        patterns = _patterns(name)
        Utils.counter_incr(@gl.send(:_resolve_conn, conn), name, key, amount, patterns: patterns)
      end

      def decr(name, key, amount = 1, conn: nil)
        patterns = _patterns(name)
        Utils.counter_decr(@gl.send(:_resolve_conn, conn), name, key, amount, patterns: patterns)
      end

      def set(name, key, value, conn: nil)
        patterns = _patterns(name)
        Utils.counter_set(@gl.send(:_resolve_conn, conn), name, key, value, patterns: patterns)
      end

      def get(name, key, conn: nil)
        patterns = _patterns(name)
        Utils.counter_get(@gl.send(:_resolve_conn, conn), name, key, patterns: patterns)
      end

      def delete(name, key, conn: nil)
        patterns = _patterns(name)
        Utils.counter_delete(@gl.send(:_resolve_conn, conn), name, key, patterns: patterns)
      end

      def count_keys(name, conn: nil)
        patterns = _patterns(name)
        Utils.counter_count_keys(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
      end
    end
  end
end
