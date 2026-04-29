# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  module Async
    # Async sibling of `GoldLapel::GeosAPI`. GEOGRAPHY-native; idempotent
    # `add` on member name.
    class GeosAPI
      def initialize(gl)
        @gl = gl
      end

      def _patterns(name)
        GoldLapel._validate_identifier(name)
        proxy = @gl.instance_variable_get(:@proxy)
        token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
        port = proxy&.dashboard_port
        GoldLapel::DDL.fetch_patterns(@gl, "geo", name, port, token)
      end

      def create(name)
        _patterns(name)
        nil
      end

      def add(name, member, lon, lat, conn: nil)
        patterns = _patterns(name)
        Utils.geo_add(@gl.send(:_resolve_conn, conn), name, member, lon, lat, patterns: patterns)
      end

      def pos(name, member, conn: nil)
        patterns = _patterns(name)
        Utils.geo_pos(@gl.send(:_resolve_conn, conn), name, member, patterns: patterns)
      end

      def dist(name, member_a, member_b, unit: "m", conn: nil)
        patterns = _patterns(name)
        Utils.geo_dist(@gl.send(:_resolve_conn, conn), name, member_a, member_b, unit: unit, patterns: patterns)
      end

      def radius(name, lon, lat, radius, unit: "m", limit: 50, conn: nil)
        patterns = _patterns(name)
        Utils.geo_radius(
          @gl.send(:_resolve_conn, conn), name, lon, lat, radius,
          unit: unit, limit: limit, patterns: patterns,
        )
      end

      def radius_by_member(name, member, radius, unit: "m", limit: 50, conn: nil)
        patterns = _patterns(name)
        Utils.geo_radius_by_member(
          @gl.send(:_resolve_conn, conn), name, member, radius,
          unit: unit, limit: limit, patterns: patterns,
        )
      end

      def remove(name, member, conn: nil)
        patterns = _patterns(name)
        Utils.geo_remove(@gl.send(:_resolve_conn, conn), name, member, patterns: patterns)
      end

      def count(name, conn: nil)
        patterns = _patterns(name)
        Utils.geo_count(@gl.send(:_resolve_conn, conn), name, patterns: patterns)
      end
    end
  end
end
