# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  # Hash namespace API — `gl.hashes.<verb>(...)`.
  #
  # Phase 5 of schema-to-core. The proxy's v1 hash schema is row-per-field
  # (`hash_key`, `field`, `value`) — NOT the legacy JSONB-blob-per-key shape.
  # Every method threads `hash_key` as the first positional arg after the
  # namespace `name`. `value` is JSON-encoded so callers can store arbitrary
  # structured payloads.
  class HashesAPI
    def initialize(gl)
      @gl = gl
    end

    def _patterns(name)
      GoldLapel._validate_identifier(name)
      proxy = @gl.instance_variable_get(:@proxy)
      token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = proxy&.dashboard_port
      GoldLapel::DDL.fetch_patterns(@gl, "hash", name, port, token)
    end

    def create(name)
      _patterns(name)
      nil
    end

    def set(name, hash_key, field, value, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_set(@gl.send(:_resolve_conn, conn), name, hash_key, field, value, patterns: patterns)
    end

    def get(name, hash_key, field, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_get(@gl.send(:_resolve_conn, conn), name, hash_key, field, patterns: patterns)
    end

    def get_all(name, hash_key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_get_all(@gl.send(:_resolve_conn, conn), name, hash_key, patterns: patterns)
    end

    def keys(name, hash_key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_keys(@gl.send(:_resolve_conn, conn), name, hash_key, patterns: patterns)
    end

    def values(name, hash_key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_values(@gl.send(:_resolve_conn, conn), name, hash_key, patterns: patterns)
    end

    def exists(name, hash_key, field, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_exists(@gl.send(:_resolve_conn, conn), name, hash_key, field, patterns: patterns)
    end

    def delete(name, hash_key, field, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_delete(@gl.send(:_resolve_conn, conn), name, hash_key, field, patterns: patterns)
    end

    def len(name, hash_key, conn: nil)
      patterns = _patterns(name)
      GoldLapel.hash_len(@gl.send(:_resolve_conn, conn), name, hash_key, patterns: patterns)
    end
  end
end
