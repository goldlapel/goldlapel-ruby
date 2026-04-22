# frozen_string_literal: true

# DDL API client — fetches canonical helper-table DDL + query patterns from
# the Rust proxy's dashboard port so the wrapper never hand-writes CREATE TABLE
# for helper families (streams, docs, counters, ...).
#
# Architecture: see docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md in the goldlapel repo.
#
# - One HTTP call per (family, name) per session (cached).
# - Cache key: (family, name). Value: { tables:, query_patterns: }.
# - Cache lives in an instance-variable on the owner (typically the Instance).
# - Errors: HTTP failures raise RuntimeError with actionable text.

require "json"
require "net/http"
require "uri"

module GoldLapel
  module DDL
    SUPPORTED_VERSIONS = {
      "stream" => "v1",
    }.freeze

    class << self
      def supported_version(family)
        SUPPORTED_VERSIONS.fetch(family)
      end

      # Priority: GOLDLAPEL_DASHBOARD_TOKEN env > ~/.goldlapel/dashboard-token file.
      def token_from_env_or_file
        env = ENV["GOLDLAPEL_DASHBOARD_TOKEN"]
        return env.strip if env && !env.strip.empty?

        path = File.join(Dir.home, ".goldlapel", "dashboard-token")
        if File.exist?(path)
          begin
            text = File.read(path, encoding: "UTF-8").strip
            return text unless text.empty?
          rescue StandardError
            # fall through
          end
        end
        nil
      end

      def fetch(owner, family, name, dashboard_port, dashboard_token)
        cache = _cache_for(owner)
        key = [family, name]
        return cache[key] if cache.key?(key)

        if dashboard_token.nil? || dashboard_token.to_s.empty?
          raise RuntimeError,
            "No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let " \
            "GoldLapel.start spawn the proxy (which provisions a token automatically)."
        end
        if dashboard_port.nil? || dashboard_port.to_i <= 0
          raise RuntimeError,
            "No dashboard port available. Gold Lapel's helper families (#{family}, ...) " \
            "require the proxy's dashboard to be reachable."
        end

        url = URI.parse("http://127.0.0.1:#{dashboard_port.to_i}/api/ddl/#{family}/create")
        body_hash = { "name" => name, "schema_version" => supported_version(family) }
        status, body = _post(url, dashboard_token, body_hash)

        unless status == 200
          error = body.is_a?(Hash) ? (body["error"] || "unknown") : "unknown"
          detail = body.is_a?(Hash) ? (body["detail"] || body.inspect) : body.to_s
          if status == 409 && error == "version_mismatch"
            raise RuntimeError,
              "Gold Lapel schema version mismatch for #{family} '#{name}': #{detail}. " \
              "Upgrade the proxy or the wrapper so versions agree."
          end
          if status == 403
            raise RuntimeError,
              "Gold Lapel dashboard rejected the DDL request (403). " \
              "The dashboard token is missing or incorrect — check " \
              "GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token."
          end
          raise RuntimeError,
            "Gold Lapel DDL API #{family}/#{name} failed with #{status} #{error}: #{detail}"
        end

        entry = {
          tables: body["tables"],
          query_patterns: body["query_patterns"],
        }
        cache[key] = entry
        entry
      end

      def invalidate(owner)
        if owner.instance_variables.include?(:@_gl_ddl_cache)
          owner.instance_variable_set(:@_gl_ddl_cache, nil)
        end
      end

      # -- internals (exposed for test spies) --

      def _cache_for(owner)
        cache = owner.instance_variable_get(:@_gl_ddl_cache)
        if cache.nil?
          cache = {}
          owner.instance_variable_set(:@_gl_ddl_cache, cache)
        end
        cache
      end

      # Swappable HTTP layer — tests replace this with a counting spy.
      def _post(url, token, body_hash)
        req = Net::HTTP::Post.new(url.path)
        req["Content-Type"] = "application/json"
        req["X-GL-Dashboard"] = token
        req.body = JSON.generate(body_hash)

        begin
          resp = Net::HTTP.start(url.host, url.port, open_timeout: 5, read_timeout: 10) do |http|
            http.request(req)
          end
        rescue StandardError => e
          raise RuntimeError,
            "Gold Lapel dashboard not reachable at #{url}: #{e.message}. " \
            "Is `goldlapel` running? The dashboard port must be open for " \
            "helper families (streams, docs, ...) to work."
        end

        text = resp.body.to_s
        parsed =
          begin
            text.empty? ? {} : JSON.parse(text)
          rescue JSON::ParserError
            { "_raw" => text }
          end
        [resp.code.to_i, parsed]
      end
    end
  end
end
