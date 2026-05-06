require "uri"
require "goldlapel"

module GoldLapel
  module Rails
    def self.build_upstream_url(params)
      host = (params[:host].nil? || params[:host].empty?) ? "localhost" : params[:host]
      port = (params[:port].nil? || params[:port].to_s.empty?) ? "5432" : params[:port].to_s

      if host.start_with?("/")
        raise ArgumentError, "Gold Lapel cannot proxy Unix socket connections (host: #{host})"
      end

      userinfo = nil
      if params[:user] && !params[:user].empty?
        userinfo = URI.encode_uri_component(params[:user])
        if params[:password] && !params[:password].empty?
          userinfo += ":#{URI.encode_uri_component(params[:password])}"
        end
      end

      dbname = params[:dbname] ? URI.encode_uri_component(params[:dbname]) : ""

      authority = userinfo ? "#{userinfo}@#{host}:#{port}" : "#{host}:#{port}"
      "postgresql://#{authority}/#{dbname}"
    end

    module PostgreSQLExtension
      # Concern 4 (RLS hardening) — AR's connection pool does NOT
      # auto-issue `DISCARD ALL` on connection checkin. With our
      # wire-observed unsafe-GUC fingerprint folded into the L1
      # cache key, that's a problem: a connection checked back in
      # while still holding `app.user_id = '42'` will, on its next
      # checkout by a different request, start with stale state
      # (wrapper sees no SET, returns hash 0; server still has
      # 42). Cache key for sh=0 may already be populated from a
      # peer connection — we'd serve those rows to a request whose
      # actual server-side `app.user_id` is 42.
      #
      # AR's `AbstractAdapter#expire` is the documented checkin
      # hook (called by `ConnectionPool#checkin`). Prepend it to
      # issue `DISCARD ALL` on the wrapped connection first. If
      # the wrapper is the goldlapel `CachedConnection` we delegate
      # to its `discard_all_on_release!` (which also resets the in-
      # process state map); otherwise we no-op (fallback path
      # where `wrap` failed and the adapter is talking to raw pg).
      #
      # The hook is also a no-op mid-transaction: AR pool checkin
      # should never happen inside a tx, but if it does (buggy app
      # code calling `release_connection` while holding a tx), we
      # don't want to abort the user's work.
      def expire
        begin
          conn = @raw_connection
          if conn.respond_to?(:discard_all_on_release!)
            conn.discard_all_on_release!
          end
        rescue StandardError => e
          # Pool checkin is critical-path; never raise into AR's
          # pool from our state-reset hook. Log and continue —
          # next checkout will get a fresh connection if this one
          # is torn down.
          if defined?(::Rails) && ::Rails.respond_to?(:logger) && ::Rails.logger
            ::Rails.logger.warn(
              "[Gold Lapel] DISCARD ALL on pool checkin failed: #{e.message}"
            )
          end
        end
        super
      end

      private

      def connect
        unless @goldlapel_started
          # database.yml `goldlapel:` block follows the canonical snake_case
          # surface: proxy_port, dashboard_port, invalidation_port, log_level,
          # mode, license, config_file, config, extra_args.
          gl_config = @config.is_a?(Hash) ? @config[:goldlapel] || {} : {}
          gl_config = gl_config.transform_keys(&:to_sym) if gl_config.is_a?(Hash)
          proxy_port_opt = gl_config[:proxy_port]
          config = gl_config[:config]
          extra_args = gl_config[:extra_args] || []
          @goldlapel_invalidation_port = gl_config[:invalidation_port]
          # Wave 3 promoted these flags out of the `config:` bag onto
          # the canonical top-level surface; surface them on the
          # railtie too so Rails users can reach them via
          # `database.yml` without dropping down to direct
          # `GoldLapel.start_proxy`. `disable_native_cache` is the
          # only flag that belongs on `wrap` (L1 lives in the
          # wrapper); the rest go to the proxy spawn.
          @goldlapel_disable_native_cache = gl_config[:disable_native_cache] ? true : false
          # Aggressive-verify (concern 6 mitigation) — :auto runs a
          # one-time pg_trigger classifier on first wrap; :on/:off
          # force the behaviour. `||` is not safe here because
          # `false` is a valid value (alias for :off); use `key?`
          # so a deliberate `false` survives.
          @goldlapel_aggressive_verify =
            if gl_config.is_a?(Hash) && gl_config.key?(:aggressive_verify)
              gl_config[:aggressive_verify]
            else
              :auto
            end
          # `nil` from YAML resolves back to :auto.
          @goldlapel_aggressive_verify = :auto if @goldlapel_aggressive_verify.nil?

          upstream = GoldLapel::Rails.build_upstream_url(@connection_parameters)
          @goldlapel_upstream = upstream

          begin
            # Rails manages its own pg connections; only spawn the proxy here.
            # (`start_proxy` is the low-level, connection-less variant of
            # `GoldLapel.start` that returns the proxy URL, not an instance.)
            GoldLapel.start_proxy(
              upstream,
              proxy_port: proxy_port_opt,
              dashboard_port: gl_config[:dashboard_port],
              invalidation_port: @goldlapel_invalidation_port,
              log_level: gl_config[:log_level],
              mode: gl_config[:mode],
              license: gl_config[:license],
              client: "rails",
              config_file: gl_config[:config_file],
              config: config,
              extra_args: extra_args,
              silent: gl_config[:silent] ? true : false,
              mesh: gl_config[:mesh] ? true : false,
              mesh_tag: gl_config[:mesh_tag],
              disable_proxy_cache: gl_config[:disable_proxy_cache] ? true : false,
              disable_matviews: gl_config[:disable_matviews] ? true : false,
              disable_sqloptimize: gl_config[:disable_sqloptimize] ? true : false,
              disable_auto_indexes: gl_config[:disable_auto_indexes] ? true : false,
            )
          rescue => e
            ::Rails.logger.warn("[Gold Lapel] Proxy failed to start: #{e.message} — falling back to direct connection")
            @goldlapel_started = true
            @goldlapel_fallback = true
            return super
          end

          proxy_port = proxy_port_opt || GoldLapel::DEFAULT_PROXY_PORT
          @goldlapel_invalidation_port ||= proxy_port + 2
          @connection_parameters[:host] = "127.0.0.1"
          @connection_parameters[:port] = proxy_port
          @goldlapel_started = true
        end

        super

        unless @goldlapel_fallback
          begin
            @raw_connection = GoldLapel.wrap(
              @raw_connection,
              invalidation_port: @goldlapel_invalidation_port,
              disable_native_cache: @goldlapel_disable_native_cache,
              aggressive_verify: @goldlapel_aggressive_verify,
              upstream: @goldlapel_upstream,
            )
          rescue => e
            ::Rails.logger.warn("[Gold Lapel] L1 cache wrap failed: #{e.message} — using unwrapped connection")
          end
        end
      end
    end

    class Railtie < ::Rails::Railtie
      initializer "goldlapel.configure" do
        ActiveSupport.on_load(:active_record) do
          require "active_record/connection_adapters/postgresql_adapter"
          ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(
            GoldLapel::Rails::PostgreSQLExtension
          )
        end
      end
    end
  end
end
