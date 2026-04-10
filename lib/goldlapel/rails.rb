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
      private

      def connect
        unless @goldlapel_started
          gl_config = @config.is_a?(Hash) ? @config[:goldlapel] || {} : {}
          gl_config = gl_config.transform_keys(&:to_sym) if gl_config.is_a?(Hash)
          port = gl_config[:port]
          config = gl_config[:config]
          extra_args = gl_config[:extra_args] || []
          @goldlapel_invalidation_port = gl_config[:invalidation_port]

          upstream = GoldLapel::Rails.build_upstream_url(@connection_parameters)

          begin
            ENV["GOLDLAPEL_CLIENT"] = "rails"
            GoldLapel.start(upstream, config: config, port: port, extra_args: extra_args)
          rescue => e
            ::Rails.logger.warn("[Gold Lapel] Proxy failed to start: #{e.message} — falling back to direct connection")
            @goldlapel_started = true
            @goldlapel_fallback = true
            return super
          end

          proxy_port = port || GoldLapel::DEFAULT_PORT
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
              invalidation_port: @goldlapel_invalidation_port
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
