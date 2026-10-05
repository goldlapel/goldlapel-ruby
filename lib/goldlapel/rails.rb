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
          # database.yml `goldlapel:` block follows the canonical snake_case
          # surface: proxy_port, dashboard_port, log_level, mode, license,
          # config_file, config, extra_args.
          gl_config = @config.is_a?(Hash) ? @config[:goldlapel] || {} : {}
          gl_config = gl_config.transform_keys(&:to_sym) if gl_config.is_a?(Hash)
          proxy_port_opt = gl_config[:proxy_port]
          config = gl_config[:config]
          extra_args = gl_config[:extra_args] || []

          upstream = GoldLapel::Rails.build_upstream_url(@connection_parameters)

          begin
            # Rails manages its own pg connections; only spawn the proxy here.
            # (`start_proxy` is the low-level, connection-less variant of
            # `GoldLapel.start` that returns the proxy URL, not an instance.)
            proxy_url = GoldLapel.start_proxy(
              upstream,
              proxy_port: proxy_port_opt,
              dashboard_port: gl_config[:dashboard_port],
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
              disable_sqloptimize: gl_config[:disable_sqloptimize] ? true : false,
              disable_auto_indexes: gl_config[:disable_auto_indexes] ? true : false,
            )
            # Without a configured proxy_port the core picks a free port
            # pair per upstream (multiple databases each get their own), so
            # read the port back from the URL rather than assuming 7932.
            proxy_port = URI.parse(proxy_url).port
          rescue => e
            ::Rails.logger.warn("[Gold Lapel] Proxy failed to start: #{e.message} — falling back to direct connection")
            @goldlapel_started = true
            return super
          end

          @connection_parameters[:host] = "127.0.0.1"
          @connection_parameters[:port] = proxy_port
          @goldlapel_started = true
        end

        super
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
