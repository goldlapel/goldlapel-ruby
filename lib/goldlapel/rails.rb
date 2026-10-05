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
      url = "postgresql://#{authority}/#{dbname}"

      # The upstream hop keeps database.yml's TLS/GSS settings (sslmode etc.).
      tls = GoldLapel::Proxy::UPSTREAM_TLS_PARAMS.filter_map do |key|
        value = params[key.to_sym]
        next if value.nil? || value.to_s.empty?
        "#{key}=#{URI.encode_www_form_component(value.to_s)}"
      end
      tls.empty? ? url : "#{url}?#{tls.join('&')}"
    end

    module PostgreSQLExtension
      private

      def connect
        unless @goldlapel_started
          # database.yml's `goldlapel:` block takes the same options as
          # GoldLapel.start (proxy_port, dashboard_port, log_level, mode,
          # license, client, config_file, config, extra_args, silent, mesh,
          # ...), forwarded as given — unknown ones raise like they do there.
          # `client` defaults to "rails" for telemetry.
          gl_config = @config.is_a?(Hash) ? @config[:goldlapel] || {} : {}
          options = gl_config.is_a?(Hash) ? gl_config.transform_keys(&:to_sym) : {}
          options[:client] ||= "rails"

          upstream = GoldLapel::Rails.build_upstream_url(@connection_parameters)

          begin
            # Rails manages its own pg connections; only spawn the proxy here.
            # (`start_proxy` is the low-level, connection-less variant of
            # `GoldLapel.start` that returns the proxy URL, not an instance.)
            # Without a configured proxy_port the core picks a free port pair
            # per upstream (multiple databases each get their own), so read
            # the port back from the registry rather than assuming 7932.
            GoldLapel.start_proxy(upstream, **options)
            proxy = GoldLapel::Proxy.instances.fetch(upstream)
          rescue => e
            ::Rails.logger.warn("[Gold Lapel] Proxy failed to start: #{e.message} — falling back to direct connection")
            @goldlapel_started = true
            return super
          end

          @connection_parameters[:host] = "127.0.0.1"
          @connection_parameters[:port] = proxy.proxy_port
          # The proxy declines TLS from the app unless it was given a
          # certificate, so the upstream TLS settings stay on its side.
          unless proxy.client_tls?
            GoldLapel::Proxy::UPSTREAM_TLS_PARAMS.each { |key| @connection_parameters.delete(key.to_sym) }
          end
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
