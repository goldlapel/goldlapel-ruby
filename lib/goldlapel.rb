# frozen_string_literal: true

require_relative "goldlapel/proxy"

module GoldLapel
  # Module-level convenience methods (multi-instance)
  def self.start(upstream, port: nil, config: {}, extra_args: [])
    Proxy.start(upstream, port: port, config: config, extra_args: extra_args)
  end

  def self.stop(upstream = nil)
    Proxy.stop(upstream)
  end

  def self.proxy_url(upstream = nil)
    Proxy.proxy_url(upstream)
  end

  def self.dashboard_url(upstream = nil)
    Proxy.dashboard_url(upstream)
  end

  def self.config_keys
    Proxy.config_keys
  end
end
