# frozen_string_literal: true

require_relative "goldlapel/proxy"

module GoldLapel
  # Module-level convenience methods (singleton pattern)
  def self.start(upstream, port: nil, config: {}, extra_args: [])
    Proxy.start(upstream, port: port, config: config, extra_args: extra_args)
  end

  def self.stop
    Proxy.stop
  end

  def self.proxy_url
    Proxy.proxy_url
  end
end
