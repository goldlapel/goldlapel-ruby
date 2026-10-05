# frozen_string_literal: true

# Shared helper for port-allocation tests. Swaps `Proxy#start`, `#running?`
# and `#stop` for in-memory fakes (no subprocess, no socket), makes every
# port probe report free, and gives the module-level registry an empty hash,
# so the registry's real allocation logic runs against a known state.
# `with_fake_proxies` wraps a block; `install` / `restore` suit setup/teardown.

require_relative "../lib/goldlapel/proxy"

module FakeProxySupport
  STUBBED = %i[start running? stop].freeze

  def self.install
    klass = GoldLapel::Proxy
    @originals = STUBBED.to_h { |m| [m, klass.instance_method(m)] }
    @original_port_free = klass.method(:port_free?)
    @saved_instances = klass.instance_variable_get(:@instances)
    klass.instance_variable_set(:@instances, {})

    @verbose_was = $VERBOSE
    $VERBOSE = nil
    klass.define_method(:start) do
      @pid = -1
      @url = self.class.make_proxy_url(@upstream, @proxy_port)
    end
    klass.define_method(:running?) { !@pid.nil? }
    klass.define_method(:stop) do
      @stopped = true
      @pid = nil
      @url = nil
    end
    klass.define_singleton_method(:port_free?) { |_port| true }
  end

  def self.restore
    klass = GoldLapel::Proxy
    @originals.each { |m, um| klass.define_method(m, um) }
    klass.define_singleton_method(:port_free?, &@original_port_free)
    klass.instance_variable_set(:@instances, @saved_instances)
    $VERBOSE = @verbose_was
  end

  def self.with_fake_proxies
    install
    begin
      yield
    ensure
      restore
    end
  end
end
