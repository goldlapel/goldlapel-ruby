# frozen_string_literal: true

# Shared helper for port-allocation tests. Swaps `Proxy#start`, `#running?`
# and `#stop` for in-memory fakes (no subprocess, no socket) and gives the
# module-level registry an empty hash for the duration of the block, so the
# registry's real allocation logic runs against a known state.

require_relative "../lib/goldlapel/proxy"

module FakeProxySupport
  STUBBED = %i[start running? stop].freeze

  def self.with_fake_proxies
    klass = GoldLapel::Proxy
    originals = STUBBED.to_h { |m| [m, klass.instance_method(m)] }
    saved_instances = klass.instance_variable_get(:@instances)
    klass.instance_variable_set(:@instances, {})

    verbose_was = $VERBOSE
    $VERBOSE = nil
    klass.define_method(:start) do
      @pid = -1
      @url = self.class.make_proxy_url(@upstream, @proxy_port)
    end
    klass.define_method(:running?) { !@pid.nil? }
    klass.define_method(:stop) do
      @pid = nil
      @url = nil
    end

    begin
      yield
    ensure
      originals.each { |m, um| klass.define_method(m, um) }
      klass.instance_variable_set(:@instances, saved_instances)
      $VERBOSE = verbose_was
    end
  end
end
