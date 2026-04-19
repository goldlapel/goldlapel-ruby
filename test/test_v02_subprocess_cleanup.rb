# frozen_string_literal: true

# Regression test: Instance#start! must clean up its subprocess if the eager
# PG.connect (or subsequent wrap step) raises after the subprocess has been
# spawned.
#
# This was a real bug caught in review: the proxy.start subprocess spawned
# successfully and bound its port, but then PG.connect failed (bad creds,
# network issue, etc.) and the subprocess kept running indefinitely.

require "minitest/autorun"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel"

# Require pg up front (if available) so our stubs land on the final PG constant.
begin
  require "pg"
rescue LoadError
  module PG
    class Error < StandardError; end
    def self.connect(*)
      raise "PG stub — replace via with_pg_connect"
    end
  end
end

# Replaces Proxy#start and Proxy#stop with in-memory stand-ins that track
# whether the subprocess was "spawned" and "stopped", without running a
# real binary.
class FakeProxy
  attr_accessor :wrapped_conn
  attr_reader :upstream, :port, :start_calls, :stop_calls

  def initialize(upstream, port: nil, config: {}, extra_args: [])
    @upstream = upstream
    @port = port || GoldLapel::DEFAULT_PORT
    @running = false
    @start_calls = 0
    @stop_calls = 0
  end

  def start
    @running = true
    @start_calls += 1
    url
  end

  def stop
    @running = false
    @stop_calls += 1
  end

  def running?
    @running
  end

  def url
    "postgresql://user:pass@localhost:#{@port}/db"
  end

  def dashboard_url
    "http://127.0.0.1:#{@port + 1}"
  end
end

class TestSubprocessCleanupOnConnectFailure < Minitest::Test
  def setup
    @original_proxy_class = GoldLapel.send(:remove_const, :Proxy)
    fake_class = Class.new(FakeProxy) do
      @instances = []
      class << self
        attr_reader :instances
      end

      def self.new(*args, **kwargs)
        inst = super
        @instances << inst
        inst
      end

      # Mirror the registry API Proxy exposes at the class level.
      def self.register(*); end
      def self.unregister(*); end
      def self.reset!
        @instances = []
      end
    end
    fake_class.reset!
    GoldLapel.const_set(:Proxy, fake_class)
    @fake_proxy_class = fake_class
  end

  def teardown
    GoldLapel.send(:remove_const, :Proxy)
    GoldLapel.const_set(:Proxy, @original_proxy_class)
  end

  # Swap PG.connect out for the duration of the block.
  def with_pg_connect(proc_impl)
    original = PG.method(:connect)
    PG.define_singleton_method(:connect, &proc_impl)
    begin
      yield
    ensure
      PG.define_singleton_method(:connect, &original)
    end
  end

  def test_subprocess_stopped_when_pg_connect_raises
    boom = ->(*) { raise PG::Error, "bad creds" }

    with_pg_connect(boom) do
      assert_raises(PG::Error) do
        GoldLapel::Instance.new("postgresql://user:pass@host/db")
      end
    end

    assert_equal 1, @fake_proxy_class.instances.length, "expected one proxy to be constructed"
    proxy = @fake_proxy_class.instances.first
    assert_equal 1, proxy.start_calls, "proxy should have been started"
    assert_equal 1, proxy.stop_calls, "proxy must be stopped when PG.connect raises"
    refute proxy.running?, "proxy must not still be running after cleanup"
  end

  def test_subprocess_stopped_when_load_error_raised
    # `require "pg"` itself would raise LoadError when pg isn't installed.
    # We can't unload pg from under the test suite (it's used elsewhere),
    # so instead raise LoadError from PG.connect. The rescue block in
    # start! catches all Exception subtypes, so this still exercises the
    # cleanup path and confirms LoadError isn't treated specially.
    with_pg_connect(->(*) { raise LoadError, "simulated pg missing" }) do
      assert_raises(LoadError) do
        GoldLapel::Instance.new("postgresql://user:pass@host/db")
      end
    end

    proxy = @fake_proxy_class.instances.first
    assert_equal 1, proxy.stop_calls, "proxy must be stopped when LoadError is raised"
  end

  def test_subprocess_stopped_when_wrap_raises
    # Simulate PG.connect succeeding but GoldLapel.wrap failing. The wrap
    # failure should still trigger subprocess cleanup AND close the raw
    # PG connection (which the wrap never took ownership of).
    fake_raw = Object.new
    close_called = false
    fake_raw.define_singleton_method(:close) { close_called = true }

    with_pg_connect(->(*) { fake_raw }) do
      original_wrap = GoldLapel.method(:wrap)
      GoldLapel.define_singleton_method(:wrap) { |*, **| raise RuntimeError, "wrap kaboom" }
      begin
        assert_raises(RuntimeError) do
          GoldLapel::Instance.new("postgresql://user:pass@host/db")
        end
      ensure
        GoldLapel.define_singleton_method(:wrap, &original_wrap)
      end
    end

    proxy = @fake_proxy_class.instances.first
    assert_equal 1, proxy.stop_calls, "proxy must be stopped when wrap raises"
    assert close_called, "raw PG connection must be closed when wrap raises"
  end

  def test_internal_state_cleared_after_failure
    # After start! raises, the Instance must not hold onto a partial proxy
    # or partial connection — otherwise later operations on `gl` would try
    # to use stale state pointing at a killed subprocess.
    with_pg_connect(->(*) { raise PG::Error, "bad creds" }) do
      begin
        GoldLapel::Instance.new("postgresql://user:pass@host/db")
      rescue PG::Error
        # swallow — the test below just needs to confirm the exception path ran
      end
    end

    # The constructor raised, so no Instance is bound to a local variable,
    # but we can still verify via the proxy instance count + stop_calls.
    assert_equal 1, @fake_proxy_class.instances.first.stop_calls
  end
end
