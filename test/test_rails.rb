require "minitest/autorun"

# Stub goldlapel gem BEFORE requiring rails.rb (which does `require "goldlapel"`).
#
# We only define the module skeleton + helpers that other test files don't
# provide (reset!, start_calls accessor). The `start_proxy` stub that records
# calls is installed per-test in `setup` and restored in `teardown` — defining
# it at file-load time leaks into every other test file loaded in the same
# process.
module GoldLapel
  DEFAULT_PROXY_PORT = 7932 unless defined?(DEFAULT_PROXY_PORT)

  @start_calls = []

  def self.start_calls
    @start_calls
  end

  def self.reset!
    @start_calls = []
  end
end
$LOADED_FEATURES << "goldlapel.rb"

# Helpers to install/restore the `start_proxy` stub. Called from each Rails
# test class's `setup` / `teardown` so the stub only applies while a Rails
# test is actually running — never across unrelated test files.
module RailsTestGoldLapelStub
  def self.install
    verbose_was = $VERBOSE
    $VERBOSE = nil

    @original_start_proxy = GoldLapel.method(:start_proxy) if GoldLapel.respond_to?(:start_proxy)

    GoldLapel.define_singleton_method(:start_proxy) do |upstream, **kwargs|
      @start_calls << { upstream: upstream, **kwargs }
    end
  ensure
    $VERBOSE = verbose_was
  end

  def self.restore
    verbose_was = $VERBOSE
    $VERBOSE = nil

    if @original_start_proxy
      GoldLapel.define_singleton_method(:start_proxy, &@original_start_proxy)
    elsif GoldLapel.singleton_class.method_defined?(:start_proxy) ||
          GoldLapel.singleton_class.private_method_defined?(:start_proxy)
      GoldLapel.singleton_class.send(:remove_method, :start_proxy)
    end

    @original_start_proxy = nil
  ensure
    $VERBOSE = verbose_was
  end

  # Per-test helper to override the recording stub (e.g. to make start_proxy
  # raise). The install/restore pair in setup/teardown reinstates the recording
  # stub between tests, so no ensure block is needed at the call site.
  def self.override_start_proxy(&block)
    verbose_was = $VERBOSE
    $VERBOSE = nil
    GoldLapel.define_singleton_method(:start_proxy, &block)
  ensure
    $VERBOSE = verbose_was
  end
end

# Stub out Rails/ActiveRecord so we can load our code without a full Rails app.
module Rails
  class Railtie
    def self.initializer(name, &block); end
  end

  class FakeLogger
    attr_reader :warnings

    def initialize
      @warnings = []
    end

    def warn(msg)
      @warnings << msg
    end
  end

  @logger = FakeLogger.new

  def self.logger
    @logger
  end
end

module ActiveSupport
  def self.on_load(name, &block); end
end

module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter; end
  end
end

require_relative "../lib/goldlapel/rails"

# ---------------------------------------------------------------------------
# URL construction tests
# ---------------------------------------------------------------------------
class TestBuildUpstreamUrl < Minitest::Test
  def test_standard_params
    url = GoldLapel::Rails.build_upstream_url(
      host: "db.example.com", port: "5432",
      user: "myuser", password: "mypass", dbname: "mydb"
    )
    assert_equal "postgresql://myuser:mypass@db.example.com:5432/mydb", url
  end

  def test_nil_host_defaults_to_localhost
    url = GoldLapel::Rails.build_upstream_url(
      host: nil, port: "5432", user: "u", password: "p", dbname: "db"
    )
    assert_equal "postgresql://u:p@localhost:5432/db", url
  end

  def test_empty_host_defaults_to_localhost
    url = GoldLapel::Rails.build_upstream_url(
      host: "", port: "5432", user: "u", password: "p", dbname: "db"
    )
    assert_equal "postgresql://u:p@localhost:5432/db", url
  end

  def test_nil_port_defaults_to_5432
    url = GoldLapel::Rails.build_upstream_url(
      host: "db.example.com", port: nil, user: "u", password: "p", dbname: "db"
    )
    assert_equal "postgresql://u:p@db.example.com:5432/db", url
  end

  def test_special_chars_percent_encoded
    url = GoldLapel::Rails.build_upstream_url(
      host: "db.example.com", port: "5432",
      user: "user@org", password: "p@ss:word/special", dbname: "my db"
    )
    assert_equal(
      "postgresql://user%40org:p%40ss%3Aword%2Fspecial@db.example.com:5432/my%20db",
      url
    )
  end

  def test_no_user_or_password
    url = GoldLapel::Rails.build_upstream_url(
      host: "db.example.com", port: "5432", dbname: "mydb"
    )
    assert_equal "postgresql://db.example.com:5432/mydb", url
  end

  def test_user_without_password
    url = GoldLapel::Rails.build_upstream_url(
      host: "db.example.com", port: "5432", user: "myuser", dbname: "mydb"
    )
    assert_equal "postgresql://myuser@db.example.com:5432/mydb", url
  end

  def test_empty_user_treated_as_no_user
    url = GoldLapel::Rails.build_upstream_url(
      host: "db.example.com", port: "5432", user: "", password: "p", dbname: "mydb"
    )
    assert_equal "postgresql://db.example.com:5432/mydb", url
  end

  def test_unix_socket_raises
    assert_raises(ArgumentError) do
      GoldLapel::Rails.build_upstream_url(
        host: "/var/run/postgresql", port: "5432", dbname: "mydb"
      )
    end
  end
end

# ---------------------------------------------------------------------------
# Connect override tests
# ---------------------------------------------------------------------------

# Minimal adapter double that includes our extension
class FakePgConnection; end

class FakeAdapter
  prepend GoldLapel::Rails::PostgreSQLExtension

  attr_accessor :connection_parameters, :config, :raw_connection
  attr_reader :super_called

  def initialize(config:, connection_parameters:)
    @config = config
    @connection_parameters = connection_parameters
    @super_called = 0
  end

  private

  def connect
    @super_called += 1
    @raw_connection = FakePgConnection.new
  end
end

class TestConnect < Minitest::Test
  def setup
    RailsTestGoldLapelStub.install
    GoldLapel.reset!
  end

  def teardown
    RailsTestGoldLapelStub.restore
  end

  def test_starts_proxy_and_swaps_params
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    assert_equal 1, GoldLapel.start_calls.length
    call = GoldLapel.start_calls.first
    assert_equal "postgresql://u:p@db.example.com:5432/mydb", call[:upstream]
    assert_nil call[:config]
    assert_nil call[:proxy_port]
    assert_equal [], call[:extra_args]

    assert_equal "127.0.0.1", adapter.connection_parameters[:host]
    assert_equal GoldLapel::DEFAULT_PROXY_PORT, adapter.connection_parameters[:port]
    assert_equal 1, adapter.super_called
  end

  def test_custom_port_from_config
    adapter = FakeAdapter.new(
      config: { goldlapel: { proxy_port: 9000 } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    assert_equal 9000, GoldLapel.start_calls.first[:proxy_port]
    assert_equal 9000, adapter.connection_parameters[:port]
  end

  def test_extra_args_from_config
    adapter = FakeAdapter.new(
      config: { goldlapel: { extra_args: ["--threshold-duration-ms", "200"] } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    assert_equal ["--threshold-duration-ms", "200"], GoldLapel.start_calls.first[:extra_args]
  end

  def test_config_hash_from_config
    adapter = FakeAdapter.new(
      config: { goldlapel: { config: { pool_mode: "transaction", pool_size: 30 } } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.start_calls.first
    assert_equal({ pool_mode: "transaction", pool_size: 30 }, call[:config])
  end

  def test_config_hash_with_port_and_extra_args
    adapter = FakeAdapter.new(
      config: {
        goldlapel: {
          proxy_port: 9000,
          mode: "waiter",
          config: { disable_n1: true },
          extra_args: ["--verbose"]
        }
      },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.start_calls.first
    assert_equal 9000, call[:proxy_port]
    assert_equal "waiter", call[:mode]
    assert_equal({ disable_n1: true }, call[:config])
    assert_equal ["--verbose"], call[:extra_args]
  end

  def test_nil_config_when_not_specified
    adapter = FakeAdapter.new(
      config: { goldlapel: { proxy_port: 9000 } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    assert_nil GoldLapel.start_calls.first[:config]
  end

  def test_missing_goldlapel_config_uses_defaults
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.start_calls.first
    assert_nil call[:config]
    assert_nil call[:proxy_port]
    assert_equal [], call[:extra_args]
  end

  def test_reconnect_skips_proxy_setup
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)
    adapter.send(:connect)

    # Proxy started only once
    assert_equal 1, GoldLapel.start_calls.length
    # But super called twice (both connects go through)
    assert_equal 2, adapter.super_called
    # Params still point at proxy
    assert_equal "127.0.0.1", adapter.connection_parameters[:host]
    assert_equal GoldLapel::DEFAULT_PROXY_PORT, adapter.connection_parameters[:port]
  end

  def test_string_keys_from_yaml_config
    # Rails YAML parsing produces string keys for nested hashes — symbolize_keys
    # is shallow, so the goldlapel sub-hash arrives with string keys.
    adapter = FakeAdapter.new(
      config: { goldlapel: { "proxy_port" => 9000, "extra_args" => ["--verbose"] } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.start_calls.first
    assert_equal 9000, call[:proxy_port]
    assert_equal ["--verbose"], call[:extra_args]
    assert_equal 9000, adapter.connection_parameters[:port]
  end

  # ----- Wave 3 canonical-surface kwarg forwarding -----
  #
  # `database.yml`'s `goldlapel:` block exposes the canonical top-
  # level surface (proxy_port, dashboard_port, ..., silent, mesh,
  # mesh_tag, disable_proxy_cache, disable_sqloptimize,
  # disable_auto_indexes). Each kwarg must thread from configuration
  # through to `start_proxy`.

  def test_silent_forwarded_to_start_proxy
    adapter = FakeAdapter.new(
      config: { goldlapel: { silent: true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    assert_equal true, GoldLapel.start_calls.first[:silent]
  end

  def test_silent_defaults_to_false
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    assert_equal false, GoldLapel.start_calls.first[:silent]
  end

  def test_mesh_and_mesh_tag_forwarded_to_start_proxy
    adapter = FakeAdapter.new(
      config: { goldlapel: { mesh: true, mesh_tag: "tenant-7" } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.start_calls.first
    assert_equal true, call[:mesh]
    assert_equal "tenant-7", call[:mesh_tag]
  end

  def test_mesh_defaults
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.start_calls.first
    assert_equal false, call[:mesh]
    assert_nil call[:mesh_tag]
  end

  def test_disable_proxy_cache_forwarded
    adapter = FakeAdapter.new(
      config: { goldlapel: { disable_proxy_cache: true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    assert_equal true, GoldLapel.start_calls.first[:disable_proxy_cache]
  end

  def test_disable_sqloptimize_forwarded
    adapter = FakeAdapter.new(
      config: { goldlapel: { disable_sqloptimize: true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    assert_equal true, GoldLapel.start_calls.first[:disable_sqloptimize]
  end

  def test_disable_auto_indexes_forwarded
    adapter = FakeAdapter.new(
      config: { goldlapel: { disable_auto_indexes: true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    assert_equal true, GoldLapel.start_calls.first[:disable_auto_indexes]
  end

  def test_disable_proxy_side_flags_default_false
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.start_calls.first
    assert_equal false, call[:disable_proxy_cache]
    assert_equal false, call[:disable_sqloptimize]
    assert_equal false, call[:disable_auto_indexes]
  end

  def test_string_keys_for_new_kwargs_from_yaml
    # Rails' YAML parsing produces string keys; transform_keys
    # symbolises shallowly so the new kwargs must work via strings too.
    adapter = FakeAdapter.new(
      config: {
        goldlapel: {
          "silent" => true,
          "mesh" => true,
          "mesh_tag" => "node-a",
          "disable_proxy_cache" => true,
          "disable_sqloptimize" => true,
          "disable_auto_indexes" => true,
        }
      },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.start_calls.first
    assert_equal true, call[:silent]
    assert_equal true, call[:mesh]
    assert_equal "node-a", call[:mesh_tag]
    assert_equal true, call[:disable_proxy_cache]
    assert_equal true, call[:disable_sqloptimize]
    assert_equal true, call[:disable_auto_indexes]
  end

  def test_graceful_fallback_on_start_failure
    RailsTestGoldLapelStub.override_start_proxy do |upstream, **kwargs|
      raise RuntimeError, "binary not found"
    end

    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    # Should not raise — falls back to direct connection
    adapter.send(:connect)

    # Connection parameters should be unchanged (no proxy rewrite)
    assert_equal "db.example.com", adapter.connection_parameters[:host]
    assert_equal "5432", adapter.connection_parameters[:port]

    # Super (actual connect) should still be called
    assert_equal 1, adapter.super_called

    # Warning logged
    assert Rails.logger.warnings.any? { |w| w.include?("binary not found") }
  end
end

# ---------------------------------------------------------------------------
# Connection handling — Rails talks to the proxy over its own plain
# PG::Connection; the railtie only rewrites host/port.
# ---------------------------------------------------------------------------
class TestPlainConnection < Minitest::Test
  def setup
    RailsTestGoldLapelStub.install
    GoldLapel.reset!
  end

  def teardown
    RailsTestGoldLapelStub.restore
  end

  def test_raw_connection_is_left_unwrapped
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    assert_kind_of FakePgConnection, adapter.raw_connection
  end

  def test_removed_cache_keys_not_forwarded
    # The in-process cache and invalidation port are gone; leftover keys
    # in an old database.yml are ignored rather than passed to the proxy.
    adapter = FakeAdapter.new(
      config: {
        goldlapel: {
          invalidation_port: 7934,
          disable_native_cache: true,
          aggressive_verify: :off,
          disable_matviews: true,
        }
      },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.start_calls.first
    %i[invalidation_port disable_native_cache aggressive_verify disable_matviews].each do |key|
      refute call.key?(key), "#{key} must not be forwarded to start_proxy"
    end
  end
end
