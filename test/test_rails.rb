require "minitest/autorun"

# Stub goldlapel gem BEFORE requiring rails.rb (which does `require "goldlapel"`).
#
# We only define the module skeleton + helpers that other test files don't
# provide (reset!, WrappedConnection, start_calls/wrap_calls accessors).
# The `start_proxy` and `wrap` stubs that record calls are installed per-test
# in `setup` and restored in `teardown` — defining them at file-load time
# leaks into every other test file loaded in the same process.
module GoldLapel
  DEFAULT_PROXY_PORT = 7932 unless defined?(DEFAULT_PROXY_PORT)

  @start_calls = []
  @wrap_calls = []

  def self.start_calls
    @start_calls
  end

  def self.wrap_calls
    @wrap_calls
  end

  def self.reset!
    @start_calls = []
    @wrap_calls = []
  end

  unless defined?(WrappedConnection)
    class WrappedConnection
      attr_reader :real_conn, :invalidation_port

      def initialize(real_conn, invalidation_port)
        @real_conn = real_conn
        @invalidation_port = invalidation_port
      end
    end
  end
end
$LOADED_FEATURES << "goldlapel.rb"

# Helpers to install/restore the `start_proxy` + `wrap` stubs. Called from
# each Rails test class's `setup` / `teardown` so the stubs only apply while
# a Rails test is actually running — never across unrelated test files.
module RailsTestGoldLapelStub
  def self.install
    verbose_was = $VERBOSE
    $VERBOSE = nil

    @original_start_proxy = GoldLapel.method(:start_proxy) if GoldLapel.respond_to?(:start_proxy)
    @original_wrap = GoldLapel.method(:wrap) if GoldLapel.respond_to?(:wrap)

    GoldLapel.define_singleton_method(:start_proxy) do |upstream, **kwargs|
      @start_calls << { upstream: upstream, **kwargs }
    end
    GoldLapel.define_singleton_method(:wrap) do |conn, invalidation_port: nil, disable_native_cache: false|
      @wrap_calls << {
        conn: conn,
        invalidation_port: invalidation_port,
        disable_native_cache: disable_native_cache,
      }
      GoldLapel::WrappedConnection.new(conn, invalidation_port)
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

    if @original_wrap
      GoldLapel.define_singleton_method(:wrap, &@original_wrap)
    elsif GoldLapel.singleton_class.method_defined?(:wrap) ||
          GoldLapel.singleton_class.private_method_defined?(:wrap)
      GoldLapel.singleton_class.send(:remove_method, :wrap)
    end

    @original_start_proxy = nil
    @original_wrap = nil
  ensure
    $VERBOSE = verbose_was
  end

  # Per-test helpers to override the recording stub (e.g. to make start_proxy
  # raise). The install/restore pair in setup/teardown reinstates the recording
  # stub between tests, so no ensure block is needed at the call site.
  def self.override_start_proxy(&block)
    verbose_was = $VERBOSE
    $VERBOSE = nil
    GoldLapel.define_singleton_method(:start_proxy, &block)
  ensure
    $VERBOSE = verbose_was
  end

  def self.override_wrap(&block)
    verbose_was = $VERBOSE
    $VERBOSE = nil
    GoldLapel.define_singleton_method(:wrap, &block)
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
  # mesh_tag, disable_proxy_cache, disable_matviews,
  # disable_sqloptimize, disable_auto_indexes, disable_native_cache).
  # Each kwarg must thread from configuration through to
  # `start_proxy` (or `wrap`, for `disable_native_cache`).

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

  def test_disable_matviews_forwarded
    adapter = FakeAdapter.new(
      config: { goldlapel: { disable_matviews: true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    assert_equal true, GoldLapel.start_calls.first[:disable_matviews]
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
    assert_equal false, call[:disable_matviews]
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
          "disable_matviews" => true,
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
    assert_equal true, call[:disable_matviews]
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
# L1 native cache wrapping tests
# ---------------------------------------------------------------------------
class TestL1CacheWrapping < Minitest::Test
  def setup
    RailsTestGoldLapelStub.install
    GoldLapel.reset!
  end

  def teardown
    RailsTestGoldLapelStub.restore
  end

  def test_connect_wraps_raw_connection
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    assert_equal 1, GoldLapel.wrap_calls.length
    assert_kind_of GoldLapel::WrappedConnection, adapter.raw_connection
  end

  def test_wrap_receives_raw_pg_connection
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.wrap_calls.first
    assert_kind_of FakePgConnection, call[:conn]
  end

  def test_default_invalidation_port_is_proxy_plus_two
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.wrap_calls.first
    assert_equal GoldLapel::DEFAULT_PROXY_PORT + 2, call[:invalidation_port]
  end

  def test_custom_invalidation_port_from_config
    adapter = FakeAdapter.new(
      config: { goldlapel: { invalidation_port: 8888 } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.wrap_calls.first
    assert_equal 8888, call[:invalidation_port]
  end

  def test_invalidation_port_derives_from_custom_proxy_port
    adapter = FakeAdapter.new(
      config: { goldlapel: { proxy_port: 9000 } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.wrap_calls.first
    assert_equal 9002, call[:invalidation_port]
  end

  def test_invalidation_port_string_key_from_yaml
    adapter = FakeAdapter.new(
      config: { goldlapel: { "invalidation_port" => 7777 } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)

    call = GoldLapel.wrap_calls.first
    assert_equal 7777, call[:invalidation_port]
  end

  def test_reconnect_wraps_each_time
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    adapter.send(:connect)
    adapter.send(:connect)

    # Proxy started once, but wrap called twice (each connect gets a new PG connection)
    assert_equal 1, GoldLapel.start_calls.length
    assert_equal 2, GoldLapel.wrap_calls.length
  end

  def test_disable_native_cache_forwarded_to_wrap
    adapter = FakeAdapter.new(
      config: { goldlapel: { disable_native_cache: true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.wrap_calls.first
    assert_equal true, call[:disable_native_cache]
  end

  def test_disable_native_cache_defaults_to_false
    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.wrap_calls.first
    assert_equal false, call[:disable_native_cache]
  end

  def test_disable_native_cache_string_key_from_yaml
    adapter = FakeAdapter.new(
      config: { goldlapel: { "disable_native_cache" => true } },
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )
    adapter.send(:connect)
    call = GoldLapel.wrap_calls.first
    assert_equal true, call[:disable_native_cache]
  end

  def test_no_wrap_on_fallback
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

    adapter.send(:connect)

    # Wrap should NOT be called when proxy failed to start
    assert_equal 0, GoldLapel.wrap_calls.length
    # raw_connection should be the unwrapped FakePgConnection
    assert_kind_of FakePgConnection, adapter.raw_connection
  end

  def test_graceful_fallback_on_wrap_failure
    RailsTestGoldLapelStub.override_wrap do |conn, invalidation_port: nil, disable_native_cache: false|
      @wrap_calls << {
        conn: conn,
        invalidation_port: invalidation_port,
        disable_native_cache: disable_native_cache,
      }
      raise RuntimeError, "wrap exploded"
    end

    adapter = FakeAdapter.new(
      config: {},
      connection_parameters: {
        host: "db.example.com", port: "5432",
        user: "u", password: "p", dbname: "mydb"
      }
    )

    # Should not raise
    adapter.send(:connect)

    # raw_connection should remain the unwrapped FakePgConnection
    assert_kind_of FakePgConnection, adapter.raw_connection

    # Warning logged
    assert Rails.logger.warnings.any? { |w| w.include?("L1 cache wrap failed") }
  end
end
