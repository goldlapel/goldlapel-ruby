# frozen_string_literal: true

require "minitest/autorun"
require "socket"
require "tmpdir"
require "goldlapel"

class TestFindBinary < Minitest::Test
  def test_env_var_override
    Dir.mktmpdir do |dir|
      binary = File.join(dir, "goldlapel")
      File.write(binary, "")

      old = ENV["GOLDLAPEL_BINARY"]
      ENV["GOLDLAPEL_BINARY"] = binary
      begin
        assert_equal binary, GoldLapel::Proxy.find_binary
      ensure
        old ? ENV["GOLDLAPEL_BINARY"] = old : ENV.delete("GOLDLAPEL_BINARY")
      end
    end
  end

  def test_env_var_missing_file
    old = ENV["GOLDLAPEL_BINARY"]
    ENV["GOLDLAPEL_BINARY"] = "/nonexistent/goldlapel"
    begin
      error = assert_raises(RuntimeError) { GoldLapel::Proxy.find_binary }
      assert_match(/GOLDLAPEL_BINARY/, error.message)
    ensure
      old ? ENV["GOLDLAPEL_BINARY"] = old : ENV.delete("GOLDLAPEL_BINARY")
    end
  end

  def test_not_found_raises
    old = ENV["GOLDLAPEL_BINARY"]
    ENV.delete("GOLDLAPEL_BINARY")
    old_path = ENV["PATH"]
    ENV["PATH"] = ""
    begin
      error = assert_raises(RuntimeError) { GoldLapel::Proxy.find_binary }
      assert_match(/Gold Lapel binary not found/, error.message)
    ensure
      old ? ENV["GOLDLAPEL_BINARY"] = old : ENV.delete("GOLDLAPEL_BINARY")
      ENV["PATH"] = old_path
    end
  end
end

class TestMakeProxyUrl < Minitest::Test
  # The wrapper appends `application_name=goldlapel:ruby:<version>` to the
  # rewritten URL so the proxy can classify wrapper-vs-raw traffic and skip
  # L2 result cache for wrappers (they have their own L1).
  APP_NAME_SUFFIX = "application_name=#{GoldLapel::Proxy.application_name_marker}"

  def setup
    @orig_pgappname = ENV["PGAPPNAME"]
    ENV.delete("PGAPPNAME")
  end

  def teardown
    @orig_pgappname ? ENV["PGAPPNAME"] = @orig_pgappname : ENV.delete("PGAPPNAME")
  end

  def test_postgresql_url
    url = "postgresql://user:pass@remotehost:5432/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_postgres_url
    url = "postgres://user:pass@dbhost:5432/mydb"
    assert_equal "postgres://user:pass@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_bare_host_port
    # Bare-host form skips the marker — atypical caller path.
    assert_equal "localhost:7932",
                 GoldLapel::Proxy.make_proxy_url("remotehost:5432", 7932)
  end

  def test_host_only
    assert_equal "localhost:7932",
                 GoldLapel::Proxy.make_proxy_url("remotehost", 7932)
  end

  def test_preserves_params
    url = "postgresql://user:pass@remotehost:5432/mydb?sslmode=require"
    assert_equal "postgresql://user:pass@localhost:7932/mydb?sslmode=require&#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_preserves_percent_encoded_password
    url = "postgresql://user:p%40ss@remotehost:5432/mydb"
    assert_equal "postgresql://user:p%40ss@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_no_userinfo
    url = "postgresql://remotehost:5432/mydb"
    assert_equal "postgresql://localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_pg_url_without_port
    url = "postgresql://user:pass@remotehost/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_pg_url_without_port_or_path
    url = "postgresql://user:pass@remotehost"
    assert_equal "postgresql://user:pass@localhost:7932?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_no_userinfo_no_port
    url = "postgresql://remotehost/mydb"
    assert_equal "postgresql://localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_literal_at_in_password
    url = "postgresql://user:p@ss@remotehost:5432/mydb"
    assert_equal "postgresql://user:p@ss@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_at_sign_in_password_without_port
    url = "postgresql://user:p@ss@host/mydb"
    assert_equal "postgresql://user:p@ss@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_at_sign_in_password_with_query_params
    url = "postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue"
    assert_equal "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue&#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_localhost_stays_localhost
    url = "postgresql://user:pass@localhost:5432/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb?#{APP_NAME_SUFFIX}",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end
end


class TestApplicationNameMarker < Minitest::Test
  # L2-router architecture: wrappers identify themselves to the proxy via PG
  # `application_name` so the proxy can gate L2 result cache (wrapper has L1;
  # raw clients don't).

  def setup
    @orig_pgappname = ENV["PGAPPNAME"]
    ENV.delete("PGAPPNAME")
  end

  def teardown
    @orig_pgappname ? ENV["PGAPPNAME"] = @orig_pgappname : ENV.delete("PGAPPNAME")
  end

  def test_marker_format
    marker = GoldLapel::Proxy.application_name_marker
    assert_match(/\Agoldlapel:ruby:.+\z/, marker)
  end

  def test_marker_appended_with_no_existing_query
    out = GoldLapel::Proxy.make_proxy_url("postgresql://localhost:5432/mydb", 7932)
    assert_includes out, "?application_name=goldlapel:ruby:"
  end

  def test_marker_appended_with_existing_query
    out = GoldLapel::Proxy.make_proxy_url("postgresql://localhost:5432/mydb?sslmode=require", 7932)
    assert_includes out, "sslmode=require"
    assert_includes out, "&application_name=goldlapel:ruby:"
  end

  def test_user_override_via_url_respected
    out = GoldLapel::Proxy.make_proxy_url("postgresql://localhost:5432/mydb?application_name=my-app", 7932)
    assert_includes out, "application_name=my-app"
    refute_includes out, "goldlapel:ruby"
  end

  def test_user_override_via_pgappname_respected
    ENV["PGAPPNAME"] = "my-app"
    out = GoldLapel::Proxy.make_proxy_url("postgresql://localhost:5432/mydb", 7932)
    refute_includes out, "application_name="
    refute_includes out, "goldlapel:ruby"
  end
end

class TestWaitForPort < Minitest::Test
  def test_open_port
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    begin
      assert GoldLapel::Proxy.wait_for_port("127.0.0.1", port, 1.0)
    ensure
      server.close
    end
  end

  def test_closed_port_timeout
    refute GoldLapel::Proxy.wait_for_port("127.0.0.1", 19999, 0.2)
  end
end

class TestProxyClass < Minitest::Test
  def test_default_port
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    assert_equal 7932, proxy.port
  end

  def test_custom_port
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", proxy_port: 9000)
    assert_equal 9000, proxy.port
  end

  def test_port_zero
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", proxy_port: 0)
    assert_equal 0, proxy.port
  end

  def test_not_running_initially
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    refute proxy.running?
    assert_nil proxy.url
  end

  def test_stop_is_noop_when_never_started
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    proxy.stop
    refute proxy.running?
    assert_nil proxy.url
    assert_nil proxy.dashboard_url
    assert_nil proxy.instance_variable_get(:@pid)
  end

  def test_stop_is_idempotent
    # Double-stop is reachable in real code: atexit hooks, signal handlers,
    # try/ensure chains, test teardown loops. A buggy second-stop (NPE,
    # double-close of subprocess stream) would mask the root error or
    # crash the interpreter. Guard against regressions here.
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    proxy.stop
    proxy.stop # must not raise
    refute proxy.running?
    assert_nil proxy.url
    assert_nil proxy.dashboard_url
    assert_nil proxy.instance_variable_get(:@pid)
    assert_nil proxy.instance_variable_get(:@stderr_reader)
  end
end

class TestConfigToArgs < Minitest::Test
  def test_string_key
    result = GoldLapel::Proxy.config_to_args({ "pool_mode" => "transaction" })
    assert_equal ["--pool-mode", "transaction"], result
  end

  def test_symbol_key
    result = GoldLapel::Proxy.config_to_args({ pool_mode: "transaction" })
    assert_equal ["--pool-mode", "transaction"], result
  end

  def test_numeric_value
    result = GoldLapel::Proxy.config_to_args({ pool_size: 20 })
    assert_equal ["--pool-size", "20"], result
  end

  def test_boolean_true
    result = GoldLapel::Proxy.config_to_args({ disable_matviews: true })
    assert_equal ["--disable-matviews"], result
  end

  def test_boolean_false_skipped
    result = GoldLapel::Proxy.config_to_args({ disable_matviews: false })
    assert_equal [], result
  end

  def test_list_key
    result = GoldLapel::Proxy.config_to_args({ replica: ["r1:5433", "r2:5434"] })
    assert_equal ["--replica", "r1:5433", "--replica", "r2:5434"], result
  end

  def test_unknown_key_raises
    error = assert_raises(ArgumentError) do
      GoldLapel::Proxy.config_to_args({ bogus: "val" })
    end
    assert_match(/Unknown config key: bogus/, error.message)
  end

  def test_multiple_keys
    result = GoldLapel::Proxy.config_to_args({
      pool_mode: "transaction",
      pool_size: 10,
      disable_pool: true,
    })
    assert_includes result, "--pool-mode"
    assert_includes result, "transaction"
    assert_includes result, "--pool-size"
    assert_includes result, "10"
    assert_includes result, "--disable-pool"
  end

  def test_log_level_in_config_map_rejected
    # Regression guard: log_level was promoted to a top-level kwarg.
    assert_raises(ArgumentError, /Unknown config key/) do
      GoldLapel::Proxy.config_to_args({ log_level: "info" })
    end
  end

  def test_mode_in_config_map_rejected
    assert_raises(ArgumentError, /Unknown config key/) do
      GoldLapel::Proxy.config_to_args({ mode: "waiter" })
    end
  end

  def test_empty_hash
    assert_equal [], GoldLapel::Proxy.config_to_args({})
  end

  def test_nil_config
    assert_equal [], GoldLapel::Proxy.config_to_args(nil)
  end

  def test_boolean_key_with_non_bool_raises
    error = assert_raises(TypeError) do
      GoldLapel::Proxy.config_to_args({ disable_matviews: "yes" })
    end
    assert_match(/expects a boolean/, error.message)
  end

  def test_constructor_stores_config
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      config: { pool_mode: "transaction" }
    )
    assert_equal({ pool_mode: "transaction" }, proxy.config)
  end
end

class TestConfigKeys < Minitest::Test
  def test_returns_array_of_strings
    keys = GoldLapel::Proxy.config_keys
    assert_kind_of Array, keys
    keys.each { |k| assert_kind_of String, k }
  end

  def test_contains_known_keys
    # Tuning knobs still live in the structured config map.
    keys = GoldLapel::Proxy.config_keys
    assert_includes keys, "pool_size"
    assert_includes keys, "disable_matviews"
    assert_includes keys, "replica"
  end

  def test_does_not_contain_promoted_top_level_keys
    # Top-level concepts (mode, log_level, dashboard_port, etc.) were
    # promoted out of the structured config map.
    keys = GoldLapel::Proxy.config_keys
    %w[mode log_level dashboard_port invalidation_port config license client].each do |promoted|
      refute_includes keys, promoted
    end
  end

  def test_expected_count
    keys = GoldLapel::Proxy.config_keys
    assert_equal GoldLapel::Proxy::VALID_CONFIG_KEYS.size, keys.size
  end

  def test_returns_copy
    keys = GoldLapel::Proxy.config_keys
    keys << "bogus"
    refute_includes GoldLapel::Proxy.config_keys, "bogus"
  end

  def test_module_level_delegates
    assert_equal GoldLapel::Proxy.config_keys, GoldLapel.config_keys
  end
end

class TestDashboardUrl < Minitest::Test
  def test_default_dashboard_port
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    assert_equal GoldLapel::DEFAULT_DASHBOARD_PORT,
                 proxy.instance_variable_get(:@dashboard_port)
  end

  def test_dashboard_port_derives_from_custom_proxy_port
    # Regression: when proxy_port is customized and dashboard_port is NOT
    # set explicitly, the dashboard port must be proxy_port + 1 (matching
    # what the Rust proxy binary binds), not the default 7933.
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      proxy_port: 17932
    )
    assert_equal 17933, proxy.instance_variable_get(:@dashboard_port)
  end

  def test_explicit_dashboard_port_overrides_derivation
    # When dashboard_port is explicitly set as a top-level kwarg, it wins
    # over the proxy_port + 1 derivation.
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      proxy_port: 17932,
      dashboard_port: 25000
    )
    assert_equal 25000, proxy.instance_variable_get(:@dashboard_port)
  end

  def test_custom_dashboard_port_top_level_kwarg
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      dashboard_port: 9090
    )
    assert_equal 9090, proxy.instance_variable_get(:@dashboard_port)
  end

  def test_dashboard_port_in_config_map_rejected
    # Regression guard: dashboard_port was promoted to a top-level kwarg
    # on the canonical surface. Passing it through `config` must raise.
    assert_raises(ArgumentError, /Unknown config key/) do
      GoldLapel::Proxy.new(
        "postgresql://localhost:5432/mydb",
        config: { dashboard_port: 8080 }
      )
    end
  end

  def test_disabled_dashboard_port_zero
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      dashboard_port: 0
    )
    assert_equal 0, proxy.instance_variable_get(:@dashboard_port)
  end

  def test_invalidation_port_derives_from_custom_proxy_port
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      proxy_port: 17932
    )
    assert_equal 17934, proxy.invalidation_port
  end

  def test_explicit_invalidation_port_overrides_derivation
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      proxy_port: 17932,
      invalidation_port: 9999
    )
    assert_equal 9999, proxy.invalidation_port
  end

  def test_dashboard_url_nil_when_not_running
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    assert_nil proxy.dashboard_url
  end
end

class TestModuleFunctions < Minitest::Test
  def test_proxy_url_none_when_not_started
    GoldLapel.stop
    assert_nil GoldLapel.proxy_url
  end

  def test_dashboard_url_none_when_not_started
    GoldLapel.stop
    assert_nil GoldLapel.dashboard_url
  end

  def test_stop_specific_upstream
    GoldLapel.stop
    assert_nil GoldLapel.proxy_url("postgresql://host1:5432/db1")
  end

  def test_stop_with_no_args_clears_all
    GoldLapel.stop
    assert_equal({}, GoldLapel::Proxy.instances)
  end
end

class TestMultiInstance < Minitest::Test
  # The wrapper appends `application_name=goldlapel:ruby:<version>` to every
  # rewritten URL. We compute it once and append in each `expected_url` below.
  APP_NAME_SUFFIX = "?application_name=#{GoldLapel::Proxy.application_name_marker}"

  # Helper: inject a fake proxy instance into the registry for testing
  # without actually spawning a binary.
  FakeProxy = Struct.new(:upstream, :url, :dashboard_url, :alive) do
    def running?
      alive
    end

    def stop
      self.alive = false
      self.url = nil
      self.dashboard_url = nil
    end

    def start
      self.alive = true
      url
    end
  end

  def setup
    @orig_pgappname = ENV["PGAPPNAME"]
    ENV.delete("PGAPPNAME")
    GoldLapel::Proxy.stop
  end

  def teardown
    GoldLapel::Proxy.stop
    @orig_pgappname ? ENV["PGAPPNAME"] = @orig_pgappname : ENV.delete("PGAPPNAME")
  end

  def inject_fake(upstream, port)
    proxy_url = GoldLapel::Proxy.make_proxy_url(upstream, port)
    dashboard = "http://127.0.0.1:#{port + 1}"
    fake = FakeProxy.new(upstream, proxy_url, dashboard, true)
    GoldLapel::Proxy.instance_variable_get(:@mutex).synchronize do
      GoldLapel::Proxy.instance_variable_get(:@instances)[upstream] = fake
    end
    fake
  end

  def test_instances_returns_empty_hash_initially
    assert_equal({}, GoldLapel::Proxy.instances)
  end

  def test_instances_returns_copy
    copy = GoldLapel::Proxy.instances
    copy["bogus"] = "should not leak"
    refute_includes GoldLapel::Proxy.instances, "bogus"
  end

  def test_multiple_upstreams_tracked
    up1 = "postgresql://host1:5432/db1"
    up2 = "postgresql://host2:5432/db2"
    inject_fake(up1, 7932)
    inject_fake(up2, 7934)

    instances = GoldLapel::Proxy.instances
    assert_equal 2, instances.size
    assert_includes instances.keys, up1
    assert_includes instances.keys, up2
  end

  def test_proxy_url_with_specific_upstream
    up1 = "postgresql://host1:5432/db1"
    up2 = "postgresql://host2:5432/db2"
    inject_fake(up1, 7932)
    inject_fake(up2, 7934)

    assert_equal "postgresql://localhost:7932/db1#{APP_NAME_SUFFIX}", GoldLapel::Proxy.proxy_url(up1)
    assert_equal "postgresql://localhost:7934/db2#{APP_NAME_SUFFIX}", GoldLapel::Proxy.proxy_url(up2)
  end

  def test_proxy_url_without_upstream_returns_first
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_equal "postgresql://localhost:7932/db1#{APP_NAME_SUFFIX}", GoldLapel::Proxy.proxy_url
  end

  def test_dashboard_url_with_specific_upstream
    up1 = "postgresql://host1:5432/db1"
    up2 = "postgresql://host2:5432/db2"
    inject_fake(up1, 7932)
    inject_fake(up2, 7934)

    assert_equal "http://127.0.0.1:7933", GoldLapel::Proxy.dashboard_url(up1)
    assert_equal "http://127.0.0.1:7935", GoldLapel::Proxy.dashboard_url(up2)
  end

  def test_dashboard_url_without_upstream_returns_first
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_equal "http://127.0.0.1:7933", GoldLapel::Proxy.dashboard_url
  end

  def test_stop_specific_upstream_leaves_others
    up1 = "postgresql://host1:5432/db1"
    up2 = "postgresql://host2:5432/db2"
    fake1 = inject_fake(up1, 7932)
    inject_fake(up2, 7934)

    GoldLapel::Proxy.stop(up1)

    refute fake1.running?
    assert_nil GoldLapel::Proxy.proxy_url(up1)
    assert_equal "postgresql://localhost:7934/db2#{APP_NAME_SUFFIX}", GoldLapel::Proxy.proxy_url(up2)
    assert_equal 1, GoldLapel::Proxy.instances.size
  end

  def test_stop_all_clears_everything
    up1 = "postgresql://host1:5432/db1"
    up2 = "postgresql://host2:5432/db2"
    fake1 = inject_fake(up1, 7932)
    fake2 = inject_fake(up2, 7934)

    GoldLapel::Proxy.stop

    refute fake1.running?
    refute fake2.running?
    assert_equal({}, GoldLapel::Proxy.instances)
  end

  def test_stop_nonexistent_upstream_is_noop
    GoldLapel::Proxy.stop("postgresql://nonexistent:5432/db")
    assert_equal({}, GoldLapel::Proxy.instances)
  end

  def test_module_level_stop_specific
    up1 = "postgresql://host1:5432/db1"
    up2 = "postgresql://host2:5432/db2"
    inject_fake(up1, 7932)
    inject_fake(up2, 7934)

    GoldLapel.stop(up1)

    assert_nil GoldLapel.proxy_url(up1)
    assert_equal "postgresql://localhost:7934/db2#{APP_NAME_SUFFIX}", GoldLapel.proxy_url(up2)
  end

  def test_module_level_proxy_url_delegates_upstream
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_equal "postgresql://localhost:7932/db1#{APP_NAME_SUFFIX}", GoldLapel.proxy_url(up1)
  end

  def test_module_level_dashboard_url_delegates_upstream
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_equal "http://127.0.0.1:7933", GoldLapel.dashboard_url(up1)
  end

  def test_start_returns_existing_url_for_same_upstream
    up1 = "postgresql://host1:5432/db1"
    fake = inject_fake(up1, 7932)

    # Calling start on same upstream should return existing URL without creating new
    url = GoldLapel::Proxy.instance_variable_get(:@mutex).synchronize do
      existing = GoldLapel::Proxy.instance_variable_get(:@instances)[up1]
      existing.url if existing&.running?
    end
    assert_equal "postgresql://localhost:7932/db1#{APP_NAME_SUFFIX}", url
  end

  def test_proxy_url_returns_nil_for_unknown_upstream
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_nil GoldLapel::Proxy.proxy_url("postgresql://unknown:5432/db")
  end

  def test_dashboard_url_returns_nil_for_unknown_upstream
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_nil GoldLapel::Proxy.dashboard_url("postgresql://unknown:5432/db")
  end
end
