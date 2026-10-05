# frozen_string_literal: true

require "minitest/autorun"
require "socket"
require "tmpdir"
require "goldlapel"
require_relative "_fake_proxy_helper"

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
  # rewritten URL so Postgres-side tooling can see wrapper traffic.
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
  # Wrappers tag their connections with PG `application_name`. The proxy
  # passes it through to Postgres untouched (`pg_stat_activity`, ops
  # dashboards) and caches these connections like any other client.

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

  def test_nil_config_accepted
    # The Rails integration forwards database.yml's absent `config:` as nil.
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", config: nil)
    assert_equal({}, proxy.config)
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

class TestPortAllocation < Minitest::Test
  UP1 = "postgresql://localhost:5432/one"
  UP2 = "postgresql://localhost:5432/two"
  UP3 = "postgresql://localhost:5432/three"

  def port_of(upstream)
    GoldLapel::Proxy.instances[upstream].proxy_port
  end

  def test_first_proxy_gets_default_port
    FakeProxySupport.with_fake_proxies do
      url = GoldLapel::Proxy.start(UP1)
      assert_equal 7932, port_of(UP1)
      assert_includes url, "localhost:7932/"
    end
  end

  def test_two_upstreams_without_ports_get_distinct_pairs
    FakeProxySupport.with_fake_proxies do
      GoldLapel::Proxy.start(UP1)
      url2 = GoldLapel::Proxy.start(UP2)
      assert_equal 7932, port_of(UP1)
      assert_equal 7934, port_of(UP2)
      assert_equal 7935, GoldLapel::Proxy.instances[UP2].dashboard_port
      assert_includes url2, "localhost:7934/"
    end
  end

  def test_same_upstream_reuses_running_proxy
    FakeProxySupport.with_fake_proxies do
      url1 = GoldLapel::Proxy.start(UP1)
      url2 = GoldLapel::Proxy.start(UP1)
      assert_equal url1, url2
      assert_equal 1, GoldLapel::Proxy.instances.size
    end
  end

  def test_explicit_proxy_port_counts_as_claimed
    FakeProxySupport.with_fake_proxies do
      GoldLapel::Proxy.start(UP1, proxy_port: 7934)
      GoldLapel::Proxy.start(UP2)
      GoldLapel::Proxy.start(UP3)
      assert_equal 7934, port_of(UP1)
      assert_equal 7932, port_of(UP2)
      # 7934 and 7935 are UP1's pair, so the next free pair is 7936/7937.
      assert_equal 7936, port_of(UP3)
    end
  end

  def test_explicit_dashboard_port_is_skipped
    FakeProxySupport.with_fake_proxies do
      GoldLapel::Proxy.start(UP1, dashboard_port: 7934)
      GoldLapel::Proxy.start(UP2)
      assert_equal 7932, port_of(UP1)
      # 7933 would put UP2's dashboard on UP1's 7934; 7934 is UP1's dashboard.
      assert_equal 7935, port_of(UP2)
    end
  end

  def test_disabled_dashboard_claims_only_proxy_port
    FakeProxySupport.with_fake_proxies do
      GoldLapel::Proxy.start(UP1, dashboard_port: 0)
      GoldLapel::Proxy.start(UP2)
      assert_equal 7932, port_of(UP1)
      assert_equal 7933, port_of(UP2)
    end
  end

  def test_own_explicit_dashboard_port_is_not_taken_as_proxy_port
    FakeProxySupport.with_fake_proxies do
      GoldLapel::Proxy.start(UP1, dashboard_port: 7932)
      assert_equal 7933, port_of(UP1)
      assert_equal 7932, GoldLapel::Proxy.instances[UP1].dashboard_port
    end
  end

  def test_stop_releases_ports
    FakeProxySupport.with_fake_proxies do
      GoldLapel::Proxy.start(UP1)
      GoldLapel::Proxy.start(UP2)
      GoldLapel::Proxy.stop(UP1)
      GoldLapel::Proxy.start(UP3)
      assert_equal 7932, port_of(UP3)
    end
  end

  def test_registered_proxies_get_distinct_pairs
    # GoldLapel.start (Instance) builds its own Proxy and registers it.
    FakeProxySupport.with_fake_proxies do
      p1 = GoldLapel::Proxy.new(UP1)
      p2 = GoldLapel::Proxy.new(UP2)
      p3 = GoldLapel::Proxy.new(UP3, proxy_port: 9000)
      [p1, p2, p3].each { |p| GoldLapel::Proxy.register(p) }
      assert_equal 7932, p1.proxy_port
      assert_equal 7934, p2.proxy_port
      assert_equal 7935, p2.dashboard_port
      assert_equal 9000, p3.proxy_port

      GoldLapel::Proxy.unregister(p1)
      p4 = GoldLapel::Proxy.new("postgresql://localhost:5432/four")
      GoldLapel::Proxy.register(p4)
      assert_equal 7932, p4.proxy_port
    end
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
    # `disable_btree_indexes` remains in the structured config map; the
    # cache-/optimization-level disables (disable_proxy_cache,
    # disable_sqloptimize, disable_auto_indexes) are top-level kwargs now
    # and rejected by the config-map path.
    result = GoldLapel::Proxy.config_to_args({ disable_btree_indexes: true })
    assert_equal ["--disable-btree-indexes"], result
  end

  def test_boolean_false_skipped
    result = GoldLapel::Proxy.config_to_args({ disable_btree_indexes: false })
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
      GoldLapel::Proxy.config_to_args({ disable_btree_indexes: "yes" })
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
    assert_includes keys, "disable_btree_indexes"
    assert_includes keys, "replica"
  end

  def test_does_not_contain_promoted_top_level_keys
    # Top-level concepts (mode, log_level, dashboard_port, etc.) were
    # promoted out of the structured config map. The three
    # cache-/optimization-disable flags are also top-level now.
    keys = GoldLapel::Proxy.config_keys
    %w[
      mode log_level dashboard_port config license client
      disable_proxy_cache disable_sqloptimize disable_auto_indexes
    ].each do |promoted|
      refute_includes keys, promoted
    end
  end

  def test_does_not_contain_removed_keys
    # The proxy dropped materialized views and the wrappers' in-process
    # cache; their settings are gone from the config map too.
    keys = GoldLapel::Proxy.config_keys
    %w[
      invalidation_port native_cache_size disable_native_cache
      aggressive_verify disable_matviews disable_consolidation
      disable_rewrite disable_shadow_mode refresh_interval_secs
      pattern_ttl_secs max_tables_per_view max_columns_per_view
    ].each do |removed|
      refute_includes keys, removed
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

  def test_removed_kwargs_rejected
    # Atomic break: the invalidation port and matviews are gone from the
    # proxy, so their kwargs are gone too (no aliases).
    %i[invalidation_port disable_matviews].each do |kwarg|
      assert_raises(ArgumentError) do
        GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", kwarg => true)
      end
    end
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
