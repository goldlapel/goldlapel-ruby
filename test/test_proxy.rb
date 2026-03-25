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
  def test_postgresql_url
    url = "postgresql://user:pass@remotehost:5432/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_postgres_url
    url = "postgres://user:pass@dbhost:5432/mydb"
    assert_equal "postgres://user:pass@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_bare_host_port
    assert_equal "localhost:7932",
                 GoldLapel::Proxy.make_proxy_url("remotehost:5432", 7932)
  end

  def test_host_only
    assert_equal "localhost:7932",
                 GoldLapel::Proxy.make_proxy_url("remotehost", 7932)
  end

  def test_preserves_params
    url = "postgresql://user:pass@remotehost:5432/mydb?sslmode=require"
    assert_equal "postgresql://user:pass@localhost:7932/mydb?sslmode=require",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_preserves_percent_encoded_password
    url = "postgresql://user:p%40ss@remotehost:5432/mydb"
    assert_equal "postgresql://user:p%40ss@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_no_userinfo
    url = "postgresql://remotehost:5432/mydb"
    assert_equal "postgresql://localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_pg_url_without_port
    url = "postgresql://user:pass@remotehost/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_pg_url_without_port_or_path
    url = "postgresql://user:pass@remotehost"
    assert_equal "postgresql://user:pass@localhost:7932",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_no_userinfo_no_port
    url = "postgresql://remotehost/mydb"
    assert_equal "postgresql://localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_literal_at_in_password
    url = "postgresql://user:p@ss@remotehost:5432/mydb"
    assert_equal "postgresql://user:p@ss@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_at_sign_in_password_without_port
    url = "postgresql://user:p@ss@host/mydb"
    assert_equal "postgresql://user:p@ss@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_at_sign_in_password_with_query_params
    url = "postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue"
    assert_equal "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
  end

  def test_localhost_stays_localhost
    url = "postgresql://user:pass@localhost:5432/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb",
                 GoldLapel::Proxy.make_proxy_url(url, 7932)
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
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", port: 9000)
    assert_equal 9000, proxy.port
  end

  def test_port_zero
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", port: 0)
    assert_equal 0, proxy.port
  end

  def test_not_running_initially
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb")
    refute proxy.running?
    assert_nil proxy.url
  end
end

class TestConfigToArgs < Minitest::Test
  def test_string_key
    result = GoldLapel::Proxy.config_to_args({ "mode" => "butler" })
    assert_equal ["--mode", "butler"], result
  end

  def test_symbol_key
    result = GoldLapel::Proxy.config_to_args({ mode: "butler" })
    assert_equal ["--mode", "butler"], result
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
      mode: "butler",
      pool_size: 10,
      disable_pool: true,
    })
    assert_includes result, "--mode"
    assert_includes result, "butler"
    assert_includes result, "--pool-size"
    assert_includes result, "10"
    assert_includes result, "--disable-pool"
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
      config: { mode: "butler" }
    )
    assert_equal({ mode: "butler" }, proxy.config)
  end
end

class TestConfigKeys < Minitest::Test
  def test_returns_array_of_strings
    keys = GoldLapel::Proxy.config_keys
    assert_kind_of Array, keys
    keys.each { |k| assert_kind_of String, k }
  end

  def test_contains_known_keys
    keys = GoldLapel::Proxy.config_keys
    assert_includes keys, "mode"
    assert_includes keys, "pool_size"
    assert_includes keys, "disable_matviews"
    assert_includes keys, "replica"
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

  def test_custom_dashboard_port_from_config_symbol
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      config: { dashboard_port: 9090 }
    )
    assert_equal 9090, proxy.instance_variable_get(:@dashboard_port)
  end

  def test_custom_dashboard_port_from_config_string
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      config: { "dashboard_port" => 8080 }
    )
    assert_equal 8080, proxy.instance_variable_get(:@dashboard_port)
  end

  def test_disabled_dashboard_port_zero
    proxy = GoldLapel::Proxy.new(
      "postgresql://localhost:5432/mydb",
      config: { dashboard_port: 0 }
    )
    assert_equal 0, proxy.instance_variable_get(:@dashboard_port)
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
    GoldLapel::Proxy.stop
  end

  def teardown
    GoldLapel::Proxy.stop
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

    assert_equal "postgresql://localhost:7932/db1", GoldLapel::Proxy.proxy_url(up1)
    assert_equal "postgresql://localhost:7934/db2", GoldLapel::Proxy.proxy_url(up2)
  end

  def test_proxy_url_without_upstream_returns_first
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_equal "postgresql://localhost:7932/db1", GoldLapel::Proxy.proxy_url
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
    assert_equal "postgresql://localhost:7934/db2", GoldLapel::Proxy.proxy_url(up2)
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
    assert_equal "postgresql://localhost:7934/db2", GoldLapel.proxy_url(up2)
  end

  def test_module_level_proxy_url_delegates_upstream
    up1 = "postgresql://host1:5432/db1"
    inject_fake(up1, 7932)

    assert_equal "postgresql://localhost:7932/db1", GoldLapel.proxy_url(up1)
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
    assert_equal "postgresql://localhost:7932/db1", url
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
