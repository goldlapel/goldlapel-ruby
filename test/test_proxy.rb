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
    assert_equal 7932, proxy.instance_variable_get(:@port)
  end

  def test_custom_port
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", port: 9000)
    assert_equal 9000, proxy.instance_variable_get(:@port)
  end

  def test_port_zero
    proxy = GoldLapel::Proxy.new("postgresql://localhost:5432/mydb", port: 0)
    assert_equal 0, proxy.instance_variable_get(:@port)
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
    assert_equal({ mode: "butler" }, proxy.instance_variable_get(:@config))
  end
end

class TestModuleFunctions < Minitest::Test
  def test_proxy_url_none_when_not_started
    GoldLapel.stop
    assert_nil GoldLapel.proxy_url
  end
end
