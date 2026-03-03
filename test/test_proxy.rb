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
      # Stub __dir__ to a nonexistent location so bundled binary isn't found,
      # and clear PATH so which() fails, and stub home so dev binary isn't found.
      GoldLapel::Proxy.stub(:which, nil) do
        error = assert_raises(RuntimeError) { GoldLapel::Proxy.find_binary }
        assert_match(/Gold Lapel binary not found/, error.message)
      end
    ensure
      old ? ENV["GOLDLAPEL_BINARY"] = old : ENV.delete("GOLDLAPEL_BINARY")
      ENV["PATH"] = old_path
    end
  end
end

class TestReplacePort < Minitest::Test
  def test_postgresql_url
    url = "postgresql://user:pass@localhost:5432/mydb"
    assert_equal "postgresql://user:pass@localhost:7932/mydb",
                 GoldLapel::Proxy.replace_port(url, 7932)
  end

  def test_postgres_url
    url = "postgres://user:pass@dbhost:5432/mydb"
    assert_equal "postgres://user:pass@dbhost:7932/mydb",
                 GoldLapel::Proxy.replace_port(url, 7932)
  end

  def test_bare_host_port
    assert_equal "localhost:7932",
                 GoldLapel::Proxy.replace_port("localhost:5432", 7932)
  end

  def test_host_only
    assert_equal "localhost:7932",
                 GoldLapel::Proxy.replace_port("localhost", 7932)
  end

  def test_preserves_params
    url = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"
    assert_equal "postgresql://user:pass@localhost:7932/mydb?sslmode=require",
                 GoldLapel::Proxy.replace_port(url, 7932)
  end

  def test_preserves_percent_encoded_password
    url = "postgresql://user:p%40ss@localhost:5432/mydb"
    assert_equal "postgresql://user:p%40ss@localhost:7932/mydb",
                 GoldLapel::Proxy.replace_port(url, 7932)
  end

  def test_no_userinfo
    url = "postgresql://localhost:5432/mydb"
    assert_equal "postgresql://localhost:7932/mydb",
                 GoldLapel::Proxy.replace_port(url, 7932)
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

class TestModuleFunctions < Minitest::Test
  def test_proxy_url_none_when_not_started
    GoldLapel.stop
    assert_nil GoldLapel.proxy_url
  end
end
