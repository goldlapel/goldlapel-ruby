# frozen_string_literal: true

require "minitest/autorun"
require "open3"
require "tmpdir"
require "rbconfig"

class TestCli < Minitest::Test
  EXE = File.expand_path("../exe/goldlapel", __dir__)
  LIB = File.expand_path("../lib", __dir__)
  RUBY = RbConfig.ruby

  def test_forwards_args_to_binary
    Dir.mktmpdir do |dir|
      fake_binary = File.join(dir, "fake-goldlapel")
      File.write(fake_binary, <<~SH)
        #!/bin/sh
        echo "$@"
      SH
      File.chmod(0o755, fake_binary)

      env = { "GOLDLAPEL_BINARY" => fake_binary, "RUBYLIB" => LIB }
      stdout, stderr, status = Open3.capture3(env, RUBY, EXE, "activate", "tok_abc123")

      assert status.success?, "Expected exit 0, got #{status.exitstatus}. stderr: #{stderr}"
      assert_equal "activate tok_abc123", stdout.strip
    end
  end

  def test_forwards_no_args
    Dir.mktmpdir do |dir|
      fake_binary = File.join(dir, "fake-goldlapel")
      File.write(fake_binary, <<~SH)
        #!/bin/sh
        echo "no-args"
      SH
      File.chmod(0o755, fake_binary)

      env = { "GOLDLAPEL_BINARY" => fake_binary, "RUBYLIB" => LIB }
      stdout, _stderr, status = Open3.capture3(env, RUBY, EXE)

      assert status.success?
      assert_equal "no-args", stdout.strip
    end
  end

  def test_propagates_exit_code
    Dir.mktmpdir do |dir|
      fake_binary = File.join(dir, "fake-goldlapel")
      File.write(fake_binary, <<~SH)
        #!/bin/sh
        exit 42
      SH
      File.chmod(0o755, fake_binary)

      env = { "GOLDLAPEL_BINARY" => fake_binary, "RUBYLIB" => LIB }
      _stdout, _stderr, status = Open3.capture3(env, RUBY, EXE, "--version")

      assert_equal 42, status.exitstatus
    end
  end

  def test_error_when_binary_not_found
    env = {
      "GOLDLAPEL_BINARY" => "/nonexistent/path/goldlapel",
      "RUBYLIB" => LIB,
    }
    _stdout, stderr, status = Open3.capture3(env, RUBY, EXE)

    assert_equal 1, status.exitstatus
    assert_match(/Error:/, stderr)
    assert_match(/GOLDLAPEL_BINARY/, stderr)
  end

  def test_error_when_no_binary_available
    env = {
      "GOLDLAPEL_BINARY" => nil,
      "PATH" => "",
      "RUBYLIB" => LIB,
      # Clear RUBYOPT so bundler/setup (injected by `bundle exec`) doesn't
      # shell out through the empty PATH and crash the child with 127 before
      # our code runs. Local dev rarely hits this, but CI via bundle exec
      # does.
      "RUBYOPT" => nil,
      "BUNDLE_GEMFILE" => nil,
    }
    _stdout, stderr, status = Open3.capture3(env, RUBY, EXE)

    assert_equal 1, status.exitstatus
    assert_match(/Error:/, stderr)
    assert_match(/Gold Lapel binary not found/, stderr)
  end
end
