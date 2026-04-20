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
    # Build a clean env with PATH="" to simulate "no goldlapel on PATH".
    # We must strip all bundler-related env vars (BUNDLE_*, BUNDLER_*,
    # BUNDLER_ORIG_*) plus RUBYOPT and the GEM_HOME/GEM_PATH vars from
    # the parent env before spawning — otherwise, under `bundle exec`,
    # the child Ruby process re-runs bundler/setup, which (a) prepends
    # the bundled gem's bin dir to PATH, (b) finds a generated
    # `goldlapel` wrapper script whose `#!/usr/bin/env ruby` shebang
    # then fails because the cleared PATH has no `env` or `ruby`, dying
    # with 127 before our find_binary rescue in exe/goldlapel can run.
    # `unsetenv_others: true` is belt-and-suspenders: the child starts
    # from an empty env populated only with what we explicitly pass.
    env = ENV.to_h.reject { |k, _|
      k.start_with?("BUNDLE_") ||
        k.start_with?("BUNDLER_") ||
        k == "RUBYOPT" ||
        k == "GEM_HOME" ||
        k == "GEM_PATH"
    }.merge(
      "GOLDLAPEL_BINARY" => nil,
      "PATH" => "",
      "RUBYLIB" => LIB,
    )
    _stdout, stderr, status = Open3.capture3(env, RUBY, EXE, unsetenv_others: true)

    assert_equal 1, status.exitstatus
    assert_match(/Error:/, stderr)
    assert_match(/Gold Lapel binary not found/, stderr)
  end
end
