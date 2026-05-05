# frozen_string_literal: true

# Regression tests for the startup banner:
#
#   1. The banner must go to $stderr, never $stdout. Library code that
#      unconditionally prints to stdout pollutes app output, CI logs, and
#      stdout captured by test runners (minitest --capture, rspec, etc.).
#
#   2. A `silent: true` option must suppress the banner entirely on both
#      streams.
#
#   3. `silent` is a wrapper-only concern — it must never be forwarded to
#      the spawned Rust binary as a `--silent` CLI flag.
#
# These tests stub out binary discovery, subprocess spawning, and the
# port-wait loop so no real binary runs; they exercise `Proxy#start`'s
# in-process banner logic directly.

require "minitest/autorun"
require "stringio"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel"

module BannerTestSupport
  # Install stubs on `GoldLapel::Proxy` that make `#start` run its full logic
  # without actually spawning a subprocess. The stubs record the command-line
  # array that would have been passed to the binary so tests can assert what
  # args were (and weren't) forwarded.
  #
  # Yields a hash containing:
  #   :cmd         — the command + args that would have been executed
  #   :spawn_calls — number of times Process.spawn was "called"
  def self.with_stubbed_spawn
    recorded = { cmd: nil, spawn_calls: 0 }

    original_find_binary = GoldLapel::Proxy.method(:find_binary)
    original_wait_for_port = GoldLapel::Proxy.method(:wait_for_port)

    # Silence "method redefined" warnings that fire when we replace and
    # restore singleton methods repeatedly across tests.
    verbose_was = $VERBOSE
    $VERBOSE = nil

    GoldLapel::Proxy.define_singleton_method(:find_binary) { "/fake/goldlapel" }
    GoldLapel::Proxy.define_singleton_method(:wait_for_port) { |*_args| true }

    # Patch Process.spawn on the singleton of Process. We only need to
    # intercept the specific signature used by Proxy#start: (env, *cmd, opts).
    original_spawn = Process.method(:spawn)
    Process.define_singleton_method(:spawn) do |*args, **opts|
      recorded[:spawn_calls] += 1
      # args is [env_hash, *cmd_strings]. First element is the env hash.
      recorded[:cmd] = args.drop(1)
      # Close the stderr_write FD the caller opened so the pipe doesn't leak.
      if opts[:err].is_a?(IO)
        opts[:err].close unless opts[:err].closed?
      end
      12345 # fake PID — not used since running? is never queried before stop
    end

    begin
      yield recorded
    ensure
      GoldLapel::Proxy.define_singleton_method(:find_binary, &original_find_binary)
      GoldLapel::Proxy.define_singleton_method(:wait_for_port, &original_wait_for_port)
      Process.define_singleton_method(:spawn, &original_spawn)
      $VERBOSE = verbose_was
    end
  end

  # Capture writes to both $stdout and $stderr during the block.
  # Returns [stdout_string, stderr_string].
  def self.capture_both
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = StringIO.new
    $stderr = StringIO.new
    begin
      yield
      [$stdout.string, $stderr.string]
    ensure
      $stdout = original_stdout
      $stderr = original_stderr
    end
  end

  # Build + start a bare Proxy with the given kwargs. We swallow any "stop"
  # errors since the fake PID isn't a real process.
  def self.start_proxy(**kwargs)
    proxy = GoldLapel::Proxy.new("postgresql://user:pass@host:5432/db", **kwargs)
    proxy.start
    proxy
  ensure
    begin
      proxy&.stop
    rescue StandardError
      # fake PID — Process.kill fails, that's fine
    end
  end
end

class TestBannerToStderr < Minitest::Test
  def test_banner_writes_to_stderr_not_stdout
    BannerTestSupport.with_stubbed_spawn do |_recorded|
      out, err = BannerTestSupport.capture_both do
        BannerTestSupport.start_proxy(proxy_port: 17932)
      end

      assert_match(/goldlapel →/, err, "banner must be written to $stderr")
      assert_match(/\(proxy\)/, err)
      assert_match(/\(dashboard\)/, err)
      refute_match(/goldlapel →/, out, "banner must NOT appear on $stdout")
      refute_match(/\(proxy\)/, out)
    end
  end

  def test_banner_without_dashboard_still_on_stderr
    # When dashboard_port is explicitly 0, the banner omits the dashboard
    # URL but must still route through $stderr.
    BannerTestSupport.with_stubbed_spawn do |_recorded|
      out, err = BannerTestSupport.capture_both do
        BannerTestSupport.start_proxy(proxy_port: 17932, dashboard_port: 0)
      end

      assert_match(/goldlapel → :17932 \(proxy\)/, err)
      refute_match(/dashboard/, err)
      assert_equal "", out
    end
  end
end

class TestSilentSuppressesBanner < Minitest::Test
  def test_silent_option_suppresses_banner_on_both_streams
    BannerTestSupport.with_stubbed_spawn do |_recorded|
      out, err = BannerTestSupport.capture_both do
        BannerTestSupport.start_proxy(proxy_port: 17932, silent: true)
      end

      refute_match(/goldlapel/, out, "silent=true must suppress banner on $stdout")
      refute_match(/goldlapel/, err, "silent=true must suppress banner on $stderr")
      assert_equal "", out
      assert_equal "", err
    end
  end

  def test_default_silent_false_prints_banner
    # Explicitly verifying the default: silent is false, banner appears on stderr.
    BannerTestSupport.with_stubbed_spawn do |_recorded|
      _out, err = BannerTestSupport.capture_both do
        BannerTestSupport.start_proxy(proxy_port: 17932, silent: false)
      end
      assert_match(/goldlapel →/, err)
    end
  end
end

class TestSilentNotForwardedToBinary < Minitest::Test
  def test_silent_true_is_not_passed_as_cli_flag
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.capture_both do
        BannerTestSupport.start_proxy(proxy_port: 17932, silent: true)
      end

      assert_equal 1, recorded[:spawn_calls], "spawn should have been invoked once"
      cmd = recorded[:cmd]
      refute_nil cmd
      # silent is a wrapper-only concern — must never reach the binary.
      refute_includes cmd, "--silent", "wrapper must not forward --silent to the binary"
      refute_includes cmd, "-silent"
      refute_includes cmd, "silent"
      # Sanity — the args we DO forward should still be present.
      assert_includes cmd, "--upstream"
      assert_includes cmd, "--proxy-port"
      assert_includes cmd, "17932"
    end
  end

  def test_silent_false_also_not_passed_as_cli_flag
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.capture_both do
        BannerTestSupport.start_proxy(proxy_port: 17932, silent: false)
      end

      cmd = recorded[:cmd]
      refute_includes cmd, "--silent"
      refute_includes cmd, "silent"
    end
  end
end

class TestSilentPlumbsThroughPublicAPI < Minitest::Test
  # Regression: silent must plumb from GoldLapel.start → Instance → Proxy, not
  # get stored and ignored along the way. We use `eager_connect: false` to
  # skip the pg connection step and just verify the value lands in the
  # Instance's stored state (which is what `start!` forwards to `Proxy.new`).
  def test_silent_stored_on_instance_when_passed_to_new
    inst = GoldLapel::Instance.new(
      "postgresql://user:pass@host/db",
      eager_connect: false,
      silent: true,
    )
    assert_equal true, inst.instance_variable_get(:@silent)
  end

  def test_silent_defaults_to_false_on_instance
    inst = GoldLapel::Instance.new(
      "postgresql://user:pass@host/db",
      eager_connect: false,
    )
    assert_equal false, inst.instance_variable_get(:@silent)
  end

  def test_silent_stored_on_proxy_when_passed_to_new
    proxy = GoldLapel::Proxy.new(
      "postgresql://user:pass@host/db",
      silent: true,
    )
    assert_equal true, proxy.instance_variable_get(:@silent)
  end

  def test_silent_not_in_valid_config_keys
    # Guardrail: if someone ever adds "silent" to VALID_CONFIG_KEYS it would
    # start leaking to the CLI. Codify that it is not a config key.
    refute_includes GoldLapel::Proxy::VALID_CONFIG_KEYS, "silent"
  end

  def test_silent_not_in_config_to_args_output
    # Passing `{ silent: true }` as a config hash must raise (unknown key),
    # NOT silently become a `--silent` CLI flag.
    assert_raises(ArgumentError) do
      GoldLapel::Proxy.config_to_args({ silent: true })
    end
  end
end

class TestMeshKwargs < Minitest::Test
  # Mesh startup kwargs — top-level canonical-surface options.
  # mesh (bool) + mesh_tag (string) translate to --mesh / --mesh-tag CLI flags.

  def test_mesh_defaults
    proxy = GoldLapel::Proxy.new("postgresql://user@host/db")
    assert_equal false, proxy.mesh
    assert_nil proxy.mesh_tag
  end

  def test_mesh_stored
    proxy = GoldLapel::Proxy.new("postgresql://user@host/db", mesh: true, mesh_tag: "prod-east")
    assert_equal true, proxy.mesh
    assert_equal "prod-east", proxy.mesh_tag
  end

  def test_mesh_tag_empty_string_normalized_to_nil
    proxy = GoldLapel::Proxy.new("postgresql://user@host/db", mesh: true, mesh_tag: "")
    assert_nil proxy.mesh_tag
  end

  def test_mesh_flags_forwarded_to_binary
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.start_proxy(
        proxy_port: 17934, mesh: true, mesh_tag: "prod-east", silent: true,
      )
      assert_includes recorded[:cmd], "--mesh"
      idx = recorded[:cmd].index("--mesh-tag")
      refute_nil idx
      assert_equal "prod-east", recorded[:cmd][idx + 1]
    end
  end

  def test_mesh_absent_when_not_set
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.start_proxy(proxy_port: 17935, silent: true)
      refute_includes recorded[:cmd], "--mesh"
      refute_includes recorded[:cmd], "--mesh-tag"
    end
  end

  def test_mesh_without_tag_only_emits_bool_flag
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.start_proxy(proxy_port: 17936, mesh: true, silent: true)
      assert_includes recorded[:cmd], "--mesh"
      refute_includes recorded[:cmd], "--mesh-tag"
    end
  end

  def test_mesh_not_in_valid_config_keys
    refute_includes GoldLapel::Proxy::VALID_CONFIG_KEYS, "mesh"
    refute_includes GoldLapel::Proxy::VALID_CONFIG_KEYS, "mesh_tag"
  end

  def test_mesh_in_config_map_rejected
    assert_raises(ArgumentError) do
      GoldLapel::Proxy.config_to_args({ mesh: true })
    end
    assert_raises(ArgumentError) do
      GoldLapel::Proxy.config_to_args({ mesh_tag: "prod" })
    end
  end
end

class TestDisableTopLevelKwargs < Minitest::Test
  # The four cache-/optimization-disable flags
  # (disable_proxy_cache, disable_matviews, disable_sqloptimize,
  # disable_auto_indexes) are top-level canonical-surface kwargs. Each
  # maps 1:1 to a CLI flag on the spawned proxy binary. None of them
  # belong in the structured `config:` map — passing them through there
  # raises ArgumentError. (Atomic break for `disable_proxy_cache` and
  # `disable_matviews`, which used to live in the config map.)

  DISABLE_FLAGS = {
    disable_proxy_cache: "--disable-proxy-cache",
    disable_matviews: "--disable-matviews",
    disable_sqloptimize: "--disable-sqloptimize",
    disable_auto_indexes: "--disable-auto-indexes",
  }.freeze

  def test_defaults_are_false
    proxy = GoldLapel::Proxy.new("postgresql://user@host/db")
    assert_equal false, proxy.disable_proxy_cache
    assert_equal false, proxy.disable_matviews
    assert_equal false, proxy.disable_sqloptimize
    assert_equal false, proxy.disable_auto_indexes
  end

  def test_stored_when_true
    proxy = GoldLapel::Proxy.new(
      "postgresql://user@host/db",
      disable_proxy_cache: true,
      disable_matviews: true,
      disable_sqloptimize: true,
      disable_auto_indexes: true,
    )
    assert_equal true, proxy.disable_proxy_cache
    assert_equal true, proxy.disable_matviews
    assert_equal true, proxy.disable_sqloptimize
    assert_equal true, proxy.disable_auto_indexes
  end

  def test_truthy_normalized_to_true
    proxy = GoldLapel::Proxy.new(
      "postgresql://user@host/db",
      disable_proxy_cache: "yes",
      disable_matviews: 1,
      disable_sqloptimize: Object.new,
      disable_auto_indexes: "no", # any truthy string normalizes to true
    )
    assert_equal true, proxy.disable_proxy_cache
    assert_equal true, proxy.disable_matviews
    assert_equal true, proxy.disable_sqloptimize
    assert_equal true, proxy.disable_auto_indexes
  end

  def test_each_flag_forwarded_when_true
    DISABLE_FLAGS.each_with_index do |(kwarg, cli_flag), i|
      BannerTestSupport.with_stubbed_spawn do |recorded|
        BannerTestSupport.start_proxy(
          proxy_port: 17940 + i, kwarg => true, silent: true,
        )
        assert_includes recorded[:cmd], cli_flag,
          "expected #{cli_flag} when #{kwarg}: true"
      end
    end
  end

  def test_flags_absent_by_default
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.start_proxy(proxy_port: 17945, silent: true)
      DISABLE_FLAGS.each_value do |cli_flag|
        refute_includes recorded[:cmd], cli_flag,
          "default proxy must not emit #{cli_flag}"
      end
    end
  end

  def test_flags_absent_when_false
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.start_proxy(
        proxy_port: 17946,
        disable_proxy_cache: false,
        disable_matviews: false,
        disable_sqloptimize: false,
        disable_auto_indexes: false,
        silent: true,
      )
      DISABLE_FLAGS.each_value do |cli_flag|
        refute_includes recorded[:cmd], cli_flag
      end
    end
  end

  def test_all_four_flags_emitted_together
    BannerTestSupport.with_stubbed_spawn do |recorded|
      BannerTestSupport.start_proxy(
        proxy_port: 17947,
        disable_proxy_cache: true,
        disable_matviews: true,
        disable_sqloptimize: true,
        disable_auto_indexes: true,
        silent: true,
      )
      DISABLE_FLAGS.each_value do |cli_flag|
        assert_includes recorded[:cmd], cli_flag
      end
    end
  end

  def test_not_in_valid_config_keys
    DISABLE_FLAGS.each_key do |kwarg|
      refute_includes GoldLapel::Proxy::VALID_CONFIG_KEYS, kwarg.to_s,
        "#{kwarg} was promoted to a top-level kwarg; must NOT live in VALID_CONFIG_KEYS"
    end
  end

  def test_in_config_map_rejected
    # Atomic break: `disable_proxy_cache` and `disable_matviews` used to be
    # valid `config:` keys. After promotion to top-level kwargs they must
    # raise on the config-map path instead of silently turning into a CLI
    # flag (otherwise users have two ways to set them and we have a config
    # surface drift).
    DISABLE_FLAGS.each_key do |kwarg|
      assert_raises(ArgumentError, "passing #{kwarg} via config: must raise") do
        GoldLapel::Proxy.config_to_args({ kwarg => true })
      end
    end
  end

  def test_plumbs_through_factory_to_instance_state
    # Regression: each kwarg must plumb GoldLapel.start → Instance → Proxy,
    # not get stored on Instance and then dropped before Proxy.new.
    inst = GoldLapel::Instance.new(
      "postgresql://user:pass@host/db",
      eager_connect: false,
      disable_proxy_cache: true,
      disable_matviews: true,
      disable_sqloptimize: true,
      disable_auto_indexes: true,
    )
    assert_equal true, inst.instance_variable_get(:@disable_proxy_cache)
    assert_equal true, inst.instance_variable_get(:@disable_matviews)
    assert_equal true, inst.instance_variable_get(:@disable_sqloptimize)
    assert_equal true, inst.instance_variable_get(:@disable_auto_indexes)
  end

  def test_defaults_to_false_on_instance
    inst = GoldLapel::Instance.new(
      "postgresql://user:pass@host/db", eager_connect: false,
    )
    assert_equal false, inst.instance_variable_get(:@disable_proxy_cache)
    assert_equal false, inst.instance_variable_get(:@disable_matviews)
    assert_equal false, inst.instance_variable_get(:@disable_sqloptimize)
    assert_equal false, inst.instance_variable_get(:@disable_auto_indexes)
  end
end
