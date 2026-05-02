# frozen_string_literal: true

require "socket"
require "timeout"
require "rbconfig"

module GoldLapel
  DEFAULT_PROXY_PORT = 7932
  DEFAULT_DASHBOARD_PORT = 7933
  STARTUP_TIMEOUT = 10.0
  STARTUP_POLL_INTERVAL = 0.05

  class Proxy
    attr_reader :url, :upstream, :dashboard_url, :proxy_port, :config, :dashboard_port,
                :invalidation_port, :dashboard_token, :mesh, :mesh_tag
    attr_accessor :wrapped_conn

    # Keys that are valid inside the structured `config` hash. Top-level
    # concepts (proxy_port, dashboard_port, invalidation_port, log_level,
    # mode, license, client, config_file) are exposed as their own keyword
    # arguments on GoldLapel.start / Proxy.new and are NOT valid keys here
    # — passing them through `config` raises.
    VALID_CONFIG_KEYS = %w[
      min_pattern_count refresh_interval_secs pattern_ttl_secs
      max_tables_per_view max_columns_per_view deep_pagination_threshold
      report_interval_secs result_cache_size batch_cache_size
      batch_cache_ttl_secs pool_size pool_timeout_secs
      pool_mode mgmt_idle_timeout fallback read_after_write_secs
      n1_threshold n1_window_ms n1_cross_threshold
      tls_cert tls_key tls_client_ca
      disable_matviews disable_consolidation disable_btree_indexes
      disable_trigram_indexes disable_expression_indexes
      disable_partial_indexes disable_rewrite disable_prepared_cache
      disable_result_cache disable_pool
      disable_n1 disable_n1_cross_connection disable_shadow_mode
      enable_coalescing replica exclude_tables
    ].freeze

    BOOLEAN_KEYS = %w[
      disable_matviews disable_consolidation disable_btree_indexes
      disable_trigram_indexes disable_expression_indexes
      disable_partial_indexes disable_rewrite disable_prepared_cache
      disable_result_cache disable_pool
      disable_n1 disable_n1_cross_connection disable_shadow_mode
      enable_coalescing
    ].freeze

    LIST_KEYS = %w[
      replica exclude_tables
    ].freeze

    def self.config_keys
      VALID_CONFIG_KEYS.dup
    end

    # Translate a log-level string into the proxy's count-based verbosity
    # flag (`-v` / `-vv` / `-vvv`). Returns nil when no flag should be
    # emitted (warn/error map to the binary's default level). Raises on
    # invalid input.
    def self.log_level_to_verbose_flag(level)
      return nil if level.nil?
      unless level.is_a?(String)
        raise TypeError, "log_level expects a string, got #{level.class}"
      end
      case level.downcase
      when "trace" then "-vvv"
      when "debug" then "-vv"
      when "info"  then "-v"
      when "warn", "warning", "error" then nil
      else
        raise ArgumentError,
              "log_level must be one of: trace, debug, info, warn, error"
      end
    end

    def self.config_to_args(config)
      return [] if config.nil? || config.empty?

      args = []
      config.each do |key, value|
        key = key.to_s
        unless VALID_CONFIG_KEYS.include?(key)
          raise ArgumentError, "Unknown config key: #{key}"
        end

        flag = "--#{key.tr('_', '-')}"

        if BOOLEAN_KEYS.include?(key)
          unless value == true || value == false
            raise TypeError, "Config key '#{key}' expects a boolean, got #{value.class}"
          end
          args << flag if value
        elsif LIST_KEYS.include?(key)
          Array(value).each do |item|
            args.push(flag, item.to_s)
          end
        else
          args.push(flag, value.to_s)
        end
      end
      args
    end

    # `silent` is a wrapper-only concern: when true the startup banner is
    # suppressed entirely. Default (false) prints the banner to $stderr (never
    # $stdout — libraries shouldn't pollute app stdout or captured test output).
    # `silent` is deliberately NOT part of @config, so it is never forwarded to
    # the Rust binary as a CLI flag.
    def initialize(
      upstream,
      proxy_port: nil,
      dashboard_port: nil,
      invalidation_port: nil,
      log_level: nil,
      mode: nil,
      license: nil,
      client: nil,
      config_file: nil,
      config: {},
      extra_args: [],
      silent: false,
      mesh: false,
      mesh_tag: nil
    )
      @upstream = upstream
      @proxy_port = proxy_port || DEFAULT_PROXY_PORT

      # Dashboard / invalidation ports default to proxy_port + 1 / + 2 when
      # unset. An explicit value (including 0 for "disable dashboard")
      # overrides the derivation.
      @dashboard_port_explicit = !dashboard_port.nil?
      @dashboard_port = @dashboard_port_explicit ? dashboard_port.to_i : @proxy_port + 1
      @invalidation_port_explicit = !invalidation_port.nil?
      @invalidation_port = @invalidation_port_explicit ? invalidation_port.to_i : @proxy_port + 2

      @log_level = log_level
      @mode = mode
      @license = license
      @client = client
      @config_file = config_file

      # Validate structured-config keys eagerly so a test that constructs
      # without spawning still catches bad keys.
      config.each do |k, _|
        key = k.to_s
        unless VALID_CONFIG_KEYS.include?(key)
          raise ArgumentError, "Unknown config key: #{key}"
        end
      end
      @config = config
      @extra_args = extra_args
      @silent = silent ? true : false
      # Mesh membership (startup intent — HQ enforces license).
      @mesh = mesh ? true : false
      tag = mesh_tag.to_s
      @mesh_tag = tag.empty? ? nil : tag
      @pid = nil
      @url = nil
      @dashboard_url = nil
      @dashboard_token = nil
      @stderr_reader = nil
    end

    # Backwards-compat alias for the rest of the wrapper (ddl.rb etc.) that
    # still speaks in terms of `.port`.
    alias_method :port, :proxy_port

    def start
      return @url if running?

      binary = self.class.find_binary
      cmd = [
        binary,
        "--upstream", @upstream,
        "--proxy-port", @proxy_port.to_s,
      ]
      # Top-level options (promoted out of the config map by the canonical
      # surface) emit their own CLI flags. Each is suppressed when the user
      # hasn't set it, so the Rust binary applies its own defaults.
      if @dashboard_port_explicit
        cmd.push("--dashboard-port", @dashboard_port.to_s)
      end
      if @invalidation_port_explicit
        cmd.push("--invalidation-port", @invalidation_port.to_s)
      end
      verbose_flag = self.class.log_level_to_verbose_flag(@log_level)
      cmd.push(verbose_flag) if verbose_flag
      cmd.push("--mode", @mode) if @mode
      cmd.push("--license", @license) if @license
      cmd.push("--client", @client) if @client
      cmd.push("--config", @config_file) if @config_file
      cmd.push("--mesh") if @mesh
      cmd.push("--mesh-tag", @mesh_tag) if @mesh_tag
      cmd.concat(self.class.config_to_args(@config))
      cmd.concat(@extra_args)

      env = ENV.to_h
      # GOLDLAPEL_CLIENT env var is only set when the user hasn't opted in
      # via the top-level `client` kwarg (which emits --client and takes
      # precedence over the env var).
      env["GOLDLAPEL_CLIENT"] ||= "ruby" if @client.nil?
      # Provision a session-scoped dashboard token so ddl.rb can POST to
      # /api/ddl/* without relying on ~/.goldlapel/dashboard-token. Pre-set
      # env wins (user may already have their own token configured).
      if env["GOLDLAPEL_DASHBOARD_TOKEN"] && !env["GOLDLAPEL_DASHBOARD_TOKEN"].empty?
        @dashboard_token = env["GOLDLAPEL_DASHBOARD_TOKEN"]
      else
        require "securerandom"
        @dashboard_token = SecureRandom.hex(32)
        env["GOLDLAPEL_DASHBOARD_TOKEN"] = @dashboard_token
      end
      stderr_read, stderr_write = IO.pipe
      @pid = Process.spawn(env, *cmd,
        in: File::NULL,
        out: File::NULL,
        err: stderr_write)
      stderr_write.close
      @stderr_reader = stderr_read

      unless self.class.wait_for_port("127.0.0.1", @proxy_port, STARTUP_TIMEOUT)
        Process.kill("KILL", @pid) rescue Errno::ESRCH
        Process.wait(@pid) rescue Errno::ECHILD
        stderr_output = stderr_read.read
        stderr_read.close
        @pid = nil
        @stderr_reader = nil
        raise "Gold Lapel failed to start on port #{@proxy_port} " \
              "within #{STARTUP_TIMEOUT}s.\nstderr: #{stderr_output}"
      end

      @stderr_reader.close
      @stderr_reader = nil
      @url = self.class.make_proxy_url(@upstream, @proxy_port)
      @dashboard_url = @dashboard_port > 0 ? "http://127.0.0.1:#{@dashboard_port}" : nil

      # Banner — $stderr (not $stdout), and only when not silenced. Library
      # code must never unconditionally write to $stdout: it pollutes app
      # output, CI logs, and stdout captured in test runs. `silent: true`
      # suppresses the banner entirely.
      unless @silent
        if @dashboard_port > 0
          $stderr.puts "goldlapel → :#{@proxy_port} (proxy) | http://127.0.0.1:#{@dashboard_port} (dashboard)"
        else
          $stderr.puts "goldlapel → :#{@proxy_port} (proxy)"
        end
      end

      @url
    end

    def stop
      if @pid
        begin
          Process.kill("TERM", @pid)
          Timeout.timeout(5) { Process.wait(@pid) }
        rescue Errno::ESRCH, Errno::ECHILD
          # Process already exited
        rescue Timeout::Error
          Process.kill("KILL", @pid) rescue Errno::ESRCH
          Process.wait(@pid) rescue Errno::ECHILD
        end
        @stderr_reader&.close rescue IOError
        @pid = nil
        @url = nil
        @dashboard_url = nil
        @dashboard_token = nil
        @stderr_reader = nil
      end
    end

    def running?
      return false unless @pid
      Process.kill(0, @pid)
      true
    rescue Errno::ESRCH, Errno::EPERM
      false
    end

    # --- Class-level helpers ---

    def self.find_binary
      # 1. Explicit override via env var
      env_path = ENV["GOLDLAPEL_BINARY"]
      if env_path
        return env_path if File.file?(env_path)
        raise "GOLDLAPEL_BINARY points to #{env_path} but file not found"
      end

      # 2. Bundled binary (inside the installed gem)
      system_name = case RbConfig::CONFIG["host_os"]
                    when /linux/i then "linux"
                    when /darwin/i then "darwin"
                    when /mswin|mingw|cygwin/i then "windows"
                    else RbConfig::CONFIG["host_os"]
                    end
      machine = RbConfig::CONFIG["host_cpu"]
      arch = case machine
             when /x86_64|amd64/i then "x86_64"
             when /arm64|aarch64/i then "aarch64"
             else machine
             end

      binary_name = "goldlapel-#{system_name}-#{arch}"
      binary_name += ".exe" if system_name == "windows"
      bundled = File.join(__dir__, "..", "..", "bin", binary_name)
      return bundled if File.file?(bundled)

      # 3. On PATH
      on_path = which("goldlapel")
      return on_path if on_path

      raise "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " \
            "install the platform-specific package, or ensure 'goldlapel' is on PATH."
    end

    # Wrapper version, read from the loaded gem spec or the GEM_VERSION env
    # var (set by CI at publish time). Local dev installs return "0.0.0".
    # Used to build the application_name marker on PG connections so the proxy
    # can classify wrapper-vs-raw traffic and gate L2 result cache.
    def self.wrapper_version
      spec = Gem.loaded_specs["goldlapel"]
      return spec.version.to_s if spec && spec.version
      ENV.fetch("GEM_VERSION", "0.0.0")
    rescue StandardError
      "0.0.0"
    end

    def self.application_name_marker
      "goldlapel:ruby:#{wrapper_version}"
    end

    # Append `application_name=goldlapel:ruby:<version>` to `url` unless it
    # already has one (or PGAPPNAME is set in the env). The marker tells the
    # proxy this is wrapper traffic, so it can skip L2 result cache (the
    # wrapper has its own L1). Idempotent and override-respecting.
    def self.inject_application_name(url)
      return url if url =~ /[?&]application_name=/
      return url if ENV["PGAPPNAME"] && !ENV["PGAPPNAME"].empty?
      sep = url.include?("?") ? "&" : "?"
      "#{url}#{sep}application_name=#{application_name_marker}"
    end

    def self.make_proxy_url(upstream, port)
      # pg URL with explicit port
      if upstream =~ /\A(postgres(?:ql)?:\/\/(?:.*@)?)([^:\/?#]+):(\d+)(.*)\z/
        return inject_application_name("#{$1}localhost:#{port}#{$4}")
      end
      # pg URL without port
      if upstream =~ /\A(postgres(?:ql)?:\/\/(?:.*@)?)([^:\/?#]+)(.*)\z/
        return inject_application_name("#{$1}localhost:#{port}#{$3}")
      end
      # bare host:port (guard against scheme colons).
      # Bare-host form skips the marker — atypical caller path.
      if !upstream.include?("://") && upstream.include?(":")
        return "localhost:#{port}"
      end
      # bare host
      "localhost:#{port}"
    end

    def self.wait_for_port(host, port, timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        begin
          sock = TCPSocket.new(host, port)
          sock.close
          return true
        rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, Errno::EHOSTUNREACH
          sleep STARTUP_POLL_INTERVAL
        end
      end
      false
    end

    # Module-level multi-instance registry keyed by upstream URL
    @instances = {}
    @mutex = Mutex.new
    @cleanup_registered = false

    class << self
      # Low-level entry point — spawns a Proxy and returns the proxy URL
      # string. Does NOT open a PG connection or wrap it. Used by
      # `GoldLapel.start_proxy` and tests.
      def start(
        upstream,
        proxy_port: nil,
        dashboard_port: nil,
        invalidation_port: nil,
        log_level: nil,
        mode: nil,
        license: nil,
        client: nil,
        config_file: nil,
        config: {},
        extra_args: [],
        silent: false,
        mesh: false,
        mesh_tag: nil
      )
        @mutex.synchronize do
          existing = @instances[upstream]
          return existing.url if existing&.running?

          proxy = Proxy.new(
            upstream,
            proxy_port: proxy_port,
            dashboard_port: dashboard_port,
            invalidation_port: invalidation_port,
            log_level: log_level,
            mode: mode,
            license: license,
            client: client,
            config_file: config_file,
            config: config,
            extra_args: extra_args,
            silent: silent,
            mesh: mesh,
            mesh_tag: mesh_tag,
          )
          unless @cleanup_registered
            at_exit { cleanup }
            @cleanup_registered = true
          end
          proxy.start
          @instances[upstream] = proxy
          proxy.url
        end
      end

      # Register an externally-constructed proxy in the module registry
      # (used by Instance so the at_exit cleanup covers it).
      def register(proxy)
        @mutex.synchronize do
          unless @cleanup_registered
            at_exit { cleanup }
            @cleanup_registered = true
          end
          @instances[proxy.upstream] = proxy
        end
      end

      def unregister(proxy)
        @mutex.synchronize do
          existing = @instances[proxy.upstream]
          @instances.delete(proxy.upstream) if existing.equal?(proxy)
        end
      end

      def stop(upstream = nil)
        @mutex.synchronize do
          if upstream
            instance = @instances.delete(upstream)
            instance&.stop
          else
            @instances.each_value(&:stop)
            @instances.clear
          end
        end
      end

      def proxy_url(upstream = nil)
        @mutex.synchronize do
          instance = if upstream
            @instances[upstream]
          else
            @instances.values.first
          end
          instance&.url
        end
      end

      def dashboard_url(upstream = nil)
        @mutex.synchronize do
          instance = if upstream
            @instances[upstream]
          else
            @instances.values.first
          end
          instance&.dashboard_url
        end
      end

      def instances
        @mutex.synchronize { @instances.dup }
      end

      private

      def cleanup
        @instances.each_value(&:stop)
        @instances.clear
      end

      def which(cmd)
        exts = ENV["PATHEXT"] ? ENV["PATHEXT"].split(";") : [""]
        (ENV["PATH"] || "").split(File::PATH_SEPARATOR).each do |path|
          exts.each do |ext|
            full = File.join(path, "#{cmd}#{ext}")
            return full if File.executable?(full) && File.file?(full)
          end
        end
        nil
      end
    end
  end
end
