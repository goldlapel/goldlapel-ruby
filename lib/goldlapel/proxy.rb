# frozen_string_literal: true

require "socket"
require "timeout"
require "rbconfig"

module GoldLapel
  DEFAULT_PROXY_PORT = 7932
  STARTUP_TIMEOUT = 10.0
  STARTUP_POLL_INTERVAL = 0.05

  class Proxy
    attr_reader :url, :upstream, :dashboard_url, :proxy_port, :config, :dashboard_port,
                :dashboard_token, :mesh, :mesh_tag,
                :disable_proxy_cache, :disable_sqloptimize, :disable_auto_indexes

    # Keys that are valid inside the structured `config` hash. Top-level
    # concepts (proxy_port, dashboard_port, log_level, mode, license,
    # client, config_file) are exposed as their own keyword
    # arguments on GoldLapel.start / Proxy.new and are NOT valid keys here
    # — passing them through `config` raises.
    #
    # The three cache-/optimization-disable flags
    # (disable_proxy_cache, disable_sqloptimize, disable_auto_indexes) are also top-level kwargs now and intentionally
    # absent from this list — the config-map path rejects them.
    VALID_CONFIG_KEYS = %w[
      min_pattern_count deep_pagination_threshold report_interval_secs proxy_cache_size batch_cache_size
      batch_cache_ttl_secs pool_size pool_timeout_secs
      pool_mode mgmt_idle_timeout fallback read_after_write_secs
      n1_threshold n1_window_ms n1_cross_threshold
      tls_cert tls_key tls_client_ca
      disable_btree_indexes disable_trigram_indexes
      disable_expression_indexes disable_partial_indexes
      disable_rewrite_prepared_cache disable_pool
      disable_n1 disable_n1_cross_connection
      disable_coalescing replica exclude_tables
    ].freeze

    BOOLEAN_KEYS = %w[
      disable_btree_indexes disable_trigram_indexes
      disable_expression_indexes disable_partial_indexes
      disable_rewrite_prepared_cache disable_pool
      disable_n1 disable_n1_cross_connection
      disable_coalescing
    ].freeze

    LIST_KEYS = %w[
      replica exclude_tables
    ].freeze

    MATVIEWS_REMOVED = "materialized views were removed"
    WRAPPER_CACHE_REMOVED = "it was removed with the in-process cache"

    # Options that used to exist, with why they're gone — so passing one
    # says so instead of a bare "unknown keyword".
    REMOVED_OPTIONS = {
      "invalidation_port" => WRAPPER_CACHE_REMOVED,
      "disable_native_cache" => WRAPPER_CACHE_REMOVED,
      "native_cache_size" => WRAPPER_CACHE_REMOVED,
      "aggressive_verify" => WRAPPER_CACHE_REMOVED,
      "disable_matviews" => MATVIEWS_REMOVED,
    }.freeze

    REMOVED_CONFIG_KEYS = {
      "refresh_interval_secs" => MATVIEWS_REMOVED,
      "pattern_ttl_secs" => MATVIEWS_REMOVED,
      "max_tables_per_view" => MATVIEWS_REMOVED,
      "max_columns_per_view" => MATVIEWS_REMOVED,
      "disable_consolidation" => MATVIEWS_REMOVED,
      "disable_rewrite" => MATVIEWS_REMOVED,
      "disable_shadow_mode" => MATVIEWS_REMOVED,
      "enable_coalescing" => "coalescing is on by default; use disable_coalescing",
    }.freeze

    # Connection parameters for the TLS/GSS hop to the upstream. The proxy
    # keeps using them upstream, but declines TLS from the app unless it was
    # started with --tls-cert/--tls-key, so they come off the app's URL.
    UPSTREAM_TLS_PARAMS = %w[
      sslmode sslcert sslkey sslrootcert sslcrl sslcrldir sslpassword sslsni
      sslnegotiation ssl_min_protocol_version ssl_max_protocol_version
      requiressl channel_binding gssencmode krbsrvname gsslib
    ].freeze

    def self.config_keys
      VALID_CONFIG_KEYS.dup
    end

    # Raise for keyword options the entry points don't know, naming the ones
    # that were removed.
    def self.reject_unknown_options(options)
      return if options.nil? || options.empty?
      key = options.keys.first.to_s
      reason = REMOVED_OPTIONS[key]
      raise ArgumentError, "Unknown option: #{key} (#{reason})" if reason
      raise ArgumentError, "Unknown option: #{key}"
    end

    def self.check_config_key(key)
      return if VALID_CONFIG_KEYS.include?(key)
      reason = REMOVED_CONFIG_KEYS[key]
      raise ArgumentError, "Unknown config key: #{key} (#{reason})" if reason
      raise ArgumentError, "Unknown config key: #{key}"
    end

    # Translate a log level (string or symbol, any case) into the proxy's
    # count-based verbosity flag (`-v` / `-vv` / `-vvv`) — the binary uses
    # clap's ArgAction::Count, not `--log-level <value>`. Returns nil when no
    # flag should be emitted (warn/error are the binary's default level).
    # Invalid values raise instead of producing a cryptic "unknown argument"
    # error from the spawned binary.
    def self.log_level_to_verbose_flag(level)
      return nil if level.nil?
      name = level.to_s.downcase if level.is_a?(String) || level.is_a?(Symbol)
      case name
      when "trace" then "-vvv"
      when "debug" then "-vv"
      when "info"  then "-v"
      when "warn", "warning", "error" then nil
      else
        raise ArgumentError,
              "log_level must be one of: trace, debug, info, warn, error " \
              "(got #{level.inspect})"
      end
    end

    def self.config_to_args(config)
      return [] if config.nil? || config.empty?

      args = []
      config.each do |key, value|
        key = key.to_s
        check_config_key(key)

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
      log_level: nil,
      mode: nil,
      license: nil,
      client: nil,
      config_file: nil,
      config: {},
      extra_args: [],
      silent: false,
      mesh: false,
      mesh_tag: nil,
      disable_proxy_cache: false,
      disable_sqloptimize: false,
      disable_auto_indexes: false,
      **unknown
    )
      self.class.reject_unknown_options(unknown)
      @upstream = upstream
      # Without an explicit proxy_port the registry (Proxy.start /
      # Proxy.acquire) moves this proxy to the first free port pair.
      @proxy_port_explicit = !proxy_port.nil?
      @proxy_port = proxy_port || DEFAULT_PROXY_PORT

      # Dashboard port defaults to proxy_port + 1 when unset. An explicit
      # value (including 0 for "disable dashboard") overrides the derivation.
      @dashboard_port_explicit = !dashboard_port.nil?
      @dashboard_port = @dashboard_port_explicit ? dashboard_port.to_i : @proxy_port + 1

      self.class.log_level_to_verbose_flag(log_level) # raises when invalid
      @log_level = log_level
      @mode = mode
      @license = license
      @client = client
      @config_file = config_file

      # Validate structured-config keys eagerly so a test that constructs
      # without spawning still catches bad keys. nil (the Rails integration
      # passes database.yml's absent `config:` straight through) means none.
      config ||= {}
      config.each_key { |k| self.class.check_config_key(k.to_s) }
      @config = config
      @extra_args = extra_args || []
      @silent = silent ? true : false
      # Mesh membership (startup intent — HQ enforces license).
      @mesh = mesh ? true : false
      tag = mesh_tag.to_s
      @mesh_tag = tag.empty? ? nil : tag
      # Top-level disable flags promoted out of the structured config map.
      # Each maps 1:1 to a CLI flag on the spawned binary.
      @disable_proxy_cache = disable_proxy_cache ? true : false
      @disable_sqloptimize = disable_sqloptimize ? true : false
      @disable_auto_indexes = disable_auto_indexes ? true : false
      @pid = nil
      @exit_status = nil
      @url = nil
      @dashboard_url = nil
      @dashboard_token = nil
      @stderr_reader = nil
      @holders = 0
      @stopped = false
    end

    def proxy_port_explicit?
      @proxy_port_explicit
    end

    def dashboard_port_explicit?
      @dashboard_port_explicit
    end

    # Set by the registry, under its mutex, when no proxy_port was given.
    # A derived dashboard port follows the proxy port.
    def assign_proxy_port(port)
      @proxy_port = port
      @dashboard_port = port + 1 unless @dashboard_port_explicit
    end

    # True once the proxy was stopped or its process exited. Before start
    # it is false, so a proxy being started still claims its ports.
    def dead?
      @stopped || (!@pid.nil? && !running?)
    end

    # Whether the app's URL keeps the upstream TLS parameters: only when the
    # proxy itself accepts TLS from the app.
    def client_tls?
      return true if @config.any? { |k, v| %w[tls_cert tls_key].include?(k.to_s) && v }
      @extra_args.any? { |a| a.to_s.start_with?("--tls-cert", "--tls-key") }
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
      verbose_flag = self.class.log_level_to_verbose_flag(@log_level)
      cmd.push(verbose_flag) if verbose_flag
      cmd.push("--mode", @mode) if @mode
      cmd.push("--license", @license) if @license
      cmd.push("--client", @client) if @client
      cmd.push("--config", @config_file) if @config_file
      cmd.push("--mesh") if @mesh
      cmd.push("--mesh-tag", @mesh_tag) if @mesh_tag
      cmd.push("--disable-proxy-cache") if @disable_proxy_cache
      cmd.push("--disable-sqloptimize") if @disable_sqloptimize
      cmd.push("--disable-auto-indexes") if @disable_auto_indexes
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
      # Someone already listening on the proxy port would answer the
      # readiness check in our proxy's place. The proxy refuses a port in
      # use, so then wait for it to exit and report why.
      port_taken = !self.class.port_free?(@proxy_port)

      stderr_read, stderr_write = IO.pipe
      @exit_status = nil
      @stopped = false
      @pid = Process.spawn(env, *cmd,
        in: File::NULL,
        out: File::NULL,
        err: stderr_write)
      stderr_write.close
      @stderr_reader = stderr_read

      # Ready means the port answers AND our child is still alive: another
      # process answering on the port (or our child exiting because the port
      # is taken) is a failed start, not a ready one.
      if port_taken
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + STARTUP_TIMEOUT
        sleep STARTUP_POLL_INTERVAL while running? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        ready = false
      else
        ready = self.class.wait_for_port("127.0.0.1", @proxy_port, STARTUP_TIMEOUT) { running? }
      end
      unless ready && running?
        exited = !running?
        unless exited
          Process.kill("KILL", @pid) rescue Errno::ESRCH
          Process.wait(@pid) rescue Errno::ECHILD
        end
        stderr_tail = stderr_read.read.to_s.lines.last(20).join
        stderr_read.close
        status = @exit_status
        @pid = nil
        @exit_status = nil
        @stderr_reader = nil
        if exited
          how = if status&.exitstatus then " with status #{status.exitstatus}"
                elsif status&.termsig then " on signal #{status.termsig}"
                end
          raise "Gold Lapel exited#{how} before it was ready on port #{@proxy_port}." \
                "\nstderr: #{stderr_tail}"
        end
        if port_taken
          raise "Gold Lapel could not start: port #{@proxy_port} is already in use." \
                "\nstderr: #{stderr_tail}"
        end
        raise "Gold Lapel failed to start on port #{@proxy_port} " \
              "within #{STARTUP_TIMEOUT}s.\nstderr: #{stderr_tail}"
      end

      @stderr_reader.close
      @stderr_reader = nil
      @url = self.class.make_proxy_url(@upstream, @proxy_port, strip_tls: !client_tls?)
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
      @stopped = true
      if @pid
        if running?
          begin
            Process.kill("TERM", @pid)
            Timeout.timeout(5) { Process.wait(@pid) }
          rescue Errno::ESRCH, Errno::ECHILD
            # Process already exited
          rescue Timeout::Error
            Process.kill("KILL", @pid) rescue Errno::ESRCH
            Process.wait(@pid) rescue Errno::ECHILD
          end
        end
        @stderr_reader&.close rescue IOError
        @pid = nil
        @exit_status = nil
        @url = nil
        @dashboard_url = nil
        @dashboard_token = nil
        @stderr_reader = nil
      end
    end

    # Reaps the child when it has exited (kill(0) would still succeed on an
    # unreaped zombie, so a crashed proxy would read as running forever).
    def running?
      return false if @pid.nil? || @exit_status
      return true unless Process.waitpid(@pid, Process::WNOHANG)
      @exit_status = $?
      false
    rescue Errno::ECHILD
      false
    end

    # Reference count of the holders sharing this proxy (each GoldLapel.start
    # instance and each start_proxy call). Changed under the registry mutex.
    def hold
      @holders += 1
    end

    def release_hold
      @holders -= 1 if @holders > 0
      @holders
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
    # Used to build the application_name marker on PG connections.
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
    # already has one (or PGAPPNAME is set in the env). The proxy passes it
    # through to Postgres untouched, so `pg_stat_activity` and ops dashboards
    # can see the wrapper/version mix; it does not change how the proxy
    # caches the connection. Idempotent and override-respecting.
    def self.inject_application_name(url)
      return url if url =~ /[?&]application_name=/
      return url if ENV["PGAPPNAME"] && !ENV["PGAPPNAME"].empty?
      sep = url.include?("?") ? "&" : "?"
      "#{url}#{sep}application_name=#{application_name_marker}"
    end

    # `strip_tls` drops the upstream TLS/GSS parameters (UPSTREAM_TLS_PARAMS)
    # from the query; Proxy#start passes false when the proxy accepts TLS
    # from the app.
    def self.make_proxy_url(upstream, port, strip_tls: true)
      # pg URL with explicit port
      if upstream =~ /\A(postgres(?:ql)?:\/\/(?:.*@)?)([^:\/?#]+):(\d+)(.*)\z/
        rest = strip_tls ? strip_tls_params($4) : $4
        return inject_application_name("#{$1}localhost:#{port}#{rest}")
      end
      # pg URL without port
      if upstream =~ /\A(postgres(?:ql)?:\/\/(?:.*@)?)([^:\/?#]+)(.*)\z/
        rest = strip_tls ? strip_tls_params($3) : $3
        return inject_application_name("#{$1}localhost:#{port}#{rest}")
      end
      # bare host:port (guard against scheme colons).
      # Bare-host form skips the marker — atypical caller path.
      if !upstream.include?("://") && upstream.include?(":")
        return "localhost:#{port}"
      end
      # bare host
      "localhost:#{port}"
    end

    # `rest` is the part of a URL after host:port ("/db?a=1&b=2").
    def self.strip_tls_params(rest)
      path, query = rest.split("?", 2)
      return rest if query.nil?
      kept = query.split("&").reject do |pair|
        UPSTREAM_TLS_PARAMS.include?(pair.split("=", 2).first.downcase)
      end
      kept.empty? ? path : "#{path}?#{kept.join('&')}"
    end

    # Polls until `port` accepts a connection. With a block, gives up early
    # (false) as soon as the block returns false — Proxy#start passes one
    # that checks its child is still alive.
    def self.wait_for_port(host, port, timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        return false if block_given? && !yield
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

    # Whether `port` can be bound right now — the same bind the proxy does
    # before it starts (0.0.0.0, SO_REUSEADDR, no SO_REUSEPORT).
    def self.port_free?(port)
      sock = Socket.new(:INET, :STREAM)
      sock.setsockopt(:SOCKET, :REUSEADDR, true)
      sock.bind(Addrinfo.tcp("0.0.0.0", port))
      true
    rescue SystemCallError
      false
    ensure
      sock&.close
    end

    # `url` with the password in its userinfo replaced by `***`, for errors.
    def self.redact_password(url)
      url.sub(%r{\A([^:/?#]+://[^:/?#@]*:)[^/?#]*@}, '\1***@')
    end

    # Module-level multi-instance registry keyed by upstream URL: one proxy
    # per upstream per process, shared by everything that starts it.
    # Proxies started without a proxy_port are given the smallest port
    # P >= 7932 such that neither P nor its dashboard port is held by another
    # live proxy of this process or bound by anything else on the machine.
    @instances = {}
    @mutex = Mutex.new
    @cleanup_registered = false

    class << self
      # Low-level entry point — spawns a Proxy (or reuses the running one for
      # this upstream) and returns the proxy URL string. Does NOT open a PG
      # connection or wrap it. Used by `GoldLapel.start_proxy`, the Rails
      # integration and tests. Each call holds the proxy until
      # `GoldLapel.stop(upstream)` or process exit.
      def start(upstream, **options)
        acquire(upstream, **options).url
      end

      # Returns the running proxy for `upstream`, starting one if there is
      # none, and counts the caller as a holder — `release` it when done.
      # A running proxy is reused as is: options given here only apply when a
      # new proxy is started.
      def acquire(upstream, **options)
        # Constructing validates the options, so a bad one raises even when
        # the running proxy is reused.
        proxy = Proxy.new(upstream, **options)
        @mutex.synchronize do
          existing = @instances[upstream]
          if existing && !existing.dead?
            existing.hold
            return existing
          end
          @instances.delete(upstream)&.stop

          unless @cleanup_registered
            at_exit { cleanup }
            @cleanup_registered = true
          end
          claim_port(proxy)
          @instances[upstream] = proxy
          begin
            proxy.start
          rescue Exception # rubocop:disable Lint/RescueException
            @instances.delete(upstream) if @instances[upstream].equal?(proxy)
            proxy.stop
            raise
          end
          proxy.hold
          proxy
        end
      end

      # Drops one holder; stops the proxy when it was the last.
      def release(proxy)
        @mutex.synchronize do
          next if proxy.release_hold > 0
          @instances.delete(proxy.upstream) if @instances[proxy.upstream].equal?(proxy)
          proxy.stop
        end
      end

      # Stops the proxy for `upstream` (every proxy without one) whoever
      # still holds it.
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

      # Caller holds @mutex. An explicit port another live proxy of this
      # process listens on is an error: the proxy would refuse it anyway, and
      # before it could report that, a readiness check would pass against the
      # other proxy. Without a proxy_port, picks the first pair that is
      # neither claimed nor bound by another process.
      def claim_port(proxy)
        claimed = {}
        @instances.each_value do |other|
          next if other.equal?(proxy) || other.dead?
          claimed[other.proxy_port] = [other.upstream, "proxy"]
          claimed[other.dashboard_port] = [other.upstream, "dashboard"] if other.dashboard_port > 0
        end

        if proxy.proxy_port_explicit?
          check_unclaimed(claimed, proxy.proxy_port, "proxy")
          check_unclaimed(claimed, proxy.dashboard_port, "dashboard") if proxy.dashboard_port > 0
          return
        end
        if proxy.dashboard_port_explicit? && proxy.dashboard_port > 0
          check_unclaimed(claimed, proxy.dashboard_port, "dashboard")
        end

        # An explicit dashboard port is fixed, so only the proxy port has to
        # step around it; a derived one moves with the proxy port.
        (DEFAULT_PROXY_PORT...65535).each do |port|
          next if claimed.key?(port)
          if proxy.dashboard_port_explicit?
            next if port == proxy.dashboard_port || !port_free?(port)
          else
            next if claimed.key?(port + 1)
            next unless port_free?(port) && port_free?(port + 1)
          end
          proxy.assign_proxy_port(port)
          return
        end
        raise "Gold Lapel could not find a free proxy port"
      end

      def check_unclaimed(claimed, port, role)
        return unless claimed.key?(port)
        upstream, held_as = claimed[port]
        raise ArgumentError,
              "Gold Lapel cannot use port #{port} as the #{role} port: this " \
              "process's proxy for #{redact_password(upstream)} already holds " \
              "it as its #{held_as} port. Choose another port, or omit " \
              "proxy_port and dashboard_port to have a free pair assigned."
      end

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
