# frozen_string_literal: true

require "open3"
require "socket"
require "rbconfig"

module GoldLapel
  DEFAULT_PORT = 7932
  DEFAULT_DASHBOARD_PORT = 7933
  STARTUP_TIMEOUT = 10.0
  STARTUP_POLL_INTERVAL = 0.05

  class Proxy
    attr_reader :url, :upstream, :dashboard_url

    VALID_CONFIG_KEYS = %w[
      mode min_pattern_count refresh_interval_secs pattern_ttl_secs
      max_tables_per_view max_columns_per_view deep_pagination_threshold
      report_interval_secs result_cache_size batch_cache_size
      batch_cache_ttl_secs pool_size pool_timeout_secs
      pool_mode mgmt_idle_timeout fallback read_after_write_secs
      n1_threshold n1_window_ms n1_cross_threshold
      tls_cert tls_key tls_client_ca config dashboard_port
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

    def initialize(upstream, port: nil, config: {}, extra_args: [])
      @upstream = upstream
      @port = port || DEFAULT_PORT
      @config = config
      @extra_args = extra_args
      @pid = nil
      @url = nil
      @dashboard_url = nil
      @stderr_reader = nil

      @dashboard_port = if config.key?(:dashboard_port) || config.key?("dashboard_port")
        config.fetch(:dashboard_port, config.fetch("dashboard_port", DEFAULT_DASHBOARD_PORT)).to_i
      else
        DEFAULT_DASHBOARD_PORT
      end
    end

    def start
      return @url if running?

      binary = self.class.find_binary
      cmd = [
        binary,
        "--upstream", @upstream,
        "--port", @port.to_s,
        *self.class.config_to_args(@config),
        *@extra_args,
      ]

      env = ENV.to_h
      env["GOLDLAPEL_CLIENT"] ||= "ruby"
      stdin, stdout, stderr, wait_thr = Open3.popen3(env, *cmd)
      stdin.close
      stdout.close
      @pid = wait_thr.pid
      @stderr_reader = stderr
      @wait_thr = wait_thr

      unless self.class.wait_for_port("127.0.0.1", @port, STARTUP_TIMEOUT)
        Process.kill("KILL", @pid) rescue Errno::ESRCH
        @wait_thr.join rescue nil
        stderr_output = stderr.read
        stderr.close
        @pid = nil
        @wait_thr = nil
        @stderr_reader = nil
        raise "Gold Lapel failed to start on port #{@port} " \
              "within #{STARTUP_TIMEOUT}s.\nstderr: #{stderr_output}"
      end

      @stderr_reader.close
      @stderr_reader = nil
      @url = self.class.make_proxy_url(@upstream, @port)
      @dashboard_url = @dashboard_port > 0 ? "http://127.0.0.1:#{@dashboard_port}" : nil

      if @dashboard_port > 0
        puts "goldlapel → :#{@port} (proxy) | http://127.0.0.1:#{@dashboard_port} (dashboard)"
      else
        puts "goldlapel → :#{@port} (proxy)"
      end

      @url
    end

    def stop
      if @pid
        begin
          Process.kill("TERM", @pid)
          unless @wait_thr.join(5)
            Process.kill("KILL", @pid) rescue Errno::ESRCH
            @wait_thr.join(5) rescue nil
          end
        rescue Errno::ESRCH
          # Process already exited
        end
        @stderr_reader&.close rescue IOError
        @pid = nil
        @url = nil
        @dashboard_url = nil
        @wait_thr = nil
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

    def self.make_proxy_url(upstream, port)
      # pg URL with explicit port
      if upstream =~ /\A(postgres(?:ql)?:\/\/(?:.*@)?)([^:\/?#]+):(\d+)(.*)\z/
        return "#{$1}localhost:#{port}#{$4}"
      end
      # pg URL without port
      if upstream =~ /\A(postgres(?:ql)?:\/\/(?:.*@)?)([^:\/?#]+)(.*)\z/
        return "#{$1}localhost:#{port}#{$3}"
      end
      # bare host:port (guard against scheme colons)
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
      def start(upstream, port: nil, config: {}, extra_args: [])
        @mutex.synchronize do
          existing = @instances[upstream]
          if existing&.running?
            return existing.url
          end

          proxy = Proxy.new(upstream, port: port, config: config, extra_args: extra_args)
          unless @cleanup_registered
            at_exit { cleanup }
            @cleanup_registered = true
          end
          url = proxy.start
          @instances[upstream] = proxy
          url
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
