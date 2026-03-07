# frozen_string_literal: true

require "open3"
require "socket"
require "rbconfig"

module GoldLapel
  DEFAULT_PORT = 7932
  STARTUP_TIMEOUT = 10.0
  STARTUP_POLL_INTERVAL = 0.05

  class Proxy
    attr_reader :url, :upstream

    def initialize(upstream, port: nil, extra_args: [])
      @upstream = upstream
      @port = port || DEFAULT_PORT
      @extra_args = extra_args
      @pid = nil
      @url = nil
      @stderr_reader = nil
    end

    def start
      return @url if running?

      binary = self.class.find_binary
      cmd = [
        binary,
        "--upstream", @upstream,
        "--port", @port.to_s,
        *@extra_args,
      ]

      stdin, stdout, stderr, wait_thr = Open3.popen3(*cmd)
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

    # Module-level singleton
    @instance = nil
    @cleanup_registered = false

    class << self
      def start(upstream, port: nil, extra_args: [])
        if @instance&.running?
          if @instance.upstream != upstream
            raise "Gold Lapel is already running for a different upstream. " \
                  "Call GoldLapel.stop before starting with a new upstream."
          end
          return @instance.url
        end

        @instance = Proxy.new(upstream, port: port, extra_args: extra_args)
        unless @cleanup_registered
          at_exit { cleanup }
          @cleanup_registered = true
        end
        @instance.start
      end

      def stop
        if @instance
          @instance.stop
          @instance = nil
        end
      end

      def proxy_url
        @instance&.url
      end

      private

      def cleanup
        if @instance
          @instance.stop
          @instance = nil
        end
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
