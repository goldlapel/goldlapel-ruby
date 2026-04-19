# frozen_string_literal: true

require_relative "goldlapel/cache"
require_relative "goldlapel/wrap"
require_relative "goldlapel/proxy"
require_relative "goldlapel/utils"
require_relative "goldlapel/instance"

module GoldLapel
  # Map `log_level` strings → the proxy binary's count-based `-v` flag.
  #
  # The Rust proxy CLI uses `-v` / `-vv` / `-vvv` (clap's `ArgAction::Count`)
  # rather than `--log-level <value>`. We accept the friendlier string names
  # here and translate. Invalid values raise loudly instead of producing a
  # cryptic "unknown argument" error from the spawned binary.
  LOG_LEVELS = %w[trace debug info warn warning error].freeze

  def self.log_level_to_args(log_level)
    return [] if log_level.nil?

    level = log_level.to_s.downcase
    unless LOG_LEVELS.include?(level)
      raise ArgumentError,
            "log_level must be one of: trace, debug, info, warn, error " \
            "(got #{log_level.inspect})"
    end

    case level
    when "trace"           then ["-vvv"]
    when "debug"           then ["-vv"]
    when "info"            then ["-v"]
    when "warn", "warning" then []  # default verbosity
    when "error"           then []  # default verbosity
    end
  end

  # v0.2.0 factory API — the primary entry point.
  #
  # Spawns the Gold Lapel binary, opens an internal Postgres connection, and
  # returns a Goldlapel::GoldLapel instance with all wrapper methods attached.
  #
  # The returned instance responds to:
  #   - `gl.url`        — proxy connection string (use with PG.connect for raw SQL)
  #   - `gl.stop`       — stop the proxy + close the internal connection
  #   - `gl.using(conn) { |gl| ... }` — scope a block to a caller-supplied connection
  #   - All ~54 wrapper methods (doc_insert, search, hset, zadd, ...)
  #
  # Each wrapper method accepts a `conn:` kwarg; when nil, the internal
  # connection (or the scoped `using` connection) is used.
  #
  # Example:
  #   gl = Goldlapel.start("postgresql://localhost/mydb", port: 7932)
  #   hits = gl.search("articles", "body", "postgres tuning")
  #   PG.connect(gl.url) { |conn| conn.exec("SELECT ...") }
  #   gl.stop
  def self.start(upstream, port: nil, log_level: nil, config: {}, extra_args: [], silent: false)
    # Translate log_level → `-v` count flag on the spawned proxy CLI.
    extra = extra_args.dup
    extra.concat(log_level_to_args(log_level))
    # `silent` is a wrapper-only option (suppresses the startup banner). It is
    # deliberately passed as a separate kwarg and NOT merged into `config`, so
    # it can never leak to the Rust binary as a `--silent` CLI flag.
    Instance.new(upstream, port: port, config: config, extra_args: extra, eager_connect: true, silent: silent)
  end

  def self.new(upstream, port: nil, config: {}, extra_args: [], silent: false)
    # Legacy/advanced: construct without eagerly spawning or connecting.
    Instance.new(upstream, port: port, config: config, extra_args: extra_args, eager_connect: false, silent: silent)
  end

  # Lower-level helpers (still supported for plugin/adapter code)

  def self.start_proxy(upstream, port: nil, config: {}, extra_args: [], silent: false)
    Proxy.start(upstream, port: port, config: config, extra_args: extra_args, silent: silent)
  end

  def self.stop(upstream = nil)
    Proxy.stop(upstream)
  end

  def self.proxy_url(upstream = nil)
    Proxy.proxy_url(upstream)
  end

  def self.dashboard_url(upstream = nil)
    Proxy.dashboard_url(upstream)
  end

  def self.config_keys
    Proxy.config_keys
  end
end

# Alias the camel-case name to the snake-style module so existing integrations
# (goldlapel/rails, 3rd-party plugins) can `require "goldlapel"` and either
# `GoldLapel.start(...)` or `Goldlapel.start(...)` — matches the PR spec.
Goldlapel = GoldLapel unless defined?(Goldlapel)
