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
  # returns a GoldLapel::Instance with all wrapper methods attached.
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
  #   gl = GoldLapel.start("postgresql://localhost/mydb", proxy_port: 7932)
  #   hits = gl.search("articles", "body", "postgres tuning")
  #   PG.connect(gl.url) { |conn| conn.exec("SELECT ...") }
  #   gl.stop
  def self.start(
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
    mesh_tag: nil,
    enable_l2_for_wrappers: false
  )
    Instance.new(
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
      eager_connect: true,
      silent: silent,
      mesh: mesh,
      mesh_tag: mesh_tag,
      enable_l2_for_wrappers: enable_l2_for_wrappers,
    )
  end

  def self.new(
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
    mesh_tag: nil,
    enable_l2_for_wrappers: false
  )
    # Legacy/advanced: construct without eagerly spawning or connecting.
    Instance.new(
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
      eager_connect: false,
      silent: silent,
      mesh: mesh,
      mesh_tag: mesh_tag,
      enable_l2_for_wrappers: enable_l2_for_wrappers,
    )
  end

  # Lower-level helpers (still supported for plugin/adapter code)

  def self.start_proxy(
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
    mesh_tag: nil,
    enable_l2_for_wrappers: false
  )
    Proxy.start(
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
      enable_l2_for_wrappers: enable_l2_for_wrappers,
    )
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
