# frozen_string_literal: true

require_relative "goldlapel/proxy"
require_relative "goldlapel/utils"
require_relative "goldlapel/instance"

module GoldLapel
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
    Instance.new(
      upstream,
      proxy_port: proxy_port,
      dashboard_port: dashboard_port,
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
      disable_proxy_cache: disable_proxy_cache,
      disable_sqloptimize: disable_sqloptimize,
      disable_auto_indexes: disable_auto_indexes,
      **unknown,
    )
  end

  def self.new(
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
    # Legacy/advanced: construct without eagerly spawning or connecting.
    Instance.new(
      upstream,
      proxy_port: proxy_port,
      dashboard_port: dashboard_port,
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
      disable_proxy_cache: disable_proxy_cache,
      disable_sqloptimize: disable_sqloptimize,
      disable_auto_indexes: disable_auto_indexes,
      **unknown,
    )
  end

  # Lower-level helpers (still supported for plugin/adapter code)

  def self.start_proxy(
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
    Proxy.start(
      upstream,
      proxy_port: proxy_port,
      dashboard_port: dashboard_port,
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
      disable_proxy_cache: disable_proxy_cache,
      disable_sqloptimize: disable_sqloptimize,
      disable_auto_indexes: disable_auto_indexes,
      **unknown,
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
