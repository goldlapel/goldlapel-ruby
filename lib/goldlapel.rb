# frozen_string_literal: true

require_relative "goldlapel/cache"
require_relative "goldlapel/wrap"
require_relative "goldlapel/proxy"
require_relative "goldlapel/utils"
require_relative "goldlapel/instance"

module GoldLapel
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
  def self.start(upstream, port: nil, log_level: nil, config: {}, extra_args: [])
    # Merge log_level into config/extra_args if provided
    extra = extra_args.dup
    if log_level
      extra.push("--log-level", log_level.to_s)
    end
    Instance.new(upstream, port: port, config: config, extra_args: extra, eager_connect: true)
  end

  def self.new(upstream, port: nil, config: {}, extra_args: [])
    # Legacy/advanced: construct without eagerly spawning or connecting.
    Instance.new(upstream, port: port, config: config, extra_args: extra_args, eager_connect: false)
  end

  # Lower-level helpers (still supported for plugin/adapter code)

  def self.start_proxy(upstream, port: nil, config: {}, extra_args: [])
    Proxy.start(upstream, port: port, config: config, extra_args: extra_args)
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
