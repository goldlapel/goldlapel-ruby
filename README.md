# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
gem install goldlapel
```

Or add to your Gemfile:

```ruby
gem "goldlapel"
```

## Quick Start

```ruby
require "goldlapel"

# Start the proxy — returns a connection string pointing at Gold Lapel
url = GoldLapel.start("postgresql://user:pass@localhost:5432/mydb")

# Use the URL with any Postgres driver
require "pg"
conn = PG.connect(url)

# Or Sequel, ActiveRecord, ROM — anything that speaks Postgres
```

Gold Lapel is driver-agnostic. `start` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

## API

### `GoldLapel.start(upstream, port: nil, config: {}, extra_args: [])`

Starts the Gold Lapel proxy and returns the proxy connection string.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `port` — proxy port (default: 7932)
- `config` — configuration hash (see [Configuration](#configuration) below)
- `extra_args` — additional CLI flags passed to the binary (e.g. `["--threshold-impact", "5000"]`)

### `GoldLapel.stop`

Stops the proxy. Also called automatically on process exit.

### `GoldLapel.proxy_url`

Returns the current proxy URL, or `nil` if not running.

### `GoldLapel.dashboard_url`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `nil` if not running. The dashboard port defaults to 7933 and can be configured via `config: { dashboard_port: 9090 }` or disabled with `dashboard_port: 0`.

### `GoldLapel.config_keys`

Returns an array of all valid configuration key names (as strings).

### `GoldLapel::Proxy.new(upstream, port: nil, config: {}, extra_args: [])`

Class interface for managing multiple instances:

```ruby
proxy = GoldLapel::Proxy.new("postgresql://user:pass@localhost:5432/mydb", port: 7932)
url = proxy.start
# ...
proxy.stop
```

## Configuration

Pass a config hash to configure the proxy:

```ruby
require "goldlapel"

url = GoldLapel.start("postgresql://user:pass@localhost/mydb", config: {
  mode: "butler",
  pool_size: 50,
  disable_matviews: true,
  replica: ["postgresql://user:pass@replica1/mydb"],
})
```

Keys use `snake_case` (symbols or strings) and map to CLI flags (`pool_size` -> `--pool-size`). Boolean keys are flags -- `true` enables them. Array keys produce repeated flags.

Unknown keys raise `ArgumentError`. To see all valid keys:

```ruby
GoldLapel.config_keys
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also pass raw CLI flags via `extra_args`, or set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) -- the binary reads them automatically.

## How It Works

This gem bundles the Gold Lapel Rust binary for your platform. When you call `start`, it:

1. Locates the binary (bundled in gem, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy
5. Cleans up automatically on process exit

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
