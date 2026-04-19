# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
gem install goldlapel
```

Or add to your Gemfile:

```ruby
gem "goldlapel"
gem "pg"   # required driver
```

## Quick Start

```ruby
require "goldlapel"
require "pg"

# Factory — spawns the proxy, opens an internal connection, returns an instance.
gl = GoldLapel.start("postgresql://user:pass@localhost:5432/mydb",
                     port: 7932,
                     log_level: "info")

# Raw SQL through the proxy using the `pg` gem:
conn = PG.connect(gl.url)
conn.exec("SELECT * FROM users WHERE id = $1", [42])

# High-level wrapper methods run against the internal connection:
hits = gl.search("articles", "body", "postgres tuning")
gl.doc_insert("events", { type: "signup" })

# Stop when done (or let at_exit handle it).
gl.stop
```

## Scoped connection: `gl.using(conn) { ... }`

For transactions or request-scoped pools, use the block form. Inside the block, every wrapper method uses `conn` instead of the internal connection. The scope unwinds cleanly on exceptions (fiber-local, so it composes with `async` too).

```ruby
conn = PG.connect(gl.url)
conn.transaction do |tx_conn|
  gl.using(tx_conn) do |gl|
    gl.doc_insert("events", { type: "order.created" })
    gl.incr("counters", "orders_today")
    # All wrapper calls above use tx_conn — one atomic transaction.
  end
end
```

You can also pass `conn:` explicitly to any method:

```ruby
gl.doc_insert("events", { type: "x" }, conn: my_conn)
```

## Async

For fiber-based concurrency via the `async` gem:

```ruby
require "async"
require "goldlapel/async"

Async do
  gl = GoldLapel::Async.start("postgresql://user:pass@localhost/mydb")
  hits = gl.search("articles", "body", "query")
  gl.stop
end
```

The `async` gem is an optional dependency — install it separately (`gem install async`).

Under the hood, `GoldLapel::Async::Instance` routes every wrapper call through a parallel utility layer (`GoldLapel::Async::Utils`) that uses `pg`'s native non-blocking method variants: `async_exec_params`, `async_exec`, and `wait_for_notify` (which is already fiber-scheduler aware). Inside an `Async { ... }` block, Postgres IO yields cooperatively via Ruby's Fiber scheduler — other fibers continue to run while a query is in flight. Same `pg` gem, same `PG::Result` return shapes, same error classes; the only change versus the sync path is that the call sites explicitly ask for the non-blocking variants.

## API Reference

### `GoldLapel.start(upstream, port:, log_level:, config:, extra_args:)`

Spawns the proxy, opens the internal Postgres connection, and returns a `GoldLapel::Instance`. This is the primary entry point.

- `upstream` — Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `port` — proxy port (default: 7932)
- `log_level` — optional, one of `trace`, `debug`, `info`, `warn`, `error` (translated to the proxy's `-v/-vv/-vvv` verbosity flag; `warn` and `error` use the binary default and add no flag)
- `config` — hash of proxy config (see [Configuration](#configuration))
- `extra_args` — additional CLI flags for the binary

### `gl.url`

Proxy connection string. Feed to `PG.connect` for raw SQL access.

### `gl.stop`

Stops the proxy and closes the internal connection. Also runs automatically on process exit.

### `gl.using(conn) { |gl| ... }`

Scopes the block to use `conn` for every wrapper method call inside. Fiber-local; unwinds on normal return and on exception. Nesting is supported.

### `gl.dashboard_url`

Dashboard URL (e.g. `http://127.0.0.1:7933`), or `nil` if not running. Defaults to `proxy_port + 1`, configurable via `config: { dashboard_port: 9090 }` or disabled with `dashboard_port: 0`.

### `GoldLapel.config_keys`

Array of all valid configuration key names (as strings).

### Wrapper methods

Every method accepts an optional `conn:` kwarg. When omitted, the active connection (from `gl.using`, or the internal one) is used.

- **Documents** — `doc_insert`, `doc_insert_many`, `doc_find`, `doc_find_cursor`, `doc_find_one`, `doc_update`, `doc_update_one`, `doc_delete`, `doc_delete_one`, `doc_count`, `doc_find_one_and_update`, `doc_find_one_and_delete`, `doc_distinct`, `doc_create_index`, `doc_aggregate`, `doc_watch`, `doc_unwatch`, `doc_create_ttl_index`, `doc_remove_ttl_index`, `doc_create_collection`, `doc_create_capped`, `doc_remove_cap`
- **Search** — `search`, `search_fuzzy`, `search_phonetic`, `similar`, `suggest`, `facets`, `aggregate`, `create_search_config`, `analyze`, `explain_score`
- **Pub/sub** — `publish`, `subscribe`
- **Queue** — `enqueue`, `dequeue`
- **Counters** — `incr`, `get_counter`, `count_distinct`
- **Hash** — `hset`, `hget`, `hgetall`, `hdel`
- **Sorted set** — `zadd`, `zincrby`, `zrange`, `zrank`, `zscore`, `zrem`
- **Geo** — `geoadd`, `georadius`, `geodist`
- **Streams** — `stream_add`, `stream_create_group`, `stream_read`, `stream_ack`, `stream_claim`
- **Percolate** — `percolate_add`, `percolate`, `percolate_delete`
- **Scripting** — `script`

## Configuration

Pass a config hash to tune the proxy:

```ruby
gl = GoldLapel.start("postgresql://user:pass@localhost/mydb", config: {
  mode: "waiter",
  pool_size: 50,
  disable_matviews: true,
  replica: ["postgresql://user:pass@replica1/mydb"],
})
```

Keys use `snake_case` (symbols or strings) and map to CLI flags (`pool_size` → `--pool-size`). Boolean keys are flags — `true` enables them, `false` omits. Array keys produce repeated flags. Unknown keys raise `ArgumentError`. See `GoldLapel.config_keys` for the full list.

You can also pass raw CLI flags via `extra_args`, or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Rails integration

The gem auto-wires into ActiveRecord's PostgreSQL adapter when loaded inside a Rails app. Add `gem "goldlapel"` to your `Gemfile` and optionally configure in `database.yml`:

```yaml
production:
  adapter: postgresql
  host: db.example.com
  # ...
  goldlapel:
    port: 7932
    config:
      mode: waiter
      pool_size: 50
```

Rails will route its connections through the proxy transparently. If the proxy fails to start, Rails falls back to the direct connection and logs a warning.

## How It Works

This gem bundles the Gold Lapel Rust binary for your platform. When you call `GoldLapel.start`, it:

1. Locates the binary (bundled in gem, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the proxy port to be ready
4. Opens an internal PG connection through the proxy
5. Returns an instance wrapping that connection, with the L1 native cache enabled
6. Cleans up automatically on process exit

The binary does all the heavy lifting — this wrapper just manages its lifecycle and exposes the Ruby-idiomatic API.

## Upgrading from v0.1.x

v0.2.0 is a breaking change. The old instance-form API:

```ruby
# OLD (v0.1)
gl = GoldLapel::GoldLapel.new(url)
gl.start
gl.doc_insert(conn, "events", { ... })
```

is replaced by the factory form:

```ruby
# NEW (v0.2)
gl = GoldLapel.start(url)                    # spawns + connects eagerly
gl.doc_insert("events", { ... })             # no conn arg — uses internal
gl.doc_insert("events", { ... }, conn: c)    # or pass one explicitly
gl.using(c) { |gl| gl.doc_insert(...) }      # or scope a block
```

No deprecation shim — please update call sites.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
