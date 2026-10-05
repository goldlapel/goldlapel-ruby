# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-ruby/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-ruby/actions/workflows/test.yml)

The Ruby wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that caches query results, creates indexes from the query patterns it sees, and optimizes SQL on the way through. Zero code changes beyond the connection string.

The gem gives you:

- **The proxy as a managed subprocess.** `GoldLapel.start` finds the binary, starts it (and stops it with your app), turns your options into proxy flags, and hands back a URL any Postgres driver can use. All caching happens in the proxy, so every connection through `gl.url` gets it — no in-process cache in the gem.
- **Postgres-backed helpers** — search and percolator, a Mongo-style document store, streams, counters, sorted sets, hashes, queues, geo, and pub/sub.
- **Rails integration** — `require "goldlapel/rails"` spawns the proxy and points ActiveRecord at it.

## Install

```bash
gem install goldlapel
```

Or in your `Gemfile`:

```ruby
gem "goldlapel"
gem "pg"   # required Postgres driver
```

## Quickstart

```ruby
require "goldlapel"
require "pg"

# Spawn the proxy in front of your upstream DB
gl = GoldLapel.start("postgresql://user:pass@localhost:5432/mydb")

# Point PG at gl.url
conn = PG.connect(gl.url)
conn.exec("SELECT * FROM users WHERE id = $1", [42])

gl.stop  # (also cleaned up automatically on process exit)
```

Point `pg` at `gl.url`. Gold Lapel sits between your app and your DB, caching results and indexing from the query patterns it sees. `gl.conn` is a plain `PG::Connection` to the proxy, if you'd rather not open your own.

The proxy listens on two ports: the proxy itself (`proxy_port`, default 7932) and the dashboard (`dashboard_port`, default proxy port + 1). Start several databases in one process without a `proxy_port` and each gets the next pair that is free — not used by another of your proxies or by anything else on the machine (Rails multi-database setups included); `gl.url` carries the port it got. A `proxy_port` that one of your other proxies already uses raises an error naming it; one some other program holds fails the start with the proxy's own "already in use" message.

Starting the same database again in one process shares the proxy that is already running; it stops when the last `gl` using it stops (or at exit). `GoldLapel.stop(url)` stops it regardless.

TLS settings in your URL (`sslmode`, `sslrootcert`, `channel_binding`, …) apply to the proxy's connection to Postgres; `gl.url` leaves them off, since your app talks to the proxy locally. They stay on when you give the proxy its own certificate (`config: { tls_cert:, tls_key: }`).

### Namespaces

Helper families live under nested sub-APIs:

```ruby
# Document store (Mongo-style API on top of JSONB)
gl.documents.insert("orders", { status: "pending", total: 99 })
gl.documents.find("orders", filter: { status: "pending" })
gl.documents.update_one("orders", { _id: id }, { "$set" => { status: "paid" } })

# Streams (Redis-style consumer groups on top of an append-only log)
gl.streams.add("events", { type: "click", url: "/" })
gl.streams.create_group("events", "workers")
gl.streams.read("events", "workers", "consumer-1", count: 10)
```

Each call routes through the proxy's DDL API on first use — Gold Lapel materializes the canonical table (`_goldlapel.doc_orders`, `_goldlapel.stream_events`) and hands back the query patterns. One HTTP round-trip per `(family, name)` per session.

The Redis-style families are nested the same way — `gl.counters`, `gl.zsets`, `gl.hashes`, `gl.queues`, `gl.geos`. Search, percolate and pub/sub (`gl.search`, `gl.publish` / `gl.subscribe`, …) are flat methods on `gl`.

Fiber-aware async via `GoldLapel::Async.start`, scoped connections via `gl.using(conn) { ... }`, and Rails auto-wiring are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.dashboard_url`:

```ruby
puts gl.dashboard_url
# -> http://127.0.0.1:7933
```

## Documentation

Full API reference, async usage, configuration, Rails integration, upgrading from v0.1, and production deployment: https://goldlapel.com/docs/ruby

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and the indexes it created from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
gem uninstall goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and the indexes it created go away.

## License

MIT. See `LICENSE`.
