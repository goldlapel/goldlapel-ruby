# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-ruby/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-ruby/actions/workflows/test.yml)

The Ruby wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Point `pg` at `gl.url`. Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Other namespaces (`gl.search`, `gl.publish` / `gl.subscribe`, `gl.incr`, `gl.zadd`, `gl.hset`, `gl.geoadd`, …) remain flat for now and will migrate to nested form in subsequent releases.

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

Before removing the package, drop Gold Lapel's helper schema and cached matviews from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
gem uninstall goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and cached matviews go away.

## License

MIT. See `LICENSE`.
