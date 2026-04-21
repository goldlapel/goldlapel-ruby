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

Fiber-aware async via `GoldLapel::Async.start`, scoped connections via `gl.using(conn) { ... }`, and Rails auto-wiring are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.dashboard_url`:

```ruby
puts gl.dashboard_url
# -> http://127.0.0.1:7933
```

## Documentation

Full API reference, async usage, configuration, Rails integration, upgrading from v0.1, and production deployment: https://goldlapel.com/docs/ruby

## License

MIT. See `LICENSE`.
