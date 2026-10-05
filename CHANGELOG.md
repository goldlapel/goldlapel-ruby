# Changelog

## Unreleased

### Breaking changes

**The in-process cache (L1) is gone.** The proxy's result cache now caches
every client the same way, wrapper or not, so the gem no longer keeps its own
cache in front of `pg`. `gl.conn` (sync and `GoldLapel::Async`) is now the plain
`PG::Connection` to the proxy instead of a caching wrapper, and the Rails
integration no longer wraps ActiveRecord's connection or issues `DISCARD ALL`
on pool checkin — it spawns the proxy and rewrites host/port, nothing more.
Deleted with it: `GoldLapel.wrap`, `CachedConnection`, `NativeCache`,
`CachedResult`, the session-settings (GUC) tracker, the invalidation-socket
client and stats reporting to the proxy.

Removed options (no aliases — passing them raises `ArgumentError`):

- `invalidation_port:` — the proxy no longer serves an invalidation port; it
  listens on two ports, proxy and dashboard (proxy port + 1).
- `disable_native_cache:` and `aggressive_verify:` — both only configured the
  in-process cache.
- `disable_matviews:` — the proxy no longer builds materialized views.
- The matview tuning keys in the `config:` map: `refresh_interval_secs`,
  `pattern_ttl_secs`, `max_tables_per_view`, `max_columns_per_view`,
  `disable_consolidation`, `disable_rewrite`, `disable_shadow_mode`.
- The `GOLDLAPEL_NATIVE_CACHE`, `GOLDLAPEL_NATIVE_CACHE_SIZE` and
  `GOLDLAPEL_REPORT_STATS` environment variables are no longer read.
- `config: { enable_coalescing: … }` is now `disable_coalescing`, the only
  coalescing flag the proxy has (it rejected `--enable-coalescing`, so the old
  key stopped the proxy from starting).

In `database.yml`, the `goldlapel:` keys `invalidation_port`,
`disable_native_cache`, `aggressive_verify` and `disable_matviews` are ignored.

**Doc-store and stream methods moved under nested namespaces.** The flat
`gl.doc_*` and `gl.stream_*` methods are gone; document and stream operations
now live under `gl.documents.<verb>` and `gl.streams.<verb>`. No
backwards-compat aliases — search and replace once.

Migration map:

| Old (flat)                                  | New (nested)                                  |
| ------------------------------------------- | --------------------------------------------- |
| `gl.doc_insert(name, doc)`                  | `gl.documents.insert(name, doc)`              |
| `gl.doc_insert_many(name, docs)`            | `gl.documents.insert_many(name, docs)`        |
| `gl.doc_find(name, filter:)`                | `gl.documents.find(name, filter:)`            |
| `gl.doc_find_one(name, filter:)`            | `gl.documents.find_one(name, filter:)`        |
| `gl.doc_find_cursor(name, ...)`             | `gl.documents.find_cursor(name, ...)`         |
| `gl.doc_update(name, f, u)`                 | `gl.documents.update(name, f, u)`             |
| `gl.doc_update_one(name, f, u)`             | `gl.documents.update_one(name, f, u)`         |
| `gl.doc_delete(name, f)`                    | `gl.documents.delete(name, f)`                |
| `gl.doc_delete_one(name, f)`                | `gl.documents.delete_one(name, f)`            |
| `gl.doc_find_one_and_update(...)`           | `gl.documents.find_one_and_update(...)`       |
| `gl.doc_find_one_and_delete(...)`           | `gl.documents.find_one_and_delete(...)`       |
| `gl.doc_distinct(name, field, filter:)`     | `gl.documents.distinct(name, field, filter:)` |
| `gl.doc_count(name, filter:)`               | `gl.documents.count(name, filter:)`           |
| `gl.doc_create_index(name, keys:)`          | `gl.documents.create_index(name, keys:)`      |
| `gl.doc_aggregate(name, pipeline)`          | `gl.documents.aggregate(name, pipeline)`      |
| `gl.doc_watch(name, &block)`                | `gl.documents.watch(name, &block)`            |
| `gl.doc_unwatch(name)`                      | `gl.documents.unwatch(name)`                  |
| `gl.doc_create_ttl_index(name, field, ...)` | `gl.documents.create_ttl_index(name, field, ...)` |
| `gl.doc_remove_ttl_index(name)`             | `gl.documents.remove_ttl_index(name)`         |
| `gl.doc_create_capped(name, max:)`          | `gl.documents.create_capped(name, max:)`      |
| `gl.doc_remove_cap(name)`                   | `gl.documents.remove_cap(name)`               |
| `gl.doc_create_collection(name, ...)`       | `gl.documents.create_collection(name, ...)`   |
| `gl.stream_add(name, payload)`              | `gl.streams.add(name, payload)`               |
| `gl.stream_create_group(name, group)`       | `gl.streams.create_group(name, group)`        |
| `gl.stream_read(name, g, c, count:)`        | `gl.streams.read(name, g, c, count:)`         |
| `gl.stream_ack(name, group, id)`            | `gl.streams.ack(name, group, id)`             |
| `gl.stream_claim(name, g, c, ...)`          | `gl.streams.claim(name, g, c, ...)`           |

The same migration applies to the async wrapper — `GoldLapel::Async::Instance`
moved its `doc_*` and `stream_*` methods under `gl.documents` and `gl.streams`
in lockstep with the sync surface.

Other namespaces (`gl.search`, `gl.publish` / `gl.subscribe`, `gl.incr`,
`gl.zadd`, `gl.hset`, `gl.geoadd`, …) remain flat and will migrate to
nested form in subsequent releases (one namespace per schema-to-core
phase).

**Doc-store DDL is now owned by the proxy.** The wrapper no longer emits
`CREATE TABLE _goldlapel.doc_<name>` SQL when a collection is first used.
Instead, `gl.documents.<verb>` calls `POST /api/ddl/doc_store/create`
against the proxy's dashboard port; the proxy runs the canonical DDL on its
management connection and returns the table reference plus query patterns.
The wrapper caches `(tables, query_patterns)` per session — one HTTP
round-trip per `(family, name)` per session.

Canonical doc-store schema (v1) standardizes the column shape across every
Gold Lapel wrapper:

```
_id        UUID PRIMARY KEY DEFAULT gen_random_uuid()
data       JSONB NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Any wrapper (Python, JS, Ruby, Java, PHP, Go, .NET) writing to a doc-store
collection now produces the same table.

**Upgrade path for dev databases:** wipe and recreate. There is no in-place
migration. Pre-1.0, dev databases get rebuilt freely.

```bash
goldlapel clean   # drops _goldlapel.* tables
# ...drop/recreate your DB if needed...
```

If you have a pre-Phase-4 wrapper running against a post-Phase-4 proxy, the
wrapper's first `gl.documents.<verb>` call surfaces a clear
`version_mismatch` error pointing to this CHANGELOG.

### Fixes

**Several databases in one process no longer collide on port 7932.** A proxy
started without `proxy_port` now takes the first port pair from 7932 up that
no other proxy this process started is using — the proxy port and its
dashboard port (proxy port + 1, or your explicit `dashboard_port`). The first
proxy still gets 7932; a second upstream gets 7934. An explicit `proxy_port`
is used as given, and stopping a proxy frees its ports. This applies to
`GoldLapel.start`, `GoldLapel.start_proxy` and the Rails integration, which
now points each database at the port its proxy actually got.

**The Rails integration starts the proxy again.** Unless `database.yml` set a
`goldlapel: config:` map — including when there was no `goldlapel:` block at
all — the proxy failed to start on a nil config map, and Rails logged a
warning and fell back to a direct connection.
