# native-async-pg-driver — Step 0 Findings

**Branch:** `native-async-pg-driver` (off `wrapper-v0.2-factory-api`)
**Date:** 2026-04-18
**Status:** STOPPED at Step 0 — `async-pg` does not exist. User re-decision needed.

## TL;DR

There is no gem called `async-pg` on rubygems.org. The two candidates that exist
are either abandoned or have an API that does not match our "drop-in native
async pg" assumption. A third option (pg gem's own native async methods under
Fiber scheduler) delivers the architectural clarity the task is after without
taking on a new dependency. Recommending option C.

## The gem landscape

### A. `async-postgres` (socketry/async-postgres)

- **RubyGems version:** 0.1.0
- **Last release:** 2018-10-22 (~7.5 years ago)
- **Repo status:** **archived** since 2021-05-22
- **README says, verbatim:** "This gem is experimental and unmaintained. Please
  see https://github.com/socketry/db-postgres for an event driven driver for
  Postgres."
- **Verdict:** dead. Not an option.

### B. `db-postgres` (socketry/db-postgres)

- Current: v0.9.0, actively maintained under the socketry umbrella.
- Built on FFI bindings to libpq. Event-driven, works under async reactor.
- **Not a drop-in for `pg`.** The API shape is materially different:
  - No `exec_params(sql, [p1, p2])` with `$1, $2` placeholders. Queries go
    through `session.query(sql_with_%{name}_placeholders, name: value).call`,
    which does string escaping (via `literal()` / `identifier()`) rather than
    libpq-level parameter binding.
  - Connection surface is `send_query` / `next_result` at the low level, or
    `DB::Client` + `session.call(sql)` at the high level.
  - Results iterate as arrays (with `field_names` as separate metadata),
    not per-row Hashes like `PG::Result`.
  - **No LISTEN/NOTIFY**. Searched the public source (`connection.rb`,
    `native.rb`, `native/connection.rb`) and the public docs — no `listen`,
    `notify`, `wait_for_notify`, or notification-callback surface.
- **Impact on us:**
  - `utils.rb` is 1,675 lines of `conn.exec_params($1, …)` usage. Porting to
    db-postgres means rewriting every query site to the `%{name}` template
    style AND losing libpq parameter binding semantics (type coercion, `nil`
    vs `NULL`, binary formats). Risk of subtle semantic divergence is high.
  - `doc_watch` / `doc_unwatch` use LISTEN/NOTIFY. db-postgres has no path
    for that. We'd have to keep the sync pg path for those two methods —
    but then we're running two Postgres drivers in the same process.
  - Result shape differs. Every return value from utils.rb would need a
    shim layer to convert arrays back to hash-rows to keep the public API
    unchanged.
- **Verdict:** technically usable, but the rewrite is large and carries
  real semantic risk. Not a "drop-in swap of the internal driver."

### C. `pg` gem's own native async methods (the path I'd recommend)

The `pg` gem (≥ 1.3) has first-class native-async methods that DO cooperate
with Ruby's Fiber scheduler:

- `conn.async_exec_params(sql, params)` — non-blocking libpq call
- `conn.async_exec(sql)`
- `conn.wait_for_notify(timeout)` — already yields to the Fiber scheduler
- `conn.socket_io.wait_readable` — explicit yield point if we want it

These are the same libpq primitives, same parameter binding semantics, same
`PG::Result` return type — just the non-blocking variants. Under an Async
reactor, they yield cooperatively via the Fiber scheduler. Under no reactor,
they still work (blocking, same as before).

**What changes for users:** nothing. Same `pg` dep, same result shapes,
same error classes.

**What changes for us:**
- Replace `conn.exec_params(...)` with `conn.async_exec_params(...)` in the
  async utility layer (mirror of `utils.rb`).
- Keep `GoldLapel::Async.start(url)` reactor-required (already is).
- `doc_watch` stays on `wait_for_notify` (which is already scheduler-aware).
- No new gem dep, no gemspec conflict risk, no API surface translation.

**What this buys us:**
- Code honesty: the async path explicitly calls the async variants. No more
  "sync code that happens to work under a scheduler" footnote.
- Symmetry with the sync path — utility functions differ only in the method
  name (`exec_params` vs `async_exec_params`), so review is easy.
- Zero new surface area for the user.

**What it does NOT buy us:**
- It's still the pg C driver. Falcon / async-http composition is already fine
  because the Fiber scheduler handles the IO. We're not unlocking anything
  that's structurally blocked today — we're making the intent explicit in code.

### D. `em-pg-client`

EventMachine-based. Wrong reactor. Not an option.

## The task's stated motivation vs reality

> Motivation: code honesty (native async all the way down), better composition
> with other async Ruby libraries (Falcon, async-http), better behavior at
> extreme concurrency.

- **Code honesty**: achievable with Option C (use `async_exec_params` instead
  of `exec_params` in the async path). This is the 80% of the motivation.
- **Composition with Falcon/async-http**: already works today via the Fiber
  scheduler. Option C doesn't change this. Option B would change it only if
  db-postgres's event loop is genuinely "more native" than libpq's async
  mode under the Fiber scheduler — and I don't see evidence it is.
- **Extreme concurrency**: db-postgres claims throughput wins over sync
  ActiveRecord, but our workload is "one connection per instance plus
  user-brought connections," so we're not the target benchmark scenario.

## Recommendation

Proceed with **Option C** on this branch: swap `exec_params` → `async_exec_params`
in a parallel `lib/goldlapel/async/utils.rb`, route `GoldLapel::Async` methods
through it, port `doc_watch` to the async variant of `wait_for_notify`, update
tests and README.

This gives us the code-honesty win the task is really after, with zero new
dependency risk and no public-API churn. If the user later wants the full
db-postgres rewrite (driver swap + parameter-style translation + shim for
Hash-row results + a separate path for LISTEN/NOTIFY), that's a much bigger
project and should be scoped separately.

## What I did not do

- Did not add any dependency to the gemspec
- Did not modify `lib/goldlapel/async.rb` beyond what's already there
- Did not run any tests
- Did not touch `utils.rb`, `instance.rb`, `rails.rb`

Waiting on user decision: Option C (recommended) vs Option B (db-postgres
full rewrite) vs revisit the motivation.
