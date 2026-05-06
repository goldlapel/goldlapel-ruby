# frozen_string_literal: true

require_relative "cache"
require_relative "guc_state"

module GoldLapel
  # Cheap pre-check used by `update_transaction_state` to avoid running
  # the statement splitter on multi-statement bodies that contain no tx-
  # control keyword. Matches BEGIN / START / COMMIT / ROLLBACK / END
  # anywhere in the body, case-insensitively. False positives (a literal
  # like `'BEGIN of a story'`) just route through the splitter — the
  # per-segment regex anchors then correctly classify them as not-a-
  # tx-control statement.
  TX_KEYWORD_HINT = /\b(BEGIN|START|COMMIT|ROLLBACK|END)\b/i

  # Top-level `SELECT <ident>(...)` — statement is a function call.
  # Concern 6: when the wrapper observes one of these on the wire,
  # it can't tell whether the function body issues `SET app.user_id
  # = ...` server-side. Schedule a post-call async verify that
  # reconciles the GUC state hash with `pg_settings`. The regex is
  # anchored to the start (so it doesn't fire for `SELECT col FROM
  # (SELECT funcname(...))`) and tolerates an optional schema
  # prefix.
  #
  # Catches `count(*)`, `current_setting(...)`, `set_config(...)`,
  # `now()`, custom functions, etc. — without trying to enumerate
  # which are pure. The cheap pg_settings round-trip is the
  # fallback price for not introspecting function bodies.
  TOP_LEVEL_FUNCALL = /\A\s*SELECT\s+(?:[a-zA-Z_]\w*\s*\.\s*)?[a-zA-Z_]\w*\s*\(/i

  # `pg_settings` query for verify-on-checkout (concern 5) and post-
  # call async verify (concern 6). `source = 'session'` filters to
  # rows the user (or set_config) modified during the session,
  # excluding the noise of every default the server reports. The
  # wrapper's `replace_from_settings` then filters again through
  # `unsafe_guc?` so only rows that affect cache safety land in
  # the state map.
  PG_SETTINGS_VERIFY_SQL =
    "SELECT name, setting FROM pg_settings WHERE source = 'session'"

  def self.wrap(conn, invalidation_port: nil, disable_native_cache: false)
    cache = NativeCache.instance
    # Set the flag before the invalidation thread connects so the very
    # first `wrapper_connected` snapshot carries the correct
    # `disabled` field — Manor/HQ see the wrapper's native-cache state
    # from the first emit, not after a subsequent state-change.
    cache.disable_native_cache = disable_native_cache
    invalidation_port ||= detect_invalidation_port
    cache.connect_invalidation(invalidation_port) unless cache.connected?
    CachedConnection.new(conn, cache)
  end

  def self.detect_invalidation_port
    instances = Proxy.instances rescue {}
    if instances.any?
      inst = instances.values.first
      # invalidation_port is resolved at Proxy construction.
      inst.invalidation_port
    else
      DEFAULT_PROXY_PORT + 2
    end
  end

  class CachedConnection
    # Exposed for tests and rare adapters. Don't mutate from outside.
    attr_reader :guc_state

    def initialize(real_conn, cache)
      @real = real_conn
      @cache = cache
      @in_transaction = false
      # Per-connection unsafe-GUC fingerprint. Folded into the L1
      # cache key so custom-GUC RLS (`SET app.user_id = '42'`) can
      # never leak user A's rows to user B. See `goldlapel/guc_state.rb`.
      @guc_state = GucState::ConnectionGucState.new
      # Serialises `@real` access between the user thread and any
      # post-call verify Thread (concern 6). pg connections are
      # not safe to use concurrently from multiple threads; the
      # mutex prevents the verify path from interleaving with a
      # user query.
      @real_mutex = Mutex.new
      # Tracks the most recent in-flight async verify Thread (if
      # any). Set when `schedule_post_call_verify` spawns; cleared
      # on completion. `close` joins it briefly so a
      # connection.close() doesn't leak a verify Thread that
      # outlives the underlying pg connection.
      @verify_thread = nil
      @closed = false
    end

    def exec(sql, &block)
      handle_query(sql, nil, :exec, block)
    end
    alias_method :query, :exec

    def async_exec(sql, &block)
      handle_query(sql, nil, :async_exec, block)
    end

    def exec_params(sql, params = [], result_format = 0, &block)
      handle_query(sql, params, :exec_params, block, result_format)
    end

    def async_exec_params(sql, params = [], result_format = 0, &block)
      handle_query(sql, params, :async_exec_params, block, result_format)
    end

    def close
      # Mark closed BEFORE killing the verify thread so the verify
      # body's closed-check has a fresh value if it races with
      # close(). Then join briefly — verify queries are short
      # (single pg_settings round-trip) so a 1s ceiling is
      # generous; if it overruns, kill rather than hang the
      # caller's close. Mutex acquisition there is internal to the
      # verify body; if we held @real_mutex here we'd deadlock
      # with a verify mid-flight, so don't.
      @closed = true
      t = @verify_thread
      if t && t.alive?
        t.join(1)
        t.kill if t.alive?
      end
      @verify_thread = nil
      @real.close
    end

    def finished?
      @real.finished?
    end

    # Reconcile `@guc_state` with the server's session GUCs, but
    # only if the state map is currently dirty. Used by the verify-
    # on-checkout fallback (concern 5) and as a public hook the
    # railtie's `expire` patch can call (concern 4) when the
    # adapter returns to the AR pool. No-op when:
    #
    # * the state is clean (`dirty?` false)
    # * the connection is mid-transaction (pg_settings would still
    #   succeed, but mutating state mid-transaction breaks the
    #   wrapper's invariant that cache only participates outside
    #   transactions; reconciling is harmless but pointless)
    # * the underlying pg connection is closed
    #
    # Returns true if a reconciliation actually ran.
    def ensure_state_clean!
      return false unless @guc_state.dirty?
      return false if @in_transaction
      return false if @closed
      reconcile_guc_state_from_pg_settings
      true
    end

    # Force a `DISCARD ALL` on the wire, then drop the wrapper's
    # GUC state to the empty-baseline. Used by the railtie's
    # connection-pool checkin hook (concern 4) — AR doesn't auto-
    # reset on checkin, so the wrapper drives it. Safe to call when
    # the state is already empty (still issues DISCARD ALL on the
    # wire, in case the server has stale prepared statements / temp
    # tables / advisory locks from a previous checkout cycle).
    #
    # Inside an active transaction, DISCARD ALL would abort the
    # transaction — guard against that by checking
    # `@in_transaction` first. AR pool checkin should never happen
    # mid-transaction, but a hostile caller (or buggy adapter)
    # could; better to be a no-op than to abort the tx.
    def discard_all_on_release!
      return false if @closed
      return false if @in_transaction
      ok = @real_mutex.synchronize do
        if @closed
          false
        else
          begin
            @real.exec("DISCARD ALL")
            true
          rescue StandardError
            # Connection error during release — treat as a clean
            # close path. The next checkout will get a fresh
            # connection from AR's pool, which will re-init state.
            false
          end
        end
      end
      return false unless ok
      @guc_state = GucState::ConnectionGucState.new
      true
    end

    def method_missing(name, *args, **kwargs, &block)
      @real.send(name, *args, **kwargs, &block)
    end

    def respond_to_missing?(name, include_private = false)
      @real.respond_to?(name, include_private) || super
    end

    private

    def handle_query(sql, params, method, block, result_format = nil)
      # Verify-on-checkout fallback (concern 5). If a previous
      # async post-call verify either failed or has not yet
      # reconciled state — and the connection is now executing a
      # new query — reconcile state synchronously before classifying
      # this query's cache behaviour. The pg_settings round-trip is
      # paid once per dirty cycle; clean queries pay nothing
      # (`dirty?` short-circuits at zero cost).
      ensure_state_clean! if @guc_state.dirty?

      # Observe SET / RESET on every query so the per-connection
      # unsafe-GUC state stays current. Runs first — even DDL/writes
      # and the in-transaction bypass paths must update state if the
      # SQL body contains a `SET app.x = ...`. Update is in-place;
      # reads of `@guc_state.state_hash` are O(1).
      @guc_state.observe_sql(sql) if sql.is_a?(String)

      # Top-level function call (concern 6) — schedule async post-
      # call verify after the user's query returns. Function bodies
      # may issue server-side SETs the wire layer can't see; the
      # verify reconciles state with pg_settings. Done here (pre-
      # delegate) so we know the user's *intent* even if the query
      # raises; the actual schedule fires post-success in the cache-
      # miss / write paths below.
      should_post_verify =
        sql.is_a?(String) && top_level_funcall?(sql) &&
        # `set_config` was already routed through the parser; no
        # need to redundantly verify when we know the inline mutation
        # is exact.
        !GucState::SET_CONFIG_RE.match?(sql)

      # Write detection + self-invalidation.
      #
      # Multi-statement Q messages (e.g. `SET app.user_id = '42';
      # INSERT INTO orders VALUES (1)`) need every segment classified —
      # the single-token `detect_write` would otherwise see `SET` and
      # let the INSERT escape invalidation, leaving stale cached
      # SELECTs from `orders` to survive the write. Reuse the GUC-
      # state splitter (already wired for SET observation) and union
      # the resulting invalidations. DDL_SENTINEL short-circuits to
      # invalidate_all.
      #
      # Runs BEFORE the tx-state walk so that
      # `SET app.x = 'y'; INSERT INTO t VALUES (1); SELECT 1` properly
      # invalidates `t`. Standalone `BEGIN`/`COMMIT` flow through the
      # collector cheaply (single segment, no `;`, empty result) and
      # then hit the tx-state walk below.
      tables_to_invalidate, all_invalid = collect_write_invalidations(sql)
      if all_invalid
        @cache.invalidate_all
      elsif !tables_to_invalidate.empty?
        tables_to_invalidate.each { |t| @cache.invalidate_table(t) }
      end
      wrote_something = all_invalid || !tables_to_invalidate.empty?

      # Transaction tracking — segment-aware so multi-statement bodies
      # like `BEGIN; INSERT INTO t VALUES (1); COMMIT` leave the wrapper's
      # `@in_transaction` matching the server's actual tx state. Walking
      # the prefix only would set `in_transaction=true` from the leading
      # BEGIN, never see the trailing COMMIT, and silently bypass the
      # cache forever until the next process restart. Same gap covers
      # bodies that *start* with a non-tx token but contain BEGIN/COMMIT
      # later (e.g. `SET app.x = 'y'; BEGIN; INSERT ...`).
      tx_changed = update_transaction_state(sql)
      if tx_changed
        result = delegate(method, sql, params, result_format, &block)
        schedule_post_call_verify if should_post_verify
        return result
      end

      # If the SQL contained a write (single-statement or in a multi-
      # statement body), it must NOT be served from cache — dispatch
      # to the underlying connection so the server actually runs it.
      if wrote_something
        result = delegate(method, sql, params, result_format, &block)
        schedule_post_call_verify if should_post_verify
        return result
      end

      # Inside transaction: bypass cache
      if @in_transaction
        result = delegate(method, sql, params, result_format, &block)
        # Don't schedule verify mid-transaction — the verify body
        # must run outside any transaction (its own pg_settings
        # query is cheap, but spawning it while a tx is still open
        # would either share the tx (corrupting state checks) or
        # block on the connection mutex until the user issues
        # COMMIT. Mark dirty so the verify runs on the next post-
        # commit checkout instead.
        @guc_state.mark_dirty! if should_post_verify
        return result
      end

      # Read path: check L1 cache. Pass the per-connection state hash
      # so cache slots never cross connections with different unsafe
      # GUC state.
      sh = @guc_state.state_hash
      entry = @cache.get(sql, params, sh)
      if entry
        result = CachedResult.new(entry[:values], entry[:fields])
        # No real query went on the wire — don't schedule verify
        # for cache hits. The function call's body never executed,
        # so there's nothing to reconcile.
        if block
          block.call(result)
          return result
        end
        return result
      end

      # Cache miss: execute WITHOUT block so we can cache before PG clears the result
      result = delegate(method, sql, params, result_format)

      # Cache the result if it carries column schema. SET / RESET /
      # LISTEN / UNLISTEN / NOTIFY / SAVEPOINT all return a result with
      # empty `fields` and empty `values` — those are session-state
      # commands, not cacheable reads. `fields.empty?` is a clean
      # signal: any real SELECT (even one with zero rows) has a non-
      # empty field list. Skipping the put avoids bloating the cache
      # with no-row entries that never serve real data.
      if result && result.respond_to?(:values) && result.respond_to?(:fields) &&
         !result.fields.empty?
        @cache.put(sql, params, result.values, result.fields, sh)
      end

      # Yield to block if provided (matching PG gem's block API)
      if block
        block.call(result)
        result.clear if result.respond_to?(:clear) && !result.is_a?(CachedResult)
      end

      schedule_post_call_verify if should_post_verify
      result
    end

    # Detect a top-level `SELECT <ident>(...)` — see
    # `TOP_LEVEL_FUNCALL` for the regex rationale. Cheap; no SQL
    # parsing.
    def top_level_funcall?(sql)
      TOP_LEVEL_FUNCALL.match?(sql)
    end

    # Reconcile the state hash with `pg_settings`. Caller has
    # already verified that `@dirty?` is true and the connection
    # is outside any transaction. Acquires `@real_mutex` so an
    # in-flight async verify (or the user's next query) doesn't
    # interleave on the underlying pg connection.
    #
    # On any error: leave dirty set so the next checkout retries.
    # Never raises — verification failures must not break the
    # user's query path.
    def reconcile_guc_state_from_pg_settings
      # Computed inside the synchronized block; nil signals "skip
      # the state replace step." `return` from a synchronize block
      # returns from the surrounding method only via a non-local
      # exit — relying on it would cross a layered ensure. Use a
      # local flag instead so the mutex unwinds normally.
      rows = nil
      @real_mutex.synchronize do
        unless @closed
          begin
            result = @real.exec(PG_SETTINGS_VERIFY_SQL)
            rows = result.values if result.respond_to?(:values)
          rescue StandardError
            # Connection error / transient failure — leave dirty.
            # Next checkout will retry. `rows` stays nil.
            rows = nil
          end
        end
      end
      return if rows.nil?
      @guc_state.replace_from_settings(rows)
    end

    # Schedule a background reconciliation against `pg_settings`.
    # Marks dirty unconditionally first so the next user-thread
    # query will verify-on-checkout if the background thread races
    # behind it. Spawned only when we're not already mid-
    # transaction, not closed, and no other verify thread is
    # in-flight (single-thread invariant — multiple concurrent
    # verifies on one pg conn would just contend for `@real_mutex`
    # without adding value).
    def schedule_post_call_verify
      return if @closed
      return if @in_transaction
      @guc_state.mark_dirty!
      # Don't spawn a duplicate verifier if one is already running.
      # The user's next query will verify-on-checkout if the in-
      # flight verifier finishes after the next query starts.
      existing = @verify_thread
      return if existing && existing.alive?

      @verify_thread = Thread.new do
        begin
          reconcile_guc_state_from_pg_settings
        rescue StandardError
          # Defensive — `reconcile_guc_state_from_pg_settings`
          # already swallows StandardError. Anything that escapes
          # is exotic (e.g. NoMemoryError) and shouldn't crash the
          # process via an unhandled background-thread exception.
          @guc_state.mark_dirty!
        end
      end
      # Background verify must not crash the parent process if it
      # raises an unexpected non-StandardError. Behave like a
      # daemon-style telemetry thread.
      @verify_thread.abort_on_exception = false
      @verify_thread.report_on_exception = false
    end

    # Walk segments of a (possibly) multi-statement SQL body and update
    # the per-connection `@in_transaction` flag once per tx-control
    # segment (BEGIN / START TRANSACTION → true; COMMIT / ROLLBACK / END →
    # false; SAVEPOINT / RELEASE SAVEPOINT are no-ops because they're
    # only legal mid-transaction and don't change the boolean flag).
    # Returns true if any segment was a tx-control statement (so the
    # caller can dispatch directly to the underlying connection —
    # tx-control statements are never cacheable anyway).
    #
    # Per-segment classification mirrors the server's view: it's the
    # *last* tx-control statement that determines the final state. For
    # `BEGIN; INSERT INTO t VALUES (1); COMMIT`, BEGIN flips to true,
    # COMMIT flips back to false — final wrapper state = false, matching
    # the server.
    #
    # Fast path: SQL with no top-level `;` skips the splitter — the
    # common case (one SELECT/INSERT/UPDATE/... per query) only pays
    # two anchored regex matches.
    def update_transaction_state(sql)
      return false unless sql.is_a?(String)

      # Single-statement fast path: no top-level `;` means we can match
      # the whole string against the anchored regex without splitting.
      # Covers the overwhelmingly common case (one SELECT/INSERT/etc.
      # per query) without paying the splitter cost.
      unless sql.include?(";")
        if GoldLapel::TX_START.match?(sql)
          @in_transaction = true
          return true
        end
        if GoldLapel::TX_END.match?(sql)
          @in_transaction = false
          return true
        end
        return false
      end

      # Multi-statement body: cheap keyword pre-check, then walk every
      # segment if (and only if) a tx-control keyword appears anywhere.
      # `SELECT a; SELECT b` — common case for legitimate batched reads —
      # short-circuits without paying the splitter.
      return false unless TX_KEYWORD_HINT.match?(sql)

      changed = false
      GucState.split_statements(sql).each do |seg|
        if GoldLapel::TX_START.match?(seg)
          @in_transaction = true
          changed = true
        elsif GoldLapel::TX_END.match?(seg)
          @in_transaction = false
          changed = true
        end
      end
      changed
    end

    # Run `detect_write` over every segment of a (possibly) multi-
    # statement SQL body and return `[tables_set, all_invalid]`.
    #
    # `tables_set` is a Set of bare table names that need
    # invalidation; `all_invalid` is `true` when any segment hit
    # DDL_SENTINEL (CREATE / ALTER / DROP / WITH-write CTE / ...) and
    # the entire L1 cache must be cleared.
    #
    # Fast path: SQL with no top-level `;` skips the splitter entirely
    # and runs `detect_write` once on the whole string.
    def collect_write_invalidations(sql)
      return [Set.new, false] unless sql.is_a?(String)

      segments =
        if sql.include?(";")
          GucState.split_statements(sql)
        else
          [sql]
        end

      tables = Set.new
      segments.each do |seg|
        t = GoldLapel.detect_write(seg)
        next if t.nil?
        if t == GoldLapel::DDL_SENTINEL
          return [Set.new, true]
        end
        tables.add(t)
      end
      [tables, false]
    end

    # Dispatch to the underlying pg connection. Serialised on
    # `@real_mutex` so a post-call async verify Thread (concern 6)
    # cannot interleave with a user's query on the same connection
    # — pg connections are not safe to use from multiple threads.
    # Recursive-acquire guard via `Mutex#owned?` makes the lock a
    # no-op when the user thread is already inside a synchronized
    # path (e.g. `reconcile_guc_state_from_pg_settings` calling
    # back into `@real.exec`).
    def delegate(method, sql, params, result_format, &block)
      if @real_mutex.owned?
        return delegate_unlocked(method, sql, params, result_format, &block)
      end
      @real_mutex.synchronize do
        delegate_unlocked(method, sql, params, result_format, &block)
      end
    end

    def delegate_unlocked(method, sql, params, result_format, &block)
      case method
      when :exec
        @real.exec(sql, &block)
      when :async_exec
        @real.async_exec(sql, &block)
      when :exec_params
        if result_format
          @real.exec_params(sql, params, result_format, &block)
        else
          @real.exec_params(sql, params, &block)
        end
      when :async_exec_params
        if result_format
          @real.async_exec_params(sql, params, result_format, &block)
        else
          @real.async_exec_params(sql, params, &block)
        end
      end
    end
  end
end
