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
      @real.close
    end

    def finished?
      @real.finished?
    end

    def method_missing(name, *args, **kwargs, &block)
      @real.send(name, *args, **kwargs, &block)
    end

    def respond_to_missing?(name, include_private = false)
      @real.respond_to?(name, include_private) || super
    end

    private

    def handle_query(sql, params, method, block, result_format = nil)
      # Observe SET / RESET on every query so the per-connection
      # unsafe-GUC state stays current. Runs first — even DDL/writes
      # and the in-transaction bypass paths must update state if the
      # SQL body contains a `SET app.x = ...`. Update is in-place;
      # reads of `@guc_state.state_hash` are O(1).
      @guc_state.observe_sql(sql) if sql.is_a?(String)

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
        return delegate(method, sql, params, result_format, &block)
      end

      # If the SQL contained a write (single-statement or in a multi-
      # statement body), it must NOT be served from cache — dispatch
      # to the underlying connection so the server actually runs it.
      return delegate(method, sql, params, result_format, &block) if wrote_something

      # Inside transaction: bypass cache
      return delegate(method, sql, params, result_format, &block) if @in_transaction

      # Read path: check L1 cache. Pass the per-connection state hash
      # so cache slots never cross connections with different unsafe
      # GUC state.
      sh = @guc_state.state_hash
      entry = @cache.get(sql, params, sh)
      if entry
        result = CachedResult.new(entry[:values], entry[:fields])
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

      result
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

    def delegate(method, sql, params, result_format, &block)
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
