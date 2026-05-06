# frozen_string_literal: true

# Per-connection GUC state tracking for L1 cache-key safety.
#
# Custom-GUC-driven RLS (e.g. `SET app.user_id = '42'; SELECT * FROM
# accounts;` where the RLS policy reads `current_setting('app.user_id')`)
# can leak user A's results to user B if the wrapper's L1 cache or the
# proxy cache groups requests purely by SQL+params. This module
# fingerprints the subset of GUC settings that can change query results,
# so the cache key includes the fingerprint and never crosses security
# boundaries.
#
# Design (Option Y, mirrors `goldlapel/src/guc_state.rs` from proxy
# commit 3e02359):
#
# 1. The wrapper observes every `SET ... = ...` and `RESET ...` it sees
#    pass through `CachedConnection#handle_query` (before delegating to
#    the underlying pg connection).
# 2. A GUC name is **unsafe** if it is in a short hardcoded list (search
#    path, role, isolation, etc.) OR contains a `.` (namespaced —
#    `app.*`, `myapp.*`).
# 3. Unsafe GUC values are stored in an instance Hash keyed by the
#    lowercased GUC name. The hash's content hash is the connection's
#    state hash, recomputed on every change.
# 4. The state hash is folded into the L1 cache key so two connections
#    with different unsafe GUC state never share a cache slot.
#
# `SET LOCAL` is intentionally ignored for state-hash purposes: the
# wrapper already bypasses the cache inside transactions
# (CachedConnection#handle_query short-circuits on TX_START..TX_END), so
# any in-transaction-only setting is invisible to the cache anyway.

module GoldLapel
  module GucState
    # GUC names whose value can change query results — or the bytes
    # PG returns for the same row — without changing the SQL text.
    # Matched case-insensitively. Any GUC with a `.` in the name is
    # also treated as unsafe (namespaced GUCs are the canonical
    # custom-RLS pattern).
    #
    # The list covers two categories:
    #
    # 1. Security-relevant — RLS / role / search_path settings whose
    #    value gates which rows the user is allowed to see.
    # 2. Output-formatting — datetime / locale / bytea encoding
    #    settings that change the bytes PG serialises for a value
    #    even when the underlying row is identical. The wrapper
    #    caches raw bytes; serving connection A's UTC-formatted
    #    timestamp to connection B (set to America/New_York) would
    #    return the wrong wall-clock string. Treating these as state-
    #    affecting fragments the cache by formatting context, which
    #    is the correct behaviour.
    UNSAFE_GUC_SHORT_LIST = %w[
      search_path
      role
      session_authorization
      default_transaction_isolation
      default_transaction_read_only
      transaction_isolation
      row_security
      datestyle
      intervalstyle
      timezone
      bytea_output
      lc_messages
      lc_monetary
      lc_numeric
      lc_time
    ].freeze

    # Classify a GUC name as state-affecting (`true`) or harmless
    # (`false`). A GUC is unsafe if it's in the short hardcoded list
    # OR contains a `.` (namespaced — `app.*`, `myapp.*`, etc.).
    # Comparison is case-insensitive.
    def self.unsafe_guc?(name)
      lower = name.to_s.downcase
      return true if lower.include?(".")
      UNSAFE_GUC_SHORT_LIST.include?(lower)
    end

    # Split a SQL string on top-level `;` characters, respecting
    # single- and double-quoted string literals (with PG-style doubled
    # quote escapes). Returns each segment with surrounding whitespace
    # trimmed; empty segments are dropped.
    #
    # Lightest possible "statement splitter" — does not understand
    # dollar-quoted strings, comments, or other lexical nuance. Good
    # enough for splitting `SET foo = 'a'; SELECT 1`-style multi-
    # statement bodies, which is the entire reason it exists.
    def self.split_statements(sql)
      bytes = sql.bytes
      out = []
      start = 0
      quote = nil
      i = 0
      while i < bytes.length
        c = bytes[i]
        if quote
          if c == quote
            # Handle doubled-quote escape (`''` inside a `'...'`
            # literal, `""` inside a `"..."` quoted identifier).
            if i + 1 < bytes.length && bytes[i + 1] == quote
              i += 2
              next
            end
            quote = nil
          end
        else
          if c == 39 || c == 34 # ' or "
            quote = c
          elsif c == 59 # ;
            segment = sql[start...i].strip
            out << segment unless segment.empty?
            start = i + 1
          end
        end
        i += 1
      end
      tail = sql[start..].strip
      out << tail unless tail.empty?
      out
    end

    # Parse a `SET` / `RESET` / `DISCARD` / `SELECT set_config(...)`
    # command out of a single SQL statement.
    #
    # Recognises:
    # * `SET name = value`, `SET name TO value`
    # * `SET SESSION name = value`, `SET SESSION name TO value`
    # * `SET LOCAL name = value`, `SET LOCAL name TO value`
    # * `RESET name`
    # * `RESET ALL`
    # * `DISCARD ALL` — full session reset (equivalent to RESET ALL
    #   for the unsafe-GUC state hash; PG also drops temp tables,
    #   prepared statements, advisory locks, etc., none of which
    #   affect the wrapper's cache-key safety model).
    # * `DISCARD PLANS` — drops PG's prepared-statement plan cache.
    #   Returned as `:discard_plans` so a future prepared-statement
    #   cache layer can hook the signal; `ConnectionGucState`
    #   currently treats it as a no-op for the GUC state map.
    # * `DISCARD SEQUENCES` / `DISCARD TEMP` / `DISCARD TEMPORARY` —
    #   no-ops for the state hash; classified for completeness so
    #   callers can distinguish "saw a DISCARD we don't care about"
    #   from "saw something that wasn't a DISCARD at all."
    # * `SELECT set_config('app.user_id', '42', false)` — Supabase /
    #   PostgREST canonical pattern for setting per-request GUCs from
    #   a JWT. Function form of SET; routed through the same state-
    #   hash mutation as a regular SET (or no-op for is_local=true).
    #   Also recognises the `pg_catalog.set_config` schema-qualified
    #   form.
    #
    # Returns nil for anything else (including `SET TIME ZONE ...` —
    # the legacy two-word `SET TIME ZONE` form. The bare GUC
    # `timezone` is unsafe (output formatting), but `SET TIME ZONE`
    # uses an unusual two-word "name" that this parser doesn't
    # recognise. Currently classified as not-a-SET — see
    # `parse_set_time_zone` for the dedicated path that brings it
    # back into the unsafe map. Conservative for now: a missed `SET
    # TIME ZONE` only fragments cache slightly less than the SET
    # equivalent would; the dedicated form mutates state hash via
    # the standard `:set` shape with name `"timezone"`.
    #
    # Returns a Hash on success:
    #   { kind: :set,             name: <lowercased>, value: <stripped> }
    #   { kind: :set_local,       name: <lowercased>, value: <stripped> }
    #   { kind: :reset,           name: <lowercased> }
    #   { kind: :reset_all }
    #   { kind: :discard_all }
    #   { kind: :discard_plans }
    #   { kind: :discard_sequences }
    #   { kind: :discard_temp }
    def self.parse_set_command(sql)
      s = sql.strip
      s = s.chomp(";").rstrip
      return nil if s.empty?

      # Function-form `SELECT set_config(name, value, is_local)` —
      # parse before the generic SET branch so it routes through the
      # right shape regardless of whitespace / casing inside the
      # SELECT body.
      sc = parse_set_config_call(s)
      return sc if sc

      tokens = s.split(/\s+/)
      head = tokens.shift
      return nil if head.nil?

      if head.casecmp("RESET").zero?
        target = tokens.shift
        return nil if target.nil?
        # `RESET name` — anything after `name` is junk we don't expect.
        return nil unless tokens.empty?
        return { kind: :reset_all } if target.casecmp("ALL").zero?
        name = normalize_guc_name(target)
        return nil if name.nil?
        return { kind: :reset, name: name }
      end

      if head.casecmp("DISCARD").zero?
        target = tokens.shift
        return nil if target.nil?
        return nil unless tokens.empty?
        upper = target.upcase
        case upper
        when "ALL"        then return { kind: :discard_all }
        when "PLANS"      then return { kind: :discard_plans }
        when "SEQUENCES"  then return { kind: :discard_sequences }
        when "TEMP", "TEMPORARY" then return { kind: :discard_temp }
        else
          return nil
        end
      end

      # SET TIME ZONE 'UTC' — legacy two-word PG form. The bare GUC
      # `timezone` is unsafe (output formatting affects cached bytes),
      # so this needs to mutate the state hash too. Pre-empt the
      # generic SET branch since the next token is "TIME" not a GUC
      # name.
      if head.casecmp("SET").zero? &&
         tokens.length >= 2 &&
         tokens[0].casecmp("TIME").zero? &&
         tokens[1].casecmp("ZONE").zero?
        # `SET TIME ZONE <value>`
        value_tokens = tokens[2..] || []
        joined = value_tokens.join(" ").strip
        return nil if joined.empty?
        value = strip_value_quotes(joined)
        return { kind: :set, name: "timezone", value: value }
      end

      return nil unless head.casecmp("SET").zero?

      # Optional `LOCAL` / `SESSION` modifier. PG's grammar permits
      # both; `SESSION` is the default and behaves the same as bare
      # `SET`.
      nxt = tokens.shift
      return nil if nxt.nil?
      is_local = false
      if nxt.casecmp("LOCAL").zero?
        is_local = true
        nxt = tokens.shift
        return nil if nxt.nil?
      elsif nxt.casecmp("SESSION").zero?
        nxt = tokens.shift
        return nil if nxt.nil?
      end

      # `nxt` is now the GUC name — but it may have an `=` glued onto
      # it (e.g. `SET app.user='42'`). Split on `=` if present.
      eq_idx = nxt.index("=")
      glued_value = nil
      if eq_idx
        name_token = nxt[0...eq_idx]
        v = nxt[(eq_idx + 1)..]
        glued_value = v unless v.nil? || v.empty?
      else
        name_token = nxt
      end

      name = normalize_guc_name(name_token)
      return nil if name.nil?

      # The token after the name is either `=`, `TO`, or — if the `=`
      # was glued onto the name — the start of the value.
      value_str =
        if glued_value
          rest_after = tokens.join(" ")
          rest_after.empty? ? glued_value : "#{glued_value} #{rest_after}"
        else
          sep = tokens.shift
          return nil if sep.nil?
          unless sep == "=" || sep.casecmp("TO").zero?
            return nil
          end
          tokens.join(" ")
        end

      trimmed = value_str.strip
      return nil if trimmed.empty?

      value = strip_value_quotes(trimmed)

      if is_local
        { kind: :set_local, name: name, value: value }
      else
        { kind: :set, name: name, value: value }
      end
    end

    # `SELECT set_config('app.user_id', '42', false)` — function-form
    # SET. Used heavily by Supabase / PostgREST to apply per-request
    # GUCs from a JWT claim. Args: (setting_name, new_value,
    # is_local). When is_local is `true`, the setting reverts at end
    # of transaction — same semantics as `SET LOCAL` and the same
    # cache implications (none, because the wrapper bypasses cache
    # inside transactions).
    #
    # Recognises:
    # * `SELECT set_config(name, value, is_local)`
    # * `SELECT pg_catalog.set_config(name, value, is_local)`
    # * Optional outer parens / trailing semicolon already stripped
    #   by the caller.
    #
    # Returns nil for any malformed call. The match is intentionally
    # narrow — it only catches the literal-arg form. Indirect forms
    # (`set_config(name, current_setting('x'), false)` etc.) cannot
    # be evaluated client-side without a round-trip and fall back to
    # post-call verify (concern 6).
    SET_CONFIG_RE = /
      \A\s*SELECT\s+
      (?:pg_catalog\s*\.\s*)?
      set_config\s*\(\s*
      (?<name>'(?:[^']|'')*'|"(?:[^"]|"")*")
      \s*,\s*
      (?<value>'(?:[^']|'')*'|"(?:[^"]|"")*"|NULL)
      \s*,\s*
      (?<is_local>TRUE|FALSE|'t'|'f'|'true'|'false'|0|1|'0'|'1')
      \s*\)\s*\z
    /xi

    def self.parse_set_config_call(sql)
      m = SET_CONFIG_RE.match(sql)
      return nil unless m

      raw_name = m[:name]
      raw_value = m[:value]
      is_local_token = m[:is_local].downcase

      # PG-style doubled-quote escapes: `''` inside `'...'`, `""`
      # inside `"..."`. Mirror the strip used elsewhere in this
      # module.
      name_inner = raw_name[1..-2]
      name_inner = name_inner.gsub(raw_name[0] * 2, raw_name[0])
      name = normalize_guc_name(name_inner)
      return nil if name.nil?

      if raw_value.upcase == "NULL"
        # `set_config(name, NULL, ...)` — PG treats this as RESET.
        is_local =
          ["true", "'t'", "'true'", "1", "'1'"].include?(is_local_token)
        # SET LOCAL NULL is still a no-op for cache purposes.
        return nil if is_local
        return { kind: :reset, name: name }
      end

      value_inner = raw_value[1..-2]
      value_inner = value_inner.gsub(raw_value[0] * 2, raw_value[0])

      is_local =
        ["true", "'t'", "'true'", "1", "'1'"].include?(is_local_token)

      if is_local
        { kind: :set_local, name: name, value: value_inner }
      else
        { kind: :set, name: name, value: value_inner }
      end
    end

    # Lowercase the GUC name and strip surrounding double quotes (PG
    # treats `"app.user_id"` and `app.user_id` as the same identifier
    # when it's a configuration parameter; double-quoted form just
    # preserves case, which we discard anyway).
    def self.normalize_guc_name(token)
      trimmed = token.gsub(/\A"+|"+\z/, "")
      return nil if trimmed.empty?
      trimmed.downcase
    end

    # Strip a single layer of matching surrounding quotes (`'...'` or
    # `"..."`) from a value. Multi-token quoted values like
    # `'foo bar'` arrive as the joined string already; this just peels
    # the outer quotes. Unquoted values are returned trimmed.
    def self.strip_value_quotes(value)
      v = value.strip
      return v if v.length < 2
      first = v[0]
      last = v[-1]
      if (first == "'" && last == "'") || (first == '"' && last == '"')
        return v[1..-2]
      end
      v
    end

    # Per-connection GUC state. Stores values for unsafe GUCs only;
    # harmless GUCs (timezone, application_name, planner cost knobs,
    # etc.) never enter the map and never affect the hash.
    class ConnectionGucState
      def initialize
        # Insertion-ordered Hash. The state-hash function sorts keys
        # before hashing so two connections that arrived at the same
        # state via different SET orders produce the same hash.
        @values = {}
        # `0` for the empty (default) state — a fresh connection's
        # state_hash matches "no GUCs set" cache slots from peer
        # connections, exactly as we want.
        @state_hash = 0
        # Dirty flag — set when the wrapper observes a wire pattern
        # that *might* have mutated server-side GUC state in a way
        # the parser couldn't fully decode (top-level function call,
        # an async post-call verify failure, etc.). The verify-on-
        # checkout path consults this flag and re-reads pg_settings
        # if true, then clears it.
        @dirty = false
      end

      # Current state hash. `0` for empty state.
      def state_hash
        @state_hash
      end

      # Whether the connection's state map is potentially out-of-
      # sync with the server. See `mark_dirty!` and the verify-on-
      # checkout path in `CachedConnection`.
      def dirty?
        @dirty
      end

      # Mark the state map potentially stale. Cheap; setters are
      # idempotent. Called by the wrapper when it observes a top-
      # level function call (concern 6) or when an async post-call
      # verify fails.
      def mark_dirty!
        @dirty = true
      end

      # Clear the dirty flag. Called by the wrapper after a
      # successful pg_settings reconciliation.
      def mark_clean!
        @dirty = false
      end

      # Replace the unsafe-GUC map wholesale with values
      # reconstructed from a `pg_settings` query. Filters through
      # the same `unsafe_guc?` classifier as wire-observed SETs so
      # the resulting state hash matches what an equivalent SET
      # sequence would have produced. Used by the verify-on-
      # checkout fallback (concern 5) and the async post-call
      # verify (concern 6) when those paths reconcile state with
      # the server.
      #
      # Always clears `dirty?` afterward — whether the rebuild
      # changed the hash or not, the map now reflects ground truth.
      def replace_from_settings(settings)
        @values = {}
        settings.each do |name, value|
          next if name.nil? || value.nil?
          lower = name.to_s.downcase
          next unless GucState.unsafe_guc?(lower)
          @values[lower] = value.to_s
        end
        recompute_hash
        mark_clean!
      end

      # Read-only view of the unsafe-GUC map (lowercased name → raw
      # value). Returned as a fresh Hash so callers can't mutate
      # internal state.
      def values
        @values.dup
      end

      # Apply a parsed `SET` / `RESET` / `DISCARD` /
      # `SELECT set_config(...)` command. No-op for `:set_local`
      # (transient — cache is bypassed inside transactions anyway),
      # no-op for safe GUC names, no-op for DISCARD shapes that
      # don't touch the GUC state map (PLANS / SEQUENCES / TEMP).
      def apply(cmd)
        return if cmd.nil?
        case cmd[:kind]
        when :set
          if GucState.unsafe_guc?(cmd[:name])
            @values[cmd[:name]] = cmd[:value]
            recompute_hash
          end
        when :set_local
          # Intentionally ignored. SET LOCAL only takes effect inside
          # a transaction, and the wrapper bypasses cache inside
          # transactions — SET LOCAL never influences a cacheable
          # response.
        when :reset
          if GucState.unsafe_guc?(cmd[:name]) && @values.delete(cmd[:name])
            recompute_hash
          end
        when :reset_all, :discard_all
          # `DISCARD ALL` is a strict superset of `RESET ALL`: it
          # also drops temp tables, prepared statements, advisory
          # locks, listen channels, and the plan cache — none of
          # which affect the wrapper's cache-key safety model. From
          # the GUC-state perspective the two are equivalent: clear
          # the unsafe-GUC map and zero the hash.
          unless @values.empty?
            @values.clear
            recompute_hash
          end
        when :discard_plans, :discard_sequences, :discard_temp
          # No-op for the GUC state map. Recognised so callers can
          # distinguish "this was a DISCARD we intentionally don't
          # track" from "this looked like junk." A future prepared-
          # statement cache layer would hook `:discard_plans` here.
        end
      end

      # Convenience: parse a SQL string and apply every recognised
      # `SET` / `RESET` it contains. Multi-statement bodies are split
      # on top-level `;` (string literals respected) so a single
      # exec like `SET app.user_id = '42'; SELECT 1` still updates
      # state. Returns true if the hash changed.
      def observe_sql(sql)
        return false if sql.nil? || sql.empty?
        before = @state_hash
        # Fast path for the common single-statement case — avoid
        # building the array from split_statements when there's no
        # inner `;`.
        trimmed = sql.strip.chomp(";")
        if trimmed.include?(";")
          GucState.split_statements(sql).each do |stmt|
            cmd = GucState.parse_set_command(stmt)
            apply(cmd) if cmd
          end
        else
          cmd = GucState.parse_set_command(sql)
          apply(cmd) if cmd
        end
        @state_hash != before
      end

      private

      def recompute_hash
        if @values.empty?
          @state_hash = 0
          return
        end
        # Sort by key so insertion order doesn't affect the output.
        # Ruby's Array#hash + the canonical pair list gives a stable,
        # process-local integer that mixes name + value for every
        # tracked GUC. The hash crosses connections within one
        # process; cross-process hashes are not required (the proxy
        # has its own state_hash on its side of the wire).
        @state_hash = @values.sort.hash
      end
    end
  end
end
