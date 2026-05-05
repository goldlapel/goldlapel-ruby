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
    # GUC names whose value can change query results without changing
    # the SQL text. Matched case-insensitively. Any GUC with a `.` in
    # the name is also treated as unsafe (namespaced GUCs are the
    # canonical custom-RLS pattern).
    UNSAFE_GUC_SHORT_LIST = %w[
      search_path
      role
      session_authorization
      default_transaction_isolation
      default_transaction_read_only
      transaction_isolation
      row_security
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

    # Parse a `SET` / `RESET` command out of a single SQL statement.
    #
    # Recognises:
    # * `SET name = value`, `SET name TO value`
    # * `SET SESSION name = value`, `SET SESSION name TO value`
    # * `SET LOCAL name = value`, `SET LOCAL name TO value`
    # * `RESET name`
    # * `RESET ALL`
    #
    # Returns nil for anything else (including `SET TIME ZONE ...` —
    # timezone is harmless and the unusual two-word GUC name doesn't
    # fit the pattern; treating it as not-a-SET is correct because it
    # doesn't affect cache safety).
    #
    # Returns a Hash on success:
    #   { kind: :set,        name: <lowercased>, value: <stripped> }
    #   { kind: :set_local,  name: <lowercased>, value: <stripped> }
    #   { kind: :reset,      name: <lowercased> }
    #   { kind: :reset_all }
    def self.parse_set_command(sql)
      s = sql.strip
      s = s.chomp(";").rstrip
      return nil if s.empty?

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
      end

      # Current state hash. `0` for empty state.
      def state_hash
        @state_hash
      end

      # Read-only view of the unsafe-GUC map (lowercased name → raw
      # value). Returned as a fresh Hash so callers can't mutate
      # internal state.
      def values
        @values.dup
      end

      # Apply a parsed `SET` / `RESET` command. No-op for `:set_local`
      # (transient — cache is bypassed inside transactions anyway),
      # no-op for safe GUC names.
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
        when :reset_all
          unless @values.empty?
            @values.clear
            recompute_hash
          end
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
