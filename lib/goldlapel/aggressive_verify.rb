# frozen_string_literal: true

# Smart-auto-enable for the post-DML async verify ("aggressive verify").
#
# Background — concern 6 in the GUC-RLS hardening series. The wire layer
# observes `SET app.user_id = '42'` directly when the client issues it,
# but a server-side trigger that fires on INSERT/UPDATE/DELETE and
# internally `SET`s a session GUC is invisible to the proxy/wrapper.
# Trigger-internal SETs are uncommon but real (compliance-heavy schemas,
# legacy multi-tenant designs, third-party extensions).
#
# The post-DML verify is a small per-write tax (~1ms async) that
# guarantees the wrapper's GUC state hash stays in sync with the server
# even when triggers mutate state behind our back. Always-on costs every
# customer for a feature only some need; off-by-default leaves the
# trigger-internal-SET case as a footgun.
#
# Smart-auto: on first connection per upstream, run a single classifier
# query against `pg_trigger` that detects whether ANY user-defined
# trigger function body contains `SET` / `RESET` / `DISCARD` /
# `set_config`. If so, auto-enable the flag for this upstream;
# otherwise leave it off. Result is cached process-lifetime, keyed by
# upstream URL — subsequent connections to the same db reuse the
# detection.
#
# Override hierarchy (highest priority first):
#   1. Explicit kwarg `aggressive_verify: :on` / `:off` — caller-set.
#   2. License-payload `aggressive_verify_active` (when the proxy
#      exposes it) — set via `GoldLapel::AggressiveVerify.set_license_active`.
#   3. Smart-auto detection — `:auto` resolves to whatever the trigger
#      classifier returned for this upstream.

module GoldLapel
  module AggressiveVerify
    # Detection SQL — true if any user-defined trigger function body
    # contains a state-mutating keyword (`SET`, `set_config`, `RESET`,
    # `DISCARD`). The `\m...\M` word-boundary anchors avoid matching
    # `SETOF` (a return-type keyword that appears in trigger function
    # bodies for other reasons).
    DETECTION_SQL = <<~SQL.freeze
      SELECT EXISTS (
          SELECT 1 FROM pg_trigger t
          JOIN pg_proc p ON t.tgfoid = p.oid
          WHERE NOT t.tgisinternal
          AND (p.prosrc ~* '\\mset\\M'
              OR p.prosrc ~* 'set_config'
              OR p.prosrc ~* '\\mreset\\M'
              OR p.prosrc ~* '\\mdiscard\\M')
      )
    SQL

    # Module-level mutex guarding both the detection cache and the
    # license-active cache. Detection on first connection per upstream
    # is the hot path; once cached, lookups are read-only.
    @mutex = Mutex.new

    # Detection results, keyed by upstream URL → boolean.
    # Populated by `detect!`; consulted by `effective?`.
    @detection_cache = {}

    # License-payload override, keyed by upstream URL → boolean.
    # Set externally (e.g. by a proxy-status listener that reads the
    # decoded license) via `set_license_active`. Takes precedence over
    # detection but loses to an explicit `:on`/`:off` kwarg.
    @license_active = {}

    class << self
      # Reset the module-level state. Test-only — production code never
      # calls this. Tests use it between cases to keep detection caches
      # from leaking across test boundaries.
      def reset!
        @mutex.synchronize do
          @detection_cache.clear
          @license_active.clear
        end
      end

      # Lookup-only: was detection ever run for this upstream? Used by
      # tests + the wrap path to decide whether to fire the classifier.
      def cached?(upstream)
        return false if upstream.nil?
        @mutex.synchronize { @detection_cache.key?(upstream) }
      end

      # Read the cached detection result. Returns nil if no detection
      # has run for this upstream yet.
      def cached_detection(upstream)
        return nil if upstream.nil?
        @mutex.synchronize { @detection_cache[upstream] }
      end

      # Run the trigger-classifier query against `conn` and cache the
      # boolean result under `upstream`. Idempotent — subsequent calls
      # for the same upstream are no-ops and return the cached value.
      #
      # Failures (connection error, missing pg_trigger, anything else)
      # are swallowed and treated as "no triggers detected" — caching
      # the false result so we don't retry on every wrap. The cost of
      # a false negative is the same as the off-by-default world we
      # had before; missing the optimisation is preferable to crashing
      # `wrap()` on a transient pg_trigger error.
      def detect!(conn, upstream)
        return false if upstream.nil?
        cached = @mutex.synchronize { @detection_cache[upstream] }
        return cached unless cached.nil?

        result = false
        begin
          r = conn.exec(DETECTION_SQL)
          if r.respond_to?(:values)
            row = r.values.first
            if row && row.first
              v = row.first
              result = v == true || v == "t" || v == "true"
            end
          end
        rescue StandardError
          # Detection failed — cache false so we don't keep retrying.
          # The user can still force-enable via `aggressive_verify: :on`.
          result = false
        end

        @mutex.synchronize { @detection_cache[upstream] = result }
        result
      end

      # External hook — called by the proxy-status listener (or any
      # other code path that reads the decoded license payload) when
      # the license carries an `aggressive_verify_active` claim.
      # Setting `nil` clears the override.
      def set_license_active(upstream, active)
        return if upstream.nil?
        @mutex.synchronize do
          if active.nil?
            @license_active.delete(upstream)
          else
            @license_active[upstream] = active ? true : false
          end
        end
      end

      # Read the license-active override (or nil if not set).
      def license_active(upstream)
        return nil if upstream.nil?
        @mutex.synchronize { @license_active[upstream] }
      end

      # Resolve the effective on/off flag from the override kwarg, the
      # license-payload override, and the cached detection result.
      # Returns true (post-DML verify enabled) or false (skip).
      #
      # Precedence (highest first):
      #   1. `override == :on`  / `override == true`  → true
      #   2. `override == :off` / `override == false` → false
      #   3. license-payload override (if set)        → true/false
      #   4. cached detection                         → true/false
      #   5. fallback                                 → false
      def effective?(upstream, override)
        case override
        when :on, true
          return true
        when :off, false
          return false
        when nil, :auto
          # fall through to license + detection lookup
        else
          raise ArgumentError,
            "aggressive_verify must be :auto, :on, :off, true, or false " \
            "(got #{override.inspect})"
        end
        license = @mutex.synchronize { @license_active[upstream] }
        return license unless license.nil?
        detected = @mutex.synchronize { @detection_cache[upstream] }
        return detected unless detected.nil?
        false
      end
    end
  end
end
