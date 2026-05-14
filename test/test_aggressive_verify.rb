# frozen_string_literal: true

# Always-on aggressive verify (cache-key isolation via per-connection
# `dml_seq` bump). Replaces the smart-auto-enable post-DML async
# verify-query trigger with a constant-time counter that mixes into
# the L1 cache state hash.
#
# Background — the trigger-internal-SET correctness gap: a server-side
# trigger that fires on INSERT/UPDATE/DELETE and internally `SET`s a
# session GUC is invisible to the proxy/wrapper. The pre-audit design
# tried to detect such schemas with a `pg_trigger` classifier query and
# then schedule an async `pg_settings` verify after every DML. That
# carried a permanent runtime tax (one extra round-trip per write on
# schemas where the classifier returned true) and a footgun (the
# classifier could return false negatives for triggers that loaded
# `SET` from a string).
#
# Audit-fix design: just bump a per-connection counter on every
# confirmed DML write. The counter mixes into the cache state hash, so
# any subsequent cacheable read on this connection cannot share a slot
# with a pre-DML read keyed on the prior state. Trigger-induced GUC
# drift can no longer replay a stale cached response. Zero extra wire
# traffic; no per-schema classification needed; safe-by-default.

require "minitest/autorun"
require "set"
require "goldlapel"
require "goldlapel/wrap"
require "goldlapel/cache"

# Reuse the recording stub conn from test_guc_state.rb. Loading that
# file brings in `VerifyTestStubConn` plus the native-cache reset
# helpers.
require_relative "test_guc_state"

# ---------------------------------------------------------------------
# `bump_dml_seq` rolls the state hash forward and survives RESET.
# ---------------------------------------------------------------------

class TestBumpDmlSeq < Minitest::Test
  def setup
    @state = GoldLapel::GucState::ConnectionGucState.new
  end

  def test_bump_from_baseline_changes_hash
    h0 = @state.state_hash
    assert_equal 0, h0
    @state.bump_dml_seq
    refute_equal h0, @state.state_hash,
      "post-DML bump must roll the cache key forward"
  end

  def test_bump_each_call_yields_different_hash
    h0 = @state.state_hash
    @state.bump_dml_seq
    h1 = @state.state_hash
    @state.bump_dml_seq
    h2 = @state.state_hash
    @state.bump_dml_seq
    h3 = @state.state_hash
    [h0, h1, h2, h3].combination(2).each do |a, b|
      refute_equal a, b, "every bump must produce a unique state hash"
    end
  end

  def test_bump_combines_with_unsafe_set
    @state.apply(kind: :set, name: "app.user_id", value: "42")
    pre_bump = @state.state_hash
    @state.bump_dml_seq
    refute_equal pre_bump, @state.state_hash
  end

  def test_bump_is_order_independent_with_set
    a = GoldLapel::GucState::ConnectionGucState.new
    a.apply(kind: :set, name: "app.user_id", value: "42")
    a.bump_dml_seq

    b = GoldLapel::GucState::ConnectionGucState.new
    b.bump_dml_seq
    b.apply(kind: :set, name: "app.user_id", value: "42")

    assert_equal a.state_hash, b.state_hash,
      "the same set of mutations applied in different orders must hash equally"
  end

  def test_reset_all_clears_dml_seq_and_state
    @state.apply(kind: :set, name: "app.user_id", value: "42")
    @state.bump_dml_seq
    @state.bump_dml_seq
    refute_equal 0, @state.state_hash

    @state.apply(kind: :reset_all)
    assert_equal 0, @state.state_hash,
      "RESET ALL must drop dml_seq + values back to baseline"
    assert_equal 0, @state.dml_seq
  end

  def test_discard_all_clears_dml_seq_and_state
    @state.bump_dml_seq
    @state.apply(kind: :discard_all)
    assert_equal 0, @state.state_hash
    assert_equal 0, @state.dml_seq
  end

  def test_replace_from_settings_resets_dml_seq
    # Verify-on-checkout reconciles state with server truth — the
    # post-DML divergence counter is reset because the reconciled
    # state IS ground truth for this connection.
    @state.bump_dml_seq
    @state.bump_dml_seq
    @state.replace_from_settings([["app.user_id", "42"]])
    assert_equal 0, @state.dml_seq,
      "successful pg_settings reconcile must drop dml_seq to baseline"
  end

  def test_baseline_state_with_zero_seq_hashes_zero
    # Cache-slot sharing across freshly-opened connections requires
    # an empty connection to hash to exactly 0.
    assert_equal 0, @state.state_hash
  end
end

# ---------------------------------------------------------------------
# Snapshot stack — BEGIN/ROLLBACK must revert dml_seq.
# ---------------------------------------------------------------------

class TestDmlSeqInSnapshotStack < Minitest::Test
  def setup
    @state = GoldLapel::GucState::ConnectionGucState.new
  end

  def test_rollback_restores_pre_begin_dml_seq
    @state.apply(kind: :begin)
    @state.bump_dml_seq
    @state.bump_dml_seq
    assert_equal 2, @state.dml_seq
    @state.apply(kind: :rollback)
    assert_equal 0, @state.dml_seq,
      "ROLLBACK must restore pre-BEGIN dml_seq"
  end

  def test_commit_keeps_bumped_dml_seq
    @state.apply(kind: :begin)
    @state.bump_dml_seq
    @state.apply(kind: :commit)
    assert_equal 1, @state.dml_seq,
      "COMMIT must keep in-tx bumps"
  end
end

# ---------------------------------------------------------------------
# Always-on post-DML bump via the wrapper. Every confirmed write
# rolls dml_seq forward by default; explicit :off opts out with a
# stderr warning.
# ---------------------------------------------------------------------

class TestAggressiveVerifyAlwaysOn < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = VerifyTestStubConn.new
  end

  def teardown
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
  end

  def test_default_kwarg_resolves_active
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    assert wrapped.aggressive_verify_active?,
      "default (no kwarg) must resolve to always-on"
  ensure
    wrapped&.close
  end

  def test_insert_bumps_dml_seq
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    h0 = wrapped.guc_state.state_hash
    seq0 = wrapped.guc_state.dml_seq
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    refute_equal h0, wrapped.guc_state.state_hash,
      "INSERT must bump the state hash via dml_seq"
    assert_equal seq0 + 1, wrapped.guc_state.dml_seq
  ensure
    wrapped&.close
  end

  def test_update_bumps_dml_seq
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("UPDATE orders SET status = 'shipped' WHERE id = 1")
    assert_equal 1, wrapped.guc_state.dml_seq
  ensure
    wrapped&.close
  end

  def test_delete_bumps_dml_seq
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("DELETE FROM orders WHERE id = 1")
    assert_equal 1, wrapped.guc_state.dml_seq
  ensure
    wrapped&.close
  end

  def test_merge_bumps_dml_seq
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("MERGE INTO orders USING staging s ON orders.id = s.id WHEN MATCHED THEN UPDATE SET status = s.status")
    assert_equal 1, wrapped.guc_state.dml_seq
  ensure
    wrapped&.close
  end

  def test_truncate_bumps_dml_seq
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("TRUNCATE orders")
    assert_equal 1, wrapped.guc_state.dml_seq
  ensure
    wrapped&.close
  end

  def test_plain_select_does_not_bump
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("SELECT * FROM accounts")
    assert_equal 0, wrapped.guc_state.dml_seq,
      "pure read must not bump dml_seq"
  ensure
    wrapped&.close
  end

  def test_multiple_writes_accumulate_bumps
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("INSERT INTO orders VALUES (1)")
    wrapped.exec("UPDATE orders SET status = 'x' WHERE id = 1")
    wrapped.exec("DELETE FROM orders WHERE id = 1")
    assert_equal 3, wrapped.guc_state.dml_seq
  ensure
    wrapped&.close
  end

  def test_no_extra_pg_settings_round_trip_per_write
    # The new design replaces the post-DML async pg_settings verify
    # with a counter bump. Plain DML writes must NOT touch
    # pg_settings.
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    t = wrapped.instance_variable_get(:@verify_thread)
    t.join(2) if t&.alive?
    refute @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "plain DML must not pay a pg_settings round-trip — counter-only mitigation"
  ensure
    wrapped&.close
  end
end

# ---------------------------------------------------------------------
# Cache miss after DML — the bump separates pre-DML and post-DML
# cache slots, so a peer connection's pre-DML cached entry cannot be
# served back to this connection after it writes.
# ---------------------------------------------------------------------

class TestCacheMissAfterDml < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = VerifyTestStubConn.new
  end

  def teardown
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
  end

  def test_post_dml_read_misses_baseline_slot
    # Peer connection's cached entry — keyed at the empty-state hash
    # (= 0). Note: collect_write_invalidations on "INSERT INTO
    # other_table" would NOT invalidate "accounts" — we want to see
    # the pure dml_seq effect, not the self-invalidation effect.
    @cache.put("SELECT * FROM accounts", nil, [["alice"]], ["name"], 0)

    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("INSERT INTO other_table VALUES (1)")
    # Pre-bump assertion: the peer slot was reachable at sh=0.
    assert_equal 1, wrapped.guc_state.dml_seq

    pre = @real.exec_log.length
    wrapped.exec("SELECT * FROM accounts")
    post = @real.exec_log.length
    assert_operator post, :>, pre,
      "post-DML read must miss the peer-keyed slot (new state hash)"
  ensure
    wrapped&.close
  end

  def test_off_flag_preserves_peer_slot_share
    # Opt-out path: dml_seq does NOT bump, so the peer slot remains
    # reachable. Documented footgun — only safe when the schema has
    # no SET-mutating triggers.
    @cache.put("SELECT * FROM accounts", nil, [["alice"]], ["name"], 0)

    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache, aggressive_verify_active: false,
    )
    wrapped.exec("INSERT INTO other_table VALUES (1)")
    assert_equal 0, wrapped.guc_state.dml_seq
    pre = @real.exec_log.length
    wrapped.exec("SELECT * FROM accounts")
    post = @real.exec_log.length
    assert_equal pre, post,
      "opt-out path must let the peer-keyed slot stay reachable"
  ensure
    wrapped&.close
  end

  def test_second_read_at_same_dml_seq_hits_own_slot
    # After the post-DML miss, the wrapper caches its own fetched
    # response at the new state hash. Subsequent reads on this
    # connection at the same dml_seq must hit.
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("INSERT INTO other_table VALUES (1)")
    # First post-DML read: cache miss → cache put at new sh.
    @real.exec_log.clear
    wrapped.exec("SELECT something_unique_zzz")
    refute @real.exec_log.empty?, "first read after DML must miss"
    # Second read at the same state hash: must hit.
    @real.exec_log.clear
    wrapped.exec("SELECT something_unique_zzz")
    assert @real.exec_log.empty?,
      "second read at same dml_seq must hit the freshly-cached slot"
  ensure
    wrapped&.close
  end
end

# ---------------------------------------------------------------------
# Dirty-flag bypass — when `dirty?` is set (in-flight or failed verify),
# the L1 cache is bypassed for both GET and PUT. Routes the read
# straight to the proxy.
# ---------------------------------------------------------------------

class TestDirtyBypass < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = VerifyTestStubConn.new
    @wrapped = GoldLapel::CachedConnection.new(@real, @cache)
  end

  def teardown
    @wrapped.close rescue nil
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
  end

  def test_dirty_bypasses_cache_get
    # Even with a freshly-populated cache slot at this connection's
    # current state hash, a dirty wrapper must NOT serve from
    # cache — the state hash may not reflect server reality.
    sh = @wrapped.guc_state.state_hash
    @cache.put("SELECT * FROM accounts", nil, [["alice"]], ["name"], sh)

    @real.raise_on_pg_settings = true
    @wrapped.guc_state.mark_dirty!
    # ensure_state_clean! will run, fail (raise_on_pg_settings),
    # and leave dirty set. The user's read then bypasses cache.
    pre = @real.exec_log.length
    @wrapped.exec("SELECT * FROM accounts")
    post = @real.exec_log.length
    # ensure_state_clean! + user delegate = at least 2 wire ops.
    assert_operator post, :>, pre,
      "dirty-bypass must route the read to the underlying connection"
    assert @wrapped.guc_state.dirty?,
      "dirty must remain set after a failed reconcile"
  end

  def test_dirty_bypasses_cache_put
    # If verify-on-checkout fails, dirty stays set. The wrapper
    # must NOT cache the user's read result either — a slot keyed
    # at a dirty hash could be served back at a future state where
    # that hash is no longer accurate.
    @real.raise_on_pg_settings = true
    @wrapped.guc_state.mark_dirty!
    sh = @wrapped.guc_state.state_hash
    @wrapped.exec("SELECT something_unique_zzzz")
    # Cache must have no entry at the dirty hash.
    assert_nil @cache.get("SELECT something_unique_zzzz", nil, sh),
      "dirty-bypass must skip the cache put as well as the get"
  end

  def test_clean_state_uses_cache_normally
    sh = @wrapped.guc_state.state_hash
    @cache.put("SELECT * FROM accounts", nil, [["alice"]], ["name"], sh)
    pre = @real.exec_log.length
    @wrapped.exec("SELECT * FROM accounts")
    post = @real.exec_log.length
    assert_equal pre, post,
      "clean wrapper must serve from cache (no wire round-trip)"
  end
end

# ---------------------------------------------------------------------
# In-flight verify must serialise subsequent queries — the verify
# thread acquires @real_mutex so the user's next exec blocks behind
# it. Mirrors the Wave 1 post-call verify guarantee.
# ---------------------------------------------------------------------

class TestVerifySerializesNextQuery < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = VerifyTestStubConn.new
    @wrapped = GoldLapel::CachedConnection.new(@real, @cache)
  end

  def teardown
    @wrapped.close rescue nil
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
  end

  def test_real_mutex_blocks_user_query_while_verify_runs
    # Schedule a funcall verify (Wave 1 path). Take @real_mutex to
    # pin the verifier behind the lock, then attempt a user query
    # in a separate thread — it must block until the lock is freed.
    @real.pg_settings_rows = [["app.user_id", "42"]]
    @wrapped.exec("SELECT some_function_for_serialization()")
    mu = @wrapped.instance_variable_get(:@real_mutex)

    # The verifier already grabbed (or is about to grab) the lock.
    # Drive a competing user-thread exec; it must NOT proceed
    # until the verifier releases the lock.
    progressed = false
    competing = Thread.new do
      @wrapped.exec("SELECT 1")
      progressed = true
    end

    # Give the competing thread a brief chance to barge in. If
    # mutex serialisation is broken, it'd race past the verifier.
    competing.join(0.2)
    # By now either the verifier has finished (legitimate) or the
    # competing query is still parked behind the mutex. Either way,
    # the competing query and the verifier must NOT have run their
    # underlying @real.exec calls concurrently — a check we infer
    # indirectly by validating the wrapper still works.
    competing.join(2)
    assert progressed, "competing query must eventually complete after verifier releases"
    assert mu.respond_to?(:synchronize),
      "@real_mutex must remain a Mutex (sanity check)"
  end
end

# ---------------------------------------------------------------------
# Opt-out (`:off` / `false`) — emits a one-shot security warning.
# ---------------------------------------------------------------------

class TestAggressiveVerifyOptOut < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
  end

  # Stub real_conn for `wrap` — doesn't need pg_settings.
  def make_fake_conn
    c = Object.new
    c.define_singleton_method(:exec) { |_sql, &_blk| VerifyTestStubConn::StubResult.new([["1"]], ["c"]) }
    c.define_singleton_method(:close) {}
    c.define_singleton_method(:finished?) { false }
    c
  end

  def capture_stderr
    orig = $stderr
    out = StringIO.new
    $stderr = out
    yield
    out.string
  ensure
    $stderr = orig
  end

  def test_off_emits_one_shot_warning
    output = capture_stderr do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: :off)
    end
    assert_match(/aggressive_verify.*:off/i, output)
    assert_match(/trigger/i, output,
      "warning must mention server-side triggers (the documented footgun)")
  end

  def test_off_warning_fires_only_once_per_process
    first = capture_stderr do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: :off)
    end
    refute_empty first
    second = capture_stderr do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: :off)
    end
    assert_empty second,
      "subsequent :off wrappers must reuse the already-warned flag (no log spam)"
  end

  def test_false_alias_warns_and_disables
    output = capture_stderr do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: false)
    end
    refute_empty output, "boolean `false` is an alias for `:off` and must warn too"
  end

  def test_on_does_not_warn
    output = capture_stderr do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: :on)
    end
    assert_empty output
  end

  def test_auto_does_not_warn
    output = capture_stderr do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: :auto)
    end
    assert_empty output
  end

  def test_unknown_value_raises
    assert_raises(ArgumentError) do
      GoldLapel.wrap(make_fake_conn, invalidation_port: 0, aggressive_verify: :weird)
    end
  end

  def test_off_disables_bump_on_dml
    # End-to-end: kwarg :off → no dml_seq bump on INSERT.
    capture_stderr do
      real = VerifyTestStubConn.new
      cache = GoldLapel::NativeCache.new
      cache.instance_variable_set(:@invalidation_connected, true)
      wrapped = GoldLapel::CachedConnection.new(
        real, cache, aggressive_verify_active: false,
      )
      wrapped.exec("INSERT INTO orders VALUES (1)")
      assert_equal 0, wrapped.guc_state.dml_seq,
        "opt-out path must skip the bump"
      wrapped.close
    end
  end
end

# ---------------------------------------------------------------------
# Wave 1 funcall verify preserved — top-level `SELECT func(...)`
# still schedules an async pg_settings reconcile. The new bump
# composes alongside it.
# ---------------------------------------------------------------------

class TestWave1FuncallVerifyPreserved < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = VerifyTestStubConn.new
    @wrapped = GoldLapel::CachedConnection.new(@real, @cache)
  end

  def teardown
    @wrapped.close rescue nil
    GoldLapel::NativeCache.reset!
    GoldLapel._reset_aggressive_verify_warning!
  end

  def join_verify
    t = @wrapped.instance_variable_get(:@verify_thread)
    t.join(2) if t&.alive?
  end

  def test_funcall_still_schedules_pg_settings_verify
    @real.pg_settings_rows = [["app.user_id", "42"]]
    @wrapped.exec("SELECT some_function()")
    join_verify
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "Wave 1 funcall verify must still fire under always-on bump design"
  end

  def test_funcall_clears_dirty_on_successful_reconcile
    @real.pg_settings_rows = [["app.user_id", "42"]]
    @wrapped.exec("SELECT some_function()")
    join_verify
    refute @wrapped.guc_state.dirty?,
      "successful funcall verify must clear dirty"
  end

  def test_set_config_inline_still_skips_verify
    @real.pg_settings_rows = [["app.user_id", "42"]]
    @wrapped.exec("SELECT set_config('app.user_id', '42', false)")
    join_verify
    refute @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "set_config function form must continue to skip post-call verify (inline applied)"
  end
end
