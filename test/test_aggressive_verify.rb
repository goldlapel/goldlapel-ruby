# frozen_string_literal: true

require "minitest/autorun"
require "set"
require "goldlapel"
require "goldlapel/wrap"
require "goldlapel/cache"
require "goldlapel/aggressive_verify"

# Reuse the recording stub conn from test_guc_state.rb. Loading that file
# brings in `VerifyTestStubConn` plus `PG::Error` shim, plus the
# native-cache reset helpers.
require_relative "test_guc_state"

# A stub conn that records the detection SQL and returns a configurable
# boolean for `pg_trigger` lookups. Mirrors VerifyTestStubConn's shape so
# we can swap it in for `wrap()` smoke tests.
class TriggerDetectStubConn
  StubResult = Struct.new(:rows, :columns) do
    def values; rows; end
    def fields; columns; end
    def clear; end
  end

  attr_accessor :trigger_match
  attr_reader :exec_log

  def initialize(trigger_match: false)
    @trigger_match = trigger_match
    @exec_log = []
  end

  def exec(sql, &block)
    @exec_log << sql
    if sql.is_a?(String) && sql.include?("pg_trigger")
      r = StubResult.new([[@trigger_match ? "t" : "f"]], ["exists"])
      block&.call(r)
      return r
    end
    r = StubResult.new([["1"]], ["c"])
    block&.call(r)
    r
  end
  alias_method :query, :exec

  def async_exec(sql, &block) = exec(sql, &block)
  def exec_params(sql, params = [], _rf = 0, &block) = exec(sql, &block)
  def async_exec_params(sql, params = [], _rf = 0, &block) = exec(sql, &block)
  def close; end
  def finished?; false; end
end

# ---------------------------------------------------------------------
# Detection module — pg_trigger classifier and module-level cache.
# ---------------------------------------------------------------------

class TestAggressiveVerifyDetection < Minitest::Test
  def setup
    GoldLapel::AggressiveVerify.reset!
  end

  def teardown
    GoldLapel::AggressiveVerify.reset!
  end

  def test_detect_returns_true_when_trigger_body_matches
    conn = TriggerDetectStubConn.new(trigger_match: true)
    assert GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/db1")
  end

  def test_detect_returns_false_when_no_matching_trigger
    conn = TriggerDetectStubConn.new(trigger_match: false)
    refute GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/db2")
  end

  def test_detect_runs_classifier_sql
    conn = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/db3")
    assert conn.exec_log.any? { |s| s.include?("pg_trigger") },
      "detect! must hit pg_trigger"
    assert conn.exec_log.any? { |s| s.include?("set_config") },
      "detection SQL must check for set_config in trigger body"
  end

  def test_detect_caches_result_per_upstream
    conn = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/cached")
    before = conn.exec_log.length
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/cached")
    after = conn.exec_log.length
    assert_equal before, after,
      "subsequent detect! calls must not re-issue the classifier SQL"
  end

  def test_detect_cache_separate_per_upstream
    conn = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/db_a")
    conn.trigger_match = false
    refute GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/db_b"),
      "different upstream URL must re-run detection"
    assert GoldLapel::AggressiveVerify.cached_detection("postgres://x/db_a"),
      "first upstream's cached result must persist"
  end

  def test_cached_returns_true_after_detect
    conn = TriggerDetectStubConn.new(trigger_match: false)
    refute GoldLapel::AggressiveVerify.cached?("postgres://x/dbq")
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/dbq")
    assert GoldLapel::AggressiveVerify.cached?("postgres://x/dbq")
  end

  def test_detect_swallows_errors_and_caches_false
    raising = Class.new do
      def exec(_sql, &_b); raise StandardError, "boom"; end
    end.new
    refute GoldLapel::AggressiveVerify.detect!(raising, "postgres://x/err")
    # Cached as false — second call doesn't re-raise either.
    refute GoldLapel::AggressiveVerify.detect!(raising, "postgres://x/err")
  end

  def test_detect_nil_upstream_is_noop
    conn = TriggerDetectStubConn.new(trigger_match: true)
    refute GoldLapel::AggressiveVerify.detect!(conn, nil)
    assert_equal 0, conn.exec_log.length
  end
end

# ---------------------------------------------------------------------
# Override resolution — :auto / :on / :off / nil / true / false.
# ---------------------------------------------------------------------

class TestAggressiveVerifyOverride < Minitest::Test
  def setup
    GoldLapel::AggressiveVerify.reset!
  end

  def teardown
    GoldLapel::AggressiveVerify.reset!
  end

  def test_explicit_on_overrides_detection
    conn = TriggerDetectStubConn.new(trigger_match: false)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/foo")
    assert GoldLapel::AggressiveVerify.effective?("postgres://x/foo", :on)
    assert GoldLapel::AggressiveVerify.effective?("postgres://x/foo", true)
  end

  def test_explicit_off_overrides_detection
    conn = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/bar")
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/bar", :off)
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/bar", false)
  end

  def test_auto_uses_cached_detection
    conn = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/auto_on")
    assert GoldLapel::AggressiveVerify.effective?("postgres://x/auto_on", :auto)

    conn2 = TriggerDetectStubConn.new(trigger_match: false)
    GoldLapel::AggressiveVerify.detect!(conn2, "postgres://x/auto_off")
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/auto_off", :auto)
  end

  def test_auto_with_no_detection_falls_back_to_off
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/none", :auto)
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/none", nil)
  end

  def test_license_active_takes_priority_over_detection
    conn = TriggerDetectStubConn.new(trigger_match: false)
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/lic1")
    GoldLapel::AggressiveVerify.set_license_active("postgres://x/lic1", true)
    assert GoldLapel::AggressiveVerify.effective?("postgres://x/lic1", :auto)
  end

  def test_license_active_loses_to_explicit_kwarg
    GoldLapel::AggressiveVerify.set_license_active("postgres://x/lic2", true)
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/lic2", :off)
    GoldLapel::AggressiveVerify.set_license_active("postgres://x/lic3", false)
    assert GoldLapel::AggressiveVerify.effective?("postgres://x/lic3", :on)
  end

  def test_license_set_to_nil_clears_override
    GoldLapel::AggressiveVerify.set_license_active("postgres://x/lic4", true)
    GoldLapel::AggressiveVerify.set_license_active("postgres://x/lic4", nil)
    refute GoldLapel::AggressiveVerify.effective?("postgres://x/lic4", :auto)
  end

  def test_unknown_override_value_raises
    assert_raises(ArgumentError) do
      GoldLapel::AggressiveVerify.effective?("postgres://x/r", :weird)
    end
  end
end

# ---------------------------------------------------------------------
# Wrap-time integration: smart-auto detection fires on first wrap; the
# CachedConnection surfaces the resolved flag and dispatches verify.
# ---------------------------------------------------------------------

class TestAggressiveVerifyWrap < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel::AggressiveVerify.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::AggressiveVerify.reset!
    GoldLapel::NativeCache.reset!
  end

  def test_wrap_runs_smart_auto_detection_on_first_connect
    conn = TriggerDetectStubConn.new(trigger_match: true)
    wrapped = GoldLapel::CachedConnection.new(
      conn, @cache,
      aggressive_verify: :auto,
      upstream: "postgres://x/wrap1",
    )
    # `CachedConnection.new` itself doesn't run detection — `wrap()` does.
    # Drive detection explicitly here mirroring the real wrap path.
    GoldLapel::AggressiveVerify.detect!(conn, "postgres://x/wrap1")
    assert wrapped.aggressive_verify_effective?
  end

  def test_wrap_skips_detection_when_override_is_on
    # When the user has already opted in, the round-trip is wasted —
    # `wrap()` short-circuits the classifier query.
    detected = false
    fake_conn = Object.new
    fake_conn.define_singleton_method(:exec) do |sql, &_blk|
      detected = true if sql.is_a?(String) && sql.include?("pg_trigger")
      VerifyTestStubConn::StubResult.new([["1"]], ["c"])
    end
    fake_conn.define_singleton_method(:close) {}

    GoldLapel.wrap(
      fake_conn,
      invalidation_port: 0,
      aggressive_verify: :on,
      upstream: "postgres://x/skip_on",
    )
    refute detected,
      "explicit :on must skip the classifier round-trip"
  end

  def test_wrap_skips_detection_when_override_is_off
    detected = false
    fake_conn = Object.new
    fake_conn.define_singleton_method(:exec) do |sql, &_blk|
      detected = true if sql.is_a?(String) && sql.include?("pg_trigger")
      VerifyTestStubConn::StubResult.new([["1"]], ["c"])
    end
    fake_conn.define_singleton_method(:close) {}

    GoldLapel.wrap(
      fake_conn,
      invalidation_port: 0,
      aggressive_verify: :off,
      upstream: "postgres://x/skip_off",
    )
    refute detected
  end

  def test_wrap_runs_detection_once_per_upstream
    # Two connections to the same upstream must share one detection.
    fake_conn = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel.wrap(
      fake_conn, invalidation_port: 0,
      aggressive_verify: :auto, upstream: "postgres://x/once",
    )
    detect_calls_after_first = fake_conn.exec_log.count { |s| s.include?("pg_trigger") }

    fake_conn2 = TriggerDetectStubConn.new(trigger_match: true)
    GoldLapel.wrap(
      fake_conn2, invalidation_port: 0,
      aggressive_verify: :auto, upstream: "postgres://x/once",
    )
    assert_equal 1, detect_calls_after_first
    assert_equal 0, fake_conn2.exec_log.count { |s| s.include?("pg_trigger") },
      "second wrap with same upstream must not re-classify"
  end
end

# ---------------------------------------------------------------------
# Post-DML async verify — when aggressive verify is on, every confirmed
# write schedules an async pg_settings reconcile via the same machinery
# Wave 1 used for top-level function calls.
# ---------------------------------------------------------------------

class TestAggressiveVerifyPostDML < Minitest::Test
  def setup
    GoldLapel::NativeCache.reset!
    GoldLapel::AggressiveVerify.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = VerifyTestStubConn.new
    @upstream = "postgres://x/dml"
  end

  def teardown
    GoldLapel::AggressiveVerify.reset!
    GoldLapel::NativeCache.reset!
  end

  # Wait for any in-flight verify thread to finish.
  def join_verify(wrapped)
    t = wrapped.instance_variable_get(:@verify_thread)
    t.join(2) if t&.alive?
  end

  def test_insert_with_flag_on_schedules_verify
    @real.pg_settings_rows = [["app.user_id", "42"]]
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "DML write must schedule pg_settings verify when aggressive flag is on"
  ensure
    wrapped&.close
  end

  def test_update_with_flag_on_schedules_verify
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("UPDATE orders SET status = 'shipped' WHERE id = 1")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_delete_with_flag_on_schedules_verify
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("DELETE FROM orders WHERE id = 1")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_truncate_with_flag_on_schedules_verify
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("TRUNCATE orders")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_merge_with_flag_on_schedules_verify
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("MERGE INTO orders USING staging s ON orders.id = s.id WHEN MATCHED THEN UPDATE SET status = s.status")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_insert_with_flag_off_does_not_schedule_verify
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :off,
      upstream: @upstream,
    )
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    refute @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "flag off must not schedule verify on plain DML"
  ensure
    wrapped&.close
  end

  def test_select_with_flag_on_does_not_schedule_verify
    # A read is not a write — the verify only fires on DML.
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("SELECT * FROM accounts")
    join_verify(wrapped)
    refute @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "plain SELECT must not pay a verify round-trip even with flag on"
  ensure
    wrapped&.close
  end

  def test_auto_with_detection_on_schedules_verify
    # Smart-auto path: detect! returned true for this upstream → verify
    # fires on DML automatically.
    GoldLapel::AggressiveVerify.detect!(
      TriggerDetectStubConn.new(trigger_match: true),
      @upstream,
    )
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :auto,
      upstream: @upstream,
    )
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_auto_with_detection_off_does_not_schedule_verify
    GoldLapel::AggressiveVerify.detect!(
      TriggerDetectStubConn.new(trigger_match: false),
      @upstream,
    )
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :auto,
      upstream: @upstream,
    )
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    refute @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_license_active_drives_post_dml_verify
    # No detection run; license-active override is the only signal.
    GoldLapel::AggressiveVerify.set_license_active(@upstream, true)
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :auto,
      upstream: @upstream,
    )
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "license-payload override must drive verify when set to true"
  ensure
    wrapped&.close
  end

  def test_dml_in_transaction_does_not_spawn_verify_thread
    # Mid-tx verify is unsafe — same gating as concern 6's funcall path.
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("BEGIN")
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    t = wrapped.instance_variable_get(:@verify_thread)
    assert t.nil? || !t.alive?,
      "verify thread must not spawn mid-tx even with aggressive flag on"
    assert wrapped.guc_state.dirty?,
      "DML mid-tx still marks dirty for post-commit checkout"
  ensure
    wrapped&.close
  end

  def test_multistatement_dml_with_flag_on_schedules_verify
    # `SET app.x = 'y'; INSERT INTO orders ...` — the SET applies inline,
    # the INSERT is the wrote_something. Verify scheduled on the INSERT
    # alone.
    @real.pg_settings_rows = [["app.x", "y"]]
    wrapped = GoldLapel::CachedConnection.new(
      @real, @cache,
      aggressive_verify: :on,
      upstream: @upstream,
    )
    wrapped.exec("SET app.x = 'y'; INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    assert @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") }
  ensure
    wrapped&.close
  end

  def test_default_kwarg_is_auto
    # No upstream, no detection — :auto resolves to false.
    wrapped = GoldLapel::CachedConnection.new(@real, @cache)
    wrapped.exec("INSERT INTO orders (id) VALUES (1)")
    join_verify(wrapped)
    refute @real.exec_log.any? { |s| s.is_a?(String) && s.include?("pg_settings") },
      "default :auto with no detection must behave like off"
  ensure
    wrapped&.close
  end
end
