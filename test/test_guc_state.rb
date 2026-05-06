# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/goldlapel/guc_state"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"

# Per-connection unsafe-GUC state hash — wrapper-side mirror of the
# proxy's `src/guc_state.rs` (Option Y). Folded into the L1 cache key so
# custom-GUC RLS can never leak rows across connections with different
# unsafe state. Tests cover:
#
#   - classifier (`unsafe_guc?`)
#   - parser (`parse_set_command`) — every SET / RESET shape
#   - statement splitter (`split_statements`) — string-literal aware
#   - `ConnectionGucState` — apply, observe_sql, hash invariants
#   - cache key correctness — different unsafe state ⇒ different key

class TestUnsafeGucClassifier < Minitest::Test
  def test_short_list_members_are_unsafe
    %w[search_path role session_authorization
       default_transaction_isolation default_transaction_read_only
       transaction_isolation row_security].each do |name|
      assert GoldLapel::GucState.unsafe_guc?(name), "#{name} must be unsafe"
    end
  end

  def test_classification_is_case_insensitive
    assert GoldLapel::GucState.unsafe_guc?("ROLE")
    assert GoldLapel::GucState.unsafe_guc?("Search_Path")
    assert GoldLapel::GucState.unsafe_guc?("SEARCH_PATH")
  end

  def test_namespaced_gucs_are_unsafe
    %w[app.user_id myapp.tenant rls.account a.b.c APP.USER].each do |name|
      assert GoldLapel::GucState.unsafe_guc?(name), "#{name} must be unsafe"
    end
  end

  def test_safe_gucs_are_safe
    # GUCs that don't change query results or output bytes:
    # planner cost knobs, statement_timeout, client_encoding (purely
    # transport-level, doesn't change cached value strings),
    # application_name (label only).
    %w[application_name statement_timeout work_mem client_encoding
       random_page_cost effective_cache_size].each do |name|
      refute GoldLapel::GucState.unsafe_guc?(name), "#{name} must be safe"
    end
  end

  def test_output_formatting_gucs_are_unsafe
    # Wave 2 expansion (concern 3): GUCs that change the bytes PG
    # serialises for a row, even when the row is identical. Folding
    # them into the state hash fragments the cache by formatting
    # context, which prevents a connection set to America/New_York
    # from receiving a UTC-formatted timestamp from a peer
    # connection's cache slot.
    %w[DateStyle datestyle IntervalStyle intervalstyle
       TimeZone timezone bytea_output
       lc_messages lc_monetary lc_numeric lc_time
       LC_TIME].each do |name|
      assert GoldLapel::GucState.unsafe_guc?(name), "#{name} must be unsafe"
    end
  end
end

class TestParseSetCommand < Minitest::Test
  def parse(sql)
    GoldLapel::GucState.parse_set_command(sql)
  end

  # ---- shapes ----

  def test_set_eq_quoted
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse("SET foo = 'bar'"))
  end

  def test_set_to_quoted
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse("SET foo TO 'bar'"))
  end

  def test_set_unquoted
    assert_equal({ kind: :set, name: "foo", value: "42" },
                 parse("SET foo = 42"))
  end

  def test_set_session_modifier
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse("SET SESSION foo = 'bar'"))
  end

  def test_set_local_modifier
    assert_equal({ kind: :set_local, name: "foo", value: "bar" },
                 parse("SET LOCAL foo = 'bar'"))
  end

  def test_reset_named
    assert_equal({ kind: :reset, name: "foo" }, parse("RESET foo"))
  end

  def test_reset_all
    assert_equal({ kind: :reset_all }, parse("RESET ALL"))
  end

  # ---- case + whitespace + semicolon ----

  def test_case_insensitive_keywords
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse("set foo = 'bar'"))
    assert_equal({ kind: :set_local, name: "foo", value: "bar" },
                 parse("Set Local foo To 'bar'"))
    assert_equal({ kind: :reset_all }, parse("reset all"))
  end

  def test_lowercases_guc_name
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SET App.User_ID = '42'"))
  end

  def test_tolerates_trailing_semicolon
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse("SET foo = 'bar';"))
    assert_equal({ kind: :reset, name: "foo" }, parse("RESET foo ;"))
  end

  def test_tolerates_extra_whitespace
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse("   SET    foo   =   'bar'   "))
  end

  def test_glued_equals
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SET app.user_id='42'"))
  end

  def test_double_quoted_value
    assert_equal({ kind: :set, name: "foo", value: "bar" },
                 parse('SET foo = "bar"'))
  end

  def test_double_quoted_name
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse('SET "app.user_id" = \'42\''))
  end

  # ---- rejects ----

  def test_rejects_non_set_statements
    assert_nil parse("SELECT 1")
    assert_nil parse("BEGIN")
    assert_nil parse("UPDATE t SET x = 1")
  end

  def test_rejects_empty
    assert_nil parse("")
    assert_nil parse("   ")
    assert_nil parse(";")
  end

  def test_rejects_set_without_value
    assert_nil parse("SET foo =")
    assert_nil parse("SET foo TO")
    assert_nil parse("SET foo")
  end

  def test_rejects_reset_with_garbage
    assert_nil parse("RESET foo bar")
  end

  def test_set_time_zone_two_word_form_routes_through_set
    # Legacy two-word PG form. Timezone is now classified as unsafe
    # (output formatting affects cached bytes), so the parser must
    # route this through the standard `:set` shape with the
    # canonical lowercased GUC name `timezone`.
    assert_equal({ kind: :set, name: "timezone", value: "UTC" },
                 parse("SET TIME ZONE 'UTC'"))
    assert_equal({ kind: :set, name: "timezone", value: "America/New_York" },
                 parse("set time zone 'America/New_York'"))
    # Trailing semicolon tolerated.
    assert_equal({ kind: :set, name: "timezone", value: "UTC" },
                 parse("SET TIME ZONE 'UTC';"))
  end
end

# ---------------------------------------------------------------------
# DISCARD parser — concern 1.
# ---------------------------------------------------------------------
class TestParseDiscard < Minitest::Test
  def parse(sql)
    GoldLapel::GucState.parse_set_command(sql)
  end

  def test_discard_all
    assert_equal({ kind: :discard_all }, parse("DISCARD ALL"))
  end

  def test_discard_all_case_insensitive
    assert_equal({ kind: :discard_all }, parse("discard all"))
    assert_equal({ kind: :discard_all }, parse("Discard All"))
  end

  def test_discard_plans
    assert_equal({ kind: :discard_plans }, parse("DISCARD PLANS"))
  end

  def test_discard_sequences
    assert_equal({ kind: :discard_sequences }, parse("DISCARD SEQUENCES"))
  end

  def test_discard_temp
    assert_equal({ kind: :discard_temp }, parse("DISCARD TEMP"))
    assert_equal({ kind: :discard_temp }, parse("DISCARD TEMPORARY"))
  end

  def test_discard_with_trailing_semicolon
    assert_equal({ kind: :discard_all }, parse("DISCARD ALL;"))
  end

  def test_discard_with_extra_whitespace
    assert_equal({ kind: :discard_all }, parse("   DISCARD   ALL   "))
  end

  def test_rejects_discard_without_target
    assert_nil parse("DISCARD")
    assert_nil parse("DISCARD;")
  end

  def test_rejects_discard_with_unknown_target
    assert_nil parse("DISCARD FOO")
    assert_nil parse("DISCARD app.user_id")
  end

  def test_rejects_discard_with_garbage_after_target
    assert_nil parse("DISCARD ALL extra")
  end
end

# ---------------------------------------------------------------------
# `SELECT set_config(...)` — concern 2. Function-form SET as used by
# Supabase / PostgREST for per-request JWT-driven GUCs.
# ---------------------------------------------------------------------
class TestParseSetConfigCall < Minitest::Test
  def parse(sql)
    GoldLapel::GucState.parse_set_command(sql)
  end

  def test_set_config_session
    # is_local=false → behaves as `SET name = value` (session-wide,
    # mutates state hash).
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', false)"))
  end

  def test_set_config_local
    # is_local=true → behaves as `SET LOCAL`, no state hash mutation
    # (cache is bypassed inside transactions anyway).
    assert_equal({ kind: :set_local, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', true)"))
  end

  def test_set_config_pg_catalog_schema
    # PG accepts the schema-qualified form too.
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT pg_catalog.set_config('app.user_id', '42', false)"))
  end

  def test_set_config_case_insensitive_keywords
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("select Set_Config('app.user_id', '42', FALSE)"))
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT SET_CONFIG('app.user_id', '42', FALSE)"))
  end

  def test_set_config_lowercases_guc_name
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('App.User_ID', '42', false)"))
  end

  def test_set_config_with_extra_whitespace
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT  set_config (  'app.user_id'  ,  '42'  ,  false  )"))
  end

  def test_set_config_with_trailing_semicolon
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', false);"))
  end

  def test_set_config_t_f_aliases_for_is_local
    # PG accepts `'t'`/`'f'`, `'true'`/`'false'`, `0`/`1` as boolean
    # literal aliases — common in Supabase JWT helpers.
    assert_equal({ kind: :set_local, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', 't')"))
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', 'f')"))
    assert_equal({ kind: :set_local, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', 1)"))
    assert_equal({ kind: :set, name: "app.user_id", value: "42" },
                 parse("SELECT set_config('app.user_id', '42', 0)"))
  end

  def test_set_config_doubled_quote_escape
    # PG escapes `'` inside a single-quoted literal by doubling.
    cmd = parse("SELECT set_config('app.user', 'it''s 42', false)")
    assert_equal({ kind: :set, name: "app.user", value: "it's 42" }, cmd)
  end

  def test_set_config_null_value_is_reset
    # PG treats `set_config(name, NULL, false)` as RESET name. The
    # parser routes it through the `:reset` kind to keep state-hash
    # bookkeeping consistent with the explicit RESET form.
    assert_equal({ kind: :reset, name: "app.user_id" },
                 parse("SELECT set_config('app.user_id', NULL, false)"))
  end

  def test_set_config_null_value_with_local_is_noop
    # SET LOCAL NULL is the no-op path (transient + reset).
    assert_nil parse("SELECT set_config('app.user_id', NULL, true)")
  end

  def test_rejects_non_literal_value_arg
    # Indirect / non-literal forms (e.g. nested current_setting)
    # cannot be evaluated client-side. Fall back to post-call verify.
    assert_nil parse("SELECT set_config('app.user_id', current_setting('x'), false)")
    assert_nil parse("SELECT set_config('app.user_id', user_id, false)")
  end

  def test_rejects_non_literal_name_arg
    assert_nil parse("SELECT set_config(name_column, '42', false)")
  end

  def test_rejects_set_config_without_third_arg
    assert_nil parse("SELECT set_config('app.user_id', '42')")
  end
end

# ---------------------------------------------------------------------
# DISCARD apply — concern 1 wired into ConnectionGucState.
# ---------------------------------------------------------------------
class TestDiscardApply < Minitest::Test
  def setup
    @s = GoldLapel::GucState::ConnectionGucState.new
  end

  def test_discard_all_clears_state
    @s.observe_sql("SET app.user_id = '42'")
    @s.observe_sql("SET search_path TO 'tenant_a'")
    refute_equal 0, @s.state_hash
    @s.observe_sql("DISCARD ALL")
    assert_equal 0, @s.state_hash
  end

  def test_discard_all_on_empty_state_is_noop
    @s.observe_sql("DISCARD ALL")
    assert_equal 0, @s.state_hash
  end

  def test_discard_plans_does_not_clear_state
    @s.observe_sql("SET app.user_id = '42'")
    h = @s.state_hash
    @s.observe_sql("DISCARD PLANS")
    assert_equal h, @s.state_hash
  end

  def test_discard_sequences_does_not_clear_state
    @s.observe_sql("SET app.user_id = '42'")
    h = @s.state_hash
    @s.observe_sql("DISCARD SEQUENCES")
    assert_equal h, @s.state_hash
  end

  def test_discard_temp_does_not_clear_state
    @s.observe_sql("SET app.user_id = '42'")
    h = @s.state_hash
    @s.observe_sql("DISCARD TEMP")
    assert_equal h, @s.state_hash
    @s.observe_sql("DISCARD TEMPORARY")
    assert_equal h, @s.state_hash
  end
end

# ---------------------------------------------------------------------
# set_config apply — concern 2 wired into ConnectionGucState. Mirrors
# the regular SET / RESET behaviour.
# ---------------------------------------------------------------------
class TestSetConfigApply < Minitest::Test
  def setup
    @s = GoldLapel::GucState::ConnectionGucState.new
  end

  def test_set_config_changes_state_hash
    @s.observe_sql("SELECT set_config('app.user_id', '42', false)")
    refute_equal 0, @s.state_hash
  end

  def test_set_config_local_does_not_change_state_hash
    @s.observe_sql("SELECT set_config('app.user_id', '42', true)")
    assert_equal 0, @s.state_hash
  end

  def test_set_config_equivalent_to_set
    a = GoldLapel::GucState::ConnectionGucState.new
    b = GoldLapel::GucState::ConnectionGucState.new
    a.observe_sql("SET app.user_id = '42'")
    b.observe_sql("SELECT set_config('app.user_id', '42', false)")
    assert_equal a.state_hash, b.state_hash
  end

  def test_set_config_null_resets_state
    @s.observe_sql("SELECT set_config('app.user_id', '42', false)")
    refute_equal 0, @s.state_hash
    @s.observe_sql("SELECT set_config('app.user_id', NULL, false)")
    assert_equal 0, @s.state_hash
  end

  def test_pg_catalog_set_config_recognised
    @s.observe_sql("SELECT pg_catalog.set_config('app.user_id', '42', false)")
    refute_equal 0, @s.state_hash
  end

  def test_set_config_with_safe_guc_does_not_change_hash
    # `application_name` is safe; routing through set_config
    # preserves that classification.
    @s.observe_sql("SELECT set_config('application_name', 'foo', false)")
    assert_equal 0, @s.state_hash
  end
end

# ---------------------------------------------------------------------
# Dirty / replace_from_settings — concerns 5 and 6 building blocks.
# ---------------------------------------------------------------------
class TestConnectionGucStateDirty < Minitest::Test
  def setup
    @s = GoldLapel::GucState::ConnectionGucState.new
  end

  def test_initial_state_is_clean
    refute @s.dirty?
  end

  def test_mark_dirty_sets_flag
    @s.mark_dirty!
    assert @s.dirty?
  end

  def test_mark_clean_clears_flag
    @s.mark_dirty!
    assert @s.dirty?
    @s.mark_clean!
    refute @s.dirty?
  end

  def test_replace_from_settings_filters_through_classifier
    # Only unsafe GUCs land in the map. Server reports a mix; the
    # wrapper keeps just the security/output-formatting subset.
    @s.replace_from_settings([
      ["app.user_id", "42"],
      ["application_name", "myapp"],
      ["search_path", "\"$user\", public"],
      ["timezone", "UTC"],
      ["statement_timeout", "0"],
      ["work_mem", "64MB"],
    ])
    assert_includes @s.values.keys, "app.user_id"
    assert_includes @s.values.keys, "search_path"
    assert_includes @s.values.keys, "timezone"
    refute_includes @s.values.keys, "application_name"
    refute_includes @s.values.keys, "statement_timeout"
    refute_includes @s.values.keys, "work_mem"
  end

  def test_replace_from_settings_clears_dirty
    @s.mark_dirty!
    @s.replace_from_settings([["app.user_id", "42"]])
    refute @s.dirty?
  end

  def test_replace_from_settings_recomputes_hash
    a = GoldLapel::GucState::ConnectionGucState.new
    b = GoldLapel::GucState::ConnectionGucState.new
    a.observe_sql("SET app.user_id = '42'")
    b.replace_from_settings([["app.user_id", "42"]])
    assert_equal a.state_hash, b.state_hash
  end

  def test_replace_from_settings_replaces_existing_values
    @s.observe_sql("SET app.user_id = '42'")
    @s.replace_from_settings([["app.user_id", "999"]])
    refute_equal 0, @s.state_hash
    assert_equal "999", @s.values["app.user_id"]
  end

  def test_replace_from_settings_with_empty_clears_state
    @s.observe_sql("SET app.user_id = '42'")
    refute_equal 0, @s.state_hash
    @s.replace_from_settings([])
    assert_equal 0, @s.state_hash
  end

  def test_replace_from_settings_lowercases_names
    @s.replace_from_settings([["APP.User_ID", "42"]])
    assert_equal "42", @s.values["app.user_id"]
  end

  def test_replace_from_settings_skips_nil_entries
    # A row from pg_settings with a missing setting (rare) shouldn't
    # crash; just skip.
    @s.replace_from_settings([
      ["app.user_id", "42"],
      [nil, "x"],
      ["search_path", nil],
    ])
    assert_equal "42", @s.values["app.user_id"]
    assert_nil @s.values["search_path"]
  end
end

class TestSplitStatements < Minitest::Test
  def split(sql)
    GoldLapel::GucState.split_statements(sql)
  end

  def test_simple_two_statements
    assert_equal ["SET foo = '42'", "SELECT 1"], split("SET foo = '42'; SELECT 1")
  end

  def test_drops_empty_segments
    assert_equal ["SET foo = '42'", "SELECT 1"],
                 split("; SET foo = '42';;SELECT 1;")
  end

  def test_respects_single_quotes
    assert_equal ["SET foo = 'a;b'", "SELECT 1"],
                 split("SET foo = 'a;b'; SELECT 1")
  end

  def test_respects_double_quotes
    assert_equal ['SET "app;guc" = \'x\'', "SELECT 1"],
                 split('SET "app;guc" = \'x\'; SELECT 1')
  end

  def test_handles_doubled_quote_escape
    # PG escapes a literal `'` inside a string by doubling: `''`.
    assert_equal ["SET foo = 'it''s; ok'", "SELECT 1"],
                 split("SET foo = 'it''s; ok'; SELECT 1")
  end

  def test_single_statement_pass_through
    assert_equal ["SET foo = '42'"], split("SET foo = '42'")
  end

  def test_empty
    assert_empty split("")
    assert_empty split("   ")
    assert_empty split(";;;")
  end
end

class TestConnectionGucState < Minitest::Test
  def setup
    @s = GoldLapel::GucState::ConnectionGucState.new
  end

  def test_empty_state_hash_is_zero
    assert_equal 0, @s.state_hash
  end

  def test_safe_set_does_not_change_hash
    # `application_name` and `statement_timeout` remain harmless;
    # `timezone` was promoted to unsafe in the Wave 2 classifier
    # expansion (output-formatting GUCs change cached bytes — see
    # `test_output_formatting_gucs_are_unsafe`).
    @s.observe_sql("SET application_name = 'foo'")
    assert_equal 0, @s.state_hash
    @s.observe_sql("SET statement_timeout = 5000")
    assert_equal 0, @s.state_hash
    @s.observe_sql("SET work_mem = '64MB'")
    assert_equal 0, @s.state_hash
  end

  def test_unsafe_set_changes_hash
    h0 = @s.state_hash
    @s.observe_sql("SET app.user_id = '42'")
    refute_equal h0, @s.state_hash
  end

  def test_same_unsafe_set_yields_same_hash_on_two_connections
    a = GoldLapel::GucState::ConnectionGucState.new
    b = GoldLapel::GucState::ConnectionGucState.new
    a.observe_sql("SET app.user_id = '42'")
    b.observe_sql("SET app.user_id = '42'")
    assert_equal a.state_hash, b.state_hash
  end

  def test_different_unsafe_values_yield_different_hashes
    a = GoldLapel::GucState::ConnectionGucState.new
    b = GoldLapel::GucState::ConnectionGucState.new
    a.observe_sql("SET app.user_id = '42'")
    b.observe_sql("SET app.user_id = '43'")
    refute_equal a.state_hash, b.state_hash
  end

  def test_insertion_order_does_not_matter
    a = GoldLapel::GucState::ConnectionGucState.new
    a.observe_sql("SET app.user_id = '42'")
    a.observe_sql("SET app.tenant = 'alpha'")

    b = GoldLapel::GucState::ConnectionGucState.new
    b.observe_sql("SET app.tenant = 'alpha'")
    b.observe_sql("SET app.user_id = '42'")

    assert_equal a.state_hash, b.state_hash
  end

  def test_reset_returns_hash_to_baseline
    baseline = @s.state_hash
    @s.observe_sql("SET app.user_id = '42'")
    refute_equal baseline, @s.state_hash
    @s.observe_sql("RESET app.user_id")
    assert_equal baseline, @s.state_hash
  end

  def test_reset_all_clears_all_unsafe_state
    @s.observe_sql("SET app.user_id = '42'")
    @s.observe_sql("SET search_path TO 'tenant_a'")
    @s.observe_sql("SET role = 'app_user'")
    refute_equal 0, @s.state_hash
    @s.observe_sql("RESET ALL")
    assert_equal 0, @s.state_hash
  end

  def test_set_local_does_not_change_hash
    @s.observe_sql("SET LOCAL app.user_id = '42'")
    assert_equal 0, @s.state_hash
  end

  def test_observe_sql_returns_change_flag
    assert @s.observe_sql("SET app.user_id = '42'"), "first set ⇒ changed"
    refute @s.observe_sql("SELECT 1"), "non-SET ⇒ unchanged"
    refute @s.observe_sql("SET application_name = 'foo'"), "safe SET ⇒ unchanged"
    assert @s.observe_sql("RESET app.user_id"), "RESET unsafe ⇒ changed"
  end

  def test_reset_safe_guc_is_noop
    @s.observe_sql("SET app.user_id = '42'")
    h = @s.state_hash
    @s.observe_sql("RESET application_name")
    assert_equal h, @s.state_hash
  end

  def test_overwrite_unsafe_value_changes_hash
    @s.observe_sql("SET app.user_id = '42'")
    h1 = @s.state_hash
    @s.observe_sql("SET app.user_id = '43'")
    refute_equal h1, @s.state_hash
  end

  def test_observe_multi_statement_applies_all_sets
    @s.observe_sql("SET app.user_id = '42'; SELECT * FROM accounts")
    refute_equal 0, @s.state_hash
  end

  def test_observe_multi_statement_applies_two_unsafe_sets
    a = GoldLapel::GucState::ConnectionGucState.new
    a.observe_sql("SET app.user_id = '42'")
    a.observe_sql("SET app.tenant = 'alpha'")

    b = GoldLapel::GucState::ConnectionGucState.new
    b.observe_sql("SET app.user_id = '42'; SET app.tenant = 'alpha'")

    assert_equal a.state_hash, b.state_hash
  end

  def test_observe_multi_statement_with_quoted_semicolon
    @s.observe_sql("SET app.tenant = 'has;semicolon'; SELECT 1")
    refute_equal 0, @s.state_hash
  end

  def test_observe_nil_or_empty_returns_false
    refute @s.observe_sql(nil)
    refute @s.observe_sql("")
    assert_equal 0, @s.state_hash
  end
end

class TestCacheKeyStateHash < Minitest::Test
  # The L1 cache key is `<state_hash_hex>\0<sql>\0<params>`. Two
  # connections with different unsafe-GUC state must map to different
  # cache slots, so cached results never leak across security
  # boundaries.

  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_default_state_hash_zero
    # Backwards-compat: existing callers that don't pass a state_hash
    # use 0 (the empty-state fingerprint).
    @cache.put("SELECT 1", nil, [["1"]], [])
    refute_nil @cache.get("SELECT 1", nil)
    refute_nil @cache.get("SELECT 1", nil, 0)
  end

  def test_different_state_hash_misses
    # Same SQL, different state hash: must miss (not return user A's
    # cached row to user B).
    @cache.put("SELECT * FROM accounts", nil, [["alice"]], ["name"], 0xDEADBEEF)
    assert_nil @cache.get("SELECT * FROM accounts", nil, 0xCAFEBABE),
      "different state_hash must miss — cache must not leak across GUC state"
  end

  def test_same_state_hash_hits
    @cache.put("SELECT * FROM accounts", nil, [["alice"]], ["name"], 0xDEADBEEF)
    refute_nil @cache.get("SELECT * FROM accounts", nil, 0xDEADBEEF)
  end

  def test_empty_state_uses_zero_slot
    # Connections at default state should share cache slots.
    @cache.put("SELECT 1", nil, [["1"]], [], 0)
    refute_nil @cache.get("SELECT 1", nil) # default arg = 0
  end

  def test_state_hash_in_make_key
    k0 = @cache.send(:make_key, "SELECT 1", nil, 0)
    k1 = @cache.send(:make_key, "SELECT 1", nil, 1)
    refute_equal k0, k1
    assert k0.start_with?("0\0"), "state_hash 0 renders as '0' hex prefix"
    assert k1.start_with?("1\0"), "state_hash 1 renders as '1' hex prefix"
  end
end

class TestCachedConnectionStateObservation < Minitest::Test
  # `CachedConnection` owns one `ConnectionGucState`; every query
  # passes through `observe_sql` and the resulting state_hash is folded
  # into the L1 cache lookup. Use a stub real_conn so we don't need pg.

  StubConn = Struct.new(:returns) do
    def exec(sql, &block)
      r = StubResult.new
      block&.call(r)
      r
    end
    alias_method :query, :exec

    def async_exec(sql, &block) = exec(sql, &block)

    def exec_params(sql, params = [], _result_format = 0, &block)
      exec(sql, &block)
    end

    def async_exec_params(sql, params = [], _result_format = 0, &block)
      exec(sql, &block)
    end

    def close; end
    def finished?; false; end
  end

  StubResult = Struct.new(:_unused) do
    def values
      [["1"]]
    end
    def fields
      ["c"]
    end
    def clear; end
  end

  def setup
    GoldLapel::NativeCache.reset!
    @cache = GoldLapel::NativeCache.new
    @cache.instance_variable_set(:@invalidation_connected, true)
    @real = StubConn.new(nil)
    @wrapped = GoldLapel::CachedConnection.new(@real, @cache)
  end

  def teardown
    GoldLapel::NativeCache.reset!
  end

  def test_set_unsafe_guc_changes_state_hash
    h0 = @wrapped.guc_state.state_hash
    @wrapped.exec("SET app.user_id = '42'")
    refute_equal h0, @wrapped.guc_state.state_hash
  end

  def test_set_safe_guc_does_not_change_state_hash
    @wrapped.exec("SET application_name = 'foo'")
    assert_equal 0, @wrapped.guc_state.state_hash
  end

  def test_select_does_not_change_state_hash
    @wrapped.exec("SELECT * FROM accounts")
    assert_equal 0, @wrapped.guc_state.state_hash
  end

  def test_state_persists_across_queries
    @wrapped.exec("SET app.user_id = '42'")
    h1 = @wrapped.guc_state.state_hash
    @wrapped.exec("SELECT 1")
    @wrapped.exec("SELECT 2")
    assert_equal h1, @wrapped.guc_state.state_hash
  end

  def test_two_wrapped_conns_have_independent_state
    other = GoldLapel::CachedConnection.new(StubConn.new(nil), @cache)
    @wrapped.exec("SET app.user_id = '42'")
    other.exec("SET app.user_id = '43'")
    refute_equal @wrapped.guc_state.state_hash, other.guc_state.state_hash
  end

  def test_cache_key_isolation_across_state
    # Conn A sets app.user_id = 42, runs a SELECT. Conn B sets
    # app.user_id = 43 with the same SELECT. They must NOT share
    # cache slots — this is the whole point of folding the state hash
    # into the cache key.
    @wrapped.exec("SET app.user_id = '42'")
    @wrapped.exec("SELECT * FROM accounts")
    sh_a = @wrapped.guc_state.state_hash

    other = GoldLapel::CachedConnection.new(StubConn.new(nil), @cache)
    other.exec("SET app.user_id = '43'")
    sh_b = other.guc_state.state_hash

    refute_equal sh_a, sh_b
    refute_nil @cache.get("SELECT * FROM accounts", nil, sh_a),
      "conn A's cached row is reachable at A's state_hash"
    assert_nil @cache.get("SELECT * FROM accounts", nil, sh_b),
      "conn B at a different state_hash must miss"
  end

  def test_multi_statement_set_observed
    @wrapped.exec("SET app.user_id = '42'; SELECT 1")
    refute_equal 0, @wrapped.guc_state.state_hash
  end

  def test_reset_returns_to_baseline
    @wrapped.exec("SET app.user_id = '42'")
    refute_equal 0, @wrapped.guc_state.state_hash
    @wrapped.exec("RESET app.user_id")
    assert_equal 0, @wrapped.guc_state.state_hash
  end

  def test_exec_params_observes_set
    # exec_params is the parameterised path — `SET` doesn't take
    # params in practice, but the hook should still fire so a wire
    # SET via prepared-protocol still updates state.
    h0 = @wrapped.guc_state.state_hash
    @wrapped.exec_params("SET app.user_id = '42'", [])
    refute_equal h0, @wrapped.guc_state.state_hash
  end

  def test_async_exec_observes_set
    h0 = @wrapped.guc_state.state_hash
    @wrapped.async_exec("SET app.user_id = '42'")
    refute_equal h0, @wrapped.guc_state.state_hash
  end
end
