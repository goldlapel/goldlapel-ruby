# frozen_string_literal: true

# Tests for the v0.2.0 factory API:
#   - GoldLapel.start(url, **opts) returns an Instance
#   - gl.using(conn) { |gl| ... } scopes conn for nested calls
#   - conn: kwarg overrides the internal connection

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/streams"
require_relative "../lib/goldlapel"

class FactoryMockResult
  attr_reader :values, :fields

  def initialize(rows = [], fields = [])
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end

  def ntuples; @rows.length; end
  def cmd_tuples; @rows.length; end

  def [](index); @rows[index]; end

  def map(&block); @rows.map(&block); end

  def each(&block); @rows.each(&block); end
end

class FactoryMockConn
  attr_reader :calls, :name

  def initialize(name)
    @name = name
    @calls = []
    # A plausible RETURNING row for most doc_* methods — lets `doc_insert`
    # and friends unwrap `result[0]["..."]` without exploding.
    @insert_row = FactoryMockResult.new(
      [{ "_id" => "00000000-0000-0000-0000-000000000000",
         "data" => "{}",
         "created_at" => "2026-04-18",
         "value" => "0",
         "count" => "0",
         "score" => "0",
         "id" => "1",
         "payload" => "{}",
         "rank" => "0" }],
      ["_id", "data", "created_at", "value", "count", "score", "id", "payload", "rank"]
    )
    @empty = FactoryMockResult.new
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql, conn: @name }
    r = @empty
    block&.call(r)
    r
  end

  def exec_params(sql, params = [], result_format = 0, &block)
    @calls << { method: :exec_params, sql: sql, params: params, conn: @name }
    r = sql.include?("RETURNING") || sql.include?("SELECT") ? @insert_row : @empty
    block&.call(r)
    r
  end

  def close; end
  def finished?; false; end
end

# Helper: build a test Instance without spawning the binary. Wires up the
# sub-APIs (`documents`, `streams`) and stubs DDL pattern fetches so tests
# can assert wrapper-side behavior in isolation from the proxy.
def fake_factory_instance(internal_conn)
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, internal_conn)
  inst.instance_variable_set(:@wrapped_conn, internal_conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  documents = GoldLapel::DocumentsAPI.new(inst)
  documents.define_singleton_method(:_patterns) do |collection, **_opts|
    {
      tables: { "main" => collection.to_s },
      query_patterns: {},
    }
  end
  streams = GoldLapel::StreamsAPI.new(inst)
  streams.define_singleton_method(:_patterns) do |stream|
    {
      tables: { "main" => "_goldlapel.stream_#{stream}" },
      query_patterns: {
        "insert" => "INSERT INTO _goldlapel.stream_#{stream} (payload) VALUES ($1) RETURNING id, created_at",
      },
    }
  end
  inst.instance_variable_set(:@documents, documents)
  inst.instance_variable_set(:@streams, streams)
  inst
end

# --- Module-level factory ---

class TestFactoryStart < Minitest::Test
  def test_start_method_exists
    # Can't call without spawning the binary; just verify the method is there.
    assert GoldLapel.respond_to?(:start)
  end

  def test_new_is_lazy
    # GoldLapel.new builds an Instance without eager connect — used for advanced
    # cases / tests where callers construct manually.
    inst = GoldLapel.new("postgresql://localhost/test")
    assert_kind_of GoldLapel::Instance, inst
    refute inst.running?
  ensure
    inst&.stop rescue nil
  end
end

# --- log_level translation ---
#
# The proxy binary uses a count-based `-v/-vv/-vvv` flag, not `--log-level`.
# The wrapper must translate friendly strings to the right `-v` count (or
# omit the flag for default-verbosity levels), and must raise on bad values.
class TestLogLevelTranslation < Minitest::Test
  def test_trace_maps_to_vvv
    assert_equal ["-vvv"], GoldLapel.log_level_to_args("trace")
  end

  def test_debug_maps_to_vv
    assert_equal ["-vv"], GoldLapel.log_level_to_args("debug")
  end

  def test_info_maps_to_v
    assert_equal ["-v"], GoldLapel.log_level_to_args("info")
  end

  def test_warn_is_default_no_flag
    assert_equal [], GoldLapel.log_level_to_args("warn")
  end

  def test_warning_alias_is_default_no_flag
    assert_equal [], GoldLapel.log_level_to_args("warning")
  end

  def test_error_is_default_no_flag
    assert_equal [], GoldLapel.log_level_to_args("error")
  end

  def test_nil_produces_no_flag
    assert_equal [], GoldLapel.log_level_to_args(nil)
  end

  def test_symbol_accepted
    assert_equal ["-vv"], GoldLapel.log_level_to_args(:debug)
  end

  def test_case_insensitive
    assert_equal ["-vv"], GoldLapel.log_level_to_args("DEBUG")
    assert_equal ["-v"], GoldLapel.log_level_to_args("Info")
  end

  def test_invalid_value_raises_argument_error
    err = assert_raises(ArgumentError) do
      GoldLapel.log_level_to_args("invalid")
    end
    assert_match(/log_level must be one of: trace, debug, info, warn, error/, err.message)
  end

  def test_empty_string_raises_argument_error
    err = assert_raises(ArgumentError) do
      GoldLapel.log_level_to_args("")
    end
    assert_match(/log_level must be one of/, err.message)
  end

  def test_numeric_value_raises_argument_error
    err = assert_raises(ArgumentError) do
      GoldLapel.log_level_to_args(2)
    end
    assert_match(/log_level must be one of/, err.message)
  end

  # End-to-end: GoldLapel.new threads log_level through to extra_args on the
  # underlying Instance without spawning a binary. This asserts the call site
  # in `start` translates correctly when it wires up Instance.new.
  def test_start_translates_log_level_through_extra_args
    # We stub out Instance.new so we don't spawn anything — we just want to
    # inspect the `extra_args` it would have received.
    captured = {}
    original = GoldLapel::Instance.method(:new)
    GoldLapel::Instance.singleton_class.send(:define_method, :new) do |*args, **kwargs|
      captured[:args] = args
      captured[:kwargs] = kwargs
      # Return a dummy so `start` can return without exploding
      Object.new
    end

    begin
      GoldLapel.start("postgresql://localhost/test", log_level: "debug")
      # log_level is a top-level kwarg on the canonical surface, passed
      # through to Instance and then to Proxy. The -vv translation happens
      # in Proxy#start at spawn time (see log_level_to_verbose_flag).
      assert_equal "debug", captured[:kwargs][:log_level]
    ensure
      GoldLapel::Instance.singleton_class.send(:define_method, :new, original)
    end
  end

  def test_log_level_to_verbose_flag_helper
    # Direct helper: validates the translation without the spawn pipeline.
    assert_equal "-vvv", GoldLapel::Proxy.log_level_to_verbose_flag("trace")
    assert_equal "-vv",  GoldLapel::Proxy.log_level_to_verbose_flag("debug")
    assert_equal "-v",   GoldLapel::Proxy.log_level_to_verbose_flag("info")
    assert_nil GoldLapel::Proxy.log_level_to_verbose_flag("warn")
    assert_nil GoldLapel::Proxy.log_level_to_verbose_flag("error")
    assert_nil GoldLapel::Proxy.log_level_to_verbose_flag(nil)
    assert_equal "-vv", GoldLapel::Proxy.log_level_to_verbose_flag("DEBUG")
  end

  def test_start_omits_flag_when_log_level_nil
    captured = {}
    original = GoldLapel::Instance.method(:new)
    GoldLapel::Instance.singleton_class.send(:define_method, :new) do |*args, **kwargs|
      captured[:kwargs] = kwargs
      Object.new
    end

    begin
      GoldLapel.start("postgresql://localhost/test")
      assert_nil captured[:kwargs][:log_level]
      assert_equal [], captured[:kwargs][:extra_args]
    ensure
      GoldLapel::Instance.singleton_class.send(:define_method, :new, original)
    end
  end

  def test_start_raises_on_invalid_log_level
    # Invalid log_level raises at spawn time (via log_level_to_verbose_flag)
    # — exercised via the helper since start()/Instance.new don't spawn.
    assert_raises(ArgumentError) do
      GoldLapel::Proxy.log_level_to_verbose_flag("invalid")
    end
  end

  def test_start_preserves_caller_extra_args_alongside_log_level
    captured = {}
    original = GoldLapel::Instance.method(:new)
    GoldLapel::Instance.singleton_class.send(:define_method, :new) do |*args, **kwargs|
      captured[:kwargs] = kwargs
      Object.new
    end

    begin
      GoldLapel.start(
        "postgresql://localhost/test",
        log_level: "info",
        extra_args: ["--custom-flag", "value"]
      )
      # log_level lives at the top level; extra_args is passed through verbatim.
      assert_equal "info", captured[:kwargs][:log_level]
      assert_equal ["--custom-flag", "value"], captured[:kwargs][:extra_args]
    ensure
      GoldLapel::Instance.singleton_class.send(:define_method, :new, original)
    end
  end
end

# --- conn: kwarg ---

class TestConnKwarg < Minitest::Test
  def test_conn_kwarg_overrides_internal
    internal = FactoryMockConn.new("internal")
    override = FactoryMockConn.new("override")
    inst = fake_factory_instance(internal)

    inst.documents.insert("events", { a: 1 }, conn: override)

    insert_call = override.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call, "override conn should have received the INSERT"
    assert_nil internal.calls.find { |c| c[:sql].include?("INSERT") },
               "internal conn should NOT have received the INSERT"
  end

  def test_conn_kwarg_on_search
    internal = FactoryMockConn.new("internal")
    override = FactoryMockConn.new("override")
    inst = fake_factory_instance(internal)

    inst.search("articles", "title", "test", conn: override)
    refute_empty override.calls
    assert_empty internal.calls
  end

  # Phase 5: legacy flat `hset` is gone — per-family conn-kwarg is exercised
  # in test/test_hashes_api.rb (and siblings for counters/zsets/queues/geos).

  def test_no_conn_kwarg_uses_internal
    internal = FactoryMockConn.new("internal")
    inst = fake_factory_instance(internal)

    inst.documents.insert("events", { a: 1 })
    refute_empty internal.calls
  end
end

# --- gl.using(conn) { |gl| ... } ---

class TestUsingBlock < Minitest::Test
  def test_using_scopes_conn
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    inst = fake_factory_instance(internal)

    inst.using(scoped) do |gl|
      gl.documents.insert("events", { a: 1 })
    end

    refute_empty scoped.calls
    assert_empty internal.calls
  end

  def test_using_unwinds_after_block
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    inst = fake_factory_instance(internal)

    inst.using(scoped) do |gl|
      gl.documents.insert("events", { a: 1 })
    end

    # Post-block call should go to internal, not scoped
    inst.documents.insert("events", { a: 2 })
    scoped_inserts = scoped.calls.count { |c| c[:sql].include?("INSERT") }
    internal_inserts = internal.calls.count { |c| c[:sql].include?("INSERT") }
    assert_equal 1, scoped_inserts, "scoped should have 1 INSERT"
    assert_equal 1, internal_inserts, "internal should have 1 INSERT after scope unwound"
  end

  def test_using_unwinds_on_exception
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    inst = fake_factory_instance(internal)

    assert_raises(RuntimeError) do
      inst.using(scoped) do |gl|
        raise "boom"
      end
    end

    # After the raised block, internal should be the active conn again
    inst.documents.insert("events", { a: 1 })
    assert_empty scoped.calls
    refute_empty internal.calls
  end

  def test_using_yields_self
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    inst = fake_factory_instance(internal)

    yielded = nil
    inst.using(scoped) { |gl| yielded = gl }
    assert_same inst, yielded
  end

  def test_using_without_block_raises
    internal = FactoryMockConn.new("internal")
    inst = fake_factory_instance(internal)

    assert_raises(ArgumentError) { inst.using(FactoryMockConn.new("x")) }
  end

  def test_nested_using
    internal = FactoryMockConn.new("internal")
    outer = FactoryMockConn.new("outer")
    inner = FactoryMockConn.new("inner")
    inst = fake_factory_instance(internal)

    inst.using(outer) do |gl|
      gl.documents.insert("events", { layer: "outer" })
      gl.using(inner) do |gl2|
        gl2.documents.insert("events", { layer: "inner" })
      end
      gl.documents.insert("events", { layer: "outer-again" })
    end

    assert_equal 2, outer.calls.count { |c| c[:sql].include?("INSERT") }
    assert_equal 1, inner.calls.count { |c| c[:sql].include?("INSERT") }
    assert_empty internal.calls
  end

  def test_conn_kwarg_wins_over_using
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    explicit = FactoryMockConn.new("explicit")
    inst = fake_factory_instance(internal)

    inst.using(scoped) do |gl|
      gl.documents.insert("events", { a: 1 }, conn: explicit)
    end

    refute_empty explicit.calls
    assert_empty scoped.calls
    assert_empty internal.calls
  end

  def test_using_isolates_across_fibers
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    inst = fake_factory_instance(internal)

    # A sibling fiber started outside `using` should not see the scoped conn.
    sibling_log = []
    sibling = Fiber.new do
      inst.documents.insert("events", { from: "sibling" })
      sibling_log << :done
    end

    inst.using(scoped) do |gl|
      sibling.resume
      gl.documents.insert("events", { from: "main" })
    end

    assert_equal [:done], sibling_log
    # Sibling wrote via internal (its own fiber's storage is empty)
    sibling_calls = internal.calls.select { |c| c[:sql].include?("INSERT") }
    assert_equal 1, sibling_calls.length
    # Main wrote via scoped
    main_calls = scoped.calls.select { |c| c[:sql].include?("INSERT") }
    assert_equal 1, main_calls.length
  end
end

# --- Async submodule loads gracefully ---

class TestAsyncSubmodule < Minitest::Test
  def test_async_requires_async_gem_with_helpful_error
    # Save and stub out $LOADED_FEATURES for "async" to simulate missing gem.
    # Actually, easier: just try to require it and see if it loads, or skip
    # if async isn't installed. The important invariant is that the error
    # message mentions the `async` gem.
    begin
      require "async"
      async_available = true
    rescue LoadError
      async_available = false
    end

    if async_available
      # Can't easily force the missing-gem path; just confirm the submodule loads.
      load_succeeded = false
      begin
        require_relative "../lib/goldlapel/async"
        load_succeeded = true
      rescue LoadError => e
        flunk "async submodule should load cleanly when async gem is present: #{e.message}"
      end
      assert load_succeeded
      assert defined?(GoldLapel::Async), "GoldLapel::Async should be defined"
      assert GoldLapel::Async.respond_to?(:start)
    else
      # Without `async` gem installed, the submodule must fail loudly.
      error = assert_raises(LoadError) do
        load File.expand_path("../lib/goldlapel/async.rb", __dir__)
      end
      assert_match(/async/, error.message.downcase)
    end
  end

  def test_async_start_requires_reactor
    begin
      require "async"
      require_relative "../lib/goldlapel/async"
    rescue LoadError
      skip "async gem not installed"
    end

    # Outside an Async block, Async.start should raise
    error = assert_raises(RuntimeError) do
      GoldLapel::Async.start("postgresql://nowhere/nope")
    end
    assert_match(/Async \{/, error.message)
  end

  # --- Native async code-path guard -------------------------------------
  #
  # This test pins down the architecture of the async submodule: an
  # async-specific Instance class (mirroring the sync one) routes every
  # wrapper call through the `GoldLapel::Async::Utils` layer, which calls
  # pg's native non-blocking method variants (`async_exec_params`,
  # `async_exec`, `wait_for_notify`).
  #
  # The sync path (lib/goldlapel/utils.rb, lib/goldlapel/instance.rb) stays
  # untouched — no async_exec_params / wait_readable there.
  def test_async_instance_uses_native_async_pg_methods
    begin
      require "async"
      require_relative "../lib/goldlapel/async"
    rescue LoadError
      skip "async gem not installed"
    end

    # The async factory returns an async-specific Instance class.
    assert defined?(GoldLapel::Async::Instance),
      "GoldLapel::Async::Instance must be defined — wrapper methods route " \
      "through it to the native-async Utils layer."

    # Async utils uses async_exec_params; no sync exec_params/exec leakage.
    async_utils_src = File.read(File.expand_path("../lib/goldlapel/async/utils.rb", __dir__))
    assert_match(/async_exec_params/, async_utils_src,
      "async/utils.rb must call async_exec_params at SQL call sites")
    refute_match(/\braw\.exec_params\b|\braw\.exec\b|\blisten_conn\.exec_params\b|\blisten_conn\.exec\b/, async_utils_src,
      "async/utils.rb must not call sync exec/exec_params — " \
      "those belong to the sync utils layer")

    # Sync utils still uses sync exec_params — the two layers are independent.
    utils_src = File.read(File.expand_path("../lib/goldlapel/utils.rb", __dir__))
    assert_match(/\braw\.exec_params\b/, utils_src,
      "utils.rb should still use sync exec_params")
    refute_match(/\braw\.async_exec_params\b/, utils_src,
      "utils.rb (sync path) should NOT use async_exec_params — " \
      "that's the async layer's job")
  end
end
