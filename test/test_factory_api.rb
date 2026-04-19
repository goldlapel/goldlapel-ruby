# frozen_string_literal: true

# Tests for the v0.2.0 factory API:
#   - Goldlapel.start(url, **opts) returns an Instance
#   - gl.using(conn) { |gl| ... } scopes conn for nested calls
#   - conn: kwarg overrides the internal connection

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/instance"
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

# Helper: build a test Instance without spawning the binary
def fake_factory_instance(internal_conn)
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, internal_conn)
  inst.instance_variable_set(:@wrapped_conn, internal_conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  inst
end

# --- Module-level factory ---

class TestFactoryStartAlias < Minitest::Test
  def test_goldlapel_constant_alias
    assert_equal GoldLapel, Goldlapel
  end

  def test_start_method_exists
    # Can't call without spawning the binary; just verify the method is there.
    assert GoldLapel.respond_to?(:start)
    assert Goldlapel.respond_to?(:start)
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

# --- conn: kwarg ---

class TestConnKwarg < Minitest::Test
  def test_conn_kwarg_overrides_internal
    internal = FactoryMockConn.new("internal")
    override = FactoryMockConn.new("override")
    inst = fake_factory_instance(internal)

    inst.doc_insert("events", { a: 1 }, conn: override)

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

  def test_conn_kwarg_on_hset
    internal = FactoryMockConn.new("internal")
    override = FactoryMockConn.new("override")
    inst = fake_factory_instance(internal)

    inst.hset("cache", "k1", "f1", "v1", conn: override)
    refute_empty override.calls
    assert_empty internal.calls
  end

  def test_no_conn_kwarg_uses_internal
    internal = FactoryMockConn.new("internal")
    inst = fake_factory_instance(internal)

    inst.doc_insert("events", { a: 1 })
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
      gl.doc_insert("events", { a: 1 })
    end

    refute_empty scoped.calls
    assert_empty internal.calls
  end

  def test_using_unwinds_after_block
    internal = FactoryMockConn.new("internal")
    scoped = FactoryMockConn.new("scoped")
    inst = fake_factory_instance(internal)

    inst.using(scoped) do |gl|
      gl.doc_insert("events", { a: 1 })
    end

    # Post-block call should go to internal, not scoped
    inst.doc_insert("events", { a: 2 })
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
    inst.doc_insert("events", { a: 1 })
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
      gl.doc_insert("events", { layer: "outer" })
      gl.using(inner) do |gl2|
        gl2.doc_insert("events", { layer: "inner" })
      end
      gl.doc_insert("events", { layer: "outer-again" })
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
      gl.doc_insert("events", { a: 1 }, conn: explicit)
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
      inst.doc_insert("events", { from: "sibling" })
      sibling_log << :done
    end

    inst.using(scoped) do |gl|
      sibling.resume
      gl.doc_insert("events", { from: "main" })
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

  # --- v0.2.0 honesty guard ---------------------------------------------
  #
  # This test pins down the current (intentionally blocking) behavior of the
  # async submodule, and guards the honesty of the public docs.
  #
  # In v0.2.0, `Goldlapel::Async.start` is a thin factory that returns the
  # same `Goldlapel::Instance` as the sync entry point. Its wrapper methods
  # call straight into `pg`'s sync `PG.connect` / `conn.exec_params`, which
  # do NOT yield to the fiber scheduler during Postgres IO. That's fine for
  # v0.2.0 (API parity) but it must not be silently advertised as true
  # non-blocking concurrency.
  #
  # When native async-pg lands, this test is the canary: update it to assert
  # the new behavior (scheduler-yielding IO, async-pg in use, etc.) and
  # update the README + docblock in lockstep so the claims stay honest.
  def test_async_v020_delegates_to_blocking_sync_factory
    begin
      require "async"
      require_relative "../lib/goldlapel/async"
    rescue LoadError
      skip "async gem not installed"
    end

    # Same module alias used throughout the gem.
    assert_equal GoldLapel, Goldlapel

    # The async factory is a thin wrapper around the sync factory — it does
    # not introduce an async-specific Instance class.
    refute defined?(GoldLapel::Async::Instance),
      "v0.2.0 should not define an async-specific Instance; " \
      "if this changes, update README + async.rb docblock to match."

    # Wrapper methods still dispatch through the sync `exec_params` path.
    # Audit the two files that contain every SQL call site — neither should
    # use `async_exec_params`, `wait_readable`, or pull in `async-pg`. The
    # day one of these files gains those APIs is the day the README + the
    # async.rb docblock need to be updated to stop claiming "blocking under
    # the hood."
    utils_src    = File.read(File.expand_path("../lib/goldlapel/utils.rb", __dir__))
    instance_src = File.read(File.expand_path("../lib/goldlapel/instance.rb", __dir__))

    [utils_src, instance_src].each do |src|
      refute_match(/async_exec_params|wait_readable|async-pg/, src,
        "v0.2.0 is documented as blocking under the hood; " \
        "if async_exec_params / wait_readable / async-pg appear in the " \
        "wrapper, flip this test and update README + async.rb docblock.")
    end

    # And the sync call surface is still live — `exec_params` is still what
    # utils uses to talk to Postgres.
    assert_match(/exec_params/, utils_src,
      "utils.rb should still use sync exec_params in v0.2.0")
  end
end
