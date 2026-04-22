# frozen_string_literal: true
#
# End-to-end streams integration tests — proxy-owned DDL (Phase 3).
# Mirrors goldlapel-python/tests/test_streams_integration.py.
#
# Skipped unless GOLDLAPEL_INTEGRATION=1. Requires:
#   - a reachable Postgres at DATABASE_URL (default: local)
#   - the goldlapel binary on PATH (or GOLDLAPEL_BINARY set)

require "minitest/autorun"

SHOULD_RUN = ENV["GOLDLAPEL_INTEGRATION"] == "1"

if SHOULD_RUN
  require "json"
  require "pg"
  require_relative "../lib/goldlapel"
  require_relative "../lib/goldlapel/ddl"

  PG_URL = ENV["DATABASE_URL"] || "postgresql://sgibson@localhost:5432/postgres"

  def direct_conn
    PG.connect(PG_URL)
  end

  class TestStreamDdlOwnership < Minitest::Test
    def setup
      port = 7700 + (Time.now.to_i % 100)
      @gl = GoldLapel.start(PG_URL, port: port, silent: true)
      @stream_name = "gl_int_stream_#{(Time.now.to_f * 1000).to_i}"
    end

    def teardown
      @gl.stop if @gl
    end

    def test_stream_add_creates_prefixed_table
      @gl.stream_add(@stream_name, { type: "click" })

      c = direct_conn
      begin
        r = c.exec_params(
          "SELECT COUNT(*) FROM information_schema.tables " \
          "WHERE table_schema = '_goldlapel' AND table_name = $1",
          ["stream_#{@stream_name}"],
        )
        assert_equal 1, r[0]["count"].to_i, "expected _goldlapel.stream_#{@stream_name}"

        # Nothing in public.
        r2 = c.exec_params(
          "SELECT COUNT(*) FROM information_schema.tables " \
          "WHERE table_schema = 'public' AND table_name = $1",
          [@stream_name],
        )
        assert_equal 0, r2[0]["count"].to_i, "no public.#{@stream_name} — proxy owns DDL"
      ensure
        c.close
      end
    end

    def test_schema_meta_row_recorded
      @gl.stream_add(@stream_name, { type: "click" })

      c = direct_conn
      begin
        r = c.exec_params(
          "SELECT family, name, schema_version FROM _goldlapel.schema_meta " \
          "WHERE family = 'stream' AND name = $1",
          [@stream_name],
        )
        assert_equal 1, r.ntuples
        assert_equal "stream", r[0]["family"]
        assert_equal @stream_name, r[0]["name"]
        assert_equal "v1", r[0]["schema_version"]
      ensure
        c.close
      end
    end

    def test_ddl_http_call_happens_once
      original = GoldLapel::DDL.method(:_post)
      count = 0
      GoldLapel::DDL.define_singleton_method(:_post) do |*a|
        count += 1
        original.call(*a)
      end

      begin
        fresh = "gl_int_stream_ct_#{(Time.now.to_f * 1000).to_i}"
        @gl.stream_add(fresh, { i: 1 })
        assert_equal 1, count, "first call posts once"
        @gl.stream_add(fresh, { i: 2 })
        @gl.stream_add(fresh, { i: 3 })
        assert_equal 1, count, "subsequent calls use cache"
      ensure
        GoldLapel::DDL.define_singleton_method(:_post, original)
      end
    end
  end

  class TestStreamRoundTrip < Minitest::Test
    def setup
      port = 7800 + (Time.now.to_i % 100)
      @gl = GoldLapel.start(PG_URL, port: port, silent: true)
      @name = "gl_int_rt_#{(Time.now.to_f * 1000).to_i}"
    end

    def teardown
      @gl.stop if @gl
    end

    def test_add_and_read_round_trip
      @gl.stream_create_group(@name, "workers")
      r1 = @gl.stream_add(@name, { i: 1 })
      r2 = @gl.stream_add(@name, { i: 2 })
      assert r2["id"] > r1["id"]

      messages = @gl.stream_read(@name, "workers", "c", count: 10)
      assert_equal 2, messages.length
      assert_equal({ "i" => 1 }, messages[0]["payload"])
      assert_equal({ "i" => 2 }, messages[1]["payload"])
    end

    def test_ack_removes_pending
      name = "#{@name}_ack"
      @gl.stream_create_group(name, "workers")
      r = @gl.stream_add(name, { i: 1 })
      @gl.stream_read(name, "workers", "c", count: 10)
      assert_equal true, @gl.stream_ack(name, "workers", r["id"])
      assert_equal false, @gl.stream_ack(name, "workers", r["id"])
    end

    def test_claim_reassigns_idle
      name = "#{@name}_claim"
      @gl.stream_create_group(name, "workers")
      @gl.stream_add(name, { i: 1 })
      @gl.stream_read(name, "workers", "consumer-a", count: 10)
      claimed = @gl.stream_claim(name, "workers", "consumer-b", min_idle_ms: 0)
      assert_equal 1, claimed.length
      assert_equal({ "i" => 1 }, claimed[0]["payload"])
    end
  end
else
  class TestStreamsIntegrationSkipped < Minitest::Test
    def test_skipped
      skip "set GOLDLAPEL_INTEGRATION=1 to run streams integration tests"
    end
  end
end
