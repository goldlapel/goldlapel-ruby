# frozen_string_literal: true
# Unit tests for goldlapel/ddl — DDL API client + per-session cache.

require "minitest/autorun"
require "json"
require "webrick"
require_relative "../lib/goldlapel/ddl"

class FakeDashboard
  attr_accessor :responses, :captured

  def initialize
    @responses = []
    @captured = []
    @server = WEBrick::HTTPServer.new(
      Port: 0,
      Logger: WEBrick::Log.new(File::NULL),
      AccessLog: [],
    )
    @server.mount_proc "/" do |req, resp|
      body = req.body.to_s
      parsed =
        begin
          body.empty? ? {} : JSON.parse(body)
        rescue JSON::ParserError
          { "_raw" => body }
        end
      hdrs = {}
      req.header.each { |k, v| hdrs[k.downcase] = Array(v).first }
      @captured << { path: req.path, headers: hdrs, body: parsed }

      status, body_hash = @responses.shift || [500, { "error" => "no_response" }]
      resp.status = status
      resp["Content-Type"] = "application/json"
      resp.body = JSON.generate(body_hash)
    end
    @thread = Thread.new { @server.start }
    # Wait for listener
    sleep 0.05
  end

  def port
    @server.config[:Port]
  end

  def stop
    @server.shutdown
    @thread.join(2)
  end
end

class FakeOwner; end

class TestTokenFromEnvOrFile < Minitest::Test
  def setup
    @saved_env = ENV["GOLDLAPEL_DASHBOARD_TOKEN"]
  end

  def teardown
    ENV["GOLDLAPEL_DASHBOARD_TOKEN"] = @saved_env
  end

  def test_returns_env_when_set
    ENV["GOLDLAPEL_DASHBOARD_TOKEN"] = "env-token"
    assert_equal "env-token", GoldLapel::DDL.token_from_env_or_file
  end

  def test_trims_env
    ENV["GOLDLAPEL_DASHBOARD_TOKEN"] = "  trimmed  "
    assert_equal "trimmed", GoldLapel::DDL.token_from_env_or_file
  end

  def test_ignores_empty_env
    ENV["GOLDLAPEL_DASHBOARD_TOKEN"] = ""
    # Can be nil (no file) or a real file-backed string; what we're asserting
    # is we didn't short-circuit to "".
    v = GoldLapel::DDL.token_from_env_or_file
    refute_equal "", v
  end
end

class TestSupportedVersion < Minitest::Test
  def test_stream_is_v1
    assert_equal "v1", GoldLapel::DDL.supported_version("stream")
  end
end

class TestFetchHappyPath < Minitest::Test
  def setup
    @srv = FakeDashboard.new
  end

  def teardown
    @srv.stop
  end

  def test_happy_path_posts_correct_body_and_headers
    @srv.responses << [200, {
      "accepted" => true,
      "family" => "stream",
      "schema_version" => "v1",
      "tables" => { "main" => "_goldlapel.stream_events" },
      "query_patterns" => { "insert" => "INSERT ..." },
    }]
    owner = FakeOwner.new
    entry = GoldLapel::DDL.fetch_patterns(owner, "stream", "events", @srv.port, "tok")
    assert_equal "_goldlapel.stream_events", entry[:tables]["main"]
    assert_equal "INSERT ...", entry[:query_patterns]["insert"]

    assert_equal 1, @srv.captured.length
    cap = @srv.captured[0]
    assert_equal "/api/ddl/stream/create", cap[:path]
    assert_equal "tok", cap[:headers]["x-gl-dashboard"]
    assert_equal({ "name" => "events", "schema_version" => "v1" }, cap[:body])
  end

  def test_cache_hit_skips_second_post
    @srv.responses << [200, {
      "tables" => { "main" => "_goldlapel.stream_events" },
      "query_patterns" => { "insert" => "X" },
    }]
    owner = FakeOwner.new
    r1 = GoldLapel::DDL.fetch_patterns(owner, "stream", "events", @srv.port, "tok")
    r2 = GoldLapel::DDL.fetch_patterns(owner, "stream", "events", @srv.port, "tok")
    assert_same r1, r2
    assert_equal 1, @srv.captured.length
  end

  def test_different_owners_isolated
    2.times do
      @srv.responses << [200, {
        "tables" => { "main" => "_goldlapel.stream_events" },
        "query_patterns" => { "insert" => "X" },
      }]
    end
    GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", @srv.port, "tok")
    GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", @srv.port, "tok")
    assert_equal 2, @srv.captured.length
  end

  def test_different_names_miss_cache
    %w[events orders].each do |name|
      @srv.responses << [200, {
        "tables" => { "main" => "_goldlapel.stream_#{name}" },
        "query_patterns" => { "insert" => "INSERT #{name}" },
      }]
    end
    owner = FakeOwner.new
    GoldLapel::DDL.fetch_patterns(owner, "stream", "events", @srv.port, "tok")
    GoldLapel::DDL.fetch_patterns(owner, "stream", "orders", @srv.port, "tok")
    assert_equal 2, @srv.captured.length, "different names must each trigger a fetch"
  end
end

class TestFetchErrors < Minitest::Test
  def setup
    @srv = FakeDashboard.new
  end

  def teardown
    @srv.stop
  end

  def test_409_version_mismatch
    @srv.responses << [409, {
      "error" => "version_mismatch",
      "detail" => "wrapper requested v1; proxy speaks v2 — upgrade proxy",
    }]
    err = assert_raises(RuntimeError) do
      GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", @srv.port, "tok")
    end
    assert_match(/schema version mismatch/, err.message)
  end

  def test_403_forbidden
    @srv.responses << [403, { "error" => "forbidden" }]
    err = assert_raises(RuntimeError) do
      GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", @srv.port, "tok")
    end
    assert_match(/dashboard token/, err.message)
  end

  def test_missing_token_raises
    err = assert_raises(RuntimeError) do
      GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", 9999, nil)
    end
    assert_match(/No dashboard token/, err.message)
  end

  def test_missing_port_raises
    err = assert_raises(RuntimeError) do
      GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", nil, "tok")
    end
    assert_match(/No dashboard port/, err.message)
  end

  def test_unreachable_raises_actionable_error
    err = assert_raises(RuntimeError) do
      GoldLapel::DDL.fetch_patterns(FakeOwner.new, "stream", "events", 1, "tok")
    end
    assert_match(/dashboard not reachable/, err.message)
  end
end

class TestInvalidate < Minitest::Test
  def setup
    @srv = FakeDashboard.new
  end

  def teardown
    @srv.stop
  end

  def test_invalidate_drops_cache
    2.times do
      @srv.responses << [200, {
        "tables" => { "main" => "_goldlapel.stream_events" },
        "query_patterns" => { "insert" => "X" },
      }]
    end
    owner = FakeOwner.new
    GoldLapel::DDL.fetch_patterns(owner, "stream", "events", @srv.port, "tok")
    GoldLapel::DDL.invalidate(owner)
    GoldLapel::DDL.fetch_patterns(owner, "stream", "events", @srv.port, "tok")
    assert_equal 2, @srv.captured.length
  end
end
