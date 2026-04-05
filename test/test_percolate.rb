# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class PercolateMockResult
  attr_reader :values, :fields

  def initialize(rows, fields)
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end

  def ntuples; @rows.length; end
  def cmd_tuples; @rows.length; end

  def [](index)
    @rows[index]
  end

  def map(&block)
    @rows.map(&block)
  end

  def each(&block)
    @rows.each(&block)
  end
end

class PercolateMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = PercolateMockResult.new([], [])
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql }
    key = @results.keys.find { |k| sql.include?(k) }
    r = key ? @results[key] : @default
    block&.call(r)
    r
  end

  def exec_params(sql, params = [], result_format = 0, &block)
    @calls << { method: :exec_params, sql: sql, params: params }
    key = @results.keys.find { |k| sql.include?(k) }
    r = key ? @results[key] : @default
    block&.call(r)
    r
  end

  def close; end
  def finished?; false; end
end

# --- percolate_add ---

class TestPercolateAdd < Minitest::Test
  def test_creates_table_and_index
    mock = PercolateMockConnection.new
    GoldLapel.percolate_add(mock, "alerts", "q1", "server error")

    table_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE TABLE IF NOT EXISTS alerts") }
    refute_nil table_call
    assert_includes table_call[:sql], "query_id TEXT PRIMARY KEY"
    assert_includes table_call[:sql], "query_text TEXT NOT NULL"
    assert_includes table_call[:sql], "tsquery TSQUERY NOT NULL"
    assert_includes table_call[:sql], "lang TEXT NOT NULL"
    assert_includes table_call[:sql], "metadata JSONB"

    idx_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE INDEX IF NOT EXISTS") }
    refute_nil idx_call
    assert_includes idx_call[:sql], "idx_alerts_tsquery"
    assert_includes idx_call[:sql], "USING GIN"
  end

  def test_upsert_with_defaults
    mock = PercolateMockConnection.new
    GoldLapel.percolate_add(mock, "alerts", "q1", "server error")

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT INTO alerts") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "plainto_tsquery($3, $2)"
    assert_includes insert_call[:sql], "ON CONFLICT (query_id) DO UPDATE"
    assert_equal ["q1", "server error", "english", nil], insert_call[:params]
  end

  def test_upsert_with_metadata
    mock = PercolateMockConnection.new
    meta = { "severity" => "high", "team" => "ops" }
    GoldLapel.percolate_add(mock, "alerts", "q1", "server error", metadata: meta)

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT INTO alerts") }
    assert_equal ["q1", "server error", "english", JSON.generate(meta)], insert_call[:params]
  end

  def test_custom_lang
    mock = PercolateMockConnection.new
    GoldLapel.percolate_add(mock, "alerts", "q1", "erreur serveur", lang: "french")

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT INTO alerts") }
    assert_equal ["q1", "erreur serveur", "french", nil], insert_call[:params]
  end

  def test_rejects_invalid_table_name
    mock = PercolateMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.percolate_add(mock, "bad table!", "q1", "query") }
  end
end

# --- percolate ---

class TestPercolate < Minitest::Test
  def test_returns_matching_queries
    match_result = PercolateMockResult.new(
      [
        { "query_id" => "q1", "query_text" => "server error", "metadata" => '{"severity":"high"}', "_score" => "0.075" },
        { "query_id" => "q2", "query_text" => "disk failure", "metadata" => nil, "_score" => "0.05" }
      ],
      ["query_id", "query_text", "metadata", "_score"]
    )
    mock = PercolateMockConnection.new("SELECT" => match_result)
    result = GoldLapel.percolate(mock, "alerts", "the server encountered a critical error")

    assert_equal 2, result.length
    assert_equal "q1", result[0]["query_id"]
    assert_equal "server error", result[0]["query_text"]
    assert_equal '{"severity":"high"}', result[0]["metadata"]
    assert_equal "0.075", result[0]["_score"]
  end

  def test_sql_structure
    mock = PercolateMockConnection.new
    GoldLapel.percolate(mock, "alerts", "server error", lang: "english", limit: 25)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "ts_rank(to_tsvector($1, $2), tsquery)"
    assert_includes call[:sql], "WHERE to_tsvector($1, $2) @@ tsquery"
    assert_includes call[:sql], "ORDER BY _score DESC LIMIT $3"
    assert_equal ["english", "server error", 25], call[:params]
  end

  def test_returns_empty_array_when_no_matches
    mock = PercolateMockConnection.new
    result = GoldLapel.percolate(mock, "alerts", "unrelated text")

    assert_equal [], result
  end

  def test_custom_lang_and_limit
    mock = PercolateMockConnection.new
    GoldLapel.percolate(mock, "alerts", "erreur serveur", lang: "french", limit: 10)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["french", "erreur serveur", 10], call[:params]
  end

  def test_rejects_invalid_table_name
    mock = PercolateMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.percolate(mock, "bad table!", "text") }
  end
end

# --- percolate_delete ---

class TestPercolateDelete < Minitest::Test
  def test_returns_true_when_deleted
    delete_result = PercolateMockResult.new(
      [{ "query_id" => "q1" }],
      ["query_id"]
    )
    mock = PercolateMockConnection.new("DELETE" => delete_result)
    result = GoldLapel.percolate_delete(mock, "alerts", "q1")

    assert_equal true, result
  end

  def test_returns_false_when_not_found
    mock = PercolateMockConnection.new
    result = GoldLapel.percolate_delete(mock, "alerts", "nonexistent")

    assert_equal false, result
  end

  def test_sql_structure
    mock = PercolateMockConnection.new
    GoldLapel.percolate_delete(mock, "alerts", "q1")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "DELETE FROM alerts WHERE query_id = $1"
    assert_includes call[:sql], "RETURNING query_id"
    assert_equal ["q1"], call[:params]
  end

  def test_rejects_invalid_table_name
    mock = PercolateMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.percolate_delete(mock, "bad table!", "q1") }
  end
end
