# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class RelevanceMockResult
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

class RelevanceMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = RelevanceMockResult.new([], [])
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

# --- analyze ---

class TestAnalyze < Minitest::Test
  def test_returns_array_of_hashes
    debug_result = RelevanceMockResult.new(
      [
        { "alias" => "asciiword", "description" => "Word, all ASCII",
          "token" => "quick", "dictionaries" => "{english_stem}",
          "dictionary" => "english_stem", "lexemes" => "{quick}" },
        { "alias" => "blank", "description" => "Space symbols",
          "token" => " ", "dictionaries" => "{}",
          "dictionary" => nil, "lexemes" => nil }
      ],
      ["alias", "description", "token", "dictionaries", "dictionary", "lexemes"]
    )
    mock = RelevanceMockConnection.new("ts_debug" => debug_result)
    result = GoldLapel.analyze(mock, "quick brown fox")

    assert_equal 2, result.length
    assert_equal "asciiword", result[0]["alias"]
    assert_equal "Word, all ASCII", result[0]["description"]
    assert_equal "quick", result[0]["token"]
    assert_equal "{english_stem}", result[0]["dictionaries"]
    assert_equal "english_stem", result[0]["dictionary"]
    assert_equal "{quick}", result[0]["lexemes"]
  end

  def test_sql_structure
    mock = RelevanceMockConnection.new
    GoldLapel.analyze(mock, "hello world", lang: "english")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "ts_debug($1, $2)"
    assert_includes call[:sql], "alias"
    assert_includes call[:sql], "description"
    assert_includes call[:sql], "token"
    assert_includes call[:sql], "dictionaries"
    assert_includes call[:sql], "dictionary"
    assert_includes call[:sql], "lexemes"
    assert_equal ["english", "hello world"], call[:params]
  end

  def test_custom_lang
    mock = RelevanceMockConnection.new
    GoldLapel.analyze(mock, "bonjour le monde", lang: "french")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["french", "bonjour le monde"], call[:params]
  end

  def test_returns_empty_array_for_empty_text
    mock = RelevanceMockConnection.new
    result = GoldLapel.analyze(mock, "")

    assert_equal [], result
  end

  def test_default_lang_is_english
    mock = RelevanceMockConnection.new
    GoldLapel.analyze(mock, "test")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["english", "test"], call[:params]
  end
end

# --- explain_score ---

class TestExplainScore < Minitest::Test
  def test_returns_single_hash
    explain_result = RelevanceMockResult.new(
      [
        { "document_text" => "The quick brown fox jumps over the lazy dog",
          "document_tokens" => "'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2",
          "query_tokens" => "'quick' & 'fox'",
          "matches" => "t",
          "score" => "0.0991032",
          "headline" => "The **quick** brown **fox** jumps over the lazy dog" }
      ],
      ["document_text", "document_tokens", "query_tokens", "matches", "score", "headline"]
    )
    mock = RelevanceMockConnection.new("ts_rank" => explain_result)
    result = GoldLapel.explain_score(mock, "articles", "body", "quick fox", "id", 42)

    refute_nil result
    assert_equal "The quick brown fox jumps over the lazy dog", result["document_text"]
    assert_includes result["document_tokens"], "'quick':2"
    assert_equal "'quick' & 'fox'", result["query_tokens"]
    assert_equal "t", result["matches"]
    assert_equal "0.0991032", result["score"]
    assert_includes result["headline"], "**quick**"
    assert_includes result["headline"], "**fox**"
  end

  def test_sql_structure
    mock = RelevanceMockConnection.new
    GoldLapel.explain_score(mock, "articles", "body", "quick fox", "id", 42, lang: "english")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "body AS document_text"
    assert_includes call[:sql], "to_tsvector($1, body)::text AS document_tokens"
    assert_includes call[:sql], "plainto_tsquery($1, $2)::text AS query_tokens"
    assert_includes call[:sql], "to_tsvector($1, body) @@ plainto_tsquery($1, $2) AS matches"
    assert_includes call[:sql], "ts_rank(to_tsvector($1, body), plainto_tsquery($1, $2)) AS score"
    assert_includes call[:sql], "ts_headline($1, body, plainto_tsquery($1, $2)"
    assert_includes call[:sql], "StartSel=**, StopSel=**"
    assert_includes call[:sql], "MaxWords=50, MinWords=20"
    assert_includes call[:sql], "FROM articles WHERE id = $3"
    assert_equal ["english", "quick fox", 42], call[:params]
  end

  def test_returns_nil_when_row_not_found
    mock = RelevanceMockConnection.new
    result = GoldLapel.explain_score(mock, "articles", "body", "query", "id", 999)

    assert_nil result
  end

  def test_custom_lang
    mock = RelevanceMockConnection.new
    GoldLapel.explain_score(mock, "articles", "body", "recherche", "id", 1, lang: "french")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["french", "recherche", 1], call[:params]
  end

  def test_default_lang_is_english
    mock = RelevanceMockConnection.new
    GoldLapel.explain_score(mock, "articles", "body", "test", "id", 1)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal "english", call[:params][0]
  end

  def test_string_id_value
    mock = RelevanceMockConnection.new
    GoldLapel.explain_score(mock, "articles", "body", "test", "slug", "my-article")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["english", "test", "my-article"], call[:params]
    assert_includes call[:sql], "WHERE slug = $3"
  end
end

class TestExplainScoreValidation < Minitest::Test
  def test_rejects_invalid_table
    mock = RelevanceMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.explain_score(mock, "bad table!", "body", "q", "id", 1)
    end
  end

  def test_rejects_invalid_column
    mock = RelevanceMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.explain_score(mock, "articles", "bad col!", "q", "id", 1)
    end
  end

  def test_rejects_invalid_id_column
    mock = RelevanceMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.explain_score(mock, "articles", "body", "q", "bad id!", 1)
    end
  end
end
