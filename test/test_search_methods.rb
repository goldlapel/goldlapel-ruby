# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class SearchMethodsMockResult
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

class SearchMethodsMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = SearchMethodsMockResult.new([], [])
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

# --- search ---

class TestSearch < Minitest::Test
  def test_returns_rows_with_string_keys
    search_result = SearchMethodsMockResult.new(
      [
        { "id" => "1", "title" => "Ruby Guide", "_score" => "0.075" },
        { "id" => "2", "title" => "Ruby Tips", "_score" => "0.05" }
      ],
      ["id", "title", "_score"]
    )
    mock = SearchMethodsMockConnection.new("ts_rank" => search_result)
    result = GoldLapel.search(mock, "articles", "title", "ruby")

    assert_equal 2, result.length
    assert_equal "1", result[0]["id"]
    assert_equal "Ruby Guide", result[0]["title"]
    assert_equal "0.075", result[0]["_score"]
  end

  def test_sql_structure_single_column
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "ts_rank(to_tsvector($1, coalesce(title, '')), plainto_tsquery($2, $3)) AS _score"
    assert_includes call[:sql], "FROM articles"
    assert_includes call[:sql], "WHERE to_tsvector($4, coalesce(title, '')) @@ plainto_tsquery($5, $6)"
    assert_includes call[:sql], "ORDER BY _score DESC LIMIT $7"
    assert_equal ["english", "english", "ruby", "english", "english", "ruby", 50], call[:params]
  end

  def test_multi_column_coalesce_wrapping
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", ["title", "body"], "ruby")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "coalesce(title, '') || ' ' || coalesce(body, '')"
  end

  def test_three_columns
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", ["title", "body", "tags"], "ruby")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "coalesce(title, '') || ' ' || coalesce(body, '') || ' ' || coalesce(tags, '')"
  end

  def test_custom_limit
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby", limit: 10)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal 10, call[:params].last
  end

  def test_custom_lang
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby", lang: "french")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal "french", call[:params][0]
    assert_equal "french", call[:params][1]
    assert_equal "french", call[:params][3]
    assert_equal "french", call[:params][4]
  end

  def test_default_lang_is_english
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal "english", call[:params][0]
  end

  def test_returns_empty_array_when_no_matches
    mock = SearchMethodsMockConnection.new
    result = GoldLapel.search(mock, "articles", "title", "nonexistent")

    assert_equal [], result
  end

  def test_rejects_invalid_table
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search(mock, "bad table!", "title", "q") }
  end

  def test_rejects_invalid_column
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search(mock, "articles", "bad col!", "q") }
  end

  def test_rejects_invalid_column_in_array
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search(mock, "articles", ["title", "bad col!"], "q") }
  end
end

class TestSearchWithHighlight < Minitest::Test
  def test_highlight_sql_includes_ts_headline
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby", highlight: true)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "ts_headline"
    assert_includes call[:sql], "StartSel=<mark>, StopSel=</mark>"
    assert_includes call[:sql], "MaxWords=35, MinWords=15"
    assert_includes call[:sql], "AS _highlight"
  end

  def test_highlight_params
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby", highlight: true)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    # highlight uses 10 params: lang x3, query x3, lang x3, query x1, limit x1
    assert_equal ["english", "english", "ruby", "english", "english", "ruby", "english", "english", "ruby", 50], call[:params]
  end

  def test_highlight_uses_first_column_for_headline
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", ["title", "body"], "ruby", highlight: true)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    # ts_headline should use the first column (title), not the tsvec combo
    assert_includes call[:sql], "ts_headline($4, title, plainto_tsquery($5, $6)"
  end

  def test_highlight_result_includes_highlight_field
    search_result = SearchMethodsMockResult.new(
      [{ "id" => "1", "title" => "Ruby Guide", "_score" => "0.075",
         "_highlight" => "The <mark>Ruby</mark> Guide" }],
      ["id", "title", "_score", "_highlight"]
    )
    mock = SearchMethodsMockConnection.new("ts_headline" => search_result)
    result = GoldLapel.search(mock, "articles", "title", "ruby", highlight: true)

    assert_equal "The <mark>Ruby</mark> Guide", result[0]["_highlight"]
  end

  def test_no_highlight_by_default
    mock = SearchMethodsMockConnection.new
    GoldLapel.search(mock, "articles", "title", "ruby")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_includes call[:sql], "ts_headline"
    refute_includes call[:sql], "_highlight"
  end
end

# --- search_fuzzy ---

class TestSearchFuzzy < Minitest::Test
  def test_returns_rows_with_string_keys
    fuzzy_result = SearchMethodsMockResult.new(
      [
        { "id" => "1", "name" => "postgresql", "_score" => "0.5" },
        { "id" => "2", "name" => "postgres", "_score" => "0.4" }
      ],
      ["id", "name", "_score"]
    )
    mock = SearchMethodsMockConnection.new("similarity" => fuzzy_result)
    result = GoldLapel.search_fuzzy(mock, "tools", "name", "postgre")

    assert_equal 2, result.length
    assert_equal "postgresql", result[0]["name"]
    assert_equal "0.5", result[0]["_score"]
  end

  def test_sql_structure
    mock = SearchMethodsMockConnection.new
    GoldLapel.search_fuzzy(mock, "tools", "name", "postgre")

    # First call creates the extension
    ext_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("pg_trgm") }
    refute_nil ext_call
    assert_includes ext_call[:sql], "CREATE EXTENSION IF NOT EXISTS pg_trgm"

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "similarity(name, $1) AS _score"
    assert_includes call[:sql], "FROM tools"
    assert_includes call[:sql], "WHERE similarity(name, $2) > $3"
    assert_includes call[:sql], "ORDER BY _score DESC LIMIT $4"
    assert_equal ["postgre", "postgre", 0.3, 50], call[:params]
  end

  def test_custom_threshold
    mock = SearchMethodsMockConnection.new
    GoldLapel.search_fuzzy(mock, "tools", "name", "postgre", threshold: 0.5)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal 0.5, call[:params][2]
  end

  def test_custom_limit
    mock = SearchMethodsMockConnection.new
    GoldLapel.search_fuzzy(mock, "tools", "name", "postgre", limit: 5)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal 5, call[:params][3]
  end

  def test_returns_empty_array_when_no_matches
    mock = SearchMethodsMockConnection.new
    result = GoldLapel.search_fuzzy(mock, "tools", "name", "zzzzz")

    assert_equal [], result
  end

  def test_rejects_invalid_table
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search_fuzzy(mock, "bad table!", "name", "q") }
  end

  def test_rejects_invalid_column
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search_fuzzy(mock, "tools", "bad col!", "q") }
  end
end

# --- search_phonetic ---

class TestSearchPhonetic < Minitest::Test
  def test_returns_rows_with_string_keys
    phonetic_result = SearchMethodsMockResult.new(
      [
        { "id" => "1", "name" => "Smith", "_score" => "0.6" },
        { "id" => "2", "name" => "Smyth", "_score" => "0.4" }
      ],
      ["id", "name", "_score"]
    )
    mock = SearchMethodsMockConnection.new("soundex" => phonetic_result)
    result = GoldLapel.search_phonetic(mock, "people", "name", "Smith")

    assert_equal 2, result.length
    assert_equal "Smith", result[0]["name"]
    assert_equal "0.6", result[0]["_score"]
  end

  def test_sql_structure
    mock = SearchMethodsMockConnection.new
    GoldLapel.search_phonetic(mock, "people", "name", "Smith")

    # Creates both extensions
    ext_calls = mock.calls.select { |c| c[:method] == :exec && c[:sql].include?("CREATE EXTENSION") }
    ext_names = ext_calls.map { |c| c[:sql] }
    assert ext_names.any? { |s| s.include?("fuzzystrmatch") }
    assert ext_names.any? { |s| s.include?("pg_trgm") }

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "similarity(name, $1) AS _score"
    assert_includes call[:sql], "FROM people"
    assert_includes call[:sql], "WHERE soundex(name) = soundex($2)"
    assert_includes call[:sql], "ORDER BY _score DESC, name LIMIT $3"
    assert_equal ["Smith", "Smith", 50], call[:params]
  end

  def test_custom_limit
    mock = SearchMethodsMockConnection.new
    GoldLapel.search_phonetic(mock, "people", "name", "Smith", limit: 20)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal 20, call[:params][2]
  end

  def test_returns_empty_array_when_no_matches
    mock = SearchMethodsMockConnection.new
    result = GoldLapel.search_phonetic(mock, "people", "name", "Xyzzy")

    assert_equal [], result
  end

  def test_rejects_invalid_table
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search_phonetic(mock, "bad table!", "name", "q") }
  end

  def test_rejects_invalid_column
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.search_phonetic(mock, "people", "bad col!", "q") }
  end
end

# --- similar ---

class TestSimilar < Minitest::Test
  def test_returns_rows_with_string_keys
    similar_result = SearchMethodsMockResult.new(
      [
        { "id" => "1", "title" => "Doc A", "_score" => "0.15" },
        { "id" => "2", "title" => "Doc B", "_score" => "0.32" }
      ],
      ["id", "title", "_score"]
    )
    mock = SearchMethodsMockConnection.new("<=>" => similar_result)
    result = GoldLapel.similar(mock, "docs", "embedding", [0.1, 0.2, 0.3])

    assert_equal 2, result.length
    assert_equal "Doc A", result[0]["title"]
    assert_equal "0.15", result[0]["_score"]
  end

  def test_sql_structure
    mock = SearchMethodsMockConnection.new
    GoldLapel.similar(mock, "docs", "embedding", [0.1, 0.2, 0.3])

    # Creates the extension
    ext_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("vector") }
    refute_nil ext_call
    assert_includes ext_call[:sql], "CREATE EXTENSION IF NOT EXISTS vector"

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "(embedding <=> $1::vector) AS _score"
    assert_includes call[:sql], "FROM docs"
    assert_includes call[:sql], "ORDER BY _score LIMIT $2"
    assert_equal ["[0.1,0.2,0.3]", 10], call[:params]
  end

  def test_vector_formatting
    mock = SearchMethodsMockConnection.new
    GoldLapel.similar(mock, "docs", "embedding", [1, 2, 3])

    call = mock.calls.find { |c| c[:method] == :exec_params }
    # integers should be converted to float strings
    assert_equal "[1.0,2.0,3.0]", call[:params][0]
  end

  def test_custom_limit
    mock = SearchMethodsMockConnection.new
    GoldLapel.similar(mock, "docs", "embedding", [0.1, 0.2], limit: 5)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal 5, call[:params][1]
  end

  def test_returns_empty_array_when_no_matches
    mock = SearchMethodsMockConnection.new
    result = GoldLapel.similar(mock, "docs", "embedding", [0.1, 0.2])

    assert_equal [], result
  end

  def test_rejects_invalid_table
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.similar(mock, "bad table!", "embedding", [0.1]) }
  end

  def test_rejects_invalid_column
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.similar(mock, "docs", "bad col!", [0.1]) }
  end
end

# --- suggest ---

class TestSuggest < Minitest::Test
  def test_returns_rows_with_string_keys
    suggest_result = SearchMethodsMockResult.new(
      [
        { "id" => "1", "name" => "postgresql", "_score" => "0.5" },
        { "id" => "2", "name" => "postfix", "_score" => "0.3" }
      ],
      ["id", "name", "_score"]
    )
    mock = SearchMethodsMockConnection.new("ILIKE" => suggest_result)
    result = GoldLapel.suggest(mock, "tools", "name", "post")

    assert_equal 2, result.length
    assert_equal "postgresql", result[0]["name"]
    assert_equal "0.5", result[0]["_score"]
  end

  def test_sql_structure
    mock = SearchMethodsMockConnection.new
    GoldLapel.suggest(mock, "tools", "name", "post")

    # Creates the extension
    ext_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("pg_trgm") }
    refute_nil ext_call

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "similarity(name, $1) AS _score"
    assert_includes call[:sql], "FROM tools"
    assert_includes call[:sql], "WHERE name ILIKE $2"
    assert_includes call[:sql], "ORDER BY _score DESC, name LIMIT $3"
    assert_equal ["post", "post%", 10], call[:params]
  end

  def test_prefix_pattern_has_percent_suffix
    mock = SearchMethodsMockConnection.new
    GoldLapel.suggest(mock, "tools", "name", "abc")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal "abc%", call[:params][1]
  end

  def test_custom_limit
    mock = SearchMethodsMockConnection.new
    GoldLapel.suggest(mock, "tools", "name", "post", limit: 5)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal 5, call[:params][2]
  end

  def test_returns_empty_array_when_no_matches
    mock = SearchMethodsMockConnection.new
    result = GoldLapel.suggest(mock, "tools", "name", "zzz")

    assert_equal [], result
  end

  def test_rejects_invalid_table
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.suggest(mock, "bad table!", "name", "q") }
  end

  def test_rejects_invalid_column
    mock = SearchMethodsMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.suggest(mock, "tools", "bad col!", "q") }
  end
end
