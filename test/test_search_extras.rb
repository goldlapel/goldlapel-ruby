# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class SearchExtrasMockResult
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

class SearchExtrasMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = SearchExtrasMockResult.new([], [])
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

# --- facets ---

class TestFacetsWithoutQuery < Minitest::Test
  def test_returns_value_count_hashes
    facet_result = SearchExtrasMockResult.new(
      [
        { "value" => "electronics", "count" => "42" },
        { "value" => "clothing", "count" => "17" }
      ],
      ["value", "count"]
    )
    mock = SearchExtrasMockConnection.new("GROUP BY" => facet_result)
    result = GoldLapel.facets(mock, "products", "category")

    assert_equal 2, result.length
    assert_equal "electronics", result[0]["value"]
    assert_equal 42, result[0]["count"]
    assert_equal "clothing", result[1]["value"]
    assert_equal 17, result[1]["count"]

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "GROUP BY category"
    assert_includes call[:sql], "ORDER BY count DESC"
    assert_equal [50], call[:params]
  end

  def test_custom_limit
    mock = SearchExtrasMockConnection.new
    GoldLapel.facets(mock, "products", "category", limit: 10)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_equal [10], call[:params]
  end
end

class TestFacetsWithQuery < Minitest::Test
  def test_filters_by_fulltext_search
    facet_result = SearchExtrasMockResult.new(
      [{ "value" => "electronics", "count" => "5" }],
      ["value", "count"]
    )
    mock = SearchExtrasMockConnection.new("GROUP BY" => facet_result)
    result = GoldLapel.facets(mock, "products", "category", query: "laptop", query_column: "name")

    assert_equal 1, result.length
    assert_equal 5, result[0]["count"]

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "to_tsvector"
    assert_includes call[:sql], "plainto_tsquery"
    assert_equal ["english", "english", "laptop", 50], call[:params]
  end

  def test_multi_column_query
    mock = SearchExtrasMockConnection.new
    GoldLapel.facets(mock, "products", "category", query: "laptop", query_column: ["name", "description"])

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "coalesce(name, '') || ' ' || coalesce(description, '')"
  end
end

class TestFacetsValidation < Minitest::Test
  def test_rejects_invalid_table
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.facets(mock, "bad table!", "col") }
  end

  def test_rejects_invalid_column
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.facets(mock, "products", "bad col!") }
  end

  def test_rejects_invalid_query_column
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.facets(mock, "products", "category", query: "x", query_column: "bad col!")
    end
  end
end

# --- aggregate ---

class TestAggregateWithoutGroupBy < Minitest::Test
  def test_count_returns_array_with_one_element
    agg_result = SearchExtrasMockResult.new(
      [{ "value" => "100" }],
      ["value"]
    )
    mock = SearchExtrasMockConnection.new("SELECT" => agg_result)
    result = GoldLapel.aggregate(mock, "orders", "id", "count")

    assert_equal [{ "value" => "100" }], result

    call = mock.calls.find { |c| c[:method] == :exec }
    refute_nil call
    assert_includes call[:sql], "COUNT(*)"
    refute_includes call[:sql], "GROUP BY"
  end

  def test_sum_uses_column
    agg_result = SearchExtrasMockResult.new(
      [{ "value" => "5000.50" }],
      ["value"]
    )
    mock = SearchExtrasMockConnection.new("SELECT" => agg_result)
    result = GoldLapel.aggregate(mock, "orders", "total", "sum")

    assert_equal [{ "value" => "5000.50" }], result

    call = mock.calls.find { |c| c[:method] == :exec }
    assert_includes call[:sql], "SUM(total)"
  end

  def test_avg_uses_column
    mock = SearchExtrasMockConnection.new
    GoldLapel.aggregate(mock, "orders", "total", "avg")

    call = mock.calls.find { |c| c[:method] == :exec }
    assert_includes call[:sql], "AVG(total)"
  end

  def test_min_uses_column
    mock = SearchExtrasMockConnection.new
    GoldLapel.aggregate(mock, "orders", "total", "min")

    call = mock.calls.find { |c| c[:method] == :exec }
    assert_includes call[:sql], "MIN(total)"
  end

  def test_max_uses_column
    mock = SearchExtrasMockConnection.new
    GoldLapel.aggregate(mock, "orders", "total", "max")

    call = mock.calls.find { |c| c[:method] == :exec }
    assert_includes call[:sql], "MAX(total)"
  end

  def test_returns_array_with_nil_value_when_empty
    mock = SearchExtrasMockConnection.new
    result = GoldLapel.aggregate(mock, "orders", "total", "sum")
    assert_equal [{ "value" => nil }], result
  end
end

class TestAggregateWithGroupBy < Minitest::Test
  def test_groups_and_limits
    agg_result = SearchExtrasMockResult.new(
      [
        { "region" => "US", "value" => "5000" },
        { "region" => "EU", "value" => "3000" }
      ],
      ["region", "value"]
    )
    mock = SearchExtrasMockConnection.new("GROUP BY" => agg_result)
    result = GoldLapel.aggregate(mock, "orders", "total", "sum", group_by: "region", limit: 25)

    assert_equal 2, result.length
    assert_equal "US", result[0]["region"]
    assert_equal "5000", result[0]["value"]

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "SUM(total)"
    assert_includes call[:sql], "GROUP BY region"
    assert_includes call[:sql], "ORDER BY value DESC"
    assert_equal [25], call[:params]
  end

  def test_count_with_group_by
    agg_result = SearchExtrasMockResult.new(
      [{ "status" => "active", "value" => "50" }],
      ["status", "value"]
    )
    mock = SearchExtrasMockConnection.new("GROUP BY" => agg_result)
    result = GoldLapel.aggregate(mock, "users", "id", "count", group_by: "status")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "COUNT(*)"
    assert_includes call[:sql], "GROUP BY status"
  end
end

class TestAggregateValidation < Minitest::Test
  def test_rejects_invalid_func
    mock = SearchExtrasMockConnection.new
    err = assert_raises(ArgumentError) { GoldLapel.aggregate(mock, "t", "c", "drop") }
    assert_includes err.message, "Invalid aggregate function"
  end

  def test_rejects_invalid_table
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.aggregate(mock, "bad table!", "c", "count") }
  end

  def test_rejects_invalid_column
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.aggregate(mock, "t", "bad col!", "count") }
  end

  def test_rejects_invalid_group_by
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.aggregate(mock, "t", "c", "count", group_by: "bad col!")
    end
  end

  def test_func_is_case_insensitive
    mock = SearchExtrasMockConnection.new
    GoldLapel.aggregate(mock, "orders", "total", "SUM")
    call = mock.calls.find { |c| c[:method] == :exec }
    assert_includes call[:sql], "SUM(total)"
  end
end

# --- create_search_config ---

class TestCreateSearchConfig < Minitest::Test
  def test_creates_config_when_not_exists
    mock = SearchExtrasMockConnection.new
    GoldLapel.create_search_config(mock, "my_config")

    check_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("pg_ts_config") }
    refute_nil check_call
    assert_equal ["my_config"], check_call[:params]

    create_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE TEXT SEARCH") }
    refute_nil create_call
    assert_includes create_call[:sql], "my_config"
    assert_includes create_call[:sql], "COPY = english"
  end

  def test_skips_creation_when_exists
    exists_result = SearchExtrasMockResult.new(
      [{ "?column?" => "1" }],
      ["?column?"]
    )
    mock = SearchExtrasMockConnection.new("pg_ts_config" => exists_result)
    GoldLapel.create_search_config(mock, "existing_config")

    create_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE TEXT SEARCH") }
    assert_nil create_call
  end

  def test_custom_copy_from
    mock = SearchExtrasMockConnection.new
    GoldLapel.create_search_config(mock, "my_config", copy_from: "simple")

    create_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE TEXT SEARCH") }
    refute_nil create_call
    assert_includes create_call[:sql], "COPY = simple"
  end
end

class TestCreateSearchConfigValidation < Minitest::Test
  def test_rejects_invalid_name
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.create_search_config(mock, "bad name!") }
  end

  def test_rejects_invalid_copy_from
    mock = SearchExtrasMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.create_search_config(mock, "my_config", copy_from: "bad; drop")
    end
  end
end
