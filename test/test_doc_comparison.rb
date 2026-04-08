# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class CompMockResult
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

class CompMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = CompMockResult.new([], [])
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

# --- $gt / $gte / $lt / $lte numeric ---

class TestComparisonNumeric < Minitest::Test
  def test_gt_numeric_cast
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "products", filter: { "price" => { "$gt" => 100 } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "(data->>'price')::numeric > $1"
    assert_equal [100], call[:params]
  end

  def test_gte_lte_combined
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "products", filter: { "price" => { "$gte" => 10, "$lte" => 50 } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "(data->>'price')::numeric >= $1"
    assert_includes call[:sql], "(data->>'price')::numeric <= $2"
    assert_equal [10, 50], call[:params]
  end

  def test_lt_numeric
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "products", filter: { "stock" => { "$lt" => 5 } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "(data->>'stock')::numeric < $1"
    assert_equal [5], call[:params]
  end
end

# --- $eq / $ne string ---

class TestComparisonEqNe < Minitest::Test
  def test_eq_string
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "status" => { "$eq" => "active" } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->>'status' = $1"
    assert_equal ["active"], call[:params]
  end

  def test_ne_string
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "role" => { "$ne" => "admin" } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->>'role' != $1"
    assert_equal ["admin"], call[:params]
  end
end

# --- $in / $nin ---

class TestComparisonInNin < Minitest::Test
  def test_in_operator
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "role" => { "$in" => %w[admin editor] } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->>'role' IN ($1, $2)"
    assert_equal %w[admin editor], call[:params]
  end

  def test_nin_operator
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "status" => { "$nin" => %w[banned suspended] } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->>'status' NOT IN ($1, $2)"
    assert_equal %w[banned suspended], call[:params]
  end
end

# --- $exists ---

class TestComparisonExists < Minitest::Test
  def test_exists_true
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "email" => { "$exists" => true } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data ? $1"
    assert_equal ["email"], call[:params]
  end

  def test_exists_false
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "phone" => { "$exists" => false } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "NOT (data ? $1)"
    assert_equal ["phone"], call[:params]
  end
end

# --- $regex ---

class TestComparisonRegex < Minitest::Test
  def test_regex_operator
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "name" => { "$regex" => "^A" } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->>'name' ~ $1"
    assert_equal ["^A"], call[:params]
  end
end

# --- dot notation ---

class TestComparisonDotNotation < Minitest::Test
  def test_nested_field_path
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "address.zip" => { "$eq" => "90210" } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->'address'->>'zip' = $1"
    assert_equal ["90210"], call[:params]
  end

  def test_deep_nested_field_path
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "a.b.c" => { "$gt" => 5 } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "(data->'a'->'b'->>'c')::numeric > $1"
    assert_equal [5], call[:params]
  end
end

# --- mixed filters (operators + containment) ---

class TestComparisonMixed < Minitest::Test
  def test_operator_plus_containment
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "products", filter: { "category" => "electronics", "price" => { "$gt" => 100 } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    # operator clause gets $1, containment gets $2 (appended after operators)
    assert_includes call[:sql], "(data->>'price')::numeric > $1"
    assert_includes call[:sql], "data @> $2::jsonb"
    assert_equal [100, JSON.generate({ "category" => "electronics" })], call[:params]
  end
end

# --- parameter numbering with limit/skip ---

class TestComparisonParamNumbering < Minitest::Test
  def test_operator_with_limit_skip
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "products", filter: { "price" => { "$gte" => 10, "$lte" => 50 } }, limit: 20, skip: 5)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "(data->>'price')::numeric >= $1"
    assert_includes call[:sql], "(data->>'price')::numeric <= $2"
    assert_includes call[:sql], "LIMIT $3"
    assert_includes call[:sql], "OFFSET $4"
    assert_equal [10, 50, 20, 5], call[:params]
  end
end

# --- operators in non-find functions ---

class TestComparisonInOtherMethods < Minitest::Test
  def test_doc_count_with_operator
    count_result = CompMockResult.new([{ "count" => "3" }], ["count"])
    mock = CompMockConnection.new("COUNT" => count_result)
    result = GoldLapel.doc_count(mock, "products", filter: { "price" => { "$gt" => 50 } })

    assert_equal 3, result
    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("COUNT") }
    refute_nil call
    assert_includes call[:sql], "WHERE (data->>'price')::numeric > $1"
    assert_equal [50], call[:params]
  end

  def test_doc_delete_with_operator
    mock = CompMockConnection.new
    GoldLapel.doc_delete(mock, "products", { "price" => { "$lt" => 0 } })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DELETE") }
    refute_nil call
    assert_includes call[:sql], "DELETE FROM products WHERE (data->>'price')::numeric < $1"
    assert_equal [0], call[:params]
  end

  def test_doc_update_with_operator
    mock = CompMockConnection.new
    GoldLapel.doc_update(mock, "products", { "stock" => { "$lte" => 0 } }, { "status" => "out_of_stock" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "SET data = data || $2::jsonb"
    assert_includes call[:sql], "WHERE (data->>'stock')::numeric <= $1"
    assert_equal [0, JSON.generate({ "status" => "out_of_stock" })], call[:params]
  end
end

# --- key validation ---

class TestComparisonKeyValidation < Minitest::Test
  def test_rejects_invalid_filter_key
    mock = CompMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: { "bad key!" => { "$gt" => 1 } })
    end
  end

  def test_rejects_unsupported_operator
    mock = CompMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: { "age" => { "$bogus" => 1 } })
    end
  end
end

# --- dot notation expansion in plain containment filters ---

class TestDotNotationExpansion < Minitest::Test
  def test_single_level
    result = GoldLapel.send(:_expand_dot_keys, { "addr.city" => "NY" })
    assert_equal({ "addr" => { "city" => "NY" } }, result)
  end

  def test_deep_nesting
    result = GoldLapel.send(:_expand_dot_keys, { "a.b.c" => 1 })
    assert_equal({ "a" => { "b" => { "c" => 1 } } }, result)
  end

  def test_mixed_with_plain
    result = GoldLapel.send(:_expand_dot_keys, { "status" => "active", "addr.city" => "NY" })
    assert_equal({ "status" => "active", "addr" => { "city" => "NY" } }, result)
  end

  def test_merge_siblings
    result = GoldLapel.send(:_expand_dot_keys, { "a.b" => 1, "a.c" => 2 })
    assert_equal({ "a" => { "b" => 1, "c" => 2 } }, result)
  end

  def test_no_dots_unchanged
    result = GoldLapel.send(:_expand_dot_keys, { "status" => "active" })
    assert_equal({ "status" => "active" }, result)
  end

  def test_dot_with_operators_in_build_filter
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "addr.city" => "NY", "age" => { "$gt" => 25 } })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data @> $2::jsonb"
    containment_json = call[:params][1]
    assert_equal({ "addr" => { "city" => "NY" } }, JSON.parse(containment_json))
    assert_includes call[:sql], "(data->>'age')::numeric > $1"
    assert_equal 25, call[:params][0]
  end

  def test_dot_in_doc_find
    mock = CompMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { "addr.city" => "NY" })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_equal({ "addr" => { "city" => "NY" } }, JSON.parse(call[:params][0]))
  end

  def test_dot_in_doc_count
    count_result = CompMockResult.new([{ "count" => "3" }], ["count"])
    mock = CompMockConnection.new("COUNT" => count_result)
    GoldLapel.doc_count(mock, "users", filter: { "addr.city" => "NY" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("COUNT") }
    refute_nil call
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_equal({ "addr" => { "city" => "NY" } }, JSON.parse(call[:params][0]))
  end
end
