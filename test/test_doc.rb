# frozen_string_literal: true

require "minitest/autorun"
require "json"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"

class DocMockResult
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

class DocMockConnection
  attr_reader :calls

  def initialize(results = {})
    @calls = []
    @results = results
    @default = DocMockResult.new([], [])
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

# --- doc_insert ---

class TestDocInsert < Minitest::Test
  def test_creates_table_and_inserts
    insert_result = DocMockResult.new(
      [{ "id" => "1", "data" => '{"name":"Alice","age":30}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("INSERT" => insert_result)
    result = GoldLapel.doc_insert(mock, "users", { name: "Alice", age: 30 })

    assert_equal 1, result["id"]
    assert_equal({ "name" => "Alice", "age" => 30 }, result["data"])
    assert_equal "2026-04-07 00:00:00+00", result["created_at"]
  end

  def test_table_ddl
    insert_result = DocMockResult.new(
      [{ "id" => "1", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("INSERT" => insert_result)
    GoldLapel.doc_insert(mock, "users", { name: "Alice" })

    create_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE TABLE") }
    refute_nil create_call
    assert_includes create_call[:sql], "CREATE TABLE IF NOT EXISTS users"
    assert_includes create_call[:sql], "id BIGSERIAL PRIMARY KEY"
    assert_includes create_call[:sql], "data JSONB NOT NULL"
    assert_includes create_call[:sql], "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
  end

  def test_insert_sql_and_params
    insert_result = DocMockResult.new(
      [{ "id" => "1", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("INSERT" => insert_result)
    GoldLapel.doc_insert(mock, "users", { name: "Alice" })

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "INSERT INTO users (data) VALUES ($1::jsonb)"
    assert_includes insert_call[:sql], "RETURNING id, data, created_at"
    assert_equal [JSON.generate({ name: "Alice" })], insert_call[:params]
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_insert(mock, "bad table!", {}) }
  end
end

# --- doc_insert_many ---

class TestDocInsertMany < Minitest::Test
  def test_batch_inserts
    insert_result = DocMockResult.new(
      [
        { "id" => "1", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" },
        { "id" => "2", "data" => '{"name":"Bob"}', "created_at" => "2026-04-07 00:00:00+00" }
      ],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("INSERT" => insert_result)
    result = GoldLapel.doc_insert_many(mock, "users", [{ name: "Alice" }, { name: "Bob" }])

    assert_equal 2, result.length
    assert_equal 1, result[0]["id"]
    assert_equal({ "name" => "Alice" }, result[0]["data"])
    assert_equal 2, result[1]["id"]
    assert_equal({ "name" => "Bob" }, result[1]["data"])
  end

  def test_placeholders_and_params
    mock = DocMockConnection.new
    GoldLapel.doc_insert_many(mock, "users", [{ a: 1 }, { b: 2 }, { c: 3 }])

    insert_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("INSERT") }
    refute_nil insert_call
    assert_includes insert_call[:sql], "($1::jsonb), ($2::jsonb), ($3::jsonb)"
    assert_equal 3, insert_call[:params].length
    assert_equal JSON.generate({ a: 1 }), insert_call[:params][0]
    assert_equal JSON.generate({ b: 2 }), insert_call[:params][1]
    assert_equal JSON.generate({ c: 3 }), insert_call[:params][2]
  end

  def test_rejects_empty_array
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_insert_many(mock, "users", []) }
  end

  def test_rejects_non_array
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_insert_many(mock, "users", "not an array") }
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_insert_many(mock, "bad table!", [{}]) }
  end
end

# --- doc_find ---

class TestDocFind < Minitest::Test
  def test_find_all_without_filter
    find_result = DocMockResult.new(
      [
        { "id" => "1", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" },
        { "id" => "2", "data" => '{"name":"Bob"}', "created_at" => "2026-04-07 00:00:00+00" }
      ],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("SELECT" => find_result)
    result = GoldLapel.doc_find(mock, "users")

    assert_equal 2, result.length
    assert_equal({ "name" => "Alice" }, result[0]["data"])
    assert_equal({ "name" => "Bob" }, result[1]["data"])
  end

  def test_find_with_filter
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { status: "active" })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_equal [JSON.generate({ status: "active" })], call[:params]
  end

  def test_find_with_sort
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", sort: { "name" => 1, "age" => -1 })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "ORDER BY data->>'name' ASC, data->>'age' DESC"
  end

  def test_find_with_limit_and_skip
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", limit: 10, skip: 20)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "LIMIT $1"
    assert_includes call[:sql], "OFFSET $2"
    assert_equal [10, 20], call[:params]
  end

  def test_find_with_filter_limit_and_skip
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: { role: "admin" }, limit: 5, skip: 10)

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "LIMIT $2"
    assert_includes call[:sql], "OFFSET $3"
    assert_equal [JSON.generate({ role: "admin" }), 5, 10], call[:params]
  end

  def test_find_returns_empty_array_when_no_matches
    mock = DocMockConnection.new
    result = GoldLapel.doc_find(mock, "users", filter: { nonexistent: true })

    assert_equal [], result
  end

  def test_find_rejects_invalid_sort_key
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", sort: { "bad key!" => 1 })
    end
  end

  def test_find_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_find(mock, "bad table!") }
  end

  def test_find_with_empty_filter_omits_where
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {})

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    refute_includes call[:sql], "WHERE"
  end
end

# --- doc_find_one ---

class TestDocFindOne < Minitest::Test
  def test_returns_single_hash
    find_result = DocMockResult.new(
      [{ "id" => "1", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("SELECT" => find_result)
    result = GoldLapel.doc_find_one(mock, "users", filter: { name: "Alice" })

    refute_nil result
    assert_equal 1, result["id"]
    assert_equal({ "name" => "Alice" }, result["data"])
  end

  def test_returns_nil_when_not_found
    mock = DocMockConnection.new
    result = GoldLapel.doc_find_one(mock, "users", filter: { name: "nobody" })

    assert_nil result
  end

  def test_sql_with_filter
    mock = DocMockConnection.new
    GoldLapel.doc_find_one(mock, "users", filter: { status: "active" })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE data @> $1::jsonb LIMIT 1"
    assert_equal [JSON.generate({ status: "active" })], call[:params]
  end

  def test_sql_without_filter
    mock = DocMockConnection.new
    GoldLapel.doc_find_one(mock, "users")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    refute_includes call[:sql], "WHERE"
    assert_includes call[:sql], "LIMIT 1"
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_find_one(mock, "bad table!") }
  end
end

# --- doc_update ---

class TestDocUpdate < Minitest::Test
  def test_returns_update_count
    update_result = DocMockResult.new(
      [{ "dummy" => "1" }, { "dummy" => "2" }],
      ["dummy"]
    )
    mock = DocMockConnection.new("UPDATE" => update_result)
    count = GoldLapel.doc_update(mock, "users", { role: "guest" }, { role: "member" })

    assert_equal 2, count
  end

  def test_sql_structure
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { status: "old" }, { status: "new" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "UPDATE users SET data = data || $2::jsonb"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_equal [JSON.generate({ status: "old" }), JSON.generate({ status: "new" })], call[:params]
  end

  def test_returns_zero_when_no_match
    mock = DocMockConnection.new
    count = GoldLapel.doc_update(mock, "users", { x: 1 }, { x: 2 })

    assert_equal 0, count
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_update(mock, "bad table!", {}, {}) }
  end
end

# --- doc_update_one ---

class TestDocUpdateOne < Minitest::Test
  def test_returns_update_count
    update_result = DocMockResult.new(
      [{ "dummy" => "1" }],
      ["dummy"]
    )
    mock = DocMockConnection.new("UPDATE" => update_result)
    count = GoldLapel.doc_update_one(mock, "users", { name: "Alice" }, { age: 31 })

    assert_equal 1, count
  end

  def test_sql_uses_cte_limit_1
    mock = DocMockConnection.new
    GoldLapel.doc_update_one(mock, "users", { name: "Alice" }, { age: 31 })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "WITH target AS ("
    assert_includes call[:sql], "SELECT id FROM users"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "LIMIT 1"
    assert_includes call[:sql], "UPDATE users SET data = data || $2::jsonb"
    assert_includes call[:sql], "FROM target WHERE users.id = target.id"
    assert_equal [JSON.generate({ name: "Alice" }), JSON.generate({ age: 31 })], call[:params]
  end

  def test_returns_zero_when_no_match
    mock = DocMockConnection.new
    count = GoldLapel.doc_update_one(mock, "users", { x: 1 }, { x: 2 })

    assert_equal 0, count
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_update_one(mock, "bad table!", {}, {}) }
  end
end

# --- doc_delete ---

class TestDocDelete < Minitest::Test
  def test_returns_delete_count
    delete_result = DocMockResult.new(
      [{ "dummy" => "1" }, { "dummy" => "2" }, { "dummy" => "3" }],
      ["dummy"]
    )
    mock = DocMockConnection.new("DELETE" => delete_result)
    count = GoldLapel.doc_delete(mock, "users", { status: "inactive" })

    assert_equal 3, count
  end

  def test_sql_structure
    mock = DocMockConnection.new
    GoldLapel.doc_delete(mock, "users", { status: "inactive" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DELETE") }
    refute_nil call
    assert_includes call[:sql], "DELETE FROM users WHERE data @> $1::jsonb"
    assert_equal [JSON.generate({ status: "inactive" })], call[:params]
  end

  def test_returns_zero_when_no_match
    mock = DocMockConnection.new
    count = GoldLapel.doc_delete(mock, "users", { nonexistent: true })

    assert_equal 0, count
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_delete(mock, "bad table!", {}) }
  end
end

# --- doc_delete_one ---

class TestDocDeleteOne < Minitest::Test
  def test_returns_delete_count
    delete_result = DocMockResult.new(
      [{ "dummy" => "1" }],
      ["dummy"]
    )
    mock = DocMockConnection.new("DELETE" => delete_result)
    count = GoldLapel.doc_delete_one(mock, "users", { name: "Alice" })

    assert_equal 1, count
  end

  def test_sql_uses_cte_limit_1
    mock = DocMockConnection.new
    GoldLapel.doc_delete_one(mock, "users", { name: "Alice" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DELETE") }
    refute_nil call
    assert_includes call[:sql], "WITH target AS ("
    assert_includes call[:sql], "SELECT id FROM users"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "LIMIT 1"
    assert_includes call[:sql], "DELETE FROM users"
    assert_includes call[:sql], "USING target WHERE users.id = target.id"
    assert_equal [JSON.generate({ name: "Alice" })], call[:params]
  end

  def test_returns_zero_when_no_match
    mock = DocMockConnection.new
    count = GoldLapel.doc_delete_one(mock, "users", { x: 1 })

    assert_equal 0, count
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_delete_one(mock, "bad table!", {}) }
  end
end

# --- doc_count ---

class TestDocCount < Minitest::Test
  def test_count_without_filter
    count_result = DocMockResult.new(
      [{ "count" => "42" }],
      ["count"]
    )
    mock = DocMockConnection.new("COUNT" => count_result)
    result = GoldLapel.doc_count(mock, "users")

    assert_equal 42, result
  end

  def test_count_without_filter_uses_exec
    count_result = DocMockResult.new(
      [{ "count" => "0" }],
      ["count"]
    )
    mock = DocMockConnection.new("COUNT" => count_result)
    GoldLapel.doc_count(mock, "users")

    call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("COUNT") }
    refute_nil call
    assert_includes call[:sql], "SELECT COUNT(*) FROM users"
    refute_includes call[:sql], "WHERE"
  end

  def test_count_with_filter
    count_result = DocMockResult.new(
      [{ "count" => "7" }],
      ["count"]
    )
    mock = DocMockConnection.new("COUNT" => count_result)
    result = GoldLapel.doc_count(mock, "users", filter: { status: "active" })

    assert_equal 7, result
  end

  def test_count_with_filter_sql
    count_result = DocMockResult.new(
      [{ "count" => "0" }],
      ["count"]
    )
    mock = DocMockConnection.new("COUNT" => count_result)
    GoldLapel.doc_count(mock, "users", filter: { status: "active" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("COUNT") }
    refute_nil call
    assert_includes call[:sql], "SELECT COUNT(*) FROM users WHERE data @> $1::jsonb"
    assert_equal [JSON.generate({ status: "active" })], call[:params]
  end

  def test_count_with_empty_filter_omits_where
    count_result = DocMockResult.new(
      [{ "count" => "0" }],
      ["count"]
    )
    mock = DocMockConnection.new("COUNT" => count_result)
    GoldLapel.doc_count(mock, "users", filter: {})

    call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("COUNT") }
    refute_nil call
    refute_includes call[:sql], "WHERE"
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_count(mock, "bad table!") }
  end
end

# --- doc_create_index ---

class TestDocCreateIndex < Minitest::Test
  def test_gin_index_without_keys
    mock = DocMockConnection.new
    result = GoldLapel.doc_create_index(mock, "users")

    assert_nil result

    call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE INDEX") }
    refute_nil call
    assert_includes call[:sql], "CREATE INDEX IF NOT EXISTS users_data_gin_idx"
    assert_includes call[:sql], "ON users USING GIN (data)"
  end

  def test_btree_index_with_keys
    mock = DocMockConnection.new
    result = GoldLapel.doc_create_index(mock, "users", keys: { "name" => 1, "age" => -1 })

    assert_nil result

    call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE INDEX") }
    refute_nil call
    assert_includes call[:sql], "CREATE INDEX IF NOT EXISTS users_name_age_idx"
    assert_includes call[:sql], "ON users ((data->>'name'), (data->>'age'))"
  end

  def test_single_key_index
    mock = DocMockConnection.new
    GoldLapel.doc_create_index(mock, "products", keys: { "sku" => 1 })

    call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE INDEX") }
    refute_nil call
    assert_includes call[:sql], "products_sku_idx"
    assert_includes call[:sql], "((data->>'sku'))"
  end

  def test_rejects_invalid_index_key
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_create_index(mock, "users", keys: { "bad key!" => 1 })
    end
  end

  def test_accepts_dotted_key
    mock = DocMockConnection.new
    GoldLapel.doc_create_index(mock, "users", keys: { "address.city" => 1 })

    call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CREATE INDEX") }
    refute_nil call
    assert_includes call[:sql], "address.city"
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_create_index(mock, "bad table!") }
  end
end

# --- doc_aggregate ---

class TestDocAggregate < Minitest::Test
  def test_full_pipeline
    agg_result = DocMockResult.new(
      [
        { "_id" => "electronics", "total" => "1500", "cnt" => "3" },
        { "_id" => "books", "total" => "200", "cnt" => "5" }
      ],
      ["_id", "total", "cnt"]
    )
    mock = DocMockConnection.new("SELECT" => agg_result)
    result = GoldLapel.doc_aggregate(mock, "orders", [
      { "$match" => { "status" => "complete" } },
      { "$group" => {
        "_id" => "$category",
        "total" => { "$sum" => "$amount" },
        "cnt" => { "$count" => true }
      }},
      { "$sort" => { "total" => -1 } },
      { "$limit" => 10 },
      { "$skip" => 2 }
    ])

    assert_equal 2, result.length
    assert_equal "electronics", result[0]["_id"]
    assert_equal "1500", result[0]["total"]

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "SUM((data->>'amount')::numeric)::numeric AS total"
    assert_includes call[:sql], "COUNT(*)::numeric AS cnt"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "GROUP BY data->>'category'"
    assert_includes call[:sql], "ORDER BY total DESC"
    assert_includes call[:sql], "LIMIT $2"
    assert_includes call[:sql], "OFFSET $3"
    assert_equal [JSON.generate({ "status" => "complete" }), 10, 2], call[:params]
  end

  def test_avg_accumulator
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => { "_id" => "$region", "avg_price" => { "$avg" => "$price" } } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "AVG((data->>'price')::numeric)::numeric AS avg_price"
    assert_includes call[:sql], "GROUP BY data->>'region'"
  end

  def test_null_id_group
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => { "_id" => nil, "total" => { "$sum" => "$amount" } } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "NULL AS _id"
    refute_includes call[:sql], "GROUP BY"
  end

  def test_match_only
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$match" => { "status" => "pending" } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "SELECT id, data, created_at FROM orders"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_equal [JSON.generate({ "status" => "pending" })], call[:params]
  end

  def test_sort_context_with_group
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => { "_id" => "$region", "total" => { "$sum" => "$amount" } } },
      { "$sort" => { "total" => -1 } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "ORDER BY total DESC"
  end

  def test_sort_context_without_group
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$sort" => { "name" => 1 } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "ORDER BY data->>'name' ASC"
  end

  def test_unsupported_stage
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$bucket" => { "groupBy" => "$price" } }
      ])
    end
  end

  def test_unsupported_accumulator
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$group" => { "_id" => "$region", "first" => { "$first" => "$name" } } }
      ])
    end
  end

  def test_empty_pipeline
    mock = DocMockConnection.new
    result = GoldLapel.doc_aggregate(mock, "orders", [])

    assert_equal [], result
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_aggregate(mock, "bad table!", []) }
  end

  def test_rejects_non_array_pipeline
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_aggregate(mock, "orders", "not an array") }
  end

  def test_rejects_invalid_sort_key
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$sort" => { "bad key!" => 1 } }
      ])
    end
  end

  def test_rejects_invalid_field_name_in_group_id
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$group" => { "_id" => "$bad field!", "total" => { "$sum" => "$amount" } } }
      ])
    end
  end

  def test_rejects_invalid_alias_in_group
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$group" => { "_id" => "$region", "bad alias!" => { "$sum" => "$amount" } } }
      ])
    end
  end

  def test_min_max_accumulators
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => {
        "_id" => "$category",
        "lo" => { "$min" => "$price" },
        "hi" => { "$max" => "$price" }
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "MIN((data->>'price')::numeric)::numeric AS lo"
    assert_includes call[:sql], "MAX((data->>'price')::numeric)::numeric AS hi"
  end

  # --- composite _id ---

  def test_composite_id_json_build_object
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => {
        "_id" => { "cat" => "$category", "region" => "$region" },
        "total" => { "$sum" => "$amount" }
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "json_build_object('cat', data->>'category', 'region', data->>'region') AS _id"
    assert_includes call[:sql], "SUM((data->>'amount')::numeric)::numeric AS total"
    assert_includes call[:sql], "GROUP BY data->>'category', data->>'region'"
  end

  def test_composite_id_with_match_and_sort
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$match" => { "status" => "complete" } },
      { "$group" => {
        "_id" => { "year" => "$year", "month" => "$month" },
        "revenue" => { "$sum" => "$amount" }
      }},
      { "$sort" => { "revenue" => -1 } },
      { "$limit" => 5 }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "json_build_object('year', data->>'year', 'month', data->>'month') AS _id"
    assert_includes call[:sql], "GROUP BY data->>'year', data->>'month'"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "ORDER BY revenue DESC"
    assert_includes call[:sql], "LIMIT $2"
    assert_equal [JSON.generate({ "status" => "complete" }), 5], call[:params]
  end

  def test_composite_id_rejects_invalid_label
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$group" => {
          "_id" => { "bad label!" => "$category" },
          "total" => { "$sum" => "$amount" }
        }}
      ])
    end
  end

  def test_composite_id_rejects_invalid_field_ref
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$group" => {
          "_id" => { "cat" => "$bad field!" },
          "total" => { "$sum" => "$amount" }
        }}
      ])
    end
  end

  # --- $push / $addToSet accumulators ---

  def test_push_accumulator
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => {
        "_id" => "$category",
        "names" => { "$push" => "$name" }
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "array_agg(data->>'name') AS names"
    assert_includes call[:sql], "GROUP BY data->>'category'"
  end

  def test_add_to_set_accumulator
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => {
        "_id" => "$category",
        "unique_regions" => { "$addToSet" => "$region" }
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "array_agg(DISTINCT data->>'region') AS unique_regions"
    assert_includes call[:sql], "GROUP BY data->>'category'"
  end

  def test_push_with_composite_id
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$group" => {
        "_id" => { "cat" => "$category", "region" => "$region" },
        "items" => { "$push" => "$name" },
        "total" => { "$sum" => "$amount" }
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "json_build_object('cat', data->>'category', 'region', data->>'region') AS _id"
    assert_includes call[:sql], "array_agg(data->>'name') AS items"
    assert_includes call[:sql], "SUM((data->>'amount')::numeric)::numeric AS total"
    assert_includes call[:sql], "GROUP BY data->>'category', data->>'region'"
  end

  def test_push_rejects_invalid_field
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$group" => {
          "_id" => "$category",
          "items" => { "$push" => "$bad field!" }
        }}
      ])
    end
  end

  # --- $project ---

  def test_project_include_fields
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$project" => { "name" => 1, "status" => 1 } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "id AS _id"
    assert_includes call[:sql], "data->>'name' AS name"
    assert_includes call[:sql], "data->>'status' AS status"
  end

  def test_project_exclude_id
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$project" => { "_id" => 0, "name" => 1, "price" => 1 } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    refute_includes call[:sql], "id AS _id"
    assert_includes call[:sql], "data->>'name' AS name"
    assert_includes call[:sql], "data->>'price' AS price"
  end

  def test_project_rename_via_field_ref
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$project" => { "_id" => 0, "customer_name" => "$name", "total" => "$amount" } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "data->>'name' AS customer_name"
    assert_includes call[:sql], "data->>'amount' AS total"
    refute_includes call[:sql], "id AS _id"
  end

  def test_project_dot_notation
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$project" => { "_id" => 0, "city" => "$address.city" } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "data->'address'->>'city' AS city"
  end

  def test_project_rejects_invalid_field
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$project" => { "bad field!" => 1 } }
      ])
    end
  end

  # --- $unwind ---

  def test_unwind_string_syntax
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$unwind" => "$tags" }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "CROSS JOIN jsonb_array_elements_text(data->'tags') AS _uw_tags"
  end

  def test_unwind_hash_syntax
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$unwind" => { "path" => "$items" } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "CROSS JOIN jsonb_array_elements_text(data->'items') AS _uw_items"
  end

  def test_unwind_with_group
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$unwind" => "$tags" },
      { "$group" => { "_id" => "$tags", "cnt" => { "$count" => true } } }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "_uw_tags AS _id"
    assert_includes call[:sql], "GROUP BY _uw_tags"
    assert_includes call[:sql], "CROSS JOIN jsonb_array_elements_text(data->'tags') AS _uw_tags"
  end

  def test_unwind_rejects_invalid_field
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$unwind" => "$bad field!" }
      ])
    end
  end

  # --- $lookup ---

  def test_lookup_correlated_subquery
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$lookup" => {
        "from" => "users",
        "localField" => "user_id",
        "foreignField" => "uid",
        "as" => "user_docs"
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "COALESCE((SELECT json_agg(users.data) FROM users WHERE users.data->>'uid' = data->>'user_id'), '[]'::json) AS user_docs"
  end

  def test_lookup_rejects_invalid_identifiers
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_aggregate(mock, "orders", [
        { "$lookup" => {
          "from" => "bad table!",
          "localField" => "user_id",
          "foreignField" => "uid",
          "as" => "user_docs"
        }}
      ])
    end
  end

  def test_lookup_with_match
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$match" => { "status" => "active" } },
      { "$lookup" => {
        "from" => "products",
        "localField" => "product_id",
        "foreignField" => "pid",
        "as" => "product_info"
      }}
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "COALESCE((SELECT json_agg(products.data) FROM products WHERE products.data->>'pid' = data->>'product_id'), '[]'::json) AS product_info"
    assert_equal [JSON.generate({ "status" => "active" })], call[:params]
  end

  def test_unwind_group_sort_pipeline
    mock = DocMockConnection.new
    GoldLapel.doc_aggregate(mock, "orders", [
      { "$match" => { "status" => "complete" } },
      { "$unwind" => "$items" },
      { "$group" => {
        "_id" => "$items",
        "total" => { "$sum" => "$amount" },
        "cnt" => { "$count" => true }
      }},
      { "$sort" => { "total" => -1 } },
      { "$limit" => 5 }
    ])

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("SELECT") }
    refute_nil call
    assert_includes call[:sql], "_uw_items AS _id"
    assert_includes call[:sql], "CROSS JOIN jsonb_array_elements_text(data->'items') AS _uw_items"
    assert_includes call[:sql], "GROUP BY _uw_items"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "ORDER BY total DESC"
    assert_includes call[:sql], "LIMIT $2"
    assert_equal [JSON.generate({ "status" => "complete" }), 5], call[:params]
  end
end

# --- Logical operators ($or, $and, $not) ---

class TestLogicalOperators < Minitest::Test
  def test_or_operator
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$or" => [
        { "status" => "active" },
        { "role" => "admin" }
      ]
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE (data @> $1::jsonb OR data @> $2::jsonb)"
    assert_equal [
      JSON.generate({ "status" => "active" }),
      JSON.generate({ "role" => "admin" })
    ], call[:params]
  end

  def test_and_operator
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$and" => [
        { "age" => { "$gte" => 18 } },
        { "age" => { "$lte" => 65 } }
      ]
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE ((data->>'age')::numeric >= $1 AND (data->>'age')::numeric <= $2)"
    assert_equal [18, 65], call[:params]
  end

  def test_not_operator
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$not" => { "status" => "banned" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "WHERE NOT (data @> $1::jsonb)"
    assert_equal [JSON.generate({ "status" => "banned" })], call[:params]
  end

  def test_or_with_operators
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "products", filter: {
      "$or" => [
        { "price" => { "$lt" => 10 } },
        { "price" => { "$gt" => 1000 } }
      ]
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "((data->>'price')::numeric < $1 OR (data->>'price')::numeric > $2)"
    assert_equal [10, 1000], call[:params]
  end

  def test_or_rejects_non_array
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: { "$or" => { "a" => 1 } })
    end
  end

  def test_or_rejects_empty_array
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: { "$or" => [] })
    end
  end

  def test_and_rejects_non_array
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: { "$and" => "not an array" })
    end
  end

  def test_not_rejects_non_hash
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: { "$not" => [{ "a" => 1 }] })
    end
  end

  def test_nested_or_and
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$or" => [
        { "$and" => [
          { "status" => "active" },
          { "age" => { "$gte" => 18 } }
        ]},
        { "role" => "admin" }
      ]
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "((data @> $1::jsonb AND (data->>'age')::numeric >= $2) OR data @> $3::jsonb)"
    assert_equal [
      JSON.generate({ "status" => "active" }),
      18,
      JSON.generate({ "role" => "admin" })
    ], call[:params]
  end

  def test_not_with_operators
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$not" => { "age" => { "$lt" => 18 } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "NOT ((data->>'age')::numeric < $1)"
    assert_equal [18], call[:params]
  end

  def test_logical_with_regular_filter
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "status" => "active",
      "$or" => [
        { "role" => "admin" },
        { "role" => "superadmin" }
      ]
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    # The containment and $or both appear
    assert_includes call[:sql], "(data @> $1::jsonb OR data @> $2::jsonb)"
    assert_includes call[:sql], "data @> $3::jsonb"
  end
end

# --- Field update operators ($set, $inc, $unset, $mul, $rename) ---

class TestUpdateOperators < Minitest::Test
  def test_set_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$set" => { "status" => "active", "role" => "admin" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "SET data = (data || $2::jsonb)"
    assert_equal JSON.generate({ "name" => "Alice" }), call[:params][0]
    assert_equal JSON.generate({ "status" => "active", "role" => "admin" }), call[:params][1]
  end

  def test_inc_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "counters", { "name" => "views" }, {
      "$inc" => { "count" => 1 }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set(data, $2::text[], to_jsonb(COALESCE((data->>'count')::numeric, 0) + $3))"
    assert_equal "{count}", call[:params][1]
    assert_equal 1, call[:params][2]
  end

  def test_unset_top_level
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$unset" => { "temp_field" => "" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "(data - $2)"
    assert_equal "temp_field", call[:params][1]
  end

  def test_unset_nested
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$unset" => { "addr.zip" => "" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "(data #- $2::text[])"
    assert_equal "{addr,zip}", call[:params][1]
  end

  def test_mul_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "products", { "sku" => "A1" }, {
      "$mul" => { "price" => 1.1 }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set(data, $2::text[], to_jsonb(COALESCE((data->>'price')::numeric, 0) * $3))"
    assert_equal "{price}", call[:params][1]
    assert_in_delta 1.1, call[:params][2], 0.001
  end

  def test_rename_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$rename" => { "old_field" => "new_field" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set((data - $2), $3::text[], data->'old_field')"
    assert_equal "old_field", call[:params][1]
    assert_equal "{new_field}", call[:params][2]
  end

  def test_rename_nested_source
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$rename" => { "addr.zip" => "postal" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set((data #- $2::text[]), $3::text[], data->'addr'->'zip')"
    assert_equal "{addr,zip}", call[:params][1]
    assert_equal "{postal}", call[:params][2]
  end

  def test_plain_update_still_works
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, { "name" => "Bob" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "SET data = data || $2::jsonb"
    assert_equal JSON.generate({ "name" => "Bob" }), call[:params][1]
  end

  def test_combined_set_and_inc
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", {}, {
      "$set" => { "status" => "active" },
      "$inc" => { "login_count" => 1 }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "(data || $1::jsonb)"
    assert_includes call[:sql], "jsonb_set("
    assert_includes call[:sql], "to_jsonb(COALESCE((data->>'login_count')::numeric, 0) + $3)"
    assert_equal JSON.generate({ "status" => "active" }), call[:params][0]
    assert_equal "{login_count}", call[:params][1]
    assert_equal 1, call[:params][2]
  end

  def test_update_one_with_set
    mock = DocMockConnection.new
    GoldLapel.doc_update_one(mock, "users", { "name" => "Alice" }, {
      "$set" => { "age" => 31 }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "WITH target AS ("
    assert_includes call[:sql], "SET data = (data || $2::jsonb)"
    assert_includes call[:sql], "FROM target WHERE users.id = target.id"
    assert_equal JSON.generate({ "name" => "Alice" }), call[:params][0]
    assert_equal JSON.generate({ "age" => 31 }), call[:params][1]
  end
end

# --- Array update operators ($push, $pull, $addToSet) ---

class TestArrayUpdateOperators < Minitest::Test
  def test_push_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$push" => { "tags" => "ruby" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set(data, $2::text[], COALESCE(data->'tags', '[]'::jsonb) || to_jsonb($3::text))"
    assert_equal "{tags}", call[:params][1]
    assert_equal "ruby", call[:params][2]
  end

  def test_push_numeric
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "docs", {}, {
      "$push" => { "scores" => 95 }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "COALESCE(data->'scores', '[]'::jsonb) || to_jsonb($2::numeric)"
    assert_equal "{scores}", call[:params][0]
    assert_equal 95, call[:params][1]
  end

  def test_pull_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$pull" => { "tags" => "old_tag" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set(data, $2::text[],"
    assert_includes call[:sql], "SELECT jsonb_agg(elem) FROM jsonb_array_elements(data->'tags') AS elem"
    assert_includes call[:sql], "WHERE elem != to_jsonb($3::text)"
    assert_equal "{tags}", call[:params][1]
    assert_equal "old_tag", call[:params][2]
  end

  def test_add_to_set_operator
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "name" => "Alice" }, {
      "$addToSet" => { "tags" => "unique" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "jsonb_set(data, $2::text[],"
    assert_includes call[:sql], "CASE WHEN COALESCE(data->'tags', '[]'::jsonb) @> to_jsonb($3::text)"
    assert_includes call[:sql], "ELSE COALESCE(data->'tags', '[]'::jsonb) || to_jsonb($4::text) END)"
    assert_equal "{tags}", call[:params][1]
    assert_equal "unique", call[:params][2]
    assert_equal "unique", call[:params][3]
  end

  def test_push_with_filter_params
    mock = DocMockConnection.new
    GoldLapel.doc_update(mock, "users", { "status" => "active" }, {
      "$push" => { "log" => "event1" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    # Filter param is $1, push path is $2, push value is $3
    assert_equal JSON.generate({ "status" => "active" }), call[:params][0]
    assert_equal "{log}", call[:params][1]
    assert_equal "event1", call[:params][2]
  end
end

# --- doc_find_one_and_update ---

class TestDocFindOneAndUpdate < Minitest::Test
  def test_returns_updated_doc
    update_result = DocMockResult.new(
      [{ "id" => "1", "data" => '{"name":"Alice","age":31}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("UPDATE" => update_result)
    result = GoldLapel.doc_find_one_and_update(mock, "users", { "name" => "Alice" }, { "age" => 31 })

    refute_nil result
    assert_equal 1, result["id"]
    assert_equal({ "name" => "Alice", "age" => 31 }, result["data"])
  end

  def test_sql_structure
    mock = DocMockConnection.new
    GoldLapel.doc_find_one_and_update(mock, "users", { "name" => "Alice" }, { "age" => 31 })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "WITH target AS ("
    assert_includes call[:sql], "SELECT id FROM users"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "LIMIT 1"
    assert_includes call[:sql], "SET data = data || $2::jsonb"
    assert_includes call[:sql], "FROM target WHERE users.id = target.id"
    assert_includes call[:sql], "RETURNING users.id, users.data, users.created_at"
    assert_equal [JSON.generate({ "name" => "Alice" }), JSON.generate({ "age" => 31 })], call[:params]
  end

  def test_returns_nil_when_no_match
    mock = DocMockConnection.new
    result = GoldLapel.doc_find_one_and_update(mock, "users", { "name" => "nobody" }, { "age" => 99 })

    assert_nil result
  end

  def test_with_set_operator
    mock = DocMockConnection.new
    GoldLapel.doc_find_one_and_update(mock, "users", { "name" => "Alice" }, {
      "$set" => { "verified" => true }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("UPDATE") }
    refute_nil call
    assert_includes call[:sql], "SET data = (data || $2::jsonb)"
    assert_includes call[:sql], "RETURNING users.id, users.data, users.created_at"
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_find_one_and_update(mock, "bad table!", {}, {}) }
  end
end

# --- doc_find_one_and_delete ---

class TestDocFindOneAndDelete < Minitest::Test
  def test_returns_deleted_doc
    delete_result = DocMockResult.new(
      [{ "id" => "1", "data" => '{"name":"Alice","age":30}', "created_at" => "2026-04-07 00:00:00+00" }],
      ["id", "data", "created_at"]
    )
    mock = DocMockConnection.new("DELETE" => delete_result)
    result = GoldLapel.doc_find_one_and_delete(mock, "users", { "name" => "Alice" })

    refute_nil result
    assert_equal 1, result["id"]
    assert_equal({ "name" => "Alice", "age" => 30 }, result["data"])
  end

  def test_sql_structure
    mock = DocMockConnection.new
    GoldLapel.doc_find_one_and_delete(mock, "users", { "name" => "Alice" })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DELETE") }
    refute_nil call
    assert_includes call[:sql], "WITH target AS ("
    assert_includes call[:sql], "SELECT id FROM users"
    assert_includes call[:sql], "WHERE data @> $1::jsonb"
    assert_includes call[:sql], "LIMIT 1"
    assert_includes call[:sql], "DELETE FROM users"
    assert_includes call[:sql], "USING target WHERE users.id = target.id"
    assert_includes call[:sql], "RETURNING users.id, users.data, users.created_at"
    assert_equal [JSON.generate({ "name" => "Alice" })], call[:params]
  end

  def test_returns_nil_when_no_match
    mock = DocMockConnection.new
    result = GoldLapel.doc_find_one_and_delete(mock, "users", { "name" => "nobody" })

    assert_nil result
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_find_one_and_delete(mock, "bad table!", {}) }
  end
end

# --- doc_distinct ---

class TestDocDistinct < Minitest::Test
  def test_returns_distinct_values
    distinct_result = DocMockResult.new(
      [{ "val" => "active" }, { "val" => "inactive" }, { "val" => "banned" }],
      ["val"]
    )
    mock = DocMockConnection.new("DISTINCT" => distinct_result)
    result = GoldLapel.doc_distinct(mock, "users", "status")

    assert_equal ["active", "inactive", "banned"], result
  end

  def test_sql_without_filter
    mock = DocMockConnection.new
    GoldLapel.doc_distinct(mock, "users", "status")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "SELECT DISTINCT data->>'status' AS val FROM users"
    assert_includes call[:sql], "WHERE data->>'status' IS NOT NULL"
    assert_equal [], call[:params]
  end

  def test_sql_with_filter
    mock = DocMockConnection.new
    GoldLapel.doc_distinct(mock, "users", "role", filter: { "status" => "active" })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "SELECT DISTINCT data->>'role' AS val FROM users"
    assert_includes call[:sql], "data->>'role' IS NOT NULL"
    assert_includes call[:sql], "data @> $1::jsonb"
    assert_equal [JSON.generate({ "status" => "active" })], call[:params]
  end

  def test_nested_field
    mock = DocMockConnection.new
    GoldLapel.doc_distinct(mock, "users", "addr.city")

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "data->'addr'->>'city' AS val"
    assert_includes call[:sql], "data->'addr'->>'city' IS NOT NULL"
  end

  def test_rejects_invalid_collection
    mock = DocMockConnection.new
    assert_raises(ArgumentError) { GoldLapel.doc_distinct(mock, "bad table!", "field") }
  end

  def test_returns_empty_array_when_no_results
    mock = DocMockConnection.new
    result = GoldLapel.doc_distinct(mock, "users", "status")

    assert_equal [], result
  end
end

# --- $elemMatch ---

class TestElemMatch < Minitest::Test
  def test_numeric_range
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "scores" => { "$elemMatch" => { "$gt" => 80, "$lt" => 90 } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "EXISTS"
    assert_includes call[:sql], "jsonb_array_elements"
    assert_includes call[:sql], "elem#>>'{}'"
    assert_includes call[:sql], "::numeric"
    assert_includes call[:params], 80
    assert_includes call[:params], 90
  end

  def test_string_regex
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "tags" => { "$elemMatch" => { "$regex" => "^py" } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "EXISTS"
    assert_includes call[:sql], "elem#>>'{}' ~ $1"
    assert_equal ["^py"], call[:params]
  end

  def test_single_condition
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "scores" => { "$elemMatch" => { "$eq" => 100 } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "EXISTS"
    assert_includes call[:sql], "elem#>>'{}'"
    assert_equal [100], call[:params]
  end

  def test_invalid_operand_raises
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: {
        "scores" => { "$elemMatch" => [1, 2] }
      })
    end
  end

  def test_unsupported_sub_op_raises
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: {
        "scores" => { "$elemMatch" => { "$foo" => 1 } }
      })
    end
  end

  def test_elem_match_uses_field_path_json
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "results.scores" => { "$elemMatch" => { "$gt" => 50 } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "jsonb_array_elements(data->'results'->'scores')"
  end

  def test_elem_match_string_comparison
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "names" => { "$elemMatch" => { "$eq" => "Alice" } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "elem#>>'{}' = $1"
    assert_equal ["Alice"], call[:params]
  end
end

# --- $text in filters ---

class TestTextFilter < Minitest::Test
  def test_top_level
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$text" => { "$search" => "hello world" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "to_tsvector"
    assert_includes call[:sql], "plainto_tsquery"
    assert_includes call[:sql], "data::text"
    assert_equal ["english", "english", "hello world"], call[:params]
  end

  def test_field_level
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "content" => { "$text" => { "$search" => "hello" } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "to_tsvector"
    assert_includes call[:sql], "plainto_tsquery"
    assert_includes call[:sql], "data->>'content'"
    assert_equal ["english", "english", "hello"], call[:params]
  end

  def test_custom_language
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$text" => { "$search" => "bonjour", "$language" => "french" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "to_tsvector"
    assert_equal ["french", "french", "bonjour"], call[:params]
  end

  def test_missing_search_raises
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: {
        "$text" => { "$language" => "english" }
      })
    end
  end

  def test_non_dict_raises
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: {
        "$text" => "hello"
      })
    end
  end

  def test_field_level_missing_search_raises
    mock = DocMockConnection.new
    assert_raises(ArgumentError) do
      GoldLapel.doc_find(mock, "users", filter: {
        "content" => { "$text" => { "$language" => "english" } }
      })
    end
  end

  def test_in_doc_count
    count_result = DocMockResult.new(
      [{ "count" => "3" }],
      ["count"]
    )
    mock = DocMockConnection.new("COUNT" => count_result)
    GoldLapel.doc_count(mock, "users", filter: {
      "bio" => { "$text" => { "$search" => "python" } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("COUNT") }
    refute_nil call
    assert_includes call[:sql], "to_tsvector"
    assert_includes call[:sql], "@@"
  end

  def test_top_level_param_indices
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "$text" => { "$search" => "search" }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "to_tsvector($1, data::text) @@ plainto_tsquery($2, $3)"
  end

  def test_field_level_param_indices
    mock = DocMockConnection.new
    GoldLapel.doc_find(mock, "users", filter: {
      "bio" => { "$text" => { "$search" => "search" } }
    })

    call = mock.calls.find { |c| c[:method] == :exec_params }
    refute_nil call
    assert_includes call[:sql], "to_tsvector($1, data->>'bio') @@ plainto_tsquery($2, $3)"
  end
end

# --- doc_find_cursor ---

class CursorMockResult
  attr_reader :values, :fields

  def initialize(rows)
    @rows = rows
  end

  def ntuples; @rows.length; end

  def [](index)
    @rows[index]
  end

  def each(&block)
    @rows.each(&block)
  end
end

class CursorMockConnection
  attr_reader :calls

  def initialize(fetch_batches: [])
    @calls = []
    @fetch_batches = fetch_batches
    @fetch_index = 0
  end

  def exec(sql, &block)
    @calls << { method: :exec, sql: sql }
    if sql.include?("FETCH")
      batch = @fetch_index < @fetch_batches.length ? @fetch_batches[@fetch_index] : []
      @fetch_index += 1
      result = CursorMockResult.new(batch)
      block&.call(result)
      result
    else
      result = CursorMockResult.new([])
      block&.call(result)
      result
    end
  end

  def exec_params(sql, params = [], result_format = 0, &block)
    @calls << { method: :exec_params, sql: sql, params: params }
    result = CursorMockResult.new([])
    block&.call(result)
    result
  end

  def close; end
  def finished?; false; end
end

class TestDocFindCursor < Minitest::Test
  def test_returns_enumerator
    mock = CursorMockConnection.new(fetch_batches: [])
    result = GoldLapel.doc_find_cursor(mock, "users")

    assert_kind_of Enumerator, result
  end

  def test_yields_rows
    rows = [
      { "id" => "1", "data" => '{"name":"Alice"}', "created_at" => "2026-04-07" },
      { "id" => "2", "data" => '{"name":"Bob"}', "created_at" => "2026-04-07" }
    ]
    mock = CursorMockConnection.new(fetch_batches: [rows, []])
    results = GoldLapel.doc_find_cursor(mock, "users").to_a

    assert_equal 2, results.length
    assert_equal "1", results[0]["id"]
    assert_equal '{"name":"Alice"}', results[0]["data"]
  end

  def test_multiple_batches
    batch1 = [{ "id" => "1", "data" => '{"a":1}', "created_at" => "ts" }]
    batch2 = [{ "id" => "2", "data" => '{"b":2}', "created_at" => "ts" }]
    mock = CursorMockConnection.new(fetch_batches: [batch1, batch2, []])
    results = GoldLapel.doc_find_cursor(mock, "users").to_a

    assert_equal 2, results.length
  end

  def test_with_filter
    mock = CursorMockConnection.new(fetch_batches: [])
    GoldLapel.doc_find_cursor(mock, "users", filter: { "status" => "active" }).to_a

    declare_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DECLARE") }
    refute_nil declare_call
    assert_includes declare_call[:sql], "WHERE"
  end

  def test_declare_and_close
    mock = CursorMockConnection.new(fetch_batches: [])
    GoldLapel.doc_find_cursor(mock, "users").to_a

    begin_call = mock.calls.find { |c| c[:method] == :exec && c[:sql] == "BEGIN" }
    refute_nil begin_call

    declare_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DECLARE") }
    refute_nil declare_call
    assert_includes declare_call[:sql], "CURSOR FOR SELECT id, data, created_at FROM users"

    close_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("CLOSE") }
    refute_nil close_call

    commit_call = mock.calls.find { |c| c[:method] == :exec && c[:sql] == "COMMIT" }
    refute_nil commit_call
  end

  def test_batch_size
    mock = CursorMockConnection.new(fetch_batches: [])
    GoldLapel.doc_find_cursor(mock, "users", batch_size: 50).to_a

    fetch_call = mock.calls.find { |c| c[:method] == :exec && c[:sql].include?("FETCH") }
    refute_nil fetch_call
    assert_includes fetch_call[:sql], "FETCH 50"
  end

  def test_rejects_invalid_collection
    mock = CursorMockConnection.new(fetch_batches: [])
    assert_raises(ArgumentError) do
      GoldLapel.doc_find_cursor(mock, "bad table!")
    end
  end

  def test_cursor_name_contains_collection
    mock = CursorMockConnection.new(fetch_batches: [])
    GoldLapel.doc_find_cursor(mock, "orders").to_a

    declare_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DECLARE") }
    refute_nil declare_call
    assert_includes declare_call[:sql], "gl_cursor_orders_"
  end

  def test_with_sort_limit_skip
    mock = CursorMockConnection.new(fetch_batches: [])
    GoldLapel.doc_find_cursor(mock, "users", sort: { "name" => 1 }, limit: 10, skip: 5).to_a

    declare_call = mock.calls.find { |c| c[:method] == :exec_params && c[:sql].include?("DECLARE") }
    refute_nil declare_call
    assert_includes declare_call[:sql], "ORDER BY data->>'name' ASC"
    assert_includes declare_call[:sql], "LIMIT"
    assert_includes declare_call[:sql], "OFFSET"
  end

  def test_cleanup_on_early_break
    rows = [
      { "id" => "1", "data" => '{"a":1}', "created_at" => "ts" },
      { "id" => "2", "data" => '{"b":2}', "created_at" => "ts" }
    ]
    # Provide extra batches that won't be consumed
    mock = CursorMockConnection.new(fetch_batches: [rows, rows, []])
    enum = GoldLapel.doc_find_cursor(mock, "users")
    # Only take the first row
    first = enum.next

    refute_nil first
    assert_equal "1", first["id"]
  end
end
