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
        { "$lookup" => { "from" => "users" } }
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
end
