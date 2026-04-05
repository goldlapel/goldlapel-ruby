# frozen_string_literal: true

require "json"

module GoldLapel
  # Redis-compatible convenience methods backed by PostgreSQL.
  #
  # These methods provide a Redis-like API using native PostgreSQL features.
  # No Redis server needed — everything runs through your existing Postgres connection.
  #
  # Usage:
  #   conn = GoldLapel.start("postgresql://localhost/mydb")
  #
  #   # Pub/sub
  #   GoldLapel.publish(conn, "orders", "new order received")
  #   GoldLapel.subscribe(conn, "orders") { |channel, payload| puts payload }
  #
  #   # Queues
  #   GoldLapel.enqueue(conn, "jobs", { task: "send_email", to: "user@example.com" })
  #   job = GoldLapel.dequeue(conn, "jobs")
  #
  #   # Counters
  #   GoldLapel.incr(conn, "page_views", "home")

  def self.publish(conn, channel, message)
    raw = _raw_conn(conn)
    raw.exec_params("SELECT pg_notify($1, $2)", [channel, message.to_s])
  end

  def self.subscribe(conn, channel, &block)
    raw = _raw_conn(conn)
    listen_conn = PG.connect(raw.conninfo_hash)
    listen_conn.exec("LISTEN #{channel}")
    loop do
      listen_conn.wait_for_notify(5) do |ch, _pid, payload|
        block.call(ch, payload)
      end
    end
  end

  def self.enqueue(conn, queue_table, payload)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{queue_table} (" \
             "id BIGSERIAL PRIMARY KEY, " \
             "payload JSONB NOT NULL, " \
             "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
    raw.exec_params(
      "INSERT INTO #{queue_table} (payload) VALUES ($1)",
      [JSON.generate(payload)]
    )
  end

  def self.dequeue(conn, queue_table)
    raw = _raw_conn(conn)
    result = raw.exec("DELETE FROM #{queue_table} " \
                      "WHERE id = (" \
                        "SELECT id FROM #{queue_table} " \
                        "ORDER BY id " \
                        "FOR UPDATE SKIP LOCKED " \
                        "LIMIT 1" \
                      ") RETURNING payload")
    return nil if result.ntuples.zero?
    JSON.parse(result[0]["payload"])
  end

  def self.incr(conn, table, key, amount: 1)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{table} (" \
             "key TEXT PRIMARY KEY, " \
             "value BIGINT NOT NULL DEFAULT 0)")
    result = raw.exec_params(
      "INSERT INTO #{table} (key, value) VALUES ($1, $2) " \
      "ON CONFLICT (key) DO UPDATE SET value = #{table}.value + $3 " \
      "RETURNING value",
      [key, amount, amount]
    )
    result[0]["value"].to_i
  end

  def self.get_counter(conn, table, key)
    raw = _raw_conn(conn)
    result = raw.exec_params("SELECT value FROM #{table} WHERE key = $1", [key])
    return 0 if result.ntuples.zero?
    result[0]["value"].to_i
  end

  def self.count_distinct(conn, table, column)
    raw = _raw_conn(conn)
    result = raw.exec("SELECT COUNT(DISTINCT #{column}) FROM #{table}")
    result[0]["count"].to_i
  end

  def self.zadd(conn, table, member, score)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{table} (" \
             "member TEXT PRIMARY KEY, " \
             "score DOUBLE PRECISION NOT NULL)")
    raw.exec_params(
      "INSERT INTO #{table} (member, score) VALUES ($1, $2) " \
      "ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score",
      [member.to_s, score.to_f]
    )
  end

  def self.zincrby(conn, table, member, amount: 1)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{table} (" \
             "member TEXT PRIMARY KEY, " \
             "score DOUBLE PRECISION NOT NULL)")
    result = raw.exec_params(
      "INSERT INTO #{table} (member, score) VALUES ($1, $2) " \
      "ON CONFLICT (member) DO UPDATE SET score = #{table}.score + $3 " \
      "RETURNING score",
      [member.to_s, amount.to_f, amount.to_f]
    )
    result[0]["score"].to_f
  end

  def self.zrange(conn, table, start: 0, stop: 10, desc: true)
    raw = _raw_conn(conn)
    order = desc ? "DESC" : "ASC"
    limit = stop - start
    result = raw.exec_params(
      "SELECT member, score FROM #{table} " \
      "ORDER BY score #{order} " \
      "LIMIT $1 OFFSET $2",
      [limit, start]
    )
    result.map { |row| [row["member"], row["score"].to_f] }
  end

  def self.zrank(conn, table, member, desc: true)
    raw = _raw_conn(conn)
    order = desc ? "DESC" : "ASC"
    result = raw.exec_params(
      "SELECT rank FROM (" \
        "SELECT member, ROW_NUMBER() OVER (ORDER BY score #{order}) - 1 AS rank " \
        "FROM #{table}" \
      ") ranked WHERE member = $1",
      [member.to_s]
    )
    return nil if result.ntuples.zero?
    result[0]["rank"].to_i
  end

  def self.zscore(conn, table, member)
    raw = _raw_conn(conn)
    result = raw.exec_params("SELECT score FROM #{table} WHERE member = $1", [member.to_s])
    return nil if result.ntuples.zero?
    result[0]["score"].to_f
  end

  def self.zrem(conn, table, member)
    raw = _raw_conn(conn)
    result = raw.exec_params("DELETE FROM #{table} WHERE member = $1", [member.to_s])
    result.cmd_tuples > 0
  end

  def self.geoadd(conn, table, name_column, geom_column, name, lon, lat)
    raw = _raw_conn(conn)
    raw.exec("CREATE EXTENSION IF NOT EXISTS postgis")
    raw.exec("CREATE TABLE IF NOT EXISTS #{table} (" \
             "id BIGSERIAL PRIMARY KEY, " \
             "#{name_column} TEXT NOT NULL, " \
             "#{geom_column} GEOMETRY(Point, 4326) NOT NULL)")
    raw.exec_params(
      "INSERT INTO #{table} (#{name_column}, #{geom_column}) " \
      "VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326))",
      [name, lon.to_f, lat.to_f]
    )
  end

  def self.georadius(conn, table, geom_column, lon, lat, radius_meters, limit: 50)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT *, ST_Distance(" \
        "#{geom_column}::geography, " \
        "ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography" \
      ") AS distance_m " \
      "FROM #{table} " \
      "WHERE ST_DWithin(" \
        "#{geom_column}::geography, " \
        "ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, " \
        "$5" \
      ") ORDER BY distance_m LIMIT $6",
      [lon.to_f, lat.to_f, lon.to_f, lat.to_f, radius_meters.to_f, limit]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.geodist(conn, table, geom_column, name_column, name_a, name_b)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT ST_Distance(a.#{geom_column}::geography, b.#{geom_column}::geography) " \
      "FROM #{table} a, #{table} b " \
      "WHERE a.#{name_column} = $1 AND b.#{name_column} = $2",
      [name_a, name_b]
    )
    return nil if result.ntuples.zero?
    result[0]["st_distance"].to_f
  end

  def self.hset(conn, table, key, field, value)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{table} (" \
             "key TEXT PRIMARY KEY, " \
             "data JSONB NOT NULL DEFAULT '{}'::jsonb)")
    raw.exec_params(
      "INSERT INTO #{table} (key, data) VALUES ($1, jsonb_build_object($2, $3::jsonb)) " \
      "ON CONFLICT (key) DO UPDATE SET data = #{table}.data || jsonb_build_object($4, $5::jsonb)",
      [key, field, JSON.generate(value), field, JSON.generate(value)]
    )
  end

  def self.hget(conn, table, key, field)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT data->>$1 FROM #{table} WHERE key = $2",
      [field, key]
    )
    return nil if result.ntuples.zero?
    val = result[0].values[0]
    return nil if val.nil?
    begin
      JSON.parse(val)
    rescue JSON::ParserError
      val
    end
  end

  def self.hgetall(conn, table, key)
    raw = _raw_conn(conn)
    result = raw.exec_params("SELECT data FROM #{table} WHERE key = $1", [key])
    return {} if result.ntuples.zero?
    val = result[0]["data"]
    return {} if val.nil?
    val.is_a?(Hash) ? val : JSON.parse(val)
  end

  def self.hdel(conn, table, key, field)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT data ? $1 AS existed FROM #{table} WHERE key = $2",
      [field, key]
    )
    return false if result.ntuples.zero? || result[0]["existed"] != "t"
    raw.exec_params(
      "UPDATE #{table} SET data = data - $1 WHERE key = $2",
      [field, key]
    )
    true
  end

  def self.script(conn, lua_code, *args)
    raw = _raw_conn(conn)
    raw.exec("CREATE EXTENSION IF NOT EXISTS pllua")
    func_name = "_gl_lua_#{rand(16**8).to_s(16)}"
    params = args.each_with_index.map { |_, i| "p#{i + 1} text" }.join(", ")
    raw.exec("CREATE OR REPLACE FUNCTION pg_temp.#{func_name}(#{params}) " \
             "RETURNS text LANGUAGE pllua AS $pllua$ #{lua_code} $pllua$")
    if args.empty?
      result = raw.exec("SELECT pg_temp.#{func_name}()")
    else
      placeholders = args.each_with_index.map { |_, i| "$#{i + 1}" }.join(", ")
      result = raw.exec_params(
        "SELECT pg_temp.#{func_name}(#{placeholders})",
        args.map(&:to_s)
      )
    end
    result.ntuples > 0 ? result[0][func_name] : nil
  end

  def self.stream_add(conn, stream, payload)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{stream} (" \
             "id BIGSERIAL PRIMARY KEY, " \
             "payload JSONB NOT NULL, " \
             "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
    result = raw.exec_params(
      "INSERT INTO #{stream} (payload) VALUES ($1) RETURNING id, payload, created_at",
      [JSON.generate(payload)]
    )
    row = result[0]
    { "id" => row["id"].to_i, "payload" => JSON.parse(row["payload"]), "created_at" => row["created_at"] }
  end

  def self.stream_create_group(conn, stream, group)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{stream}_groups (" \
             "group_name TEXT PRIMARY KEY, " \
             "last_id BIGINT NOT NULL DEFAULT 0, " \
             "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
    raw.exec("CREATE TABLE IF NOT EXISTS #{stream}_pending (" \
             "id BIGSERIAL PRIMARY KEY, " \
             "group_name TEXT NOT NULL, " \
             "consumer TEXT NOT NULL, " \
             "message_id BIGINT NOT NULL, " \
             "claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
    raw.exec_params(
      "INSERT INTO #{stream}_groups (group_name) VALUES ($1) " \
      "ON CONFLICT (group_name) DO NOTHING",
      [group]
    )
  end

  def self.stream_read(conn, stream, group, consumer, count: 1)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "WITH next AS (" \
        "SELECT id, payload, created_at FROM #{stream} " \
        "WHERE id > (SELECT last_id FROM #{stream}_groups WHERE group_name = $1) " \
        "ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $2" \
      "), bumped AS (" \
        "UPDATE #{stream}_groups SET last_id = COALESCE((SELECT MAX(id) FROM next), last_id) " \
        "WHERE group_name = $3" \
      ") SELECT id, payload, created_at FROM next",
      [group, count, group]
    )
    messages = result.map do |row|
      { "id" => row["id"].to_i, "payload" => JSON.parse(row["payload"]), "created_at" => row["created_at"] }
    end
    messages.each do |msg|
      raw.exec_params(
        "INSERT INTO #{stream}_pending (group_name, consumer, message_id) VALUES ($1, $2, $3)",
        [group, consumer, msg["id"]]
      )
    end
    messages
  end

  def self.stream_ack(conn, stream, group, message_id)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "DELETE FROM #{stream}_pending WHERE group_name = $1 AND message_id = $2",
      [group, message_id]
    )
    result.cmd_tuples > 0
  end

  def self.stream_claim(conn, stream, group, consumer, min_idle_ms: 60000)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "UPDATE #{stream}_pending SET consumer = $1, claimed_at = NOW() " \
      "WHERE group_name = $2 " \
      "AND claimed_at < NOW() - ($3 || ' milliseconds')::interval " \
      "RETURNING message_id",
      [consumer, group, min_idle_ms.to_s]
    )
    ids = result.map { |row| row["message_id"].to_i }
    return [] if ids.empty?
    placeholders = ids.each_with_index.map { |_, i| "$#{i + 1}" }.join(", ")
    messages = raw.exec_params(
      "SELECT id, payload, created_at FROM #{stream} WHERE id IN (#{placeholders})",
      ids
    )
    messages.map do |row|
      { "id" => row["id"].to_i, "payload" => JSON.parse(row["payload"]), "created_at" => row["created_at"] }
    end
  end

  def self.search(conn, table, column, query, limit: 50, lang: 'english', highlight: false)
    raw = _raw_conn(conn)
    columns = Array(column)
    _validate_identifier(table)
    columns.each { |col| _validate_identifier(col) }
    tsvec = columns.map { |col| col.to_s }.join(" || ' ' || ")
    if highlight
      hl_col = columns[0]
      result = raw.exec_params(
        "SELECT *, " \
        "ts_rank(to_tsvector($1, #{tsvec}), plainto_tsquery($2, $3)) AS _score, " \
        "ts_headline($4, #{hl_col}, plainto_tsquery($5, $6), " \
          "'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight " \
        "FROM #{table} " \
        "WHERE to_tsvector($7, #{tsvec}) @@ plainto_tsquery($8, $9) " \
        "ORDER BY _score DESC LIMIT $10",
        [lang, lang, query, lang, lang, query, lang, lang, query, limit]
      )
    else
      result = raw.exec_params(
        "SELECT *, " \
        "ts_rank(to_tsvector($1, #{tsvec}), plainto_tsquery($2, $3)) AS _score " \
        "FROM #{table} " \
        "WHERE to_tsvector($4, #{tsvec}) @@ plainto_tsquery($5, $6) " \
        "ORDER BY _score DESC LIMIT $7",
        [lang, lang, query, lang, lang, query, limit]
      )
    end
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.analyze(conn, text, lang: 'english')
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT alias, description, token, dictionaries, dictionary, lexemes " \
      "FROM ts_debug($1, $2)",
      [lang, text]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.explain_score(conn, table, column, query, id_column, id_value, lang: 'english')
    _validate_identifier(table)
    _validate_identifier(column)
    _validate_identifier(id_column)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT #{column} AS document_text, to_tsvector($1, #{column})::text AS document_tokens, " \
      "plainto_tsquery($1, $2)::text AS query_tokens, " \
      "to_tsvector($1, #{column}) @@ plainto_tsquery($1, $2) AS matches, " \
      "ts_rank(to_tsvector($1, #{column}), plainto_tsquery($1, $2)) AS score, " \
      "ts_headline($1, #{column}, plainto_tsquery($1, $2), " \
        "'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline " \
      "FROM #{table} WHERE #{id_column} = $3",
      [lang, query, id_value]
    )
    return nil if result.ntuples.zero?
    result[0].transform_keys(&:to_s)
  end

  def self.search_fuzzy(conn, table, column, query, limit: 50, threshold: 0.3)
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _raw_conn(conn)
    raw.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    result = raw.exec_params(
      "SELECT *, similarity(#{column}, $1) AS _score " \
      "FROM #{table} " \
      "WHERE similarity(#{column}, $2) > $3 " \
      "ORDER BY _score DESC LIMIT $4",
      [query, query, threshold.to_f, limit]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.search_phonetic(conn, table, column, query, limit: 50)
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _raw_conn(conn)
    raw.exec("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
    raw.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    result = raw.exec_params(
      "SELECT *, similarity(#{column}, $1) AS _score " \
      "FROM #{table} " \
      "WHERE soundex(#{column}) = soundex($2) " \
      "ORDER BY _score DESC, #{column} LIMIT $3",
      [query, query, limit]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.similar(conn, table, column, vector, limit: 10)
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _raw_conn(conn)
    raw.exec("CREATE EXTENSION IF NOT EXISTS vector")
    vec_literal = "[" + vector.map { |v| v.to_f.to_s }.join(",") + "]"
    result = raw.exec_params(
      "SELECT *, (#{column} <=> $1::vector) AS _score " \
      "FROM #{table} " \
      "ORDER BY _score LIMIT $2",
      [vec_literal, limit]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.suggest(conn, table, column, prefix, limit: 10)
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _raw_conn(conn)
    raw.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    pattern = prefix + "%"
    result = raw.exec_params(
      "SELECT *, similarity(#{column}, $1) AS _score " \
      "FROM #{table} " \
      "WHERE #{column} ILIKE $2 " \
      "ORDER BY _score DESC, #{column} LIMIT $3",
      [prefix, pattern, limit]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.facets(conn, table, column, limit: 50, query: nil, query_column: nil, lang: 'english')
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _raw_conn(conn)
    if query && query_column
      columns = Array(query_column)
      columns.each { |col| _validate_identifier(col) }
      tsvec = columns.map { |col| col.to_s }.join(" || ' ' || ")
      result = raw.exec_params(
        "SELECT #{column} AS value, COUNT(*) AS count " \
        "FROM #{table} " \
        "WHERE to_tsvector($1, #{tsvec}) @@ plainto_tsquery($2, $3) " \
        "GROUP BY #{column} ORDER BY count DESC, #{column} LIMIT $4",
        [lang, lang, query, limit]
      )
    else
      result = raw.exec_params(
        "SELECT #{column} AS value, COUNT(*) AS count " \
        "FROM #{table} " \
        "GROUP BY #{column} ORDER BY count DESC, #{column} LIMIT $1",
        [limit]
      )
    end
    result.map { |row| { "value" => row["value"], "count" => row["count"].to_i } }
  end

  AGGREGATE_FUNCS = %w[count sum avg min max].freeze
  private_constant :AGGREGATE_FUNCS

  def self.aggregate(conn, table, column, func, group_by: nil, limit: 50)
    _validate_identifier(table)
    _validate_identifier(column)
    func_lower = func.to_s.downcase
    unless AGGREGATE_FUNCS.include?(func_lower)
      raise ArgumentError, "Invalid aggregate function: #{func}. Must be one of: #{AGGREGATE_FUNCS.join(', ')}"
    end
    raw = _raw_conn(conn)
    expr = func_lower == "count" ? "COUNT(*)" : "#{func_lower.upcase}(#{column})"
    if group_by
      _validate_identifier(group_by)
      result = raw.exec_params(
        "SELECT #{group_by}, #{expr} AS value " \
        "FROM #{table} " \
        "GROUP BY #{group_by} ORDER BY value DESC LIMIT $1",
        [limit]
      )
      result.map { |row| row.transform_keys(&:to_s) }
    else
      result = raw.exec(
        "SELECT #{expr} AS value FROM #{table}"
      )
      return nil if result.ntuples.zero?
      result[0]["value"]
    end
  end

  def self.percolate_add(conn, name, query_id, query, lang: 'english', metadata: nil)
    _validate_identifier(name)
    raw = _raw_conn(conn)
    raw.exec("CREATE TABLE IF NOT EXISTS #{name} (" \
             "query_id TEXT PRIMARY KEY, " \
             "query_text TEXT NOT NULL, " \
             "tsquery TSQUERY NOT NULL, " \
             "lang TEXT NOT NULL DEFAULT 'english', " \
             "metadata JSONB)")
    raw.exec("CREATE INDEX IF NOT EXISTS idx_#{name}_tsquery " \
             "ON #{name} USING GIN (tsquery)")
    raw.exec_params(
      "INSERT INTO #{name} (query_id, query_text, tsquery, lang, metadata) " \
      "VALUES ($1, $2, plainto_tsquery($3, $2), $3, $4) " \
      "ON CONFLICT (query_id) DO UPDATE SET " \
        "query_text = EXCLUDED.query_text, " \
        "tsquery = EXCLUDED.tsquery, " \
        "lang = EXCLUDED.lang, " \
        "metadata = EXCLUDED.metadata",
      [query_id, query, lang, metadata.nil? ? nil : JSON.generate(metadata)]
    )
  end

  def self.percolate(conn, name, text, lang: 'english', limit: 50)
    _validate_identifier(name)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT query_id, query_text, metadata, " \
      "ts_rank(to_tsvector($1, $2), tsquery) AS _score " \
      "FROM #{name} " \
      "WHERE to_tsvector($1, $2) @@ tsquery " \
      "ORDER BY _score DESC LIMIT $3",
      [lang, text, limit]
    )
    result.map { |row| row.transform_keys(&:to_s) }
  end

  def self.percolate_delete(conn, name, query_id)
    _validate_identifier(name)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "DELETE FROM #{name} WHERE query_id = $1 RETURNING query_id",
      [query_id]
    )
    result.cmd_tuples.to_i > 0
  end

  def self.create_search_config(conn, name, copy_from: 'english')
    _validate_identifier(name)
    _validate_identifier(copy_from)
    raw = _raw_conn(conn)
    result = raw.exec_params(
      "SELECT 1 FROM pg_ts_config WHERE cfgname = $1",
      [name]
    )
    return if result.ntuples > 0
    raw.exec("CREATE TEXT SEARCH CONFIGURATION #{name} (COPY = #{copy_from})")
  end

  def self._validate_identifier(name)
    unless name.to_s.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/)
      raise ArgumentError, "Invalid identifier: #{name}"
    end
  end
  private_class_method :_validate_identifier

  def self._raw_conn(conn)
    conn.is_a?(CachedConnection) ? conn.send(:instance_variable_get, :@real) : conn
  end
  private_class_method :_raw_conn
end
