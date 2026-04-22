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
    _validate_identifier(channel)
    raw = _raw_conn(conn)
    raw.exec_params("SELECT pg_notify($1, $2)", [channel, message.to_s])
  end

  def self.subscribe(conn, channel, &block)
    _validate_identifier(channel)
    raw = _raw_conn(conn)
    listen_conn = PG.connect(_listener_conninfo(raw))
    listen_conn.exec("LISTEN #{channel}")
    loop do
      listen_conn.wait_for_notify(5) do |ch, _pid, payload|
        block.call(ch, payload)
      end
    end
  end

  def self.enqueue(conn, queue_table, payload)
    _validate_identifier(queue_table)
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
    _validate_identifier(queue_table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
    raw = _raw_conn(conn)
    result = raw.exec_params("SELECT value FROM #{table} WHERE key = $1", [key])
    return 0 if result.ntuples.zero?
    result[0]["value"].to_i
  end

  def self.count_distinct(conn, table, column)
    _validate_identifier(table)
    _validate_identifier(column)
    raw = _raw_conn(conn)
    result = raw.exec("SELECT COUNT(DISTINCT #{column}) FROM #{table}")
    result[0]["count"].to_i
  end

  def self.zadd(conn, table, member, score)
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
    raw = _raw_conn(conn)
    result = raw.exec_params("SELECT score FROM #{table} WHERE member = $1", [member.to_s])
    return nil if result.ntuples.zero?
    result[0]["score"].to_f
  end

  def self.zrem(conn, table, member)
    _validate_identifier(table)
    raw = _raw_conn(conn)
    result = raw.exec_params("DELETE FROM #{table} WHERE member = $1", [member.to_s])
    result.cmd_tuples > 0
  end

  def self.geoadd(conn, table, name_column, geom_column, name, lon, lat)
    _validate_identifier(table)
    _validate_identifier(name_column)
    _validate_identifier(geom_column)
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
    _validate_identifier(table)
    _validate_identifier(geom_column)
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
    _validate_identifier(table)
    _validate_identifier(geom_column)
    _validate_identifier(name_column)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
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
    _validate_identifier(table)
    raw = _raw_conn(conn)
    result = raw.exec_params("SELECT data FROM #{table} WHERE key = $1", [key])
    return {} if result.ntuples.zero?
    val = result[0]["data"]
    return {} if val.nil?
    val.is_a?(Hash) ? val : JSON.parse(val)
  end

  def self.hdel(conn, table, key, field)
    _validate_identifier(table)
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

  def self._require_patterns(patterns, fn)
    unless patterns && patterns[:query_patterns]
      raise RuntimeError,
        "#{fn} requires DDL patterns from the proxy — call via " \
        "`gl.#{fn}(...)` rather than the GoldLapel.#{fn} module function directly."
    end
    patterns[:query_patterns]
  end

  def self.stream_add(conn, stream, payload, patterns: nil)
    _validate_identifier(stream)
    qp = _require_patterns(patterns, "stream_add")
    raw = _raw_conn(conn)
    result = raw.exec_params(qp["insert"], [JSON.generate(payload)])
    row = result[0]
    # Preserve the Ruby public return-shape: the full hash including the
    # payload the caller passed in. The canonical `insert` pattern returns
    # only (id, created_at); we hydrate `payload` from the input.
    { "id" => row["id"].to_i, "payload" => payload, "created_at" => row["created_at"] }
  end

  def self.stream_create_group(conn, stream, group, patterns: nil)
    _validate_identifier(stream)
    qp = _require_patterns(patterns, "stream_create_group")
    raw = _raw_conn(conn)
    raw.exec_params(qp["create_group"], [group])
    nil
  end

  def self.stream_read(conn, stream, group, consumer, count: 1, patterns: nil)
    _validate_identifier(stream)
    qp = _require_patterns(patterns, "stream_read")
    raw = _raw_conn(conn)
    # Wrap in an explicit transaction so the FOR UPDATE lock from
    # group_get_cursor is held until we've advanced the cursor and inserted
    # pending rows. Under autocommit the row lock is released immediately,
    # allowing concurrent consumers to read the same cursor and claim the
    # same messages. pg's `transaction` block BEGINs, COMMITs on normal
    # exit, and ROLLBACKs on exception. Mirrors the pattern in
    # goldlapel-python and goldlapel-php.
    messages = nil
    raw.transaction do
      cursor_res = raw.exec_params(qp["group_get_cursor"], [group])
      if cursor_res.ntuples.zero?
        messages = []
      else
        last_id = cursor_res[0]["last_delivered_id"].to_i
        rows = raw.exec_params(qp["read_since"], [last_id, count])
        messages = rows.map do |row|
          payload_raw = row["payload"]
          payload = payload_raw.is_a?(String) ? JSON.parse(payload_raw) : payload_raw
          { "id" => row["id"].to_i, "payload" => payload, "created_at" => row["created_at"] }
        end
        unless messages.empty?
          new_last = messages.last["id"]
          raw.exec_params(qp["group_advance_cursor"], [new_last, group])
          messages.each do |msg|
            raw.exec_params(qp["pending_insert"], [msg["id"], group, consumer])
          end
        end
      end
    end
    messages
  end

  def self.stream_ack(conn, stream, group, message_id, patterns: nil)
    _validate_identifier(stream)
    qp = _require_patterns(patterns, "stream_ack")
    raw = _raw_conn(conn)
    result = raw.exec_params(qp["ack"], [group, message_id])
    result.cmd_tuples > 0
  end

  def self.stream_claim(conn, stream, group, consumer, min_idle_ms: 60000, patterns: nil)
    _validate_identifier(stream)
    qp = _require_patterns(patterns, "stream_claim")
    raw = _raw_conn(conn)
    result = raw.exec_params(qp["claim"], [consumer, group, min_idle_ms])
    ids = result.map { |row| row["message_id"].to_i }
    return [] if ids.empty?

    read_by_id = qp["read_by_id"]
    messages = []
    ids.each do |msg_id|
      r = raw.exec_params(read_by_id, [msg_id])
      next if r.ntuples == 0

      row = r[0]
      payload_raw = row["payload"]
      payload = payload_raw.is_a?(String) ? JSON.parse(payload_raw) : payload_raw
      messages << { "id" => row["id"].to_i, "payload" => payload, "created_at" => row["created_at"] }
    end
    messages
  end

  def self.search(conn, table, column, query, limit: 50, lang: 'english', highlight: false)
    raw = _raw_conn(conn)
    columns = Array(column)
    _validate_identifier(table)
    columns.each { |col| _validate_identifier(col) }
    tsvec = columns.map { |col| "coalesce(#{col}, '')" }.join(" || ' ' || ")
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
      tsvec = columns.map { |col| "coalesce(#{col}, '')" }.join(" || ' ' || ")
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
      return [{ "value" => nil }] if result.ntuples.zero?
      [{ "value" => result[0]["value"] }]
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
             "metadata JSONB, " \
             "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
    raw.exec("CREATE INDEX IF NOT EXISTS #{name}_tsq_idx " \
             "ON #{name} USING GIST (tsquery)")
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

  SORT_KEY_PATTERN = /\A[a-zA-Z_][a-zA-Z0-9_.]*\z/
  private_constant :SORT_KEY_PATTERN

  FIELD_PART_PATTERN = /\A[a-zA-Z_][a-zA-Z0-9_]*\z/
  private_constant :FIELD_PART_PATTERN

  COMPARISON_OPS = {
    "$gt" => ">", "$gte" => ">=", "$lt" => "<", "$lte" => "<=",
    "$eq" => "=", "$ne" => "!="
  }.freeze
  private_constant :COMPARISON_OPS

  LOGICAL_OPS = %w[$or $and $not].freeze
  private_constant :LOGICAL_OPS

  def self._field_path(key)
    parts = key.to_s.split(".")
    parts.each do |part|
      unless part.match?(FIELD_PART_PATTERN)
        raise ArgumentError, "Invalid filter key: #{key}"
      end
    end
    if parts.length == 1
      "data->>'#{parts[0]}'"
    else
      chain = "data"
      parts[0..-2].each { |p| chain += "->'#{p}'" }
      chain += "->>'#{parts[-1]}'"
      chain
    end
  end
  private_class_method :_field_path

  def self._expand_dot_keys(hash)
    result = {}
    hash.each do |key, value|
      parts = key.to_s.split(".")
      current = result
      parts[0..-2].each do |part|
        current[part] = {} unless current.key?(part)
        current = current[part]
      end
      current[parts[-1]] = value
    end
    result
  end
  private_class_method :_expand_dot_keys

  def self._build_filter(filter, start_param = 1)
    return ["", [], start_param] if filter.nil? || filter.empty?

    containment = {}
    clauses = []
    params = []
    idx = start_param

    filter.each do |key, value|
      key_s = key.to_s
      if key_s == "$text"
        raise ArgumentError, "$text requires {$search: 'query'}" unless value.is_a?(Hash) && value.key?("$search")
        lang = value.fetch("$language", "english")
        clauses << "to_tsvector($#{idx}, data::text) @@ plainto_tsquery($#{idx + 1}, $#{idx + 2})"
        params.concat([lang, lang, value["$search"]])
        idx += 3
      elsif LOGICAL_OPS.include?(key_s)
        if key_s == "$not"
          raise ArgumentError, "$not value must be a Hash" unless value.is_a?(Hash)
          sc, sp, idx = _build_filter(value, idx)
          if sc && !sc.empty?
            clauses << "NOT (#{sc})"
            params.concat(sp)
          end
        else
          raise ArgumentError, "#{key_s} value must be a non-empty array" unless value.is_a?(Array) && !value.empty?
          joiner = key_s == "$or" ? " OR " : " AND "
          sub_clauses = []
          value.each do |sub_filter|
            sc, sp, idx = _build_filter(sub_filter, idx)
            if sc && !sc.empty?
              sub_clauses << sc
              params.concat(sp)
            end
          end
          clauses << "(#{sub_clauses.join(joiner)})" unless sub_clauses.empty?
        end
      elsif value.is_a?(Hash) && value.keys.any? { |k| k.to_s.start_with?("$") }
        field_expr = _field_path(key)
        value.each do |op, operand|
          op_s = op.to_s
          if COMPARISON_OPS.key?(op_s)
            sql_op = COMPARISON_OPS[op_s]
            if operand.is_a?(Numeric)
              clauses << "(#{field_expr})::numeric #{sql_op} $#{idx}"
              params << operand
              idx += 1
            else
              clauses << "#{field_expr} #{sql_op} $#{idx}"
              params << operand.to_s
              idx += 1
            end
          elsif op_s == "$in"
            placeholders = operand.each_with_index.map { |_, i| "$#{idx + i}" }.join(", ")
            clauses << "#{field_expr} IN (#{placeholders})"
            operand.each { |v| params << v.to_s }
            idx += operand.length
          elsif op_s == "$nin"
            placeholders = operand.each_with_index.map { |_, i| "$#{idx + i}" }.join(", ")
            clauses << "#{field_expr} NOT IN (#{placeholders})"
            operand.each { |v| params << v.to_s }
            idx += operand.length
          elsif op_s == "$exists"
            top_key = key.to_s.split(".")[0]
            if operand
              clauses << "data ? $#{idx}"
            else
              clauses << "NOT (data ? $#{idx})"
            end
            params << top_key
            idx += 1
          elsif op_s == "$regex"
            clauses << "#{field_expr} ~ $#{idx}"
            params << operand.to_s
            idx += 1
          elsif op_s == "$elemMatch"
            raise ArgumentError, "$elemMatch value must be an object" unless operand.is_a?(Hash)
            field_json = _field_path_json(key)
            elem_clauses = []
            operand.each do |sub_op, sub_val|
              sub_op_s = sub_op.to_s
              if COMPARISON_OPS.key?(sub_op_s)
                sql_op = COMPARISON_OPS[sub_op_s]
                if sub_val.is_a?(Numeric)
                  elem_clauses << "(elem#>>'{}')::numeric #{sql_op} $#{idx}"
                  params << sub_val
                  idx += 1
                else
                  elem_clauses << "elem#>>'{}' #{sql_op} $#{idx}"
                  params << sub_val.to_s
                  idx += 1
                end
              elsif sub_op_s == "$regex"
                elem_clauses << "elem#>>'{}' ~ $#{idx}"
                params << sub_val.to_s
                idx += 1
              else
                raise ArgumentError, "Unsupported $elemMatch operator: #{sub_op_s}"
              end
            end
            unless elem_clauses.empty?
              clauses << "EXISTS (SELECT 1 FROM jsonb_array_elements(#{field_json}) AS elem WHERE #{elem_clauses.join(' AND ')})"
            end
          elsif op_s == "$text"
            raise ArgumentError, "$text requires {$search: 'query'}" unless operand.is_a?(Hash) && operand.key?("$search")
            lang = operand.fetch("$language", "english")
            clauses << "to_tsvector($#{idx}, #{field_expr}) @@ plainto_tsquery($#{idx + 1}, $#{idx + 2})"
            params.concat([lang, lang, operand["$search"]])
            idx += 3
          else
            raise ArgumentError, "Unsupported filter operator: #{op_s}"
          end
        end
      else
        containment[key.to_s] = value
      end
    end

    if !containment.empty?
      clauses << "data @> $#{idx}::jsonb"
      params << JSON.generate(_expand_dot_keys(containment))
      idx += 1
    end
    [clauses.join(" AND "), params, idx]
  end
  private_class_method :_build_filter

  def self._field_path_json(key)
    parts = key.to_s.split(".")
    parts.each do |part|
      unless part.match?(FIELD_PART_PATTERN)
        raise ArgumentError, "Invalid field key: #{key}"
      end
    end
    chain = "data"
    parts.each { |p| chain += "->'#{p}'" }
    chain
  end
  private_class_method :_field_path_json

  def self._jsonb_path(key)
    parts = key.to_s.split(".")
    parts.each do |part|
      unless part.match?(FIELD_PART_PATTERN)
        raise ArgumentError, "Invalid field key: #{key}"
      end
    end
    "{" + parts.join(",") + "}"
  end
  private_class_method :_jsonb_path

  def self._to_jsonb_expr(value, idx)
    if value == true || value == false
      ["to_jsonb($#{idx}::boolean)", value, idx + 1]
    elsif value.is_a?(Numeric)
      ["to_jsonb($#{idx}::numeric)", value, idx + 1]
    elsif value.is_a?(String)
      ["to_jsonb($#{idx}::text)", value, idx + 1]
    else
      ["$#{idx}::jsonb", JSON.generate(value), idx + 1]
    end
  end
  private_class_method :_to_jsonb_expr

  def self._build_update(update, start_param = 1)
    idx = start_param
    unless update.any? { |k, _| k.to_s.start_with?("$") }
      return ["data || $#{idx}::jsonb", [JSON.generate(update)], idx + 1]
    end

    expr = "data"
    params = []

    if update.key?("$set") || update.key?(:$set)
      set_val = update["$set"] || update[:$set]
      expr = "(#{expr} || $#{idx}::jsonb)"
      params << JSON.generate(set_val)
      idx += 1
    end

    if update.key?("$unset") || update.key?(:$unset)
      unset_val = update["$unset"] || update[:$unset]
      unset_val.each do |field, _|
        field_s = field.to_s
        parts = field_s.split(".")
        parts.each do |part|
          unless part.match?(FIELD_PART_PATTERN)
            raise ArgumentError, "Invalid field key: #{field_s}"
          end
        end
        if parts.length == 1
          expr = "(#{expr} - $#{idx})"
          params << field_s
          idx += 1
        else
          path = "{" + parts.join(",") + "}"
          expr = "(#{expr} #- $#{idx}::text[])"
          params << path
          idx += 1
        end
      end
    end

    if update.key?("$inc") || update.key?(:$inc)
      inc_val = update["$inc"] || update[:$inc]
      inc_val.each do |field, amount|
        field_s = field.to_s
        jp = _jsonb_path(field_s)
        fp = _field_path(field_s)
        expr = "jsonb_set(#{expr}, $#{idx}::text[], to_jsonb(COALESCE((#{fp})::numeric, 0) + $#{idx + 1}))"
        params << jp
        params << amount
        idx += 2
      end
    end

    if update.key?("$mul") || update.key?(:$mul)
      mul_val = update["$mul"] || update[:$mul]
      mul_val.each do |field, factor|
        field_s = field.to_s
        jp = _jsonb_path(field_s)
        fp = _field_path(field_s)
        expr = "jsonb_set(#{expr}, $#{idx}::text[], to_jsonb(COALESCE((#{fp})::numeric, 0) * $#{idx + 1}))"
        params << jp
        params << factor
        idx += 2
      end
    end

    if update.key?("$rename") || update.key?(:$rename)
      rename_val = update["$rename"] || update[:$rename]
      rename_val.each do |old_name, new_name|
        old_s = old_name.to_s
        new_s = new_name.to_s
        old_s.split(".").each do |part|
          unless part.match?(FIELD_PART_PATTERN)
            raise ArgumentError, "Invalid field key: #{old_s}"
          end
        end
        new_s.split(".").each do |part|
          unless part.match?(FIELD_PART_PATTERN)
            raise ArgumentError, "Invalid field key: #{new_s}"
          end
        end
        old_json = _field_path_json(old_s)
        new_jp = _jsonb_path(new_s)
        if old_s.include?(".")
          old_path = "{" + old_s.split(".").join(",") + "}"
          expr = "jsonb_set((#{expr} #- $#{idx}::text[]), $#{idx + 1}::text[], #{old_json})"
          params << old_path
          params << new_jp
          idx += 2
        else
          expr = "jsonb_set((#{expr} - $#{idx}), $#{idx + 1}::text[], #{old_json})"
          params << old_s
          params << new_jp
          idx += 2
        end
      end
    end

    if update.key?("$push") || update.key?(:$push)
      push_val = update["$push"] || update[:$push]
      push_val.each do |field, value|
        field_s = field.to_s
        jp = _jsonb_path(field_s)
        fj = _field_path_json(field_s)
        jp_idx = idx
        params << jp
        idx += 1
        val_expr, val_param, idx = _to_jsonb_expr(value, idx)
        params << val_param
        expr = "jsonb_set(#{expr}, $#{jp_idx}::text[], COALESCE(#{fj}, '[]'::jsonb) || #{val_expr})"
      end
    end

    if update.key?("$pull") || update.key?(:$pull)
      pull_val = update["$pull"] || update[:$pull]
      pull_val.each do |field, value|
        field_s = field.to_s
        jp = _jsonb_path(field_s)
        fj = _field_path_json(field_s)
        jp_idx = idx
        params << jp
        idx += 1
        val_expr, val_param, idx = _to_jsonb_expr(value, idx)
        params << val_param
        expr = "jsonb_set(#{expr}, $#{jp_idx}::text[], " \
               "COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(#{fj}) AS elem " \
               "WHERE elem != #{val_expr}), '[]'::jsonb))"
      end
    end

    if update.key?("$addToSet") || update.key?(:$addToSet)
      ats_val = update["$addToSet"] || update[:$addToSet]
      ats_val.each do |field, value|
        field_s = field.to_s
        jp = _jsonb_path(field_s)
        fj = _field_path_json(field_s)
        jp_idx = idx
        params << jp
        idx += 1
        val_expr, val_param, idx = _to_jsonb_expr(value, idx)
        params << val_param
        # $addToSet uses the value param twice (for @> check and for append)
        val_expr2, _, idx = _to_jsonb_expr(value, idx)
        params << val_param
        expr = "jsonb_set(#{expr}, $#{jp_idx}::text[], " \
               "CASE WHEN COALESCE(#{fj}, '[]'::jsonb) @> #{val_expr} " \
               "THEN #{fj} " \
               "ELSE COALESCE(#{fj}, '[]'::jsonb) || #{val_expr2} END)"
      end
    end

    [expr, params, idx]
  end
  private_class_method :_build_update

  def self.doc_insert(conn, collection, document)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    _ensure_collection(raw, collection)
    result = raw.exec_params(
      "INSERT INTO #{collection} (data) VALUES ($1::jsonb) " \
      "RETURNING _id, data, created_at",
      [JSON.generate(document)]
    )
    row = result[0]
    { "_id" => row["_id"], "data" => JSON.parse(row["data"]), "created_at" => row["created_at"] }
  end

  def self.doc_insert_many(conn, collection, documents)
    _validate_identifier(collection)
    raise ArgumentError, "documents must be a non-empty array" if !documents.is_a?(Array) || documents.empty?
    raw = _raw_conn(conn)
    _ensure_collection(raw, collection)
    placeholders = documents.each_with_index.map { |_, i| "($#{i + 1}::jsonb)" }.join(", ")
    params = documents.map { |doc| JSON.generate(doc) }
    result = raw.exec_params(
      "INSERT INTO #{collection} (data) VALUES #{placeholders} " \
      "RETURNING _id, data, created_at",
      params
    )
    result.map do |row|
      { "_id" => row["_id"], "data" => JSON.parse(row["data"]), "created_at" => row["created_at"] }
    end
  end

  def self.doc_find(conn, collection, filter: nil, sort: nil, limit: nil, skip: nil)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    sql = "SELECT _id, data, created_at FROM #{collection}"
    params = []
    idx = 1
    where_clause, filter_params, idx = _build_filter(filter, idx)
    unless where_clause.empty?
      sql += " WHERE #{where_clause}"
      params.concat(filter_params)
    end
    if sort
      clauses = sort.map do |key, dir|
        unless key.to_s.match?(SORT_KEY_PATTERN)
          raise ArgumentError, "Invalid sort key: #{key}"
        end
        order = dir.to_i < 0 ? "DESC" : "ASC"
        "data->>'#{key}' #{order}"
      end
      sql += " ORDER BY #{clauses.join(', ')}"
    end
    if limit
      sql += " LIMIT $#{idx}"
      params << limit
      idx += 1
    end
    if skip
      sql += " OFFSET $#{idx}"
      params << skip
    end
    result = raw.exec_params(sql, params)
    result.map do |row|
      { "_id" => row["_id"], "data" => JSON.parse(row["data"]), "created_at" => row["created_at"] }
    end
  end

  def self.doc_find_cursor(conn, collection, filter: nil, sort: nil, limit: nil, skip: nil, batch_size: 100)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    sql = "SELECT _id, data, created_at FROM #{collection}"
    params = []
    idx = 1
    where_clause, filter_params, idx = _build_filter(filter, idx)
    unless where_clause.empty?
      sql += " WHERE #{where_clause}"
      params.concat(filter_params)
    end
    if sort
      clauses = sort.map do |key, dir|
        unless key.to_s.match?(SORT_KEY_PATTERN)
          raise ArgumentError, "Invalid sort key: #{key}"
        end
        order = dir.to_i < 0 ? "DESC" : "ASC"
        "data->>'#{key}' #{order}"
      end
      sql += " ORDER BY #{clauses.join(', ')}"
    end
    if limit
      sql += " LIMIT $#{idx}"
      params << limit
      idx += 1
    end
    if skip
      sql += " OFFSET $#{idx}"
      params << skip
    end
    cursor_name = "gl_cursor_#{collection}_#{conn.object_id}"
    raw.exec("BEGIN")
    raw.exec_params("DECLARE #{cursor_name} CURSOR FOR #{sql}", params)
    Enumerator.new do |yielder|
      begin
        loop do
          result = raw.exec("FETCH #{batch_size} FROM #{cursor_name}")
          break if result.ntuples == 0
          result.each { |row| yielder.yield row }
        end
      ensure
        raw.exec("CLOSE #{cursor_name}")
        raw.exec("COMMIT")
      end
    end
  end

  def self.doc_find_one(conn, collection, filter: nil)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    sql = "SELECT _id, data, created_at FROM #{collection}"
    params = []
    where_clause, filter_params, _idx = _build_filter(filter, 1)
    unless where_clause.empty?
      sql += " WHERE #{where_clause}"
      params.concat(filter_params)
    end
    sql += " LIMIT 1"
    result = raw.exec_params(sql, params)
    return nil if result.ntuples.zero?
    row = result[0]
    { "_id" => row["_id"], "data" => JSON.parse(row["data"]), "created_at" => row["created_at"] }
  end

  def self.doc_update(conn, collection, filter, update)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    where_clause, filter_params, idx = _build_filter(filter, 1)
    update_expr, update_params, _idx = _build_update(update, idx)
    sql = "UPDATE #{collection} SET data = #{update_expr}"
    params = filter_params + update_params
    unless where_clause.empty?
      sql += " WHERE #{where_clause}"
    end
    result = raw.exec_params(sql, params)
    result.cmd_tuples
  end

  def self.doc_update_one(conn, collection, filter, update)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    where_clause, filter_params, idx = _build_filter(filter, 1)
    update_expr, update_params, _idx = _build_update(update, idx)
    cte_where = where_clause.empty? ? "" : " WHERE #{where_clause}"
    sql = "WITH target AS (" \
          "SELECT _id FROM #{collection}#{cte_where} " \
          "LIMIT 1" \
          ") UPDATE #{collection} SET data = #{update_expr} " \
          "FROM target WHERE #{collection}._id = target._id"
    params = filter_params + update_params
    result = raw.exec_params(sql, params)
    result.cmd_tuples
  end

  def self.doc_delete(conn, collection, filter)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    where_clause, filter_params, _idx = _build_filter(filter, 1)
    sql = "DELETE FROM #{collection}"
    unless where_clause.empty?
      sql += " WHERE #{where_clause}"
    end
    result = raw.exec_params(sql, filter_params)
    result.cmd_tuples
  end

  def self.doc_delete_one(conn, collection, filter)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    where_clause, filter_params, _idx = _build_filter(filter, 1)
    cte_where = where_clause.empty? ? "" : " WHERE #{where_clause}"
    sql = "WITH target AS (" \
          "SELECT _id FROM #{collection}#{cte_where} " \
          "LIMIT 1" \
          ") DELETE FROM #{collection} " \
          "USING target WHERE #{collection}._id = target._id"
    result = raw.exec_params(sql, filter_params)
    result.cmd_tuples
  end

  def self.doc_count(conn, collection, filter: nil)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    sql = "SELECT COUNT(*) FROM #{collection}"
    where_clause, filter_params, _idx = _build_filter(filter, 1)
    if where_clause.empty?
      result = raw.exec(sql)
    else
      sql += " WHERE #{where_clause}"
      result = raw.exec_params(sql, filter_params)
    end
    result[0]["count"].to_i
  end

  def self.doc_find_one_and_update(conn, collection, filter, update)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    where_clause, filter_params, idx = _build_filter(filter, 1)
    update_expr, update_params, _idx = _build_update(update, idx)
    cte_where = where_clause.empty? ? "" : " WHERE #{where_clause}"
    sql = "WITH target AS (" \
          "SELECT _id FROM #{collection}#{cte_where} " \
          "LIMIT 1" \
          ") UPDATE #{collection} SET data = #{update_expr} " \
          "FROM target WHERE #{collection}._id = target._id " \
          "RETURNING #{collection}._id, #{collection}.data, #{collection}.created_at"
    params = filter_params + update_params
    result = raw.exec_params(sql, params)
    return nil if result.ntuples.zero?
    row = result[0]
    { "_id" => row["_id"], "data" => JSON.parse(row["data"]), "created_at" => row["created_at"] }
  end

  def self.doc_find_one_and_delete(conn, collection, filter)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    where_clause, filter_params, _idx = _build_filter(filter, 1)
    cte_where = where_clause.empty? ? "" : " WHERE #{where_clause}"
    sql = "WITH target AS (" \
          "SELECT _id FROM #{collection}#{cte_where} " \
          "LIMIT 1" \
          ") DELETE FROM #{collection} " \
          "USING target WHERE #{collection}._id = target._id " \
          "RETURNING #{collection}._id, #{collection}.data, #{collection}.created_at"
    result = raw.exec_params(sql, filter_params)
    return nil if result.ntuples.zero?
    row = result[0]
    { "_id" => row["_id"], "data" => JSON.parse(row["data"]), "created_at" => row["created_at"] }
  end

  def self.doc_distinct(conn, collection, field, filter: nil)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    field_expr = _field_path(field)
    sql = "SELECT DISTINCT #{field_expr} AS val FROM #{collection}"
    params = []
    idx = 1
    where_parts = ["#{field_expr} IS NOT NULL"]
    where_clause, filter_params, _idx = _build_filter(filter, idx)
    if !where_clause.empty?
      where_parts << where_clause
      params.concat(filter_params)
    end
    sql += " WHERE #{where_parts.join(' AND ')}"
    result = raw.exec_params(sql, params)
    result.map { |row| row["val"] }
  end

  def self.doc_create_index(conn, collection, keys: nil)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    if keys.nil?
      idx_name = "#{collection}_data_gin_idx"
      raw.exec("CREATE INDEX IF NOT EXISTS #{idx_name} " \
               "ON #{collection} USING GIN (data)")
    else
      key_names = []
      exprs = []
      keys.each do |key, _dir|
        unless key.to_s.match?(SORT_KEY_PATTERN)
          raise ArgumentError, "Invalid index key: #{key}"
        end
        key_names << key.to_s
        exprs << "(data->>'#{key}')"
      end
      idx_name = "#{collection}_#{key_names.join('_')}_idx"
      raw.exec("CREATE INDEX IF NOT EXISTS #{idx_name} " \
               "ON #{collection} (#{exprs.join(', ')})")
    end
    nil
  end

  DOC_ACCUMULATORS = {
    "$count"   => "COUNT(*)",
    "$sum"     => "SUM",
    "$avg"     => "AVG",
    "$min"     => "MIN",
    "$max"     => "MAX",
    "$push"    => "array_agg",
    "$addToSet" => "array_agg"
  }.freeze
  private_constant :DOC_ACCUMULATORS

  def self._resolve_field_ref(ref, unwind_map = {})
    field = ref.to_s.sub(/^\$/, "")
    unless field.match?(SORT_KEY_PATTERN)
      raise ArgumentError, "Invalid field name: #{field}"
    end
    return unwind_map[field] if unwind_map.key?(field)
    _field_path(field)
  end
  private_class_method :_resolve_field_ref

  def self.doc_aggregate(conn, collection, pipeline)
    _validate_identifier(collection)
    raise ArgumentError, "pipeline must be an array" unless pipeline.is_a?(Array)
    return [] if pipeline.empty?

    raw = _raw_conn(conn)
    params = []
    idx = 1

    group_stage = nil
    match_stage = nil
    sort_stage = nil
    limit_val = nil
    skip_val = nil
    project_stage = nil
    unwind_stage = nil
    lookup_stages = []

    pipeline.each do |stage|
      raise ArgumentError, "pipeline stage must be a hash" unless stage.is_a?(Hash)
      raise ArgumentError, "pipeline stage must have exactly one key" unless stage.size == 1

      key = stage.keys[0].to_s
      val = stage.values[0]

      case key
      when "$group"
        group_stage = val
      when "$match"
        match_stage = val
      when "$sort"
        sort_stage = val
      when "$limit"
        limit_val = val
      when "$skip"
        skip_val = val
      when "$project"
        project_stage = val
      when "$unwind"
        unwind_stage = val
      when "$lookup"
        lookup_stages << val
      else
        raise ArgumentError, "Unsupported pipeline stage: #{key}"
      end
    end

    # Parse $unwind
    unwind_field = nil
    unwind_alias = nil
    unwind_map = {}
    if unwind_stage
      if unwind_stage.is_a?(String)
        unwind_field = unwind_stage.sub(/^\$/, "")
      elsif unwind_stage.is_a?(Hash)
        path = (unwind_stage["path"] || unwind_stage[:path]).to_s
        unwind_field = path.sub(/^\$/, "")
      else
        raise ArgumentError, "$unwind must be a string or hash with path"
      end
      unless unwind_field.match?(SORT_KEY_PATTERN)
        raise ArgumentError, "Invalid field name: #{unwind_field}"
      end
      unwind_alias = "_uw_#{unwind_field}"
      unwind_map[unwind_field] = unwind_alias
    end

    # Build $lookup subqueries
    lookup_sqls = []
    lookup_stages.each do |lk|
      from = (lk["from"] || lk[:from]).to_s
      local_field = (lk["localField"] || lk[:localField]).to_s
      foreign_field = (lk["foreignField"] || lk[:foreignField]).to_s
      as_field = (lk["as"] || lk[:as]).to_s

      _validate_identifier(from)
      raise ArgumentError, "Invalid field name: #{local_field}" unless local_field.match?(SORT_KEY_PATTERN)
      raise ArgumentError, "Invalid field name: #{foreign_field}" unless foreign_field.match?(SORT_KEY_PATTERN)
      _validate_identifier(as_field)

      local_expr = _field_path(local_field)
      foreign_expr = "#{from}.data->>'#{foreign_field}'"

      lookup_sqls << "COALESCE((SELECT json_agg(#{from}.data) " \
                     "FROM #{from} " \
                     "WHERE #{foreign_expr} = #{local_expr}), '[]'::json) AS #{as_field}"
    end

    if project_stage
      # $project stage: select specific fields
      select_parts = []
      project_stage.each do |key, val|
        unless key.to_s.match?(SORT_KEY_PATTERN)
          raise ArgumentError, "Invalid field name: #{key}"
        end
        if val.is_a?(String) && val.start_with?("$")
          # Rename: { "newName" => "$oldField" }
          select_parts << "#{_resolve_field_ref(val, unwind_map)} AS #{key}"
        elsif val == 0 || val == false
          next # exclude field (handled by omission)
        else
          # Include field: val == 1 or val == true
          if key.to_s == "_id"
            select_parts << "_id"
          else
            select_parts << "#{_resolve_field_ref("$#{key}", unwind_map)} AS #{key}"
          end
        end
      end

      # Check if _id is explicitly excluded
      if project_stage.key?("_id") && (project_stage["_id"] == 0 || project_stage["_id"] == false)
        # _id excluded, do nothing
      elsif !project_stage.key?("_id")
        # _id included by default
        select_parts.unshift("_id")
      end

      all_parts = select_parts + lookup_sqls
      sql = "SELECT #{all_parts.join(', ')} FROM #{collection}"

      if unwind_field
        sql += " CROSS JOIN jsonb_array_elements_text(data->'#{unwind_field}') AS #{unwind_alias}"
      end

      if match_stage && !match_stage.empty?
        where_clause, filter_params, idx = _build_filter(match_stage, idx)
        unless where_clause.empty?
          sql += " WHERE #{where_clause}"
          params.concat(filter_params)
        end
      end

      if sort_stage
        clauses = sort_stage.map do |skey, dir|
          unless skey.to_s.match?(SORT_KEY_PATTERN)
            raise ArgumentError, "Invalid sort key: #{skey}"
          end
          order = dir.to_i < 0 ? "DESC" : "ASC"
          "#{skey} #{order}"
        end
        sql += " ORDER BY #{clauses.join(', ')}"
      end

    elsif group_stage
      select_parts = []
      group_by_exprs = []
      group_id = group_stage["_id"]

      if group_id.nil?
        select_parts << "NULL AS _id"
      elsif group_id.is_a?(Hash)
        jbo_args = []
        group_id.each do |label, ref|
          unless label.to_s.match?(SORT_KEY_PATTERN)
            raise ArgumentError, "Invalid field name: #{label}"
          end
          resolved = _resolve_field_ref(ref, unwind_map)
          jbo_args << "'#{label}', #{resolved}"
          group_by_exprs << resolved
        end
        select_parts << "json_build_object(#{jbo_args.join(', ')}) AS _id"
      else
        resolved = _resolve_field_ref(group_id, unwind_map)
        select_parts << "#{resolved} AS _id"
        group_by_exprs << resolved
      end

      group_stage.each do |alias_name, spec|
        next if alias_name == "_id"
        unless alias_name.to_s.match?(SORT_KEY_PATTERN)
          raise ArgumentError, "Invalid alias: #{alias_name}"
        end
        raise ArgumentError, "accumulator must be a hash" unless spec.is_a?(Hash)
        raise ArgumentError, "accumulator must have exactly one operator" unless spec.size == 1

        op = spec.keys[0].to_s
        unless DOC_ACCUMULATORS.key?(op)
          raise ArgumentError, "Unsupported accumulator: #{op}"
        end

        if op == "$count"
          select_parts << "#{DOC_ACCUMULATORS[op]}::numeric AS #{alias_name}"
        elsif op == "$push"
          resolved = _resolve_field_ref(spec.values[0], unwind_map)
          select_parts << "array_agg(#{resolved}) AS #{alias_name}"
        elsif op == "$addToSet"
          resolved = _resolve_field_ref(spec.values[0], unwind_map)
          select_parts << "array_agg(DISTINCT #{resolved}) AS #{alias_name}"
        else
          resolved = _resolve_field_ref(spec.values[0], unwind_map)
          select_parts << "#{DOC_ACCUMULATORS[op]}((#{resolved})::numeric)::numeric AS #{alias_name}"
        end
      end

      sql = "SELECT #{select_parts.join(', ')} FROM #{collection}"

      if unwind_field
        sql += " CROSS JOIN jsonb_array_elements_text(data->'#{unwind_field}') AS #{unwind_alias}"
      end

      if match_stage && !match_stage.empty?
        where_clause, filter_params, idx = _build_filter(match_stage, idx)
        unless where_clause.empty?
          sql += " WHERE #{where_clause}"
          params.concat(filter_params)
        end
      end

      unless group_by_exprs.empty?
        sql += " GROUP BY #{group_by_exprs.join(', ')}"
      end

      if sort_stage
        clauses = sort_stage.map do |skey, dir|
          unless skey.to_s.match?(SORT_KEY_PATTERN)
            raise ArgumentError, "Invalid sort key: #{skey}"
          end
          order = dir.to_i < 0 ? "DESC" : "ASC"
          "#{skey} #{order}"
        end
        sql += " ORDER BY #{clauses.join(', ')}"
      end
    else
      base_cols = ["_id", "data", "created_at"]
      all_parts = base_cols + lookup_sqls
      sql = "SELECT #{all_parts.join(', ')} FROM #{collection}"

      if unwind_field
        sql += " CROSS JOIN jsonb_array_elements_text(data->'#{unwind_field}') AS #{unwind_alias}"
      end

      if match_stage && !match_stage.empty?
        where_clause, filter_params, idx = _build_filter(match_stage, idx)
        unless where_clause.empty?
          sql += " WHERE #{where_clause}"
          params.concat(filter_params)
        end
      end

      if sort_stage
        clauses = sort_stage.map do |skey, dir|
          unless skey.to_s.match?(SORT_KEY_PATTERN)
            raise ArgumentError, "Invalid sort key: #{skey}"
          end
          order = dir.to_i < 0 ? "DESC" : "ASC"
          "data->>'#{skey}' #{order}"
        end
        sql += " ORDER BY #{clauses.join(', ')}"
      end
    end

    if limit_val
      sql += " LIMIT $#{idx}"
      params << limit_val
      idx += 1
    end

    if skip_val
      sql += " OFFSET $#{idx}"
      params << skip_val
      idx += 1
    end

    result = raw.exec_params(sql, params)
    result.map { |row| row.transform_keys(&:to_s) }
  end

  # --- Change streams (doc_watch / doc_unwatch) ---

  def self.doc_watch(conn, collection, &block)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    channel = "_gl_watch_#{collection}"
    fn_name = "_gl_notify_#{collection}"

    raw.exec(
      "CREATE OR REPLACE FUNCTION #{fn_name}() RETURNS trigger AS $$ " \
      "BEGIN " \
        "PERFORM pg_notify('#{channel}', json_build_object(" \
          "'op', TG_OP, " \
          "'_id', COALESCE(NEW._id, OLD._id), " \
          "'data', COALESCE(NEW.data, OLD.data)" \
        ")::text); " \
        "RETURN COALESCE(NEW, OLD); " \
      "END; $$ LANGUAGE plpgsql"
    )

    # CREATE OR REPLACE TRIGGER (Postgres 14+) is atomic — avoids the race
    # where a DROP + CREATE pair could have two concurrent doc_watch calls
    # replace each other's triggers mid-flight and end up with a partially
    # dropped one. GL targets PG14+ across the product, so this is safe.
    raw.exec(
      "CREATE OR REPLACE TRIGGER #{fn_name}_trg " \
      "AFTER INSERT OR UPDATE OR DELETE ON #{collection} " \
      "FOR EACH ROW EXECUTE FUNCTION #{fn_name}()"
    )

    listen_conn = PG.connect(_listener_conninfo(raw))
    listen_conn.exec("LISTEN #{channel}")
    loop do
      listen_conn.wait_for_notify(5) do |_ch, _pid, payload|
        event = JSON.parse(payload)
        if event["data"].is_a?(String)
          event["data"] = JSON.parse(event["data"])
        end
        block.call(event)
      end
    end
  end

  def self.doc_unwatch(conn, collection)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    fn_name = "_gl_notify_#{collection}"
    raw.exec("DROP TRIGGER IF EXISTS #{fn_name}_trg ON #{collection}")
    raw.exec("DROP FUNCTION IF EXISTS #{fn_name}()")
  end

  # --- TTL indexes (doc_create_ttl_index / doc_remove_ttl_index) ---

  def self.doc_create_ttl_index(conn, collection, field, expire_after_seconds:)
    _validate_identifier(collection)
    # `field` is a JSONB key (interpolated below as data->>'#{field}'), not
    # a Postgres identifier — no 63-char NAMEDATALEN cap applies.
    unless field.to_s.match?(FIELD_PART_PATTERN)
      raise ArgumentError, "Invalid field key: #{field}"
    end
    raw = _raw_conn(conn)
    fn_name = "_gl_ttl_#{collection}"

    raw.exec(
      "CREATE OR REPLACE FUNCTION #{fn_name}() RETURNS trigger AS $$ " \
      "BEGIN " \
        "DELETE FROM #{collection} " \
        "WHERE (data->>'#{field}')::timestamptz + " \
          "interval '#{expire_after_seconds.to_i} seconds' < NOW(); " \
        "RETURN NEW; " \
      "END; $$ LANGUAGE plpgsql"
    )

    # CREATE OR REPLACE TRIGGER (Postgres 14+): atomic, avoids the same
    # race documented in doc_watch.
    raw.exec(
      "CREATE OR REPLACE TRIGGER #{fn_name}_trg " \
      "BEFORE INSERT ON #{collection} " \
      "FOR EACH STATEMENT EXECUTE FUNCTION #{fn_name}()"
    )
  end

  def self.doc_remove_ttl_index(conn, collection)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    fn_name = "_gl_ttl_#{collection}"
    raw.exec("DROP TRIGGER IF EXISTS #{fn_name}_trg ON #{collection}")
    raw.exec("DROP FUNCTION IF EXISTS #{fn_name}()")
  end

  # --- Capped collections (doc_create_capped / doc_remove_cap) ---

  def self.doc_create_capped(conn, collection, max:)
    _validate_identifier(collection)
    raw = _raw_conn(conn)

    _ensure_collection(raw, collection)

    fn_name = "_gl_cap_#{collection}"

    raw.exec(
      "CREATE OR REPLACE FUNCTION #{fn_name}() RETURNS trigger AS $$ " \
      "BEGIN " \
        "DELETE FROM #{collection} WHERE _id IN (" \
          "SELECT _id FROM #{collection} " \
          "ORDER BY created_at DESC " \
          "OFFSET #{max.to_i}" \
        "); " \
        "RETURN NULL; " \
      "END; $$ LANGUAGE plpgsql"
    )

    # CREATE OR REPLACE TRIGGER (Postgres 14+): atomic, avoids the same
    # race documented in doc_watch.
    raw.exec(
      "CREATE OR REPLACE TRIGGER #{fn_name}_trg " \
      "AFTER INSERT ON #{collection} " \
      "FOR EACH STATEMENT EXECUTE FUNCTION #{fn_name}()"
    )
  end

  def self.doc_remove_cap(conn, collection)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    fn_name = "_gl_cap_#{collection}"
    raw.exec("DROP TRIGGER IF EXISTS #{fn_name}_trg ON #{collection}")
    raw.exec("DROP FUNCTION IF EXISTS #{fn_name}()")
  end

  def self.doc_create_collection(conn, collection, unlogged: false)
    _validate_identifier(collection)
    raw = _raw_conn(conn)
    _ensure_collection(raw, collection, unlogged: unlogged)
  end

  def self._ensure_collection(raw, collection, unlogged: false)
    prefix = unlogged ? "CREATE UNLOGGED TABLE" : "CREATE TABLE"
    raw.exec("#{prefix} IF NOT EXISTS #{collection} (" \
             "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " \
             "data JSONB NOT NULL, " \
             "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
  end
  private_class_method :_ensure_collection

  def self._validate_identifier(name)
    # Bound to 63 chars (Postgres NAMEDATALEN-1) so identifiers match the
    # proxy's server-side regex exactly: `^[A-Za-z_][A-Za-z0-9_]{0,62}$`.
    unless name.to_s.match?(/\A[a-zA-Z_][a-zA-Z0-9_]{0,62}\z/)
      raise ArgumentError, "Invalid identifier: #{name}"
    end
  end
  private_class_method :_validate_identifier

  def self._raw_conn(conn)
    conn.is_a?(CachedConnection) ? conn.send(:instance_variable_get, :@real) : conn
  end
  private_class_method :_raw_conn

  # `PG::Connection#conninfo_hash` on pg 1.6 returns a dense hash including
  # unset keys as `nil` (e.g. `:service => nil`). Passing that straight to
  # `PG.connect` fails with `definition of service "" not found` because
  # pg treats the empty-string service as a lookup request. Strip nils
  # before reconnecting so LISTEN/NOTIFY listeners inherit only the keys
  # that were actually set on the source connection.
  def self._listener_conninfo(raw)
    raw.conninfo_hash.reject { |_, v| v.nil? }
  end
  private_class_method :_listener_conninfo
end
