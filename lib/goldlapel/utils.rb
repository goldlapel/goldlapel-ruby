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

  def self._raw_conn(conn)
    conn.is_a?(CachedConnection) ? conn.send(:instance_variable_get, :@real) : conn
  end
  private_class_method :_raw_conn
end
