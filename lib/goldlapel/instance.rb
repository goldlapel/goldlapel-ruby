# frozen_string_literal: true

module GoldLapel
  class Instance
    attr_reader :conn

    def initialize(upstream, port: nil, config: {}, extra_args: [])
      @conn = GoldLapel.start(upstream, port: port, config: config, extra_args: extra_args)
      @upstream = upstream
    end

    def stop
      GoldLapel.stop(@upstream)
      @conn = nil
    end

    def proxy_url
      GoldLapel.proxy_url(@upstream)
    end

    def dashboard_url
      GoldLapel.dashboard_url(@upstream)
    end

    # --- Document methods ---

    def doc_insert(collection, document)
      GoldLapel.doc_insert(require_conn, collection, document)
    end

    def doc_insert_many(collection, documents)
      GoldLapel.doc_insert_many(require_conn, collection, documents)
    end

    def doc_find(collection, filter: nil, sort: nil, limit: nil, skip: nil)
      GoldLapel.doc_find(require_conn, collection, filter: filter, sort: sort, limit: limit, skip: skip)
    end

    def doc_find_one(collection, filter: nil)
      GoldLapel.doc_find_one(require_conn, collection, filter: filter)
    end

    def doc_update(collection, filter, update)
      GoldLapel.doc_update(require_conn, collection, filter, update)
    end

    def doc_update_one(collection, filter, update)
      GoldLapel.doc_update_one(require_conn, collection, filter, update)
    end

    def doc_delete(collection, filter)
      GoldLapel.doc_delete(require_conn, collection, filter)
    end

    def doc_delete_one(collection, filter)
      GoldLapel.doc_delete_one(require_conn, collection, filter)
    end

    def doc_count(collection, filter: nil)
      GoldLapel.doc_count(require_conn, collection, filter: filter)
    end

    def doc_create_index(collection, keys: nil)
      GoldLapel.doc_create_index(require_conn, collection, keys: keys)
    end

    def doc_aggregate(collection, pipeline)
      GoldLapel.doc_aggregate(require_conn, collection, pipeline)
    end

    # --- Search methods ---

    def search(table, column, query, limit: 50, lang: 'english', highlight: false)
      GoldLapel.search(require_conn, table, column, query, limit: limit, lang: lang, highlight: highlight)
    end

    def search_fuzzy(table, column, query, limit: 50, threshold: 0.3)
      GoldLapel.search_fuzzy(require_conn, table, column, query, limit: limit, threshold: threshold)
    end

    def search_phonetic(table, column, query, limit: 50)
      GoldLapel.search_phonetic(require_conn, table, column, query, limit: limit)
    end

    def similar(table, column, vector, limit: 10)
      GoldLapel.similar(require_conn, table, column, vector, limit: limit)
    end

    def suggest(table, column, prefix, limit: 10)
      GoldLapel.suggest(require_conn, table, column, prefix, limit: limit)
    end

    def facets(table, column, limit: 50, query: nil, query_column: nil, lang: 'english')
      GoldLapel.facets(require_conn, table, column, limit: limit, query: query, query_column: query_column, lang: lang)
    end

    def aggregate(table, column, func, group_by: nil, limit: 50)
      GoldLapel.aggregate(require_conn, table, column, func, group_by: group_by, limit: limit)
    end

    def create_search_config(name, copy_from: 'english')
      GoldLapel.create_search_config(require_conn, name, copy_from: copy_from)
    end

    # --- Pub/sub ---

    def publish(channel, message)
      GoldLapel.publish(require_conn, channel, message)
    end

    def subscribe(channel, &block)
      GoldLapel.subscribe(require_conn, channel, &block)
    end

    # --- Queue ---

    def enqueue(queue_table, payload)
      GoldLapel.enqueue(require_conn, queue_table, payload)
    end

    def dequeue(queue_table)
      GoldLapel.dequeue(require_conn, queue_table)
    end

    # --- Counters ---

    def incr(table, key, amount: 1)
      GoldLapel.incr(require_conn, table, key, amount: amount)
    end

    def get_counter(table, key)
      GoldLapel.get_counter(require_conn, table, key)
    end

    # --- Hash methods ---

    def hset(table, key, field, value)
      GoldLapel.hset(require_conn, table, key, field, value)
    end

    def hget(table, key, field)
      GoldLapel.hget(require_conn, table, key, field)
    end

    def hgetall(table, key)
      GoldLapel.hgetall(require_conn, table, key)
    end

    def hdel(table, key, field)
      GoldLapel.hdel(require_conn, table, key, field)
    end

    # --- Sorted set methods ---

    def zadd(table, member, score)
      GoldLapel.zadd(require_conn, table, member, score)
    end

    def zincrby(table, member, amount: 1)
      GoldLapel.zincrby(require_conn, table, member, amount: amount)
    end

    def zrange(table, start: 0, stop: 10, desc: true)
      GoldLapel.zrange(require_conn, table, start: start, stop: stop, desc: desc)
    end

    def zrank(table, member, desc: true)
      GoldLapel.zrank(require_conn, table, member, desc: desc)
    end

    def zscore(table, member)
      GoldLapel.zscore(require_conn, table, member)
    end

    def zrem(table, member)
      GoldLapel.zrem(require_conn, table, member)
    end

    # --- Geo methods ---

    def georadius(table, geom_column, lon, lat, radius_meters, limit: 50)
      GoldLapel.georadius(require_conn, table, geom_column, lon, lat, radius_meters, limit: limit)
    end

    def geoadd(table, name_column, geom_column, name, lon, lat)
      GoldLapel.geoadd(require_conn, table, name_column, geom_column, name, lon, lat)
    end

    def geodist(table, geom_column, name_column, name_a, name_b)
      GoldLapel.geodist(require_conn, table, geom_column, name_column, name_a, name_b)
    end

    # --- Misc ---

    def count_distinct(table, column)
      GoldLapel.count_distinct(require_conn, table, column)
    end

    def script(lua_code, *args)
      GoldLapel.script(require_conn, lua_code, *args)
    end

    # --- Stream methods ---

    def stream_add(stream, payload)
      GoldLapel.stream_add(require_conn, stream, payload)
    end

    def stream_create_group(stream, group)
      GoldLapel.stream_create_group(require_conn, stream, group)
    end

    def stream_read(stream, group, consumer, count: 1)
      GoldLapel.stream_read(require_conn, stream, group, consumer, count: count)
    end

    def stream_ack(stream, group, message_id)
      GoldLapel.stream_ack(require_conn, stream, group, message_id)
    end

    def stream_claim(stream, group, consumer, min_idle_ms: 60000)
      GoldLapel.stream_claim(require_conn, stream, group, consumer, min_idle_ms: min_idle_ms)
    end

    # --- Percolate methods ---

    def percolate_add(name, query_id, query, lang: 'english', metadata: nil)
      GoldLapel.percolate_add(require_conn, name, query_id, query, lang: lang, metadata: metadata)
    end

    def percolate(name, text, lang: 'english', limit: 50)
      GoldLapel.percolate(require_conn, name, text, lang: lang, limit: limit)
    end

    def percolate_delete(name, query_id)
      GoldLapel.percolate_delete(require_conn, name, query_id)
    end

    # --- Change streams ---

    def doc_watch(collection, &block)
      GoldLapel.doc_watch(require_conn, collection, &block)
    end

    def doc_unwatch(collection)
      GoldLapel.doc_unwatch(require_conn, collection)
    end

    # --- TTL indexes ---

    def doc_create_ttl_index(collection, field, expire_after_seconds:)
      GoldLapel.doc_create_ttl_index(require_conn, collection, field, expire_after_seconds: expire_after_seconds)
    end

    def doc_remove_ttl_index(collection)
      GoldLapel.doc_remove_ttl_index(require_conn, collection)
    end

    # --- Collection management ---

    def doc_create_collection(collection, **opts)
      GoldLapel.doc_create_collection(require_conn, collection, **opts)
    end

    # --- Capped collections ---

    def doc_create_capped(collection, max:)
      GoldLapel.doc_create_capped(require_conn, collection, max: max)
    end

    def doc_remove_cap(collection)
      GoldLapel.doc_remove_cap(require_conn, collection)
    end

    # --- Analysis methods ---

    def analyze(text, lang: 'english')
      GoldLapel.analyze(require_conn, text, lang: lang)
    end

    def explain_score(table, column, query, id_column, id_value, lang: 'english')
      GoldLapel.explain_score(require_conn, table, column, query, id_column, id_value, lang: lang)
    end

    private

    def require_conn
      raise RuntimeError, "Connection not available (proxy stopped or not started)" unless @conn
      @conn
    end
  end
end
