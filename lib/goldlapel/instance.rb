# frozen_string_literal: true

module GoldLapel
  # Wrapper instance returned by `GoldLapel.start(url, **opts)`.
  #
  # Holds:
  #   - the spawned proxy (single upstream per instance)
  #   - an internal PG connection (eagerly opened by `start`)
  #   - proxy URL for callers who want to bring their own driver
  #
  # Thread/fiber safety: each instance owns one internal connection. For
  # multi-threaded / multi-fiber apps, open your own connection(s) via
  # `PG.connect(gl.url)` and pass them via the `conn:` kwarg or `gl.using(conn)`.
  #
  # The `using(conn)` scoped override uses a Fiber-local key so it correctly
  # composes with Ruby's `async` gem (Fiber-based concurrency).
  class Instance
    attr_reader :upstream

    def initialize(upstream, port: nil, config: {}, extra_args: [], eager_connect: true, silent: false)
      @upstream = upstream
      @port = port
      @config = config || {}
      @extra_args = extra_args || []
      @silent = silent ? true : false
      @proxy = nil
      @internal_conn = nil
      @wrapped_conn = nil
      @fiber_key = :"__goldlapel_conn_#{object_id}"
      start! if eager_connect
    end

    # Start the proxy and open the internal connection. Idempotent.
    # Returns self.
    #
    # Between spawning the proxy subprocess and opening the PG connection we
    # can hit: LoadError (pg gem missing), PG::Error (bad creds, upstream
    # unreachable), or any error from the wrap layer. Each of those would
    # otherwise leave an orphaned goldlapel subprocess running. Guard with a
    # rescue that tears the proxy back down before re-raising.
    def start!
      return self if @proxy&.running?

      @proxy = Proxy.new(@upstream, port: @port, config: @config, extra_args: @extra_args, silent: @silent)

      # Register the proxy in the module-level registry so GoldLapel.stop,
      # GoldLapel.proxy_url, etc. still see it — and so at_exit cleanup works.
      Proxy.register(@proxy)
      @proxy.start

      raw = nil
      begin
        # Lazily require pg only on connect
        begin
          require "pg"
        rescue LoadError
          raise LoadError,
            "The `pg` gem is required. Add `gem \"pg\"` to your Gemfile " \
            "or `gem install pg`."
        end

        raw = PG.connect(@proxy.url)
        inv_port = Integer(@config[:invalidation_port] || @config["invalidation_port"] || (@proxy.port + 2))
        @wrapped_conn = GoldLapel.wrap(raw, invalidation_port: inv_port)
        @internal_conn = @wrapped_conn
        @proxy.wrapped_conn = @wrapped_conn
      rescue Exception # rubocop:disable Lint/RescueException
        # Any failure between spawn and connect leaks the subprocess.
        # Stop the proxy (idempotent — SIGTERM with 5s timeout, then SIGKILL),
        # unregister it from the module-level registry, and clear internal
        # state before re-raising so the caller sees the original error.
        # Also close any raw PG connection we opened, in case the failure
        # was later in the pipeline (e.g. GoldLapel.wrap raising).
        begin
          if @wrapped_conn
            begin
              @wrapped_conn.close
            rescue StandardError
              # closing a partially-initialised conn is fine
            end
          elsif raw
            begin
              raw.close
            rescue StandardError
              # closing a partially-initialised conn is fine
            end
          end
          Proxy.unregister(@proxy)
          @proxy.stop
        ensure
          @wrapped_conn = nil
          @internal_conn = nil
          @proxy = nil
        end
        raise
      end

      self
    end

    # Proxy URL string (e.g. "postgresql://user:pass@localhost:7932/mydb").
    # Pass to PG.connect for raw SQL access through the proxy.
    def url
      @proxy&.url
    end

    # Shim for code that still expects `.conn` — returns the internal wrapped
    # connection. Prefer `gl.url` + your own `PG.connect` for per-thread/fiber
    # isolation.
    def conn
      @internal_conn
    end

    def dashboard_url
      @proxy&.dashboard_url
    end

    def proxy_url
      url
    end

    def running?
      @proxy&.running? ? true : false
    end

    def stop
      # Drop any cached DDL patterns tied to this instance.
      begin
        require "goldlapel/ddl"
        GoldLapel::DDL.invalidate(self)
      rescue StandardError
        # closing a dead cache is fine
      end
      if @internal_conn
        begin
          @internal_conn.close
        rescue StandardError
          # closing a dead conn is fine
        end
        @internal_conn = nil
        @wrapped_conn = nil
      end
      if @proxy
        Proxy.unregister(@proxy)
        @proxy.stop
        @proxy = nil
      end
      nil
    end

    # Scope a block to use `conn` as the active connection for all wrapper
    # method calls inside it. Fiber-local via `Fiber[key]=`, so it composes
    # with Ruby's `async` gem — child fibers inherit at creation time but
    # modifications stay fiber-local.
    #
    #   gl.using(tx_conn) do |gl|
    #     gl.doc_insert("events", { type: "x" })   # uses tx_conn
    #   end
    def using(conn)
      raise ArgumentError, "using() requires a block" unless block_given?
      prev = Fiber[@fiber_key]
      begin
        Fiber[@fiber_key] = conn
        yield self
      ensure
        Fiber[@fiber_key] = prev
      end
    end

    # --- Document methods ---

    def doc_insert(collection, document, conn: nil)
      GoldLapel.doc_insert(_resolve_conn(conn), collection, document)
    end

    def doc_insert_many(collection, documents, conn: nil)
      GoldLapel.doc_insert_many(_resolve_conn(conn), collection, documents)
    end

    def doc_find(collection, filter: nil, sort: nil, limit: nil, skip: nil, conn: nil)
      GoldLapel.doc_find(_resolve_conn(conn), collection, filter: filter, sort: sort, limit: limit, skip: skip)
    end

    def doc_find_cursor(collection, filter: nil, sort: nil, limit: nil, skip: nil, batch_size: 100, conn: nil)
      GoldLapel.doc_find_cursor(_resolve_conn(conn), collection, filter: filter, sort: sort, limit: limit, skip: skip, batch_size: batch_size)
    end

    def doc_find_one(collection, filter: nil, conn: nil)
      GoldLapel.doc_find_one(_resolve_conn(conn), collection, filter: filter)
    end

    def doc_update(collection, filter, update, conn: nil)
      GoldLapel.doc_update(_resolve_conn(conn), collection, filter, update)
    end

    def doc_update_one(collection, filter, update, conn: nil)
      GoldLapel.doc_update_one(_resolve_conn(conn), collection, filter, update)
    end

    def doc_delete(collection, filter, conn: nil)
      GoldLapel.doc_delete(_resolve_conn(conn), collection, filter)
    end

    def doc_delete_one(collection, filter, conn: nil)
      GoldLapel.doc_delete_one(_resolve_conn(conn), collection, filter)
    end

    def doc_count(collection, filter: nil, conn: nil)
      GoldLapel.doc_count(_resolve_conn(conn), collection, filter: filter)
    end

    def doc_find_one_and_update(collection, filter, update, conn: nil)
      GoldLapel.doc_find_one_and_update(_resolve_conn(conn), collection, filter, update)
    end

    def doc_find_one_and_delete(collection, filter, conn: nil)
      GoldLapel.doc_find_one_and_delete(_resolve_conn(conn), collection, filter)
    end

    def doc_distinct(collection, field, filter: nil, conn: nil)
      GoldLapel.doc_distinct(_resolve_conn(conn), collection, field, filter: filter)
    end

    def doc_create_index(collection, keys: nil, conn: nil)
      GoldLapel.doc_create_index(_resolve_conn(conn), collection, keys: keys)
    end

    def doc_aggregate(collection, pipeline, conn: nil)
      GoldLapel.doc_aggregate(_resolve_conn(conn), collection, pipeline)
    end

    def doc_watch(collection, conn: nil, &block)
      GoldLapel.doc_watch(_resolve_conn(conn), collection, &block)
    end

    def doc_unwatch(collection, conn: nil)
      GoldLapel.doc_unwatch(_resolve_conn(conn), collection)
    end

    def doc_create_ttl_index(collection, field, expire_after_seconds:, conn: nil)
      GoldLapel.doc_create_ttl_index(_resolve_conn(conn), collection, field, expire_after_seconds: expire_after_seconds)
    end

    def doc_remove_ttl_index(collection, conn: nil)
      GoldLapel.doc_remove_ttl_index(_resolve_conn(conn), collection)
    end

    def doc_create_collection(collection, conn: nil, **opts)
      GoldLapel.doc_create_collection(_resolve_conn(conn), collection, **opts)
    end

    def doc_create_capped(collection, max:, conn: nil)
      GoldLapel.doc_create_capped(_resolve_conn(conn), collection, max: max)
    end

    def doc_remove_cap(collection, conn: nil)
      GoldLapel.doc_remove_cap(_resolve_conn(conn), collection)
    end

    # --- Search methods ---

    def search(table, column, query, limit: 50, lang: 'english', highlight: false, conn: nil)
      GoldLapel.search(_resolve_conn(conn), table, column, query, limit: limit, lang: lang, highlight: highlight)
    end

    def search_fuzzy(table, column, query, limit: 50, threshold: 0.3, conn: nil)
      GoldLapel.search_fuzzy(_resolve_conn(conn), table, column, query, limit: limit, threshold: threshold)
    end

    def search_phonetic(table, column, query, limit: 50, conn: nil)
      GoldLapel.search_phonetic(_resolve_conn(conn), table, column, query, limit: limit)
    end

    def similar(table, column, vector, limit: 10, conn: nil)
      GoldLapel.similar(_resolve_conn(conn), table, column, vector, limit: limit)
    end

    def suggest(table, column, prefix, limit: 10, conn: nil)
      GoldLapel.suggest(_resolve_conn(conn), table, column, prefix, limit: limit)
    end

    def facets(table, column, limit: 50, query: nil, query_column: nil, lang: 'english', conn: nil)
      GoldLapel.facets(_resolve_conn(conn), table, column, limit: limit, query: query, query_column: query_column, lang: lang)
    end

    def aggregate(table, column, func, group_by: nil, limit: 50, conn: nil)
      GoldLapel.aggregate(_resolve_conn(conn), table, column, func, group_by: group_by, limit: limit)
    end

    def create_search_config(name, copy_from: 'english', conn: nil)
      GoldLapel.create_search_config(_resolve_conn(conn), name, copy_from: copy_from)
    end

    # --- Pub/sub ---

    def publish(channel, message, conn: nil)
      GoldLapel.publish(_resolve_conn(conn), channel, message)
    end

    def subscribe(channel, conn: nil, &block)
      GoldLapel.subscribe(_resolve_conn(conn), channel, &block)
    end

    # --- Queue ---

    def enqueue(queue_table, payload, conn: nil)
      GoldLapel.enqueue(_resolve_conn(conn), queue_table, payload)
    end

    def dequeue(queue_table, conn: nil)
      GoldLapel.dequeue(_resolve_conn(conn), queue_table)
    end

    # --- Counters ---

    def incr(table, key, amount: 1, conn: nil)
      GoldLapel.incr(_resolve_conn(conn), table, key, amount: amount)
    end

    def get_counter(table, key, conn: nil)
      GoldLapel.get_counter(_resolve_conn(conn), table, key)
    end

    # --- Hash methods ---

    def hset(table, key, field, value, conn: nil)
      GoldLapel.hset(_resolve_conn(conn), table, key, field, value)
    end

    def hget(table, key, field, conn: nil)
      GoldLapel.hget(_resolve_conn(conn), table, key, field)
    end

    def hgetall(table, key, conn: nil)
      GoldLapel.hgetall(_resolve_conn(conn), table, key)
    end

    def hdel(table, key, field, conn: nil)
      GoldLapel.hdel(_resolve_conn(conn), table, key, field)
    end

    # --- Sorted set methods ---

    def zadd(table, member, score, conn: nil)
      GoldLapel.zadd(_resolve_conn(conn), table, member, score)
    end

    def zincrby(table, member, amount: 1, conn: nil)
      GoldLapel.zincrby(_resolve_conn(conn), table, member, amount: amount)
    end

    def zrange(table, start: 0, stop: 10, desc: true, conn: nil)
      GoldLapel.zrange(_resolve_conn(conn), table, start: start, stop: stop, desc: desc)
    end

    def zrank(table, member, desc: true, conn: nil)
      GoldLapel.zrank(_resolve_conn(conn), table, member, desc: desc)
    end

    def zscore(table, member, conn: nil)
      GoldLapel.zscore(_resolve_conn(conn), table, member)
    end

    def zrem(table, member, conn: nil)
      GoldLapel.zrem(_resolve_conn(conn), table, member)
    end

    # --- Geo methods ---

    def georadius(table, geom_column, lon, lat, radius_meters, limit: 50, conn: nil)
      GoldLapel.georadius(_resolve_conn(conn), table, geom_column, lon, lat, radius_meters, limit: limit)
    end

    def geoadd(table, name_column, geom_column, name, lon, lat, conn: nil)
      GoldLapel.geoadd(_resolve_conn(conn), table, name_column, geom_column, name, lon, lat)
    end

    def geodist(table, geom_column, name_column, name_a, name_b, conn: nil)
      GoldLapel.geodist(_resolve_conn(conn), table, geom_column, name_column, name_a, name_b)
    end

    # --- Misc ---

    def count_distinct(table, column, conn: nil)
      GoldLapel.count_distinct(_resolve_conn(conn), table, column)
    end

    def script(lua_code, *args, conn: nil)
      GoldLapel.script(_resolve_conn(conn), lua_code, *args)
    end

    # --- Stream methods ---

    def stream_add(stream, payload, conn: nil)
      patterns = _stream_patterns(stream)
      GoldLapel.stream_add(_resolve_conn(conn), stream, payload, patterns: patterns)
    end

    def stream_create_group(stream, group, conn: nil)
      patterns = _stream_patterns(stream)
      GoldLapel.stream_create_group(_resolve_conn(conn), stream, group, patterns: patterns)
    end

    def stream_read(stream, group, consumer, count: 1, conn: nil)
      patterns = _stream_patterns(stream)
      GoldLapel.stream_read(_resolve_conn(conn), stream, group, consumer, count: count, patterns: patterns)
    end

    def stream_ack(stream, group, message_id, conn: nil)
      patterns = _stream_patterns(stream)
      GoldLapel.stream_ack(_resolve_conn(conn), stream, group, message_id, patterns: patterns)
    end

    def stream_claim(stream, group, consumer, min_idle_ms: 60000, conn: nil)
      patterns = _stream_patterns(stream)
      GoldLapel.stream_claim(_resolve_conn(conn), stream, group, consumer, min_idle_ms: min_idle_ms, patterns: patterns)
    end

    # Fetch (and cache per-instance) canonical DDL + query patterns for a stream.
    # The DDL itself runs on the proxy side — this returns only the patterns
    # the wrapper should execute.
    def _stream_patterns(stream)
      require "goldlapel/ddl"
      token = (@proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = @proxy&.dashboard_port
      GoldLapel::DDL.fetch_patterns(self, "stream", stream, port, token)
    end

    # --- Percolate methods ---

    def percolate_add(name, query_id, query, lang: 'english', metadata: nil, conn: nil)
      GoldLapel.percolate_add(_resolve_conn(conn), name, query_id, query, lang: lang, metadata: metadata)
    end

    def percolate(name, text, lang: 'english', limit: 50, conn: nil)
      GoldLapel.percolate(_resolve_conn(conn), name, text, lang: lang, limit: limit)
    end

    def percolate_delete(name, query_id, conn: nil)
      GoldLapel.percolate_delete(_resolve_conn(conn), name, query_id)
    end

    # --- Analysis methods ---

    def analyze(text, lang: 'english', conn: nil)
      GoldLapel.analyze(_resolve_conn(conn), text, lang: lang)
    end

    def explain_score(table, column, query, id_column, id_value, lang: 'english', conn: nil)
      GoldLapel.explain_score(_resolve_conn(conn), table, column, query, id_column, id_value, lang: lang)
    end

    private

    # Resolve the active connection: explicit `conn:` arg > `using` scope >
    # internal connection. Raises if none are available.
    def _resolve_conn(conn)
      return conn if conn

      scoped = Fiber[@fiber_key]
      return scoped if scoped

      unless @internal_conn
        raise RuntimeError,
          "Connection not available (proxy stopped or not started). " \
          "Call GoldLapel.start(...) or pass conn: explicitly."
      end
      @internal_conn
    end
  end
end
