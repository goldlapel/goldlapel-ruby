# frozen_string_literal: true

# Async variant of the GoldLapel wrapper, built on the `async` gem.
#
# Usage:
#   require "goldlapel/async"
#   require "async"
#
#   Async do
#     gl = GoldLapel::Async.start("postgresql://user:pass@localhost/mydb")
#     hits = gl.search("articles", "body", "postgres tuning")
#     gl.stop
#   end
#
# Implementation notes:
#
# Internally this routes through `GoldLapel::Async::Utils`, which mirrors
# the sync `GoldLapel::Utils` layer but calls `pg`'s native non-blocking
# method variants (`async_exec_params`, `async_exec`, `wait_for_notify`)
# at every SQL call site. Under an Async reactor those yield cooperatively
# via Ruby's Fiber scheduler instead of parking the reactor thread.
#
# Public API is identical to the sync `GoldLapel::Instance`:
#   - `gl = GoldLapel::Async.start(url, ...)`
#   - `gl.using(conn) { |gl| ... }` (Fiber-local scope)
#   - `conn:` per-call kwarg
#   - `gl.stop`
#   - All wrapper methods (`search`, `doc_insert`, etc.) with identical
#     signatures and return shapes.

begin
  require "async"
rescue LoadError
  raise LoadError,
    "`GoldLapel::Async` requires the `async` gem. " \
    "Add `gem \"async\"` to your Gemfile, or `gem install async`."
end

require_relative "../goldlapel"
require_relative "async/utils"

module GoldLapel
  module Async
    # Factory — start a proxy + internal connection inside an async reactor.
    # Must be called from within an `Async do ... end` block (or equivalent).
    def self.start(
      upstream,
      proxy_port: nil,
      dashboard_port: nil,
      invalidation_port: nil,
      log_level: nil,
      mode: nil,
      license: nil,
      client: nil,
      config_file: nil,
      config: {},
      extra_args: [],
      silent: false
    )
      unless ::Async::Task.current?
        raise "GoldLapel::Async.start must be called inside an Async { ... } block"
      end
      Instance.new(
        upstream,
        proxy_port: proxy_port,
        dashboard_port: dashboard_port,
        invalidation_port: invalidation_port,
        log_level: log_level,
        mode: mode,
        license: license,
        client: client,
        config_file: config_file,
        config: config,
        extra_args: extra_args,
        eager_connect: true,
        silent: silent,
      )
    end

    # Async sibling of `GoldLapel::Instance`. Same API, but every wrapper
    # method routes through `GoldLapel::Async::Utils`, which uses pg's
    # native non-blocking method variants.
    #
    # Lifecycle and connection handling mirror the sync `Instance` line-for-
    # line: eager proxy spawn + PG.connect, subprocess-leak protection if
    # anything between Popen and successful connect raises, Fiber-local
    # `using(conn)` scoped override, `conn:` per-call override.
    class Instance
      attr_reader :upstream

      def initialize(
        upstream,
        proxy_port: nil,
        dashboard_port: nil,
        invalidation_port: nil,
        log_level: nil,
        mode: nil,
        license: nil,
        client: nil,
        config_file: nil,
        config: {},
        extra_args: [],
        eager_connect: true,
        silent: false
      )
        @upstream = upstream
        @proxy_port = proxy_port
        @dashboard_port = dashboard_port
        @invalidation_port = invalidation_port
        @log_level = log_level
        @mode = mode
        @license = license
        @client = client
        @config_file = config_file
        @config = config || {}
        @extra_args = extra_args || []
        @silent = silent ? true : false
        @proxy = nil
        @internal_conn = nil
        @wrapped_conn = nil
        @fiber_key = :"__goldlapel_async_conn_#{object_id}"
        start! if eager_connect
      end

      # Start the proxy and open the internal connection. Idempotent.
      # Returns self.
      #
      # Subprocess-leak protection (mirror of sync Instance#start!): between
      # proxy spawn and successful PG.connect we can hit LoadError, PG::Error,
      # or any exception from the wrap layer. Each would otherwise leave an
      # orphaned goldlapel subprocess running. The rescue re-raises after
      # tearing down the proxy and closing any partial PG connection.
      def start!
        return self if @proxy&.running?

        @proxy = Proxy.new(
          @upstream,
          proxy_port: @proxy_port,
          dashboard_port: @dashboard_port,
          invalidation_port: @invalidation_port,
          log_level: @log_level,
          mode: @mode,
          license: @license,
          client: @client,
          config_file: @config_file,
          config: @config,
          extra_args: @extra_args,
          silent: @silent,
        )
        Proxy.register(@proxy)
        @proxy.start

        raw = nil
        begin
          begin
            require "pg"
          rescue LoadError
            raise LoadError,
              "The `pg` gem is required. Add `gem \"pg\"` to your Gemfile " \
              "or `gem install pg`."
          end

          raw = PG.connect(@proxy.url)
          @wrapped_conn = GoldLapel.wrap(raw, invalidation_port: @proxy.invalidation_port)
          @internal_conn = @wrapped_conn
          @proxy.wrapped_conn = @wrapped_conn
        rescue Exception # rubocop:disable Lint/RescueException
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

      def url
        @proxy&.url
      end

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
      # with the `async` gem — child fibers inherit at creation time but
      # modifications stay fiber-local.
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
        Utils.doc_insert(_resolve_conn(conn), collection, document)
      end

      def doc_insert_many(collection, documents, conn: nil)
        Utils.doc_insert_many(_resolve_conn(conn), collection, documents)
      end

      def doc_find(collection, filter: nil, sort: nil, limit: nil, skip: nil, conn: nil)
        Utils.doc_find(_resolve_conn(conn), collection, filter: filter, sort: sort, limit: limit, skip: skip)
      end

      def doc_find_cursor(collection, filter: nil, sort: nil, limit: nil, skip: nil, batch_size: 100, conn: nil)
        Utils.doc_find_cursor(_resolve_conn(conn), collection, filter: filter, sort: sort, limit: limit, skip: skip, batch_size: batch_size)
      end

      def doc_find_one(collection, filter: nil, conn: nil)
        Utils.doc_find_one(_resolve_conn(conn), collection, filter: filter)
      end

      def doc_update(collection, filter, update, conn: nil)
        Utils.doc_update(_resolve_conn(conn), collection, filter, update)
      end

      def doc_update_one(collection, filter, update, conn: nil)
        Utils.doc_update_one(_resolve_conn(conn), collection, filter, update)
      end

      def doc_delete(collection, filter, conn: nil)
        Utils.doc_delete(_resolve_conn(conn), collection, filter)
      end

      def doc_delete_one(collection, filter, conn: nil)
        Utils.doc_delete_one(_resolve_conn(conn), collection, filter)
      end

      def doc_count(collection, filter: nil, conn: nil)
        Utils.doc_count(_resolve_conn(conn), collection, filter: filter)
      end

      def doc_find_one_and_update(collection, filter, update, conn: nil)
        Utils.doc_find_one_and_update(_resolve_conn(conn), collection, filter, update)
      end

      def doc_find_one_and_delete(collection, filter, conn: nil)
        Utils.doc_find_one_and_delete(_resolve_conn(conn), collection, filter)
      end

      def doc_distinct(collection, field, filter: nil, conn: nil)
        Utils.doc_distinct(_resolve_conn(conn), collection, field, filter: filter)
      end

      def doc_create_index(collection, keys: nil, conn: nil)
        Utils.doc_create_index(_resolve_conn(conn), collection, keys: keys)
      end

      def doc_aggregate(collection, pipeline, conn: nil)
        Utils.doc_aggregate(_resolve_conn(conn), collection, pipeline)
      end

      def doc_watch(collection, conn: nil, &block)
        Utils.doc_watch(_resolve_conn(conn), collection, &block)
      end

      def doc_unwatch(collection, conn: nil)
        Utils.doc_unwatch(_resolve_conn(conn), collection)
      end

      def doc_create_ttl_index(collection, field, expire_after_seconds:, conn: nil)
        Utils.doc_create_ttl_index(_resolve_conn(conn), collection, field, expire_after_seconds: expire_after_seconds)
      end

      def doc_remove_ttl_index(collection, conn: nil)
        Utils.doc_remove_ttl_index(_resolve_conn(conn), collection)
      end

      def doc_create_collection(collection, conn: nil, **opts)
        Utils.doc_create_collection(_resolve_conn(conn), collection, **opts)
      end

      def doc_create_capped(collection, max:, conn: nil)
        Utils.doc_create_capped(_resolve_conn(conn), collection, max: max)
      end

      def doc_remove_cap(collection, conn: nil)
        Utils.doc_remove_cap(_resolve_conn(conn), collection)
      end

      # --- Search methods ---

      def search(table, column, query, limit: 50, lang: 'english', highlight: false, conn: nil)
        Utils.search(_resolve_conn(conn), table, column, query, limit: limit, lang: lang, highlight: highlight)
      end

      def search_fuzzy(table, column, query, limit: 50, threshold: 0.3, conn: nil)
        Utils.search_fuzzy(_resolve_conn(conn), table, column, query, limit: limit, threshold: threshold)
      end

      def search_phonetic(table, column, query, limit: 50, conn: nil)
        Utils.search_phonetic(_resolve_conn(conn), table, column, query, limit: limit)
      end

      def similar(table, column, vector, limit: 10, conn: nil)
        Utils.similar(_resolve_conn(conn), table, column, vector, limit: limit)
      end

      def suggest(table, column, prefix, limit: 10, conn: nil)
        Utils.suggest(_resolve_conn(conn), table, column, prefix, limit: limit)
      end

      def facets(table, column, limit: 50, query: nil, query_column: nil, lang: 'english', conn: nil)
        Utils.facets(_resolve_conn(conn), table, column, limit: limit, query: query, query_column: query_column, lang: lang)
      end

      def aggregate(table, column, func, group_by: nil, limit: 50, conn: nil)
        Utils.aggregate(_resolve_conn(conn), table, column, func, group_by: group_by, limit: limit)
      end

      def create_search_config(name, copy_from: 'english', conn: nil)
        Utils.create_search_config(_resolve_conn(conn), name, copy_from: copy_from)
      end

      # --- Pub/sub ---

      def publish(channel, message, conn: nil)
        Utils.publish(_resolve_conn(conn), channel, message)
      end

      def subscribe(channel, conn: nil, &block)
        Utils.subscribe(_resolve_conn(conn), channel, &block)
      end

      # --- Queue ---

      def enqueue(queue_table, payload, conn: nil)
        Utils.enqueue(_resolve_conn(conn), queue_table, payload)
      end

      def dequeue(queue_table, conn: nil)
        Utils.dequeue(_resolve_conn(conn), queue_table)
      end

      # --- Counters ---

      def incr(table, key, amount: 1, conn: nil)
        Utils.incr(_resolve_conn(conn), table, key, amount: amount)
      end

      def get_counter(table, key, conn: nil)
        Utils.get_counter(_resolve_conn(conn), table, key)
      end

      # --- Hash methods ---

      def hset(table, key, field, value, conn: nil)
        Utils.hset(_resolve_conn(conn), table, key, field, value)
      end

      def hget(table, key, field, conn: nil)
        Utils.hget(_resolve_conn(conn), table, key, field)
      end

      def hgetall(table, key, conn: nil)
        Utils.hgetall(_resolve_conn(conn), table, key)
      end

      def hdel(table, key, field, conn: nil)
        Utils.hdel(_resolve_conn(conn), table, key, field)
      end

      # --- Sorted set methods ---

      def zadd(table, member, score, conn: nil)
        Utils.zadd(_resolve_conn(conn), table, member, score)
      end

      def zincrby(table, member, amount: 1, conn: nil)
        Utils.zincrby(_resolve_conn(conn), table, member, amount: amount)
      end

      def zrange(table, start: 0, stop: 10, desc: true, conn: nil)
        Utils.zrange(_resolve_conn(conn), table, start: start, stop: stop, desc: desc)
      end

      def zrank(table, member, desc: true, conn: nil)
        Utils.zrank(_resolve_conn(conn), table, member, desc: desc)
      end

      def zscore(table, member, conn: nil)
        Utils.zscore(_resolve_conn(conn), table, member)
      end

      def zrem(table, member, conn: nil)
        Utils.zrem(_resolve_conn(conn), table, member)
      end

      # --- Geo methods ---

      def georadius(table, geom_column, lon, lat, radius_meters, limit: 50, conn: nil)
        Utils.georadius(_resolve_conn(conn), table, geom_column, lon, lat, radius_meters, limit: limit)
      end

      def geoadd(table, name_column, geom_column, name, lon, lat, conn: nil)
        Utils.geoadd(_resolve_conn(conn), table, name_column, geom_column, name, lon, lat)
      end

      def geodist(table, geom_column, name_column, name_a, name_b, conn: nil)
        Utils.geodist(_resolve_conn(conn), table, geom_column, name_column, name_a, name_b)
      end

      # --- Misc ---

      def count_distinct(table, column, conn: nil)
        Utils.count_distinct(_resolve_conn(conn), table, column)
      end

      def script(lua_code, *args, conn: nil)
        Utils.script(_resolve_conn(conn), lua_code, *args)
      end

      # --- Stream methods ---

      def stream_add(stream, payload, conn: nil)
        patterns = _stream_patterns(stream)
        Utils.stream_add(_resolve_conn(conn), stream, payload, patterns: patterns)
      end

      def stream_create_group(stream, group, conn: nil)
        patterns = _stream_patterns(stream)
        Utils.stream_create_group(_resolve_conn(conn), stream, group, patterns: patterns)
      end

      def stream_read(stream, group, consumer, count: 1, conn: nil)
        patterns = _stream_patterns(stream)
        Utils.stream_read(_resolve_conn(conn), stream, group, consumer, count: count, patterns: patterns)
      end

      def stream_ack(stream, group, message_id, conn: nil)
        patterns = _stream_patterns(stream)
        Utils.stream_ack(_resolve_conn(conn), stream, group, message_id, patterns: patterns)
      end

      def stream_claim(stream, group, consumer, min_idle_ms: 60000, conn: nil)
        patterns = _stream_patterns(stream)
        Utils.stream_claim(_resolve_conn(conn), stream, group, consumer, min_idle_ms: min_idle_ms, patterns: patterns)
      end

      # See GoldLapel::Instance#_stream_patterns — same semantics, cached on self.
      def _stream_patterns(stream)
        require "goldlapel/ddl"
        token = (@proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
        port = @proxy&.dashboard_port
        GoldLapel::DDL.fetch_patterns(self, "stream", stream, port, token)
      end

      # --- Percolate methods ---

      def percolate_add(name, query_id, query, lang: 'english', metadata: nil, conn: nil)
        Utils.percolate_add(_resolve_conn(conn), name, query_id, query, lang: lang, metadata: metadata)
      end

      def percolate(name, text, lang: 'english', limit: 50, conn: nil)
        Utils.percolate(_resolve_conn(conn), name, text, lang: lang, limit: limit)
      end

      def percolate_delete(name, query_id, conn: nil)
        Utils.percolate_delete(_resolve_conn(conn), name, query_id)
      end

      # --- Analysis methods ---

      def analyze(text, lang: 'english', conn: nil)
        Utils.analyze(_resolve_conn(conn), text, lang: lang)
      end

      def explain_score(table, column, query, id_column, id_value, lang: 'english', conn: nil)
        Utils.explain_score(_resolve_conn(conn), table, column, query, id_column, id_value, lang: lang)
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
            "Call GoldLapel::Async.start(...) or pass conn: explicitly."
        end
        @internal_conn
      end
    end
  end
end
