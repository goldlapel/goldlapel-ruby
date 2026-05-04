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
      silent: false,
      mesh: false,
      mesh_tag: nil,
      enable_l2_for_wrappers: false
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
        mesh: mesh,
        mesh_tag: mesh_tag,
        enable_l2_for_wrappers: enable_l2_for_wrappers,
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
      attr_reader :upstream, :documents, :streams,
                  :counters, :zsets, :hashes, :queues, :geos

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
        silent: false,
        mesh: false,
        mesh_tag: nil,
        enable_l2_for_wrappers: false
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
        @mesh = mesh ? true : false
        tag = mesh_tag.to_s
        @mesh_tag = tag.empty? ? nil : tag
        @enable_l2_for_wrappers = enable_l2_for_wrappers ? true : false
        @proxy = nil
        @internal_conn = nil
        @wrapped_conn = nil
        @fiber_key = :"__goldlapel_async_conn_#{object_id}"

        # Nested namespaces — async siblings of the sync sub-API classes.
        # As of Phase 5 the Redis-compat helper families (counter / zset /
        # hash / queue / geo) are nested too, alongside streams and documents.
        require "goldlapel/async/documents"
        require "goldlapel/async/streams"
        require "goldlapel/async/counters"
        require "goldlapel/async/zsets"
        require "goldlapel/async/hashes"
        require "goldlapel/async/queues"
        require "goldlapel/async/geos"
        @documents = DocumentsAPI.new(self)
        @streams = StreamsAPI.new(self)
        @counters = CountersAPI.new(self)
        @zsets = ZsetsAPI.new(self)
        @hashes = HashesAPI.new(self)
        @queues = QueuesAPI.new(self)
        @geos = GeosAPI.new(self)

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
          mesh: @mesh,
          mesh_tag: @mesh_tag,
          enable_l2_for_wrappers: @enable_l2_for_wrappers,
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

      # --- Document methods: gl.documents.<verb>(...). See goldlapel/async/documents.rb. ---

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

      # --- Phase 5 Redis-compat families: gl.counters / gl.zsets / gl.hashes /
      #     gl.queues / gl.geos. Async siblings of the sync namespaces — see
      #     goldlapel/async/{counters,zsets,hashes,queues,geos}.rb.

      # --- Misc ---

      def count_distinct(table, column, conn: nil)
        Utils.count_distinct(_resolve_conn(conn), table, column)
      end

      def script(lua_code, *args, conn: nil)
        Utils.script(_resolve_conn(conn), lua_code, *args)
      end

      # --- Stream methods: gl.streams.<verb>(...). See goldlapel/async/streams.rb. ---

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
