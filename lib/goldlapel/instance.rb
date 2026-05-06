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
      disable_native_cache: false,
      disable_proxy_cache: false,
      disable_matviews: false,
      disable_sqloptimize: false,
      disable_auto_indexes: false,
      aggressive_verify: :auto
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
      @disable_native_cache = disable_native_cache ? true : false
      @disable_proxy_cache = disable_proxy_cache ? true : false
      @disable_matviews = disable_matviews ? true : false
      @disable_sqloptimize = disable_sqloptimize ? true : false
      @disable_auto_indexes = disable_auto_indexes ? true : false
      @aggressive_verify = aggressive_verify
      @proxy = nil
      @internal_conn = nil
      @wrapped_conn = nil
      @fiber_key = :"__goldlapel_conn_#{object_id}"

      # Nested namespaces — canonical schema-to-core sub-API instances. Each
      # holds a back-reference to this client for shared state (license,
      # dashboard token, http session, conn, DDL pattern cache).
      #
      # As of Phase 5 the Redis-compat helper families (counter / zset /
      # hash / queue / geo) are nested too, alongside streams (Phase 1+2)
      # and documents (Phase 4). Search / cache / auth remain flat — they'll
      # migrate when their own schema-to-core phase fires.
      require "goldlapel/documents"
      require "goldlapel/streams"
      require "goldlapel/counters"
      require "goldlapel/zsets"
      require "goldlapel/hashes"
      require "goldlapel/queues"
      require "goldlapel/geos"
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
    # Between spawning the proxy subprocess and opening the PG connection we
    # can hit: LoadError (pg gem missing), PG::Error (bad creds, upstream
    # unreachable), or any error from the wrap layer. Each of those would
    # otherwise leave an orphaned goldlapel subprocess running. Guard with a
    # rescue that tears the proxy back down before re-raising.
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
        disable_proxy_cache: @disable_proxy_cache,
        disable_matviews: @disable_matviews,
        disable_sqloptimize: @disable_sqloptimize,
        disable_auto_indexes: @disable_auto_indexes,
      )

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
        # invalidation_port is resolved at Proxy construction: either the
        # explicit kwarg or proxy_port + 2.
        @wrapped_conn = GoldLapel.wrap(
          raw,
          invalidation_port: @proxy.invalidation_port,
          disable_native_cache: @disable_native_cache,
          aggressive_verify: @aggressive_verify,
          upstream: @upstream,
        )
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

    # --- Document methods: gl.documents.<verb>(...). See goldlapel/documents.rb. ---

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

    # --- Phase 5 Redis-compat families: gl.counters / gl.zsets / gl.hashes /
    #     gl.queues / gl.geos. The legacy flat methods (incr, hset, zadd,
    #     enqueue, geoadd, ...) are gone — see the per-family modules under
    #     lib/goldlapel/{counters,zsets,hashes,queues,geos}.rb.

    # --- Misc ---

    def count_distinct(table, column, conn: nil)
      GoldLapel.count_distinct(_resolve_conn(conn), table, column)
    end

    def script(lua_code, *args, conn: nil)
      GoldLapel.script(_resolve_conn(conn), lua_code, *args)
    end

    # --- Stream methods: gl.streams.<verb>(...). See goldlapel/streams.rb. ---

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
