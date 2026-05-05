# frozen_string_literal: true

require "socket"
require "set"
require "json"
require "securerandom"

module GoldLapel
  DDL_SENTINEL = "__ddl__"
  TX_START = /\A\s*(BEGIN|START\s+TRANSACTION)\b/i
  TX_END = /\A\s*(COMMIT|ROLLBACK|END)\b/i

  # --- L1 telemetry tuning ---
  #
  # Demand-driven model (mirrors goldlapel-python). The wrapper has NO
  # background timer. Cache counters increment on cache ops (free);
  # state-change events are emitted synchronously when a relevant counter
  # crosses a threshold; snapshot replies are sent only when the proxy
  # asks via `?:<request>`.
  #
  # Eviction-rate sliding window. `cache_full` fires when ≥
  # EVICT_RATE_HIGH of the last EVICT_RATE_WINDOW cache writes (puts)
  # caused an eviction; `cache_recovered` fires when the rate falls back
  # below EVICT_RATE_LOW. With a 32k-entry default capacity, a
  # steady-state high eviction rate means the working set exceeds the
  # cache — actionable signal for the dashboard.
  EVICT_RATE_WINDOW = 200
  EVICT_RATE_HIGH = 0.5  # 50% of recent puts evicted → cache_full
  EVICT_RATE_LOW = 0.1   # ≤ 10% → cache_recovered

  TABLE_PATTERN = /\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)/i
  SQL_KEYWORDS = Set.new(%w[
    select from where and or not in exists between like is null true false
    as on left right inner outer cross full natural group order having
    limit offset union intersect except all distinct lateral values
  ]).freeze

  # Replace the contents of `'...'` and `"..."` string literals with
  # spaces, preserving overall length so positions line up with the
  # original. PG's doubled-quote `''` / `""` escapes are handled the
  # same way as in `GucState.split_statements`. Used by `detect_write`'s
  # SELECT branch so that bare words like `INTO` inside a literal
  # (e.g. `SELECT 'INSERT INTO orders' FROM audit_log`) don't trip the
  # SELECT-INTO DDL classifier.
  def self.strip_string_literals(sql)
    return sql unless sql.is_a?(String) && !sql.empty?
    out = sql.dup
    quote = nil
    i = 0
    n = sql.length
    while i < n
      c = sql[i]
      if quote
        if c == quote
          if i + 1 < n && sql[i + 1] == quote
            # Doubled-quote escape: blank both, stay inside literal.
            out[i] = " "
            out[i + 1] = " "
            i += 2
            next
          end
          # Closing quote: leave the delimiter, drop the literal body.
          quote = nil
        else
          out[i] = " "
        end
      else
        if c == "'" || c == '"'
          quote = c
        end
      end
      i += 1
    end
    out
  end

  def self.detect_write(sql)
    trimmed = sql.strip
    tokens = trimmed.split(/\s+/)
    return nil if tokens.empty?
    first = tokens[0].upcase

    case first
    when "INSERT"
      return nil if tokens.length < 3 || tokens[1].upcase != "INTO"
      bare_table(tokens[2])
    when "UPDATE"
      return nil if tokens.length < 2
      bare_table(tokens[1])
    when "DELETE"
      return nil if tokens.length < 3 || tokens[1].upcase != "FROM"
      bare_table(tokens[2])
    when "TRUNCATE"
      return nil if tokens.length < 2
      if tokens[1].upcase == "TABLE"
        return nil if tokens.length < 3
        bare_table(tokens[2])
      else
        bare_table(tokens[1])
      end
    when "CREATE", "ALTER", "DROP", "REFRESH", "DO", "CALL"
      DDL_SENTINEL
    when "MERGE"
      return nil if tokens.length < 3 || tokens[1].upcase != "INTO"
      bare_table(tokens[2])
    when "SELECT"
      # Re-tokenize from a literal-stripped form so that bare words like
      # `INTO` or `FROM` inside `'...'` / `"..."` don't trigger the
      # SELECT-INTO DDL classifier (e.g. `SELECT 'INSERT INTO orders'
      # FROM audit_log`, `SELECT * FROM "into_table"`).
      scan_tokens = strip_string_literals(trimmed).split(/\s+/)
      saw_into = false
      into_target = nil
      scan_tokens[1..].each do |tok|
        upper = tok.upcase
        if upper == "INTO" && !saw_into
          saw_into = true
          next
        end
        if saw_into && into_target.nil?
          if %w[TEMPORARY TEMP UNLOGGED].include?(upper)
            next
          end
          into_target = tok
          next
        end
        return DDL_SENTINEL if saw_into && !into_target.nil? && upper == "FROM"
        return nil if upper == "FROM"
      end
      return nil
    when "COPY"
      return nil if tokens.length < 2
      raw = tokens[1]
      return nil if raw.start_with?("(")
      table_part = raw.split("(")[0]
      tokens[2..].each do |tok|
        upper = tok.upcase
        return bare_table(table_part) if upper == "FROM"
        return nil if upper == "TO"
      end
      nil
    when "WITH"
      rest_upper = trimmed[tokens[0].length..].upcase
      rest_upper.split(/\s+/).each do |token|
        word = token.gsub(/\A\(+/, "")
        return DDL_SENTINEL if %w[INSERT UPDATE DELETE].include?(word)
      end
      nil
    else
      nil
    end
  end

  def self.bare_table(raw)
    table = raw.split("(")[0]
    table = table.split(".").last
    table.downcase
  end

  def self.extract_tables(sql)
    tables = Set.new
    sql.scan(TABLE_PATTERN) do |_schema, table|
      t = table.downcase
      tables.add(t) unless SQL_KEYWORDS.include?(t)
    end
    tables
  end

  class CachedResult
    include Enumerable

    attr_reader :values, :fields

    def initialize(values, fields)
      @values = values
      @fields = fields
    end

    def ntuples
      @values.length
    end
    alias_method :num_tuples, :ntuples
    alias_method :length, :ntuples
    alias_method :size, :ntuples

    def count(*args, &block)
      if block || args.any?
        super
      else
        ntuples
      end
    end

    def [](idx)
      row = @values[idx]
      return nil unless row
      Hash[@fields.zip(row)]
    end

    def each(&block)
      @values.each_with_index do |row, i|
        block.call(Hash[@fields.zip(row)])
      end
    end

    def nfields
      @fields.length
    end

    def fname(idx)
      @fields[idx]
    end

    def ftype(idx)
      0 # unknown OID — safe default, ActiveRecord falls back to string
    end

    def fmod(idx)
      -1 # no modifier — safe default
    end

    def clear
      # no-op — cached results don't hold PG memory
    end

    def cmd_status
      "SELECT #{ntuples}"
    end

    def column_values(col_idx)
      @values.map { |row| row[col_idx] }
    end

    def cmd_tuples
      @values.length
    end
  end

  class NativeCache
    attr_reader :stats_hits, :stats_misses, :stats_invalidations, :stats_evictions

    def initialize
      @cache = {}
      @table_index = {}
      @access_order = {}
      @counter = 0
      @max_entries = Integer(ENV.fetch("GOLDLAPEL_NATIVE_CACHE_SIZE", "32768"))
      @enabled = ENV.fetch("GOLDLAPEL_NATIVE_CACHE", "true").downcase != "false"
      # Explicit native-cache disable — orthogonal to `@enabled` (which
      # is the GOLDLAPEL_NATIVE_CACHE env-var kill-switch) and orthogonal
      # to cache size. When set, `get` always returns nil (incrementing
      # misses) and `put` is a silent no-op. The invalidation thread
      # still runs so telemetry signal flow (wrapper_connected /
      # snapshot replies) continues to work — Manor and the dashboard
      # need to see the wrapper even when the native cache is off.
      @disable_native_cache = false
      @mutex = Mutex.new
      @invalidation_connected = false
      @invalidation_thread = nil
      @invalidation_stop = false
      @invalidation_port = 0
      @reconnect_attempt = 0
      @socket = nil
      @stats_hits = 0
      @stats_misses = 0
      @stats_invalidations = 0
      # L1 telemetry — eviction counter bumped under @mutex in evict_one.
      # Configurable opt-out: set GOLDLAPEL_REPORT_STATS=false to disable
      # all snapshot replies and state-change emissions (cache continues
      # to function; only the wire output is suppressed).
      @stats_evictions = 0
      @report_stats = ENV.fetch("GOLDLAPEL_REPORT_STATS", "true").downcase != "false"
      # Stable wrapper identity for the lifetime of the process. Lets the
      # proxy aggregate per wrapper across reconnects.
      @wrapper_id = SecureRandom.uuid
      @wrapper_lang = "ruby"
      @wrapper_version = self.class.detect_wrapper_version
      # Synchronizes writes from the recv thread (replies to ?:) and any
      # cache-op thread (state-change emissions). The socket is a single
      # full-duplex stream; concurrent writes would interleave bytes.
      # Read stays on the recv thread; writes serialize behind this mutex.
      @send_mutex = Mutex.new
      # Sliding window for eviction-rate state-change detection. A
      # bounded ring buffer; updates are O(1).
      @recent_evictions = []  # 1 = evicted, 0 = inserted; len ≤ window
      @recent_evictions_idx = 0
      # Latched state — only emit a state-change event when the state
      # transitions. Without latching the wrapper would re-emit every
      # put while the rate stays bad.
      @state_cache_full = false
    end

    attr_reader :wrapper_id, :wrapper_lang, :wrapper_version

    def report_stats?
      @report_stats
    end

    def connected?
      @invalidation_connected
    end

    def enabled?
      @enabled
    end

    # When true, `get` always returns nil (miss) and `put` is a no-op.
    # The invalidation thread continues to run and telemetry emissions
    # still fire — only the local hit path is suppressed. Set via the
    # `disable_native_cache:` kwarg to `GoldLapel.start` / `.new` / `.wrap`.
    def disable_native_cache?
      @disable_native_cache
    end

    def disable_native_cache=(value)
      @disable_native_cache = value ? true : false
    end

    def size
      @cache.size
    end

    def get(sql, params, state_hash = 0)
      return nil unless @enabled && @invalidation_connected
      # disable_native_cache: tick misses (callers measure miss rate),
      # never hit. Skip the key-build + cache lookup entirely — no point.
      if @disable_native_cache
        @mutex.synchronize { @stats_misses += 1 }
        return nil
      end
      key = make_key(sql, params, state_hash)
      return nil unless key
      @mutex.synchronize do
        entry = @cache[key]
        if entry
          @counter += 1
          @access_order[key] = @counter
          @stats_hits += 1
          entry
        else
          @stats_misses += 1
          nil
        end
      end
    end

    def put(sql, params, values, fields, state_hash = 0)
      return unless @enabled && @invalidation_connected
      # disable_native_cache: silent no-op. Don't touch cache state,
      # don't touch the eviction-rate window, don't bump counters — the
      # layer is off.
      return if @disable_native_cache
      key = make_key(sql, params, state_hash)
      return unless key
      tables = GoldLapel.extract_tables(sql)
      evicted = 0
      @mutex.synchronize do
        if @cache.key?(key)
          # Re-put refreshes LRU; no eviction.
        elsif @cache.size >= @max_entries
          evict_one
          evicted = 1
        end
        @cache[key] = { values: values, fields: fields, tables: tables }
        @counter += 1
        @access_order[key] = @counter
        tables.each do |table|
          @table_index[table] ||= Set.new
          @table_index[table].add(key)
        end
        # Window tracks every put — re-puts record 0 (no eviction).
        record_eviction_locked(evicted)
      end
      # Eviction-rate threshold check happens outside the cache mutex —
      # emit may take @send_mutex and we don't want to nest locks across
      # socket I/O.
      maybe_emit_eviction_rate_state_change
    end

    def invalidate_table(table)
      table = table.downcase
      @mutex.synchronize do
        keys = @table_index.delete(table)
        return unless keys
        keys.each do |key|
          entry = @cache.delete(key)
          @access_order.delete(key)
          if entry
            entry[:tables].each do |other_table|
              next if other_table == table
              if @table_index[other_table]
                @table_index[other_table].delete(key)
                @table_index.delete(other_table) if @table_index[other_table].empty?
              end
            end
          end
        end
        @stats_invalidations += keys.size
      end
    end

    def invalidate_all
      @mutex.synchronize do
        count = @cache.size
        @cache.clear
        @table_index.clear
        @access_order.clear
        @stats_invalidations += count
      end
    end

    def connect_invalidation(port)
      return if @invalidation_thread&.alive?
      @invalidation_port = port
      @invalidation_stop = false
      @reconnect_attempt = 0
      @invalidation_thread = Thread.new { invalidation_loop }
      @invalidation_thread.abort_on_exception = false
    end

    def stop_invalidation
      @invalidation_stop = true
      @socket&.close rescue nil
      @invalidation_thread&.join(5)
      @invalidation_thread = nil
      @invalidation_connected = false
    end

    def process_signal(line)
      # Backwards-compat: unknown prefixes are silently ignored. Older
      # proxies sent only `I:` / `C:` / `P:`; newer proxies add `?:`
      # request types. Forward-compat: the wrapper accepts any
      # well-formed prefix and routes by type.
      if line.start_with?("I:")
        table = line[2..].strip
        if table == "*"
          invalidate_all
        else
          invalidate_table(table)
        end
      elsif line.start_with?("?:")
        # Snapshot request from the proxy. Reply with R:<json>.
        process_request(line[2..])
      end
      # `C:` (config), `P:` (ping), and anything else — ignored.
    end

    # Handle `?:<request>` from the proxy. Today the only request is
    # `snapshot` — the proxy asks for a current counter snapshot and we
    # reply with `R:<json>`. Future requests can extend this without
    # breaking older proxies (they'd ignore unknown R: lines, but only
    # the proxy that sent `?:<x>` will be expecting a reply, so the
    # contract is local to the request type).
    def process_request(raw)
      body = raw ? raw.strip : ""
      if body.empty? || body == "snapshot"
        emit_response
      end
    end

    # Emit a final `wrapper_disconnected` snapshot before shutdown.
    # Called from at_exit (registered at module load) — best effort; the
    # socket may already be torn down.
    def emit_wrapper_disconnected
      emit_state_change("wrapper_disconnected")
    end

    @instance_mutex = Mutex.new

    def self.instance
      @instance_mutex.synchronize do
        @instance ||= new
      end
    end

    def self.reset!
      @instance_mutex.synchronize do
        if @instance
          @instance.stop_invalidation
          @instance = nil
        end
      end
    end

    private

    # Cache key shape: `<state_hash_hex>\0<sql>\0<params>`. The
    # `state_hash` is a per-connection fingerprint of the unsafe-GUC
    # state (see `GoldLapel::GucState::ConnectionGucState`). Two
    # connections with different unsafe-GUC state map to different
    # cache slots, so custom-GUC RLS can never leak user A's rows to
    # user B. `0` is the fingerprint for the default ("no unsafe GUCs
    # set") state — a fresh connection's keys collide with peer
    # connections that also have no unsafe state, which is what we
    # want.
    def make_key(sql, params, state_hash = 0)
      "#{state_hash.to_s(16)}\0#{sql}\0#{params&.to_s}"
    end

    def evict_one
      return if @access_order.empty?
      lru_key = @access_order.min_by { |_k, v| v }&.first
      return unless lru_key
      entry = @cache.delete(lru_key)
      @access_order.delete(lru_key)
      if entry
        entry[:tables].each do |table|
          if @table_index[table]
            @table_index[table].delete(lru_key)
            @table_index.delete(table) if @table_index[table].empty?
          end
        end
      end
      @stats_evictions += 1
    end

    # ---- L1 telemetry: sliding window ----

    # Record a put() outcome (1 evicted, 0 inserted). Caller holds
    # @mutex. Bounded ring — once at capacity, overwrites oldest in O(1).
    def record_eviction_locked(evicted)
      if @recent_evictions.length < EVICT_RATE_WINDOW
        @recent_evictions << evicted
      else
        @recent_evictions[@recent_evictions_idx] = evicted
        @recent_evictions_idx = (@recent_evictions_idx + 1) % EVICT_RATE_WINDOW
      end
    end

    # ---- L1 telemetry: snapshot + state-change emission ----

    # Build the L1 snapshot hash the proxy aggregates per-tick. All
    # counters + cache size read in a single critical section so the
    # snapshot is internally consistent (no torn reads where, e.g., hits
    # and misses straddle a concurrent get()). The proxy computes deltas
    # across ticks; we just expose the raw counters.
    def build_snapshot
      @mutex.synchronize do
        snap = {
          "wrapper_id" => @wrapper_id,
          "lang" => @wrapper_lang,
          "version" => @wrapper_version,
          "hits" => @stats_hits,
          "misses" => @stats_misses,
          "evictions" => @stats_evictions,
          "invalidations" => @stats_invalidations,
          "current_size_entries" => @cache.size,
          "capacity_entries" => @max_entries,
        }
        # Forward-compat: surface the disable flag so HQ/Manor can render
        # the wrapper's native-cache state correctly. Only emitted when
        # set; older consumers that don't know the field will simply
        # ignore it. Field name is the short form `disabled` — the
        # nesting under `native_cache.wrappers[]` disambiguates the
        # context.
        snap["disabled"] = true if @disable_native_cache
        snap
      end
    end

    # Serialize a line write under @send_mutex. Best-effort — socket
    # errors are swallowed (the recv loop will detect the broken
    # connection on its next iteration and reconnect).
    def send_line(line)
      return unless @report_stats
      sock = @socket
      return if sock.nil?
      data = line.end_with?("\n") ? line : "#{line}\n"
      @send_mutex.synchronize do
        begin
          sock.write(data)
        rescue IOError, Errno::EPIPE, Errno::ECONNRESET, Errno::EBADF, Errno::ENOTCONN
          # Connection dead — recv loop will rebuild on next iteration.
          # Don't try to repair here; we'd race the reconnect logic.
        end
      end
    end

    # Emit S:<json> with snapshot + state name.
    def emit_state_change(state)
      return unless @report_stats
      payload = build_snapshot
      payload["state"] = state
      payload["ts_ms"] = (Time.now.to_f * 1000).to_i
      line = "S:" + JSON.generate(payload)
      send_line(line)
    rescue StandardError
      # Snapshot serialization or send failure — swallow.
    end

    # Emit R:<json> snapshot reply to a `?:<request>`.
    def emit_response(snapshot = nil)
      return unless @report_stats
      snapshot ||= build_snapshot
      snapshot["ts_ms"] ||= (Time.now.to_f * 1000).to_i
      line = "R:" + JSON.generate(snapshot)
      send_line(line)
    rescue StandardError
      # Snapshot serialization or send failure — swallow.
    end

    # Check the eviction-rate sliding window and emit a state change if
    # the latched flag should flip. Hysteresis-guarded: crossing HIGH
    # emits cache_full, falling back below LOW emits cache_recovered, and
    # rates between LOW and HIGH leave the latched state unchanged (no
    # flapping).
    def maybe_emit_eviction_rate_state_change
      # Read window state + flip latched flag under @mutex so two
      # concurrent puts that both cross the threshold can't both emit.
      # Need at least a full window before reporting state — a single
      # eviction in 3 puts is noise.
      emit = nil
      @mutex.synchronize do
        n = @recent_evictions.length
        return if n < EVICT_RATE_WINDOW
        rate = @recent_evictions.sum.to_f / n
        if !@state_cache_full && rate >= EVICT_RATE_HIGH
          @state_cache_full = true
          emit = "cache_full"
        elsif @state_cache_full && rate <= EVICT_RATE_LOW
          @state_cache_full = false
          emit = "cache_recovered"
        end
      end
      # Emit outside the cache mutex — emit_state_change takes
      # @send_mutex and may block on a socket write; we don't want to
      # nest locks or hold @mutex across I/O.
      emit_state_change(emit) if emit
    end

    def invalidation_loop
      port = @invalidation_port
      sock_path = "/tmp/goldlapel-#{port}.sock"

      until @invalidation_stop
        begin
          sock = if RUBY_PLATFORM !~ /win|mingw/ && File.socket?(sock_path)
                   UNIXSocket.new(sock_path)
                 else
                   TCPSocket.new("127.0.0.1", port)
                 end

          # Stash the socket so send_line (called from cache-op threads
          # on state-change, and from process_request on this thread for
          # ?:/R:) writes to the live FD. Set before the
          # wrapper_connected emit so the very first message goes out
          # cleanly.
          @socket = sock
          @invalidation_connected = true
          @reconnect_attempt = 0
          emit_state_change("wrapper_connected")
          buf = ""

          until @invalidation_stop
            ready = IO.select([sock], nil, nil, 30)
            unless ready
              break # timeout — connection may be dead
            end
            data = sock.read_nonblock(4096)
            buf += data
            while (idx = buf.index("\n"))
              line = buf[0...idx]
              buf = buf[(idx + 1)..]
              process_signal(line)
            end
          end
        rescue EOFError, IOError, Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::ENOENT, Errno::EPIPE
          # connection failed or dropped
        ensure
          # Drop the socket reference under @send_mutex so any concurrent
          # emitter doesn't write to a closed FD.
          @send_mutex.synchronize { @socket = nil }
          if @invalidation_connected
            @invalidation_connected = false
            invalidate_all
          end
          sock&.close rescue nil
        end

        break if @invalidation_stop
        # Exponential backoff 1s → 2s → 4s → 8s → 15s, capped. Retry
        # forever — long-lived processes outlive proxy restarts.
        delay = [2**@reconnect_attempt, 15].min
        @reconnect_attempt += 1
        sleep(delay) unless @invalidation_stop
      end
    end

    def self.detect_wrapper_version
      spec = Gem.loaded_specs["goldlapel"]
      return spec.version.to_s if spec && spec.version
      v = ENV["GEM_VERSION"]
      return v if v && !v.empty?
      "unknown"
    rescue StandardError
      "unknown"
    end
  end

  # Register a single at_exit hook at module load. Best-effort emit of
  # `wrapper_disconnected` so the proxy/dashboard can show the wrapper
  # leaving cleanly. Only runs if a NativeCache instance exists; the
  # socket may already be torn down, in which case the emit is a silent
  # no-op.
  unless @at_exit_registered
    @at_exit_registered = true
    at_exit do
      begin
        cache = NativeCache.instance_variable_get(:@instance)
        cache&.emit_wrapper_disconnected
      rescue StandardError
        # Process is exiting — swallow everything.
      end
    end
  end
end
