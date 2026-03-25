require "socket"
require "set"

module GoldLapel
  DDL_SENTINEL = "__ddl__"
  TX_START = /\A\s*(BEGIN|START\s+TRANSACTION)\b/i
  TX_END = /\A\s*(COMMIT|ROLLBACK|END)\b/i

  TABLE_PATTERN = /\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)/i
  SQL_KEYWORDS = Set.new(%w[
    select from where and or not in exists between like is null true false
    as on left right inner outer cross full natural group order having
    limit offset union intersect except all distinct lateral values
  ]).freeze

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
    when "CREATE", "ALTER", "DROP"
      DDL_SENTINEL
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
    alias_method :count, :ntuples
    alias_method :length, :ntuples
    alias_method :size, :ntuples

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
    attr_reader :stats_hits, :stats_misses, :stats_invalidations

    def initialize
      @cache = {}
      @table_index = {}
      @access_order = {}
      @counter = 0
      @max_entries = Integer(ENV.fetch("GOLDLAPEL_NATIVE_CACHE_SIZE", "32768"))
      @enabled = ENV.fetch("GOLDLAPEL_NATIVE_CACHE", "true").downcase != "false"
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
    end

    def connected?
      @invalidation_connected
    end

    def enabled?
      @enabled
    end

    def size
      @cache.size
    end

    def get(sql, params)
      return nil unless @enabled && @invalidation_connected
      key = make_key(sql, params)
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

    def put(sql, params, values, fields)
      return unless @enabled && @invalidation_connected
      key = make_key(sql, params)
      return unless key
      tables = GoldLapel.extract_tables(sql)
      @mutex.synchronize do
        unless @cache.key?(key)
          evict_one if @cache.size >= @max_entries
        end
        @cache[key] = { values: values, fields: fields, tables: tables }
        @counter += 1
        @access_order[key] = @counter
        tables.each do |table|
          @table_index[table] ||= Set.new
          @table_index[table].add(key)
        end
      end
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
      if line.start_with?("I:")
        table = line[2..].strip
        if table == "*"
          invalidate_all
        else
          invalidate_table(table)
        end
      end
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

    def make_key(sql, params)
      "#{sql}\0#{params&.to_s}"
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
    end

    def invalidation_loop
      port = @invalidation_port
      sock_path = "/tmp/goldlapel-#{port}.sock"

      until @invalidation_stop
        begin
          if RUBY_PLATFORM !~ /win|mingw/ && File.socket?(sock_path)
            @socket = UNIXSocket.new(sock_path)
          else
            @socket = TCPSocket.new("127.0.0.1", port)
          end

          @invalidation_connected = true
          @reconnect_attempt = 0
          buf = ""

          until @invalidation_stop
            ready = IO.select([@socket], nil, nil, 30)
            unless ready
              break # timeout — connection may be dead
            end
            data = @socket.read_nonblock(4096)
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
          if @invalidation_connected
            @invalidation_connected = false
            invalidate_all
          end
          @socket&.close rescue nil
          @socket = nil
        end

        break if @invalidation_stop
        delay = [2**@reconnect_attempt, 15].min
        @reconnect_attempt += 1
        sleep(delay) unless @invalidation_stop
      end
    end
  end
end
