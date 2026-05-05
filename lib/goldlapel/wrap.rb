# frozen_string_literal: true

require_relative "cache"
require_relative "guc_state"

module GoldLapel
  def self.wrap(conn, invalidation_port: nil, disable_native_cache: false)
    cache = NativeCache.instance
    # Set the flag before the invalidation thread connects so the very
    # first `wrapper_connected` snapshot carries the correct
    # `disabled` field — Manor/HQ see the wrapper's native-cache state
    # from the first emit, not after a subsequent state-change.
    cache.disable_native_cache = disable_native_cache
    invalidation_port ||= detect_invalidation_port
    cache.connect_invalidation(invalidation_port) unless cache.connected?
    CachedConnection.new(conn, cache)
  end

  def self.detect_invalidation_port
    instances = Proxy.instances rescue {}
    if instances.any?
      inst = instances.values.first
      # invalidation_port is resolved at Proxy construction.
      inst.invalidation_port
    else
      DEFAULT_PROXY_PORT + 2
    end
  end

  class CachedConnection
    # Exposed for tests and rare adapters. Don't mutate from outside.
    attr_reader :guc_state

    def initialize(real_conn, cache)
      @real = real_conn
      @cache = cache
      @in_transaction = false
      # Per-connection unsafe-GUC fingerprint. Folded into the L1
      # cache key so custom-GUC RLS (`SET app.user_id = '42'`) can
      # never leak user A's rows to user B. See `goldlapel/guc_state.rb`.
      @guc_state = GucState::ConnectionGucState.new
    end

    def exec(sql, &block)
      handle_query(sql, nil, :exec, block)
    end
    alias_method :query, :exec

    def async_exec(sql, &block)
      handle_query(sql, nil, :async_exec, block)
    end

    def exec_params(sql, params = [], result_format = 0, &block)
      handle_query(sql, params, :exec_params, block, result_format)
    end

    def async_exec_params(sql, params = [], result_format = 0, &block)
      handle_query(sql, params, :async_exec_params, block, result_format)
    end

    def close
      @real.close
    end

    def finished?
      @real.finished?
    end

    def method_missing(name, *args, **kwargs, &block)
      @real.send(name, *args, **kwargs, &block)
    end

    def respond_to_missing?(name, include_private = false)
      @real.respond_to?(name, include_private) || super
    end

    private

    def handle_query(sql, params, method, block, result_format = nil)
      # Observe SET / RESET on every query so the per-connection
      # unsafe-GUC state stays current. Runs first — even DDL/writes
      # and the in-transaction bypass paths must update state if the
      # SQL body contains a `SET app.x = ...`. Update is in-place;
      # reads of `@guc_state.state_hash` are O(1).
      @guc_state.observe_sql(sql) if sql.is_a?(String)

      # Transaction tracking
      if GoldLapel::TX_START.match?(sql)
        @in_transaction = true
        return delegate(method, sql, params, result_format, &block)
      end
      if GoldLapel::TX_END.match?(sql)
        @in_transaction = false
        return delegate(method, sql, params, result_format, &block)
      end

      # Write detection + self-invalidation
      write_table = GoldLapel.detect_write(sql)
      if write_table
        if write_table == GoldLapel::DDL_SENTINEL
          @cache.invalidate_all
        else
          @cache.invalidate_table(write_table)
        end
        return delegate(method, sql, params, result_format, &block)
      end

      # Inside transaction: bypass cache
      return delegate(method, sql, params, result_format, &block) if @in_transaction

      # Read path: check L1 cache. Pass the per-connection state hash
      # so cache slots never cross connections with different unsafe
      # GUC state.
      sh = @guc_state.state_hash
      entry = @cache.get(sql, params, sh)
      if entry
        result = CachedResult.new(entry[:values], entry[:fields])
        if block
          block.call(result)
          return result
        end
        return result
      end

      # Cache miss: execute WITHOUT block so we can cache before PG clears the result
      result = delegate(method, sql, params, result_format)

      # Cache the result if it has rows
      if result && result.respond_to?(:values) && result.respond_to?(:fields)
        @cache.put(sql, params, result.values, result.fields, sh)
      end

      # Yield to block if provided (matching PG gem's block API)
      if block
        block.call(result)
        result.clear if result.respond_to?(:clear) && !result.is_a?(CachedResult)
      end

      result
    end

    def delegate(method, sql, params, result_format, &block)
      case method
      when :exec
        @real.exec(sql, &block)
      when :async_exec
        @real.async_exec(sql, &block)
      when :exec_params
        if result_format
          @real.exec_params(sql, params, result_format, &block)
        else
          @real.exec_params(sql, params, &block)
        end
      when :async_exec_params
        if result_format
          @real.async_exec_params(sql, params, result_format, &block)
        else
          @real.async_exec_params(sql, params, &block)
        end
      end
    end
  end
end
