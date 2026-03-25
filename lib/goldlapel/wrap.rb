# frozen_string_literal: true

require_relative "cache"

module GoldLapel
  def self.wrap(conn, invalidation_port: nil)
    cache = NativeCache.instance
    invalidation_port ||= detect_invalidation_port
    cache.connect_invalidation(invalidation_port) unless cache.connected?
    CachedConnection.new(conn, cache)
  end

  def self.detect_invalidation_port
    instances = Proxy.instances rescue {}
    if instances.any?
      inst = instances.values.first
      port = inst.port || DEFAULT_PORT
      config = inst.config || {}
      Integer(config[:invalidation_port] || config["invalidation_port"] || (port + 2))
    else
      DEFAULT_PORT + 2
    end
  end

  class CachedConnection
    def initialize(real_conn, cache)
      @real = real_conn
      @cache = cache
      @in_transaction = false
    end

    def exec(sql, &block)
      handle_query(sql, nil, :exec, block)
    end
    alias_method :query, :exec
    alias_method :async_exec, :exec

    def exec_params(sql, params = [], result_format = 0, &block)
      handle_query(sql, params, :exec_params, block, result_format)
    end
    alias_method :async_exec_params, :exec_params

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

      # Read path: check L1 cache
      entry = @cache.get(sql, params)
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
        @cache.put(sql, params, result.values, result.fields)
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
      when :exec_params
        if result_format
          @real.exec_params(sql, params, result_format, &block)
        else
          @real.exec_params(sql, params, &block)
        end
      end
    end
  end
end
