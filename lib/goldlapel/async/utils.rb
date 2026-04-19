# frozen_string_literal: true

require "json"

module GoldLapel
  module Async
    # Native-async sibling of `GoldLapel::Utils`.
    #
    # Every function here mirrors a sync utility in `lib/goldlapel/utils.rb`
    # but calls `pg`'s native non-blocking method variants:
    #
    #   exec_params         → async_exec_params
    #   exec                → async_exec
    #   wait_for_notify     → still `wait_for_notify` (fiber-scheduler aware)
    #
    # These are libpq's non-blocking primitives. Same parameter binding, same
    # `PG::Result` return types, same error classes — only the blocking
    # behavior changes. Under an Async reactor they yield cooperatively via
    # Ruby's Fiber scheduler; outside a reactor they still function (blocking).
    #
    # Architectural intent: the call sites say `async_exec_params` so the
    # async code path is honest about its IO contract, instead of relying on
    # sync `exec_params` + scheduler-magic to yield.
    #
    # This file is intentionally a parallel layer to `Utils` — we do NOT
    # dispatch on connection type from `Utils`, because keeping the two call
    # graphs independent makes each one readable on its own.
    module Utils
    end
  end
end
