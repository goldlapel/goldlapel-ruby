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
# Implementation notes (v0.2.0):
#
# This first cut provides the factory API shape only: `GoldLapel::Async.start`
# returns an ordinary `GoldLapel::Instance`, and its wrapper methods are
# callable from inside fiber tasks. The public API is stable.
#
# HONEST CAVEAT — v0.2.0 does NOT cooperatively yield during Postgres IO.
# Wrapper methods delegate to the sync implementation, which uses `PG.connect`
# and `conn.exec_params`. Those are blocking C calls: they do not invoke
# Ruby's fiber scheduler, so while a query is in flight on one fiber the
# reactor's thread is parked and other fibers cannot run. Inside an
# `Async { ... }` block you get the API shape today, not true non-blocking IO.
#
# Native non-blocking IO (via `async-pg` or `pg`'s `async_exec_params` +
# `socket_io.wait_readable`) is planned for a later release. When that lands,
# only the internals change — code written against this API keeps working.

begin
  require "async"
rescue LoadError
  raise LoadError,
    "`GoldLapel::Async` requires the `async` gem. " \
    "Add `gem \"async\"` to your Gemfile, or `gem install async`."
end

require_relative "../goldlapel"

module GoldLapel
  module Async
    # Factory — start a proxy + internal connection inside an async reactor.
    # Must be called from within an `Async do ... end` block (or equivalent).
    def self.start(upstream, port: nil, log_level: nil, config: {}, extra_args: [], silent: false)
      unless ::Async::Task.current?
        raise "GoldLapel::Async.start must be called inside an Async { ... } block"
      end
      GoldLapel.start(upstream, port: port, log_level: log_level, config: config, extra_args: extra_args, silent: silent)
    end
  end
end
