# frozen_string_literal: true

# Async variant of the Goldlapel wrapper, built on the `async` gem.
#
# Usage:
#   require "goldlapel/async"
#   require "async"
#
#   Async do
#     gl = Goldlapel::Async.start("postgresql://user:pass@localhost/mydb")
#     hits = gl.search("articles", "body", "postgres tuning")
#     gl.stop
#   end
#
# Implementation notes (v0.2.0):
#
# This first cut provides the factory API shape and fiber-scheduler-friendly
# behavior, but internally the wrapper methods still issue synchronous PG
# calls. When run inside an `async` reactor, Ruby's default fiber scheduler
# (via the `async` gem) yields during IO, so other fibers keep running; you
# get cooperative concurrency without blocking the reactor, even though the
# underlying calls are not native-async.
#
# Native async (via async-pg) is planned for v0.2.1.

begin
  require "async"
rescue LoadError
  raise LoadError,
    "`Goldlapel::Async` requires the `async` gem. " \
    "Add `gem \"async\"` to your Gemfile, or `gem install async`."
end

require_relative "../goldlapel"

module GoldLapel
  module Async
    # Factory — start a proxy + internal connection inside an async reactor.
    # Must be called from within an `Async do ... end` block (or equivalent).
    def self.start(upstream, port: nil, log_level: nil, config: {}, extra_args: [])
      unless ::Async::Task.current?
        raise "Goldlapel::Async.start must be called inside an Async { ... } block"
      end
      GoldLapel.start(upstream, port: port, log_level: log_level, config: config, extra_args: extra_args)
    end
  end
end

Goldlapel = GoldLapel unless defined?(Goldlapel)
