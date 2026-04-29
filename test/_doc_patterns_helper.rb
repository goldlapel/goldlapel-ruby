# frozen_string_literal: true

# Shared helper for util-level doc_* tests. Phase 4 of schema-to-core requires
# every util call to receive a `patterns:` kwarg from the proxy's DDL API.
# Production code goes through `gl.documents.<verb>(...)` which fetches them;
# util-level unit tests don't have a proxy.
#
# This helper monkey-patches `GoldLapel.doc_*` so that a missing `patterns:`
# kwarg is auto-filled with a stub that pretends the user's `collection`
# argument is the canonical proxy table. The resulting SQL still reads
# `INSERT INTO users …`, matching every pre-Phase-4 assertion verbatim.
#
# This keeps the existing test bodies unchanged — they continue to assert
# wrapper-side SQL shape without having to fabricate proxy fixtures.

require_relative "../lib/goldlapel/utils"

module DocPatternsAutoInject
  def self.stub_patterns(collection)
    {
      tables: { "main" => collection.to_s },
      query_patterns: {},
    }
  end

  # Originals captured pre-monkey-patch so tests can verify the bare util
  # behavior (e.g. that calling without `patterns:` raises) without rebuilding
  # the load chain.
  ORIGINALS = {}
  ASYNC_ORIGINALS = {}
end

# Methods that take (conn, collection, ...) followed by a `patterns:` kwarg.
# When the test invokes them without `patterns:`, we synthesize one keyed off
# the collection name. doc_create_collection's no-op path also needs this
# stub so callers exercising it directly get a deterministic result.
[
  :doc_insert,
  :doc_insert_many,
  :doc_find,
  :doc_find_cursor,
  :doc_find_one,
  :doc_update,
  :doc_update_one,
  :doc_delete,
  :doc_delete_one,
  :doc_count,
  :doc_find_one_and_update,
  :doc_find_one_and_delete,
  :doc_distinct,
  :doc_create_index,
  :doc_aggregate,
  :doc_watch,
  :doc_unwatch,
  :doc_create_ttl_index,
  :doc_remove_ttl_index,
  :doc_create_capped,
  :doc_remove_cap,
  :doc_create_collection,
].each do |sym|
  next unless GoldLapel.respond_to?(sym)
  original = GoldLapel.method(sym)
  DocPatternsAutoInject::ORIGINALS[sym] = original
  GoldLapel.singleton_class.send(:define_method, sym) do |*args, **kwargs, &block|
    if kwargs[:patterns].nil? && args.length >= 2
      kwargs = kwargs.merge(patterns: DocPatternsAutoInject.stub_patterns(args[1]))
    end
    original.call(*args, **kwargs, &block)
  end
end

# Mirror for the async utils module if it's loaded.
if defined?(GoldLapel::Async::Utils)
  [
    :doc_insert,
    :doc_insert_many,
    :doc_find,
    :doc_find_cursor,
    :doc_find_one,
    :doc_update,
    :doc_update_one,
    :doc_delete,
    :doc_delete_one,
    :doc_count,
    :doc_find_one_and_update,
    :doc_find_one_and_delete,
    :doc_distinct,
    :doc_create_index,
    :doc_aggregate,
    :doc_watch,
    :doc_unwatch,
    :doc_create_ttl_index,
    :doc_remove_ttl_index,
    :doc_create_capped,
    :doc_remove_cap,
    :doc_create_collection,
  ].each do |sym|
    next unless GoldLapel::Async::Utils.respond_to?(sym)
    original = GoldLapel::Async::Utils.method(sym)
    DocPatternsAutoInject::ASYNC_ORIGINALS[sym] = original
    GoldLapel::Async::Utils.singleton_class.send(:define_method, sym) do |*args, **kwargs, &block|
      if kwargs[:patterns].nil? && args.length >= 2
        kwargs = kwargs.merge(patterns: DocPatternsAutoInject.stub_patterns(args[1]))
      end
      original.call(*args, **kwargs, &block)
    end
  end
end
