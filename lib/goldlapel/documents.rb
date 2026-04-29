# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  # Documents namespace API — `gl.documents.<verb>(...)`.
  #
  # Wraps the doc-store methods in a sub-API instance held on the parent
  # GoldLapel client. The instance shares all state (license, dashboard
  # token, http session, conn) by reference back to the parent — no
  # duplication.
  #
  # The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call here:
  #
  #   1. Calls /api/ddl/doc_store/create (idempotent) to materialize the
  #      canonical `_goldlapel.doc_<name>` table and pull its query patterns.
  #   2. Caches `(tables, query_patterns)` on the parent GoldLapel instance
  #      for the session's lifetime (one HTTP round-trip per (family, name)
  #      per session).
  #   3. Hands the patterns off to the existing `GoldLapel.doc_*` utility
  #      functions so they execute against the canonical table name instead
  #      of CREATE-ing their own.
  #
  # Sub-API class shape mirrors `GoldLapel::StreamsAPI` — this is the
  # canonical pattern for the wrapper rollout. Other namespaces (cache,
  # search, queues, counters, hashes, zsets, geo, auth, ...) stay flat for
  # now; they migrate to nested form one-at-a-time as their own
  # schema-to-core phase fires.
  class DocumentsAPI
    # Hold a back-reference to the parent client. We never copy lifecycle
    # state (token, port, conn) onto this instance — always read through
    # `@gl` so a config change on the parent (e.g. proxy restart with a new
    # dashboard token) is reflected immediately on the next call.
    def initialize(gl)
      @gl = gl
    end

    # Fetch (and cache) canonical doc-store DDL + query patterns from the
    # proxy. Cache lives on the parent GoldLapel instance — see ddl.rb.
    #
    # `unlogged` is a creation-time option; passed only on the first call
    # for a given (family, name) since proxy `CREATE TABLE IF NOT EXISTS`
    # makes subsequent calls no-op DDL-wise. If a caller flips `unlogged`
    # across calls in the same session, the table's storage type is whatever
    # it was on first create — wrappers don't migrate it.
    def _patterns(collection, unlogged: false)
      GoldLapel._validate_identifier(collection)
      proxy = @gl.instance_variable_get(:@proxy)
      token = (proxy&.dashboard_token) || GoldLapel::DDL.token_from_env_or_file
      port = proxy&.dashboard_port
      options = unlogged ? { "unlogged" => true } : nil
      GoldLapel::DDL.fetch_patterns(
        @gl, "doc_store", collection, port, token, options: options,
      )
    end

    # -- Collection lifecycle ------------------------------------------------

    # Eagerly materialize the doc-store table. Other methods will also
    # materialize on first use, so calling this is optional — provided for
    # callers that want explicit setup at startup time.
    def create_collection(collection, unlogged: false)
      _patterns(collection, unlogged: unlogged)
      nil
    end

    # -- CRUD ----------------------------------------------------------------

    def insert(collection, document, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_insert(@gl.send(:_resolve_conn, conn), collection, document, patterns: patterns)
    end

    def insert_many(collection, documents, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_insert_many(@gl.send(:_resolve_conn, conn), collection, documents, patterns: patterns)
    end

    def find(collection, filter: nil, sort: nil, limit: nil, skip: nil, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_find(
        @gl.send(:_resolve_conn, conn), collection,
        filter: filter, sort: sort, limit: limit, skip: skip,
        patterns: patterns,
      )
    end

    def find_cursor(collection, filter: nil, sort: nil, limit: nil, skip: nil, batch_size: 100, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_find_cursor(
        @gl.send(:_resolve_conn, conn), collection,
        filter: filter, sort: sort, limit: limit, skip: skip, batch_size: batch_size,
        patterns: patterns,
      )
    end

    def find_one(collection, filter: nil, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_find_one(@gl.send(:_resolve_conn, conn), collection, filter: filter, patterns: patterns)
    end

    def update(collection, filter, update, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_update(@gl.send(:_resolve_conn, conn), collection, filter, update, patterns: patterns)
    end

    def update_one(collection, filter, update, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_update_one(@gl.send(:_resolve_conn, conn), collection, filter, update, patterns: patterns)
    end

    def delete(collection, filter, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_delete(@gl.send(:_resolve_conn, conn), collection, filter, patterns: patterns)
    end

    def delete_one(collection, filter, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_delete_one(@gl.send(:_resolve_conn, conn), collection, filter, patterns: patterns)
    end

    def find_one_and_update(collection, filter, update, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_find_one_and_update(@gl.send(:_resolve_conn, conn), collection, filter, update, patterns: patterns)
    end

    def find_one_and_delete(collection, filter, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_find_one_and_delete(@gl.send(:_resolve_conn, conn), collection, filter, patterns: patterns)
    end

    def distinct(collection, field, filter: nil, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_distinct(@gl.send(:_resolve_conn, conn), collection, field, filter: filter, patterns: patterns)
    end

    def count(collection, filter: nil, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_count(@gl.send(:_resolve_conn, conn), collection, filter: filter, patterns: patterns)
    end

    def create_index(collection, keys: nil, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_create_index(@gl.send(:_resolve_conn, conn), collection, keys: keys, patterns: patterns)
    end

    # Run a Mongo-style aggregation pipeline.
    #
    # $lookup.from references are resolved to their canonical proxy tables
    # (`_goldlapel.doc_<name>`) — each unique `from` collection triggers an
    # idempotent describe/create against the proxy and is cached for the
    # session.
    def aggregate(collection, pipeline, conn: nil)
      patterns = _patterns(collection)
      lookup_tables = {}
      if pipeline.is_a?(Array)
        pipeline.each do |stage|
          next unless stage.is_a?(Hash)
          spec = stage["$lookup"] || stage[:$lookup]
          next unless spec.is_a?(Hash)
          from_name = (spec["from"] || spec[:from]).to_s
          next if from_name.empty? || lookup_tables.key?(from_name)
          lp = _patterns(from_name)
          tables = lp[:tables] || lp["tables"]
          lookup_tables[from_name] = tables && (tables["main"] || tables[:main])
        end
      end
      GoldLapel.doc_aggregate(
        @gl.send(:_resolve_conn, conn), collection, pipeline,
        patterns: patterns, lookup_tables: lookup_tables,
      )
    end

    # -- Watch / TTL / capped ------------------------------------------------

    def watch(collection, conn: nil, &block)
      patterns = _patterns(collection)
      GoldLapel.doc_watch(@gl.send(:_resolve_conn, conn), collection, patterns: patterns, &block)
    end

    def unwatch(collection, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_unwatch(@gl.send(:_resolve_conn, conn), collection, patterns: patterns)
    end

    def create_ttl_index(collection, field, expire_after_seconds:, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_create_ttl_index(
        @gl.send(:_resolve_conn, conn), collection, field,
        expire_after_seconds: expire_after_seconds, patterns: patterns,
      )
    end

    def remove_ttl_index(collection, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_remove_ttl_index(@gl.send(:_resolve_conn, conn), collection, patterns: patterns)
    end

    def create_capped(collection, max:, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_create_capped(@gl.send(:_resolve_conn, conn), collection, max: max, patterns: patterns)
    end

    def remove_cap(collection, conn: nil)
      patterns = _patterns(collection)
      GoldLapel.doc_remove_cap(@gl.send(:_resolve_conn, conn), collection, patterns: patterns)
    end
  end
end
