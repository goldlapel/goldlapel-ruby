# frozen_string_literal: true

require "goldlapel/ddl"

module GoldLapel
  module Async
    # Async sibling of `GoldLapel::DocumentsAPI`. Same shape (verb methods,
    # back-reference to the parent client, shared DDL pattern cache) — every
    # call routes through `GoldLapel::Async::Utils.doc_*` instead of the sync
    # `GoldLapel.doc_*` so the underlying SQL uses pg's async_exec_params.
    #
    # See goldlapel/documents.rb for the rationale; only the Utils module
    # being dispatched to differs.
    class DocumentsAPI
      def initialize(gl)
        @gl = gl
      end

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

      def create_collection(collection, unlogged: false)
        _patterns(collection, unlogged: unlogged)
        nil
      end

      # -- CRUD ----------------------------------------------------------------

      def insert(collection, document, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_insert(@gl.send(:_resolve_conn, conn), collection, document, patterns: patterns)
      end

      def insert_many(collection, documents, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_insert_many(@gl.send(:_resolve_conn, conn), collection, documents, patterns: patterns)
      end

      def find(collection, filter: nil, sort: nil, limit: nil, skip: nil, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_find(
          @gl.send(:_resolve_conn, conn), collection,
          filter: filter, sort: sort, limit: limit, skip: skip, patterns: patterns,
        )
      end

      def find_cursor(collection, filter: nil, sort: nil, limit: nil, skip: nil, batch_size: 100, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_find_cursor(
          @gl.send(:_resolve_conn, conn), collection,
          filter: filter, sort: sort, limit: limit, skip: skip, batch_size: batch_size,
          patterns: patterns,
        )
      end

      def find_one(collection, filter: nil, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_find_one(@gl.send(:_resolve_conn, conn), collection, filter: filter, patterns: patterns)
      end

      def update(collection, filter, update, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_update(@gl.send(:_resolve_conn, conn), collection, filter, update, patterns: patterns)
      end

      def update_one(collection, filter, update, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_update_one(@gl.send(:_resolve_conn, conn), collection, filter, update, patterns: patterns)
      end

      def delete(collection, filter, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_delete(@gl.send(:_resolve_conn, conn), collection, filter, patterns: patterns)
      end

      def delete_one(collection, filter, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_delete_one(@gl.send(:_resolve_conn, conn), collection, filter, patterns: patterns)
      end

      def find_one_and_update(collection, filter, update, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_find_one_and_update(@gl.send(:_resolve_conn, conn), collection, filter, update, patterns: patterns)
      end

      def find_one_and_delete(collection, filter, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_find_one_and_delete(@gl.send(:_resolve_conn, conn), collection, filter, patterns: patterns)
      end

      def distinct(collection, field, filter: nil, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_distinct(@gl.send(:_resolve_conn, conn), collection, field, filter: filter, patterns: patterns)
      end

      def count(collection, filter: nil, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_count(@gl.send(:_resolve_conn, conn), collection, filter: filter, patterns: patterns)
      end

      def create_index(collection, keys: nil, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_create_index(@gl.send(:_resolve_conn, conn), collection, keys: keys, patterns: patterns)
      end

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
        Utils.doc_aggregate(
          @gl.send(:_resolve_conn, conn), collection, pipeline,
          patterns: patterns, lookup_tables: lookup_tables,
        )
      end

      # -- Watch / TTL / capped ------------------------------------------------

      def watch(collection, conn: nil, &block)
        patterns = _patterns(collection)
        Utils.doc_watch(@gl.send(:_resolve_conn, conn), collection, patterns: patterns, &block)
      end

      def unwatch(collection, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_unwatch(@gl.send(:_resolve_conn, conn), collection, patterns: patterns)
      end

      def create_ttl_index(collection, field, expire_after_seconds:, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_create_ttl_index(
          @gl.send(:_resolve_conn, conn), collection, field,
          expire_after_seconds: expire_after_seconds, patterns: patterns,
        )
      end

      def remove_ttl_index(collection, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_remove_ttl_index(@gl.send(:_resolve_conn, conn), collection, patterns: patterns)
      end

      def create_capped(collection, max:, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_create_capped(@gl.send(:_resolve_conn, conn), collection, max: max, patterns: patterns)
      end

      def remove_cap(collection, conn: nil)
        patterns = _patterns(collection)
        Utils.doc_remove_cap(@gl.send(:_resolve_conn, conn), collection, patterns: patterns)
      end
    end
  end
end
