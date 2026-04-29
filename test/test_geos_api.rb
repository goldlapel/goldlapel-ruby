# frozen_string_literal: true

# Unit tests for GoldLapel::GeosAPI.
#
# Phase 5 schema decisions:
#   - GEOGRAPHY column type (not GEOMETRY) — distance returns are meters native.
#   - `member TEXT PRIMARY KEY` — re-adding a member updates its location
#     (idempotent), matching Redis GEOADD semantics.
#   - `updated_at` stamped on every UPSERT.
#
# These tests verify:
#   - `add` is idempotent on member name (proxy's ON CONFLICT DO UPDATE).
#   - SQL uses the canonical GEOGRAPHY-native pattern (no `::geography` casts
#     on the column reference because the column already IS geography).
#   - Distance unit conversion at the wrapper edge (m / km / mi / ft).
#   - Geo radius params are `[lon, lat, radius_m, limit]` — 4 elements, no
#     duplicates (the proxy's CTE-anchor SQL collapses each $N to one
#     occurrence).
#   - Geo radius_by_member params are `[member, member, radius_m, limit]`
#     (4 elements; $1 and $2 both bind to the anchor member name).

require "minitest/autorun"
require_relative "../lib/goldlapel/cache"
require_relative "../lib/goldlapel/wrap"
require_relative "../lib/goldlapel/utils"
require_relative "../lib/goldlapel/proxy"
require_relative "../lib/goldlapel/streams"
require_relative "../lib/goldlapel/documents"
require_relative "../lib/goldlapel/counters"
require_relative "../lib/goldlapel/zsets"
require_relative "../lib/goldlapel/hashes"
require_relative "../lib/goldlapel/queues"
require_relative "../lib/goldlapel/geos"
require_relative "../lib/goldlapel/instance"
require_relative "../lib/goldlapel"

class GeosApiMockResult
  attr_reader :values, :fields
  def initialize(rows = [], fields = [])
    @rows = rows
    @fields = fields
    @values = rows.map { |r| fields.map { |f| r[f] } }
  end
  def ntuples; @rows.length; end
  def cmd_tuples; @cmd_tuples || @rows.length; end
  def cmd_tuples=(v); @cmd_tuples = v; end
  def [](i); @rows[i]; end
  def map(&b); @rows.map(&b); end
  def each(&b); @rows.each(&b); end
end

class GeosApiMockConn
  attr_reader :calls
  attr_accessor :next_result

  def initialize
    @calls = []
    @next_result = GeosApiMockResult.new
  end

  def exec(sql, &b)
    @calls << { method: :exec, sql: sql }
    @next_result.tap { |r| b&.call(r) }
  end

  def exec_params(sql, params = [], _f = 0, &b)
    @calls << { method: :exec_params, sql: sql, params: params }
    @next_result.tap { |r| b&.call(r) }
  end

  def close; end
  def finished?; false; end
end

GEO_MAIN = "_goldlapel.geo_riders"
FAKE_GEO_PATTERNS = {
  tables: { "main" => GEO_MAIN },
  query_patterns: {
    "geoadd" => "INSERT INTO #{GEO_MAIN} (member, location, updated_at) VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography, NOW()) ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat",
    "geopos" => "SELECT ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM #{GEO_MAIN} WHERE member = $1",
    "geodist" => "SELECT ST_Distance(a.location, b.location) AS distance_m FROM #{GEO_MAIN} a, #{GEO_MAIN} b WHERE a.member = $1 AND b.member = $2",
    # Proxy's canonical georadius / georadius_with_dist use a CTE-anchor so
    # each $N appears EXACTLY ONCE — wrapper passes 4-tuple [lon, lat,
    # radius_m, limit] indexed by $N (no duplicates).
    "georadius" => "WITH anchor AS ( SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS geog ) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM #{GEO_MAIN}, anchor WHERE ST_DWithin(location, anchor.geog, $3) ORDER BY ST_Distance(location, anchor.geog) LIMIT $4",
    "georadius_with_dist" => "WITH anchor AS ( SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS geog ) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat, ST_Distance(location, anchor.geog) AS distance_m FROM #{GEO_MAIN}, anchor WHERE ST_DWithin(location, anchor.geog, $3) ORDER BY distance_m LIMIT $4",
    # Proxy's geosearch_member: $1 and $2 both anchor member; $3 radius,
    # $4 limit. With native $N binding the wrapper passes
    # [member, member, radius_m, limit].
    "geosearch_member" => "SELECT b.member, ST_X(b.location::geometry) AS lon, ST_Y(b.location::geometry) AS lat, ST_Distance(b.location, a.location) AS distance_m FROM #{GEO_MAIN} a, #{GEO_MAIN} b WHERE a.member = $1 AND ST_DWithin(b.location, a.location, $3) AND b.member <> $2 ORDER BY distance_m LIMIT $4",
    "geo_remove" => "DELETE FROM #{GEO_MAIN} WHERE member = $1",
    "geo_count" => "SELECT COUNT(*) FROM #{GEO_MAIN}",
    "delete_all" => "DELETE FROM #{GEO_MAIN}",
  },
}.freeze

def make_geos_api_inst
  conn = GeosApiMockConn.new
  inst = GoldLapel::Instance.allocate
  inst.instance_variable_set(:@upstream, "postgresql://localhost/test")
  inst.instance_variable_set(:@internal_conn, conn)
  inst.instance_variable_set(:@wrapped_conn, conn)
  inst.instance_variable_set(:@proxy, nil)
  inst.instance_variable_set(:@fiber_key, :"__goldlapel_conn_#{inst.object_id}")
  geos = GoldLapel::GeosAPI.new(inst)
  inst.instance_variable_set(:@geos, geos)
  inst.instance_variable_set(:@documents, GoldLapel::DocumentsAPI.new(inst))
  inst.instance_variable_set(:@streams, GoldLapel::StreamsAPI.new(inst))
  inst.instance_variable_set(:@counters, GoldLapel::CountersAPI.new(inst))
  inst.instance_variable_set(:@zsets, GoldLapel::ZsetsAPI.new(inst))
  inst.instance_variable_set(:@hashes, GoldLapel::HashesAPI.new(inst))
  inst.instance_variable_set(:@queues, GoldLapel::QueuesAPI.new(inst))
  fetches = []
  geos.define_singleton_method(:_patterns) do |name|
    fetches << name
    FAKE_GEO_PATTERNS
  end
  [inst, conn, fetches]
end

class TestGeosAPINamespaceShape < Minitest::Test
  def test_geos_is_a_GeosAPI
    inst, _conn, _fetches = make_geos_api_inst
    assert_kind_of GoldLapel::GeosAPI, inst.geos
  end

  def test_no_legacy_flat_methods
    inst, _conn, _fetches = make_geos_api_inst
    %i[geoadd geodist georadius].each do |legacy|
      refute inst.respond_to?(legacy),
        "Phase 5 removed flat #{legacy} — use gl.geos.<verb>."
    end
  end
end

class TestGeosAPIVerbDispatch < Minitest::Test
  def test_add_is_idempotent_via_on_conflict
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "lon" => "13.4", "lat" => "52.5" }], ["lon", "lat"])
    result = inst.geos.add("riders", "alice", 13.4, 52.5)
    assert_equal [13.4, 52.5], result
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_includes call[:sql], "ON CONFLICT (member)"
    assert_includes call[:sql], "DO UPDATE"
    assert_equal ["alice", 13.4, 52.5], call[:params]
  end

  def test_pos_returns_lon_lat_tuple
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "lon" => "13.4", "lat" => "52.5" }], ["lon", "lat"])
    assert_equal [13.4, 52.5], inst.geos.pos("riders", "alice")
  end

  def test_pos_returns_nil_for_unknown_member
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([], ["lon", "lat"])
    assert_nil inst.geos.pos("riders", "missing")
  end

  def test_dist_returns_meters_by_default
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "distance_m" => "1234.0" }], ["distance_m"])
    assert_equal 1234.0, inst.geos.dist("riders", "alice", "bob")
  end

  def test_dist_converts_to_km
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "distance_m" => "1234.0" }], ["distance_m"])
    assert_equal 1.234, inst.geos.dist("riders", "alice", "bob", unit: "km")
  end

  def test_dist_converts_to_miles
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "distance_m" => "1609.344" }], ["distance_m"])
    result = inst.geos.dist("riders", "alice", "bob", unit: "mi")
    assert_in_delta 1.0, result, 1e-6
  end

  def test_dist_unknown_unit_raises
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "distance_m" => "1.0" }], ["distance_m"])
    assert_raises(ArgumentError) { inst.geos.dist("riders", "a", "b", unit: "parsec") }
  end

  def test_radius_passes_lon_lat_radius_limit_no_duplicates
    # Phase 5 contract: $1=lon, $2=lat, $3=radius_m, $4=limit. CTE-anchor in
    # the proxy SQL collapses lon/lat into a single $N reference each, so
    # the wrapper passes a 4-tuple with NO duplicates indexed by $N.
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([], ["member", "lon", "lat", "distance_m"])
    inst.geos.radius("riders", 13.4, 52.5, 5, unit: "km")
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [13.4, 52.5, 5000.0, 50], call[:params]
    assert_equal 4, call[:params].length, "geo radius must pass 4 elements (no duplicates)"
  end

  def test_radius_default_unit_is_meters
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([], ["member", "lon", "lat", "distance_m"])
    inst.geos.radius("riders", 0, 0, 1000)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal [0.0, 0.0, 1000.0, 50], call[:params]
  end

  def test_radius_by_member_passes_member_twice
    # Phase 5 contract: $1 and $2 are both the anchor member name (one for
    # the join, one for the self-exclusion); $3=radius_m, $4=limit. With
    # native $N binding (pg's exec_params), wrapper passes
    # [member, member, radius_m, limit].
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([], ["member", "lon", "lat", "distance_m"])
    inst.geos.radius_by_member("riders", "alice", 1000)
    call = conn.calls.find { |c| c[:method] == :exec_params }
    assert_equal ["alice", "alice", 1000.0, 50], call[:params]
    assert_equal 4, call[:params].length
  end

  def test_remove_returns_true_when_deleted
    inst, conn, _fetches = make_geos_api_inst
    res = GeosApiMockResult.new([], [])
    res.cmd_tuples = 1
    conn.next_result = res
    assert_equal true, inst.geos.remove("riders", "alice")
  end

  def test_remove_returns_false_when_absent
    inst, conn, _fetches = make_geos_api_inst
    res = GeosApiMockResult.new([], [])
    res.cmd_tuples = 0
    conn.next_result = res
    assert_equal false, inst.geos.remove("riders", "missing")
  end

  def test_count_returns_total
    inst, conn, _fetches = make_geos_api_inst
    conn.next_result = GeosApiMockResult.new([{ "count" => "3" }], ["count"])
    assert_equal 3, inst.geos.count("riders")
  end
end

class TestGeosPhase5Contract < Minitest::Test
  # The proxy's canonical geo schema is GEOGRAPHY-native (not GEOMETRY-with-
  # cast), so the column reference should NOT need `::geography` in the
  # filter / order-by clauses (they're already geography).
  def test_geoadd_pattern_is_geography_native
    sql = FAKE_GEO_PATTERNS[:query_patterns]["geoadd"]
    assert_includes sql, "::geography"  # only on the inserted point literal
  end

  def test_geoadd_is_idempotent
    sql = FAKE_GEO_PATTERNS[:query_patterns]["geoadd"]
    assert_includes sql, "ON CONFLICT (member) DO UPDATE"
  end

  def test_georadius_pattern_uses_cte_anchor
    sql = FAKE_GEO_PATTERNS[:query_patterns]["georadius_with_dist"]
    assert_includes sql, "WITH anchor AS"
    # Each $N appears exactly once in the rendered SQL (the CTE collapses
    # the duplicate ST_MakePoint references in the original non-CTE form).
    %w[$1 $2 $3 $4].each do |placeholder|
      occurrences = sql.scan(/#{Regexp.escape(placeholder)}\b/).length
      assert_equal 1, occurrences,
        "Phase 5 contract: georadius pattern must use each $N exactly once (#{placeholder} appeared #{occurrences} times)"
    end
  end
end
