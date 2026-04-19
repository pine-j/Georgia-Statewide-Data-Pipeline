"""Synthetic-geometry unit tests for admin_breakpoints.

Run:
    python test_admin_breakpoints.py

These exercise the algorithm in isolation without touching any real
boundary or roadway data. They are safe to run without writing to
02-Data-Staging/spatial or 02-Data-Staging/tables.
"""

from __future__ import annotations

import json
import math
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Polygon

sys.path.insert(0, str(Path(__file__).parent))

import admin_breakpoints as ab  # noqa: E402


CRS = "EPSG:32617"


def _make_crosser(name, fields, geoms, attr_map):
    gdf = gpd.GeoDataFrame(fields, geometry=geoms, crs=CRS)
    return ab.BoundaryCrosser(name=name, gdf=gdf, attribute_cols=attr_map)


def test_single_crossing_between_two_polygons() -> None:
    """A horizontal line crossing the shared x=1 edge should produce one
    milepoint at 0.5 and stamp different attrs on each side."""
    left = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    right = Polygon([(1, 0), (2, 0), (2, 1), (1, 1)])
    crosser = _make_crosser(
        "county",
        {"COUNTYFP": ["001", "003"], "NAME": ["Appling", "Bacon"]},
        [left, right],
        {"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
    )

    route = LineString([(0.5, 0.5), (1.5, 0.5)])
    crossings = ab.compute_route_crossings(route, 0.0, 1.0, [crosser])
    assert crossings == [0.5], f"expected [0.5], got {crossings}"

    left_attrs = ab.resolve_segment_admin_attrs(
        LineString([(0.5, 0.5), (1.0, 0.5)]), [crosser]
    )
    assert left_attrs == {"COUNTY_CODE": "001", "COUNTY_NAME": "Appling"}, left_attrs

    right_attrs = ab.resolve_segment_admin_attrs(
        LineString([(1.0, 0.5), (1.5, 0.5)]), [crosser]
    )
    assert right_attrs == {"COUNTY_CODE": "003", "COUNTY_NAME": "Bacon"}, right_attrs


def test_no_crossing_when_route_inside_single_polygon() -> None:
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    crosser = _make_crosser(
        "county",
        {"COUNTYFP": ["001"], "NAME": ["Appling"]},
        [poly],
        {"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
    )
    crossings = ab.compute_route_crossings(
        LineString([(0.2, 0.5), (0.8, 0.5)]), 0.0, 1.0, [crosser]
    )
    assert crossings == [], f"expected [], got {crossings}"


def test_segment_outside_all_polygons_returns_none_attrs() -> None:
    """Rural segment outside every MPO should yield None for mpo attrs."""
    mpo = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    crosser = _make_crosser(
        "mpo",
        {"MPO_ID": ["ATL"], "MPO_NAME": ["Atlanta"]},
        [mpo],
        {"MPO_ID": "MPO_ID", "MPO_NAME": "MPO_NAME"},
    )
    outside = LineString([(2.0, 0.5), (3.0, 0.5)])
    attrs = ab.resolve_segment_admin_attrs(outside, [crosser])
    assert attrs == {"MPO_ID": None, "MPO_NAME": None}, attrs


def test_two_crossers_union_milepoints() -> None:
    """Two crossers whose shared boundary sits at different x positions
    should contribute two distinct milepoints."""
    counties_left = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    counties_right = Polygon([(1, 0), (3, 0), (3, 1), (1, 1)])
    county_crosser = _make_crosser(
        "county",
        {"COUNTYFP": ["001", "003"], "NAME": ["A", "B"]},
        [counties_left, counties_right],
        {"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
    )

    district_left = Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])
    district_right = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
    district_crosser = _make_crosser(
        "district",
        {"GDOT_DISTRICT": [1, 2], "DISTRICT_NAME": ["One", "Two"]},
        [district_left, district_right],
        {"GDOT_DISTRICT": "DISTRICT", "DISTRICT_NAME": "DISTRICT_NAME"},
    )

    # Route from x=0 to x=3 along y=0.5; crosses county line at x=1, district
    # line at x=2. Milepoint range [0, 3].
    route = LineString([(0, 0.5), (3, 0.5)])
    crossings = ab.compute_route_crossings(route, 0.0, 3.0, [county_crosser, district_crosser])
    assert crossings == [1.0, 2.0], f"expected [1.0, 2.0], got {crossings}"


def test_coincident_boundaries_dedupe() -> None:
    """Area-office boundary coinciding exactly with county boundary should
    contribute only one milepoint (set deduplicates)."""
    left = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    right = Polygon([(1, 0), (2, 0), (2, 1), (1, 1)])
    county_crosser = _make_crosser(
        "county",
        {"COUNTYFP": ["001", "003"], "NAME": ["A", "B"]},
        [left, right],
        {"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
    )
    area_crosser = _make_crosser(
        "area_office",
        {"AREA_OFFICE_ID": [101, 102], "AREA_OFFICE_NAME": ["X", "Y"]},
        [left, right],
        {"AREA_OFFICE_ID": "AREA_OFFICE_ID", "AREA_OFFICE_NAME": "AREA_OFFICE_NAME"},
    )
    route = LineString([(0.25, 0.5), (1.75, 0.5)])
    crossings = ab.compute_route_crossings(route, 0.0, 1.5, [county_crosser, area_crosser])
    expected = 0.75  # (1.0 - 0.25) / (1.75 - 0.25) * 1.5 = 0.75
    assert len(crossings) == 1, f"expected single merged crossing, got {crossings}"
    assert math.isclose(crossings[0], expected, abs_tol=1e-4), crossings


def test_non_linear_milepoint_projection() -> None:
    """Milepoint math should respect the route_span ratio, not the raw
    distance. A route with route_start=100, route_end=105, length=500m
    geometry -> halfway crossing is at milepoint 102.5."""
    left = Polygon([(0, 0), (500, 0), (500, 1), (0, 1)])
    right = Polygon([(500, 0), (1000, 0), (1000, 1), (500, 1)])
    crosser = _make_crosser(
        "county",
        {"COUNTYFP": ["001", "003"], "NAME": ["A", "B"]},
        [left, right],
        {"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
    )
    route = LineString([(0, 0.5), (1000, 0.5)])  # 1000m long
    crossings = ab.compute_route_crossings(route, 100.0, 105.0, [crosser])
    assert crossings == [102.5], f"expected [102.5], got {crossings}"


def test_geometry_collection_row_is_unwrapped() -> None:
    """A GeometryCollection row containing a polygon + a stray line
    (what gpd.overlay with keep_geom_type=False can emit at shared
    edges) should be unwrapped so `covers` still works."""
    from shapely.geometry import GeometryCollection
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    stray_line = LineString([(0, 0), (1, 1)])
    mixed = GeometryCollection([poly, stray_line])
    crosser = _make_crosser(
        "mpo",
        {"MPO_ID": ["ATL"], "MPO_NAME": ["Atlanta"]},
        [mixed],
        {"MPO_ID": "MPO_ID", "MPO_NAME": "MPO_NAME"},
    )
    # After unwrap the tree holds the Polygon; a point inside it should match.
    inside = LineString([(0.25, 0.5), (0.75, 0.5)])
    attrs = ab.resolve_segment_admin_attrs(inside, [crosser])
    assert attrs == {"MPO_ID": "ATL", "MPO_NAME": "Atlanta"}, attrs


def test_overlay_majority_by_length_picks_longest() -> None:
    """apply_admin_overlay_flags picks the winner by overlap length and
    stamps legislative districts unconditionally."""
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    # House districts: two polygons split at x=2. A segment from x=0 to
    # x=10 overlaps district 1 for length 2 and district 2 for length 8.
    # Winner = district 2.
    house_a = Polygon([(0, 0), (2, 0), (2, 10), (0, 10)])
    house_b = Polygon([(2, 0), (10, 0), (10, 10), (2, 10)])
    house = gpd.GeoDataFrame(
        {"STATE_HOUSE_DISTRICT": [1, 2]},
        geometry=[house_a, house_b],
        crs=CRS,
    )
    senate = gpd.GeoDataFrame(
        {"STATE_SENATE_DISTRICT": [50]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    cong = gpd.GeoDataFrame(
        {"CONGRESSIONAL_DISTRICT": [5]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    # No cities covering the segment at all -> unincorporated.
    cities = gpd.GeoDataFrame(
        {"CITY_ID": pd.array([], dtype="Int64"), "Name": []},
        geometry=[],
        crs=CRS,
    )

    segment = LineString([(0, 5), (10, 5)])  # length 10
    segments = gpd.GeoDataFrame(
        {"unique_id": ["R1_0.0000_1.0000"]},
        geometry=[segment],
        crs=CRS,
    )
    out = normalize.apply_admin_overlay_flags(
        segments,
        house_boundaries=house,
        senate_boundaries=senate,
        congressional_boundaries=cong,
        city_boundaries=cities,
    )
    assert int(out["STATE_HOUSE_DISTRICT"].iloc[0]) == 2, out["STATE_HOUSE_DISTRICT"].iloc[0]
    assert int(out["STATE_SENATE_DISTRICT"].iloc[0]) == 50, out["STATE_SENATE_DISTRICT"].iloc[0]
    assert int(out["CONGRESSIONAL_DISTRICT"].iloc[0]) == 5, out["CONGRESSIONAL_DISTRICT"].iloc[0]
    # Unincorporated - no cities intersect
    assert pd.isna(out["CITY_ID"].iloc[0]), out["CITY_ID"].iloc[0]
    assert out["CITY_NAME"].iloc[0] is None or pd.isna(out["CITY_NAME"].iloc[0])


def test_overlay_city_50pct_threshold() -> None:
    """City winner stamps only when its share of the segment is >= 50%."""
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    house = gpd.GeoDataFrame(
        {"STATE_HOUSE_DISTRICT": [1]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    senate = gpd.GeoDataFrame(
        {"STATE_SENATE_DISTRICT": [1]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    cong = gpd.GeoDataFrame(
        {"CONGRESSIONAL_DISTRICT": [1]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    # One city covers the segment from x=0 to x=3 (30% coverage)
    small_city = Polygon([(0, 0), (3, 0), (3, 10), (0, 10)])
    cities = gpd.GeoDataFrame(
        {"CITY_ID": [100], "Name": ["Smalltown"]},
        geometry=[small_city],
        crs=CRS,
    )
    segment = LineString([(0, 5), (10, 5)])  # length 10; overlap = 3 (30%)
    segments = gpd.GeoDataFrame(
        {"unique_id": ["R1"]},
        geometry=[segment],
        crs=CRS,
    )
    out = normalize.apply_admin_overlay_flags(
        segments, house, senate, cong, cities
    )
    # 30% coverage < 50% threshold -> unincorporated
    assert pd.isna(out["CITY_ID"].iloc[0]), out["CITY_ID"].iloc[0]
    assert out["CITY_NAME"].iloc[0] is None or pd.isna(out["CITY_NAME"].iloc[0])

    # Now expand the city to cover 60% -> should stamp
    big_city = Polygon([(0, 0), (6, 0), (6, 10), (0, 10)])
    cities_big = gpd.GeoDataFrame(
        {"CITY_ID": [100], "Name": ["Biggertown"]},
        geometry=[big_city],
        crs=CRS,
    )
    out2 = normalize.apply_admin_overlay_flags(
        segments, house, senate, cong, cities_big
    )
    assert int(out2["CITY_ID"].iloc[0]) == 100, out2["CITY_ID"].iloc[0]
    assert out2["CITY_NAME"].iloc[0] == "Biggertown", out2["CITY_NAME"].iloc[0]


def test_overlay_tie_break_smaller_id() -> None:
    """When two polygons share the winning length, the smaller id wins."""
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    # Two house districts split exactly in half. A segment from x=0 to x=10.
    left = Polygon([(0, 0), (5, 0), (5, 10), (0, 10)])
    right = Polygon([(5, 0), (10, 0), (10, 10), (5, 10)])
    house = gpd.GeoDataFrame(
        {"STATE_HOUSE_DISTRICT": [42, 7]},
        geometry=[left, right],
        crs=CRS,
    )
    senate = gpd.GeoDataFrame(
        {"STATE_SENATE_DISTRICT": [1]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    cong = gpd.GeoDataFrame(
        {"CONGRESSIONAL_DISTRICT": [1]},
        geometry=[Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        crs=CRS,
    )
    cities = gpd.GeoDataFrame(
        {"CITY_ID": pd.array([], dtype="Int64"), "Name": []},
        geometry=[],
        crs=CRS,
    )
    segment = LineString([(0, 5), (10, 5)])
    segments = gpd.GeoDataFrame(
        {"unique_id": ["R1"]},
        geometry=[segment],
        crs=CRS,
    )
    out = normalize.apply_admin_overlay_flags(
        segments, house, senate, cong, cities
    )
    # 50/50 split -> smaller id (7) wins over 42
    assert int(out["STATE_HOUSE_DISTRICT"].iloc[0]) == 7, out["STATE_HOUSE_DISTRICT"].iloc[0]


def test_city_id_hash_stable_across_shuffled_objectids() -> None:
    """Primary CITY_ID must be name-hash-derived - identical city names
    produce identical ids regardless of OBJECTID renumbering between
    ARC layer republishes."""
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    first = gpd.GeoDataFrame(
        {"OBJECTID": [1, 2, 3], "Name": ["Alpharetta", "Roswell", "Sandy Springs"]},
        geometry=[poly, poly, poly],
        crs=CRS,
    )
    shuffled = gpd.GeoDataFrame(
        {"OBJECTID": [42, 100, 7], "Name": ["Alpharetta", "Roswell", "Sandy Springs"]},
        geometry=[poly, poly, poly],
        crs=CRS,
    )
    a = normalize._assign_city_id(first)
    b = normalize._assign_city_id(shuffled)
    for name in ["Alpharetta", "Roswell", "Sandy Springs"]:
        id_a = int(a.loc[a["Name"] == name, "CITY_ID"].iloc[0])
        id_b = int(b.loc[b["Name"] == name, "CITY_ID"].iloc[0])
        assert id_a == id_b, f"{name}: id shifted {id_a} -> {id_b} on OBJECTID shuffle"


def test_city_id_falls_back_to_objectid_on_duplicate_name() -> None:
    """When two rows share a Name (e.g. the ARC layer lists two
    'Bowdon' entries because of city/CDP overlap), fall back to
    OBJECTID, placed in the disjoint >= 2^48 id space."""
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    cities = gpd.GeoDataFrame(
        {"OBJECTID": [1, 2, 3], "Name": ["Atlanta", "Bowdon", "Bowdon"]},
        geometry=[poly, poly, poly],
        crs=CRS,
    )
    assigned = normalize._assign_city_id(cities)
    atlanta_id = int(assigned.loc[assigned["Name"] == "Atlanta", "CITY_ID"].iloc[0])
    bowdon_ids = assigned.loc[assigned["Name"] == "Bowdon", "CITY_ID"].astype(int).tolist()
    # Atlanta in hash space (< 2^48), both Bowdons in fallback space (>= 2^48)
    assert atlanta_id < normalize.CITY_ID_OBJECTID_OFFSET, atlanta_id
    for bid in bowdon_ids:
        assert bid >= normalize.CITY_ID_OBJECTID_OFFSET, bid
    # Duplicate names produce distinct fallback ids
    assert bowdon_ids[0] != bowdon_ids[1], bowdon_ids


def test_admin_code_snapshot_writes_sorted_ids() -> None:
    """_write_admin_code_snapshot emits a sorted id->name map, dropping
    rows with null/empty id or name, and preserves metadata keys."""
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(
        {
            "MPO_ID": ["Z", "A", "M", None, ""],
            "MPO_NAME": ["Zeta MPO", "Alpha MPO", "Mu MPO", "Null MPO", "Blank MPO"],
        },
        geometry=[poly] * 5,
        crs=CRS,
    )
    with tempfile.TemporaryDirectory() as tmp:
        out_path = Path(tmp) / "mpo_codes.json"
        normalize._write_admin_code_snapshot(
            path=out_path,
            layer_label="MPO",
            gdf=gdf,
            id_col="MPO_ID",
            name_col="MPO_NAME",
            source="test-source",
            source_url="https://example.test/",
        )
        payload = json.loads(out_path.read_text(encoding="utf-8"))
    # Sorted by id, nulls and blanks dropped
    assert list(payload["codes"].keys()) == ["A", "M", "Z"], payload["codes"]
    assert payload["codes"]["A"] == "Alpha MPO"
    assert payload["_source"] == "test-source"
    assert payload["_source_url"] == "https://example.test/"


def test_collision_guard_appends_admin_hash() -> None:
    """When two segments collide on unique_id but have different admin
    attribute combos, the guard appends distinct 6-char md5 suffixes."""
    from pandas import DataFrame
    # Import deferred so pytest-less execution still picks up the helper.
    sys.path.insert(0, str(Path(__file__).parent))
    import normalize  # noqa: E402

    df = DataFrame(
        {
            "unique_id": ["ROUTE_1_0.0000_1.0000"] * 2 + ["ROUTE_2_0.0000_1.0000"],
            "DISTRICT": [1, 2, 3],
            "COUNTY_CODE": ["001", "003", "005"],
            "AREA_OFFICE_ID": [101, 202, 303],
            "MPO_ID": ["X", "Y", None],
            "RC_ID": [1, 2, 3],
        }
    )
    out = normalize.apply_unique_id_collision_guard(df)
    uids = out["unique_id"].tolist()
    # The unique row stays untouched
    assert uids[2] == "ROUTE_2_0.0000_1.0000", uids
    # The two collided rows get hash suffixes
    assert uids[0] != uids[1], uids
    assert uids[0].startswith("ROUTE_1_0.0000_1.0000_"), uids
    assert uids[1].startswith("ROUTE_1_0.0000_1.0000_"), uids
    # Hash is 6 chars
    assert len(uids[0].rsplit("_", 1)[-1]) == 6, uids[0]


TESTS = [
    test_single_crossing_between_two_polygons,
    test_no_crossing_when_route_inside_single_polygon,
    test_segment_outside_all_polygons_returns_none_attrs,
    test_two_crossers_union_milepoints,
    test_coincident_boundaries_dedupe,
    test_non_linear_milepoint_projection,
    test_geometry_collection_row_is_unwrapped,
    test_overlay_majority_by_length_picks_longest,
    test_overlay_city_50pct_threshold,
    test_overlay_tie_break_smaller_id,
    test_city_id_hash_stable_across_shuffled_objectids,
    test_city_id_falls_back_to_objectid_on_duplicate_name,
    test_admin_code_snapshot_writes_sorted_ids,
    test_collision_guard_appends_admin_hash,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except AssertionError as exc:
            print(f"FAIL {test.__name__}: {exc}")
            failures += 1
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR {test.__name__}: {exc!r}")
            failures += 1
        else:
            print(f"ok   {test.__name__}")
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} tests passed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
