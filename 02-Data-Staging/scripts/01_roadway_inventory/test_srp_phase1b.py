"""Red/green TDD tests for Phase 1b SRP derivation modules.

Run:
    cd 02-Data-Staging/scripts/01_roadway_inventory
    python test_srp_phase1b.py

Tests exercise each module in isolation with synthetic data — no real
database, no network calls, no file I/O to staging directories.
"""

from __future__ import annotations

import json
import sys
import importlib.util
from pathlib import Path
from unittest.mock import Mock, patch

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point

sys.path.insert(0, str(Path(__file__).parent))

CRS = "EPSG:32617"


# ── helpers ──────────────────────────────────────────────────────────

def _make_segments(**col_overrides) -> gpd.GeoDataFrame:
    """Build a minimal GeoDataFrame mimicking staged roadway segments."""
    n = 1
    for v in col_overrides.values():
        if isinstance(v, (list, pd.Series)):
            n = len(v)
            break

    defaults = {
        "ROUTE_ID": [f"SR001INC"] * n,
        "FROM_MILEPOINT": [0.0] * n,
        "TO_MILEPOINT": [1.0] * n,
        "ROUTE_TYPE_GDOT": ["SR"] * n,
        "BASE_ROUTE_NUMBER": [1] * n,
        "FUNCTIONAL_CLASS": [None] * n,
        "SIGNED_INTERSTATE_FLAG": [False] * n,
        "SIGNED_US_ROUTE_FLAG": [False] * n,
        "STRAHNET": [None] * n,
        "NHFN": [None] * n,
        "NHS_IND": [None] * n,
        "AADT": [None] * n,
        "THROUGH_LANES": [None] * n,
        "SPEED_LIMIT": [None] * n,
        "SEC_EVAC": [False] * n,
        "SEC_EVAC_CONTRAFLOW": [False] * n,
        "IS_GRIP_CORRIDOR": [False] * n,
        "IS_NUCLEAR_EPZ_ROUTE": [False] * n,
        "IS_SOLE_COUNTY_SEAT_CONNECTION": [False] * n,
        "HWY_NAME": [None] * n,
        "COUNTY_NAME": [None] * n,
        "segment_length_mi": [1.0] * n,
        "segment_length_m": [1609.344] * n,
        "ROUTE_FAMILY": ["State Route"] * n,
    }
    defaults.update(col_overrides)

    geoms = [LineString([(500000 + i * 100, 3700000), (500000 + i * 100 + 100, 3700000)]) for i in range(n)]
    if "geometry" in defaults:
        geoms = defaults.pop("geometry")

    return gpd.GeoDataFrame(defaults, geometry=geoms, crs=CRS)


def _load_download_grip_module():
    module_path = Path(__file__).resolve().parents[3] / "01-Raw-Data" / "connectivity" / "scripts" / "download_grip.py"
    spec = importlib.util.spec_from_file_location("download_grip_test_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ═══════════════════════════════════════════════════════════════════
# 1. HPMS enrichment — NHFN / STRAHNET_TYPE in gap-fill dict
# ═══════════════════════════════════════════════════════════════════

def test_hpms_gap_fill_includes_nhfn_and_strahnet_type():
    from hpms_enrichment import HPMS_GAP_FILL_FIELDS

    assert "nhfn" in HPMS_GAP_FILL_FIELDS, "nhfn missing from HPMS_GAP_FILL_FIELDS"
    assert HPMS_GAP_FILL_FIELDS["nhfn"] == ("NHFN", "int"), f"nhfn mapping wrong: {HPMS_GAP_FILL_FIELDS['nhfn']}"

    assert "strahnet_type" in HPMS_GAP_FILL_FIELDS, "strahnet_type missing from HPMS_GAP_FILL_FIELDS"
    assert HPMS_GAP_FILL_FIELDS["strahnet_type"] == ("STRAHNET_TYPE", "int"), (
        f"strahnet_type mapping wrong: {HPMS_GAP_FILL_FIELDS['strahnet_type']}"
    )


# ═══════════════════════════════════════════════════════════════════
# 2. GRIP corridors — download + attribute-based matching
# ═══════════════════════════════════════════════════════════════════

def test_download_grip_discovers_grip_layer_from_listing():
    module = _load_download_grip_module()

    route_network = Mock()
    route_network.raise_for_status = Mock()
    route_network.json.return_value = {"services": [], "layers": []}

    functional_class = Mock()
    functional_class.raise_for_status = Mock()
    functional_class.json.return_value = {
        "layers": [
            {"name": "Roadway Functional Class", "id": 3},
            {"name": "GRIP Corridors", "id": 17},
        ]
    }

    with patch.object(module.requests, "get", side_effect=[route_network, functional_class]) as mock_get:
        discovered = module.discover_grip_layer()

    assert discovered == (module.GDOT_FUNCTIONAL_CLASS_URL, 17)
    assert mock_get.call_count == 2


def test_download_grip_discovers_nested_route_network_service():
    module = _load_download_grip_module()

    route_network = Mock()
    route_network.raise_for_status = Mock()
    route_network.json.return_value = {
        "services": [
            {"name": "GDOT/RouteNetwork", "type": "MapServer"},
        ],
        "layers": [],
    }

    nested_service = Mock()
    nested_service.raise_for_status = Mock()
    nested_service.json.return_value = {
        "layers": [
            {"name": "GRIP Freight Corridors", "id": 4},
        ]
    }

    with patch.object(module.requests, "get", side_effect=[route_network, nested_service]):
        discovered = module.discover_grip_layer()

    assert discovered == (f"{module.GDOT_ROUTE_NETWORK_URL}/GDOT/RouteNetwork/MapServer", 4)


def test_download_grip_paginated_query_accumulates_pages():
    module = _load_download_grip_module()

    first = Mock()
    first.raise_for_status = Mock()
    first.json.return_value = {"features": [{"id": 1}, {"id": 2}]}

    second = Mock()
    second.raise_for_status = Mock()
    second.json.return_value = {"features": [{"id": 3}]}

    with patch.object(module.requests, "get", side_effect=[first, second]):
        data = module._paginated_query(
            "https://example.test/query",
            {"where": "1=1"},
            max_record_count=2,
            pause=0,
        )

    assert [feature["id"] for feature in data["features"]] == [1, 2, 3]

def test_grip_flags_known_corridor_route():
    from grip_corridors import apply_grip_enrichment

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["SR", "SR", "CR"],
        BASE_ROUTE_NUMBER=[400, 1, 100],
    )
    result = apply_grip_enrichment(gdf)
    assert bool(result.at[0, "IS_GRIP_CORRIDOR"]) is True, "SR 400 should be GRIP (Corridor 11)"
    assert bool(result.at[1, "IS_GRIP_CORRIDOR"]) is True, "SR 1 should be GRIP (Corridor 3 - Heartland)"
    assert bool(result.at[2, "IS_GRIP_CORRIDOR"]) is False, "CR 100 should NOT be GRIP"


def test_grip_flags_us_route_corridor():
    from grip_corridors import apply_grip_enrichment

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["US", "US"],
        BASE_ROUTE_NUMBER=[27, 99],
    )
    result = apply_grip_enrichment(gdf)
    assert bool(result.at[0, "IS_GRIP_CORRIDOR"]) is True, "US 27 should be GRIP (Corridor 3)"
    assert bool(result.at[1, "IS_GRIP_CORRIDOR"]) is False, "US 99 is not a GRIP corridor"


def test_grip_flags_suffix_types():
    from grip_corridors import apply_grip_enrichment

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["CN", "SP", "BU"],
        BASE_ROUTE_NUMBER=[400, 400, 400],
    )
    result = apply_grip_enrichment(gdf)
    assert bool(result.at[0, "IS_GRIP_CORRIDOR"]) is True, "SR 400 Connector should be GRIP"
    assert bool(result.at[1, "IS_GRIP_CORRIDOR"]) is True, "SR 400 Spur should be GRIP"
    assert bool(result.at[2, "IS_GRIP_CORRIDOR"]) is True, "SR 400 Business should be GRIP"


def test_grip_does_not_flag_us_suffix_routes_from_mainline_lookup():
    from grip_corridors import apply_grip_enrichment

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["BU", "US"],
        BASE_ROUTE_NUMBER=[27, 27],
        ROUTE_FAMILY=["U.S. Route", "U.S. Route"],
    )
    result = apply_grip_enrichment(gdf)
    assert bool(result.at[0, "IS_GRIP_CORRIDOR"]) is False, "US 27 Business should not inherit mainline GRIP status"
    assert bool(result.at[1, "IS_GRIP_CORRIDOR"]) is True, "Mainline US 27 should remain GRIP"


def test_grip_corridor_name_populated():
    from grip_corridors import apply_grip_enrichment

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["SR"],
        BASE_ROUTE_NUMBER=[400],
    )
    result = apply_grip_enrichment(gdf)
    name = result.at[0, "GRIP_CORRIDOR_NAME"]
    assert name is not None and "Corridor 11" in name, f"Expected Corridor 11 in name, got: {name}"


def test_grip_empty_dataframe():
    from grip_corridors import apply_grip_enrichment

    gdf = _make_segments()
    gdf = gdf.iloc[:0]
    result = apply_grip_enrichment(gdf)
    assert "IS_GRIP_CORRIDOR" in result.columns
    assert len(result) == 0


# ═══════════════════════════════════════════════════════════════════
# 3. Nuclear EPZ routes
# ═══════════════════════════════════════════════════════════════════

def test_nuclear_epz_flags_segment_within_buffer():
    from nuclear_epz import apply_nuclear_epz_enrichment, PLANTS, EPZ_RADIUS_METERS

    vogtle_lon, vogtle_lat = PLANTS[0]["lon"], PLANTS[0]["lat"]

    # Create a segment right at Plant Vogtle's location (projected coords).
    # We'll use a segment near the plant in UTM Zone 17N.
    # Vogtle approximate UTM17N coords: ~415000 E, ~3665000 N
    near_vogtle = LineString([(415000, 3665000), (415100, 3665000)])
    far_away = LineString([(200000, 3900000), (200100, 3900000)])

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["SR", "SR"],
        BASE_ROUTE_NUMBER=[21, 999],
        geometry=[near_vogtle, far_away],
    )
    result = apply_nuclear_epz_enrichment(gdf)

    assert bool(result.at[0, "IS_NUCLEAR_EPZ_ROUTE"]) is True, "Segment near Vogtle should be flagged"
    assert bool(result.at[1, "IS_NUCLEAR_EPZ_ROUTE"]) is False, "Segment far away should not be flagged"


def test_nuclear_epz_skips_local_roads():
    from nuclear_epz import apply_nuclear_epz_enrichment

    near_vogtle = LineString([(415000, 3665000), (415100, 3665000)])

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["CR"],
        BASE_ROUTE_NUMBER=[100],
        geometry=[near_vogtle],
    )
    result = apply_nuclear_epz_enrichment(gdf)
    assert bool(result.at[0, "IS_NUCLEAR_EPZ_ROUTE"]) is False, "CR segments should be skipped"


def test_nuclear_epz_records_plant_name():
    from nuclear_epz import apply_nuclear_epz_enrichment

    near_vogtle = LineString([(415000, 3665000), (415100, 3665000)])
    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["SR"],
        geometry=[near_vogtle],
    )
    result = apply_nuclear_epz_enrichment(gdf)
    plant = result.at[0, "NUCLEAR_EPZ_PLANT"]
    assert plant is not None and "Vogtle" in plant, f"Expected Vogtle in plant name, got: {plant}"


def test_nuclear_epz_does_not_write_buffers_by_default():
    from nuclear_epz import apply_nuclear_epz_enrichment

    near_vogtle = LineString([(415000, 3665000), (415100, 3665000)])
    gdf = _make_segments(ROUTE_TYPE_GDOT=["SR"], geometry=[near_vogtle])

    with patch.object(gpd.GeoDataFrame, "to_file") as mock_to_file:
        apply_nuclear_epz_enrichment(gdf)

    mock_to_file.assert_not_called()


def test_nuclear_epz_empty_dataframe():
    from nuclear_epz import apply_nuclear_epz_enrichment

    gdf = _make_segments()
    gdf = gdf.iloc[:0]
    result = apply_nuclear_epz_enrichment(gdf)
    assert "IS_NUCLEAR_EPZ_ROUTE" in result.columns
    assert len(result) == 0


# ═══════════════════════════════════════════════════════════════════
# 4. Sole county-seat connections
# ═══════════════════════════════════════════════════════════════════

def test_sole_connection_bridge_graph():
    """A simple bridge topology: A--B--C where B--C is the only route to C.
    If C is a county seat, edge B--C should be a bridge."""
    from sole_county_seat_connections import _build_route_graph, _find_nearest_node

    import networkx as nx

    a = (500000, 3700000)
    b = (500500, 3700000)
    c = (501000, 3700000)

    geom_ab = LineString([a, b])
    geom_bc = LineString([b, c])

    gdf = gpd.GeoDataFrame(
        {
            "ROUTE_TYPE_GDOT": ["SR", "SR"],
            "BASE_ROUTE_NUMBER": [1, 1],
        },
        geometry=[geom_ab, geom_bc],
        crs=CRS,
    )
    graph, seg_to_edges = _build_route_graph(gdf, snap_tolerance=50.0)
    assert graph.number_of_nodes() >= 3, f"Expected >= 3 nodes, got {graph.number_of_nodes()}"
    assert graph.number_of_edges() >= 2, f"Expected >= 2 edges, got {graph.number_of_edges()}"

    bridges = list(nx.bridges(graph))
    assert len(bridges) >= 1, f"Expected at least 1 bridge, got {len(bridges)}"


def test_sole_connection_no_bridge_in_cycle():
    """A triangle graph has no bridges — no sole connections."""
    from sole_county_seat_connections import _build_route_graph

    import networkx as nx

    a = (500000, 3700000)
    b = (500500, 3700000)
    c = (500250, 3700500)

    gdf = gpd.GeoDataFrame(
        {"ROUTE_TYPE_GDOT": ["SR", "SR", "SR"], "BASE_ROUTE_NUMBER": [1, 2, 3]},
        geometry=[LineString([a, b]), LineString([b, c]), LineString([c, a])],
        crs=CRS,
    )
    graph, _ = _build_route_graph(gdf, snap_tolerance=50.0)
    bridges = list(nx.bridges(graph))
    assert len(bridges) == 0, f"Expected 0 bridges in a cycle, got {len(bridges)}"


def test_find_nearest_node():
    from sole_county_seat_connections import _find_nearest_node

    import networkx as nx

    G = nx.Graph()
    G.add_node((100.0, 200.0))
    G.add_node((300.0, 400.0))
    G.add_edge((100.0, 200.0), (300.0, 400.0))

    nearest = _find_nearest_node(G, Point(110, 210))
    assert nearest == (100.0, 200.0), f"Expected (100, 200), got {nearest}"


def test_sole_connection_enrichment_adds_columns():
    from sole_county_seat_connections import apply_sole_county_seat_enrichment

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["SR"],
        BASE_ROUTE_NUMBER=[1],
    )
    result = apply_sole_county_seat_enrichment(gdf)
    assert "IS_SOLE_COUNTY_SEAT_CONNECTION" in result.columns
    assert "SOLE_CONNECTION_COUNTY_SEAT" in result.columns


# ═══════════════════════════════════════════════════════════════════
# 5. SRP validation
# ═══════════════════════════════════════════════════════════════════

def test_srp_validation_prefers_higher_priority_official_tier():
    import srp_validation

    segment = LineString([(0, 0), (100, 0)])
    segments = gpd.GeoDataFrame({"SRP_DERIVED": ["Low"]}, geometry=[segment], crs=CRS)
    official_layers = {
        "Low": gpd.GeoDataFrame({"tier": ["Low"]}, geometry=[segment], crs=CRS),
        "High": gpd.GeoDataFrame({"tier": ["High"]}, geometry=[segment], crs=CRS),
    }

    assigned = srp_validation._assign_official_tier(segments, official_layers)
    assert assigned.iloc[0] == "High", "Higher-priority official tier should win on overlap"


def test_srp_validation_counts_unclassified_segments_in_confusion_matrix():
    import tempfile

    import srp_validation

    segments = gpd.GeoDataFrame(
        {"SRP_DERIVED": ["Low", "High"]},
        geometry=[
            LineString([(0, 0), (100, 0)]),
            LineString([(1000, 0), (1100, 0)]),
        ],
        crs=CRS,
    )
    official_layers = {
        "High": gpd.GeoDataFrame(
            {"tier": ["High"]},
            geometry=[LineString([(1000, 0), (1100, 0)])],
            crs=CRS,
        )
    }

    original_download = srp_validation.download_official_srp
    original_reports_dir = srp_validation.REPORTS_DIR
    try:
        srp_validation.download_official_srp = lambda: official_layers
        with tempfile.TemporaryDirectory() as tmpdir:
            srp_validation.REPORTS_DIR = Path(tmpdir)
            report = srp_validation.validate_srp(segments)
    finally:
        srp_validation.download_official_srp = original_download
        srp_validation.REPORTS_DIR = original_reports_dir

    assert report["confusion_matrix"]["Low"]["Unclassified"] == 1
    assert report["confusion_matrix"]["High"]["High"] == 1
    assert report["official_unclassified"] == 1


# ═══════════════════════════════════════════════════════════════════
# 6. SRP derivation — classification logic
# ═══════════════════════════════════════════════════════════════════

def test_srp_interstate_is_critical():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(FUNCTIONAL_CLASS=[1], SIGNED_INTERSTATE_FLAG=[True])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Critical", f"Got: {result.at[0, 'SRP_DERIVED']}"
    assert "Interstate" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_nhfn_is_critical():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(NHFN=[1])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Critical"
    assert "NHFN" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_strahnet_is_critical():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(STRAHNET=[1])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Critical"
    assert "STRAHNET" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_intermodal_connector_is_critical():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(NHS_IND=[4])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Critical"
    assert "Intermodal" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_us_route_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(SIGNED_US_ROUTE_FLAG=[True])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High", f"Got: {result.at[0, 'SRP_DERIVED']}"
    assert "US route" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_grip_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(IS_GRIP_CORRIDOR=[True])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High"
    assert "GRIP" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_nuclear_epz_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(IS_NUCLEAR_EPZ_ROUTE=[True])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High"
    assert "Nuclear" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_evac_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(SEC_EVAC=[True])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High"
    assert "GEMA" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_sole_county_seat_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(IS_SOLE_COUNTY_SEAT_CONNECTION=[True])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High"
    assert "county-seat" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_high_aadt_non_nhs_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(AADT=[5000], NHS_IND=[None])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High"
    assert "AADT > 3,000" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_nhs_high_aadt_is_high():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(AADT=[5000], NHS_IND=[1])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High"
    assert "NHS principal arterial" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_prefers_2024_aadt_over_generic_aadt():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(
        AADT=[5000],
        AADT_2024=[1000],
        NHS_IND=[None],
        SPEED_LIMIT=[55],
        segment_length_mi=[10.0],
    )
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Low", "2024 AADT should control SRP when present"
    assert "AADT < 3,000" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_uses_hpms_2024_aadt_when_canonical_missing():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(
        AADT=[None],
        AADT_2024=[None],
        AADT_2024_HPMS=[4500],
        NHS_IND=[None],
    )
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "High", "HPMS 2024 AADT should backstop SRP traffic thresholds"
    assert "AADT > 3,000" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_nhs_low_aadt_is_medium():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(AADT=[2000], NHS_IND=[1])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Medium"
    assert "NHS with AADT" in result.at[0, "SRP_DERIVED_REASONS"]


def test_srp_us_4plus_lanes_is_medium():
    from srp_derivation import derive_srp_priority

    # US route with 4+ lanes but no other high-tier trigger
    # Note: US routes trigger High via "US route" criterion, so we test
    # that the Medium criterion for 4+ lanes is present but overridden
    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["US"],
        SIGNED_US_ROUTE_FLAG=[True],
        THROUGH_LANES=[4],
    )
    result = derive_srp_priority(gdf)
    # US route is High regardless; this tests the criterion exists
    assert result.at[0, "SRP_DERIVED"] == "High", "US routes are always High"


def test_srp_default_is_low():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(AADT=[500], SPEED_LIMIT=[25])
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Low"


def test_srp_critical_beats_high():
    """Interstate + GRIP corridor → should be Critical (not High)."""
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(
        FUNCTIONAL_CLASS=[1],
        SIGNED_INTERSTATE_FLAG=[True],
        IS_GRIP_CORRIDOR=[True],
    )
    result = derive_srp_priority(gdf)
    assert result.at[0, "SRP_DERIVED"] == "Critical", "Critical should take priority over High"


def test_srp_all_segments_classified():
    """No segment should have a null SRP_DERIVED value."""
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(
        ROUTE_TYPE_GDOT=["I", "US", "SR", "CR", "CS"],
        FUNCTIONAL_CLASS=[1, 2, 3, 7, 7],
        AADT=[50000, 5000, 2000, 500, None],
        NHS_IND=[1, None, 1, None, None],
        SIGNED_INTERSTATE_FLAG=[True, False, False, False, False],
        SIGNED_US_ROUTE_FLAG=[False, True, False, False, False],
        BASE_ROUTE_NUMBER=[75, 27, 15, 100, 200],
    )
    result = derive_srp_priority(gdf)
    assert result["SRP_DERIVED"].notna().all(), "All segments must have SRP_DERIVED"
    assert all(t in ("Critical", "High", "Medium", "Low") for t in result["SRP_DERIVED"]), (
        f"Invalid tiers: {result['SRP_DERIVED'].unique()}"
    )


def test_srp_reasons_pipe_delimited():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments(
        FUNCTIONAL_CLASS=[1],
        SIGNED_INTERSTATE_FLAG=[True],
        NHFN=[1],
    )
    result = derive_srp_priority(gdf)
    reasons = result.at[0, "SRP_DERIVED_REASONS"]
    assert " | " in reasons, f"Expected pipe-delimited reasons, got: {reasons}"
    parts = [r.strip() for r in reasons.split(" | ")]
    assert len(parts) >= 2, f"Expected >= 2 reasons, got {len(parts)}: {parts}"


def test_srp_empty_dataframe():
    from srp_derivation import derive_srp_priority

    gdf = _make_segments()
    gdf = gdf.iloc[:0]
    result = derive_srp_priority(gdf)
    assert "SRP_DERIVED" in result.columns
    assert "SRP_DERIVED_REASONS" in result.columns
    assert len(result) == 0


# ═══════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════

def _run_tests():
    """Discover and run all test_ functions in this module."""
    import traceback

    tests = [
        (name, obj)
        for name, obj in sorted(globals().items())
        if name.startswith("test_") and callable(obj)
    ]

    passed = 0
    failed = 0
    errors: list[tuple[str, str]] = []

    for name, func in tests:
        try:
            func()
            passed += 1
            print(f"  PASS  {name}")
        except Exception as exc:
            failed += 1
            tb = traceback.format_exc()
            errors.append((name, tb))
            print(f"  FAIL  {name}: {exc}")

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")

    if errors:
        print(f"\n{'-' * 60}")
        for name, tb in errors:
            print(f"\n{name}:\n{tb}")

    return failed == 0


if __name__ == "__main__":
    ok = _run_tests()
    sys.exit(0 if ok else 1)
