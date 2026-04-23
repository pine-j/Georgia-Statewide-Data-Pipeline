"""Flag roadway segments on GRIP (Governor's Road Improvement Program) corridors.

GRIP established 19 economic development corridors + 3 truck access
routes connecting 95% of Georgia cities (pop > 2,500) to the Interstate
system. Total: 3,323 miles. Established by GA General Assembly in 1989.

Matching approach: attribute-based join on ROUTE_TYPE_GDOT + BASE_ROUTE_NUMBER
using the known GRIP corridor route composition. Each corridor follows a
sequence of state routes and US highways; a segment is flagged if its
route designation appears in any GRIP corridor's route list.

If a GRIP GIS layer is found at 01-Raw-Data/connectivity/grip_corridors/
grip_corridors.geojson, a spatial overlay is used instead.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
GRIP_GEOJSON = (
    PROJECT_ROOT / "01-Raw-Data" / "connectivity" / "grip_corridors" / "grip_corridors.geojson"
)

# GRIP corridor route compositions. Each corridor is a list of
# (route_type, route_number) tuples. Route types use GDOT codes:
# "I" = Interstate, "US" = US Route, "SR" = State Route.
# Sources: GDOT GRIP Fact Sheet, GDOT GRIP System Summary,
# Roads & Bridges "Setting Priorities" (2016).
GRIP_CORRIDORS: dict[str, list[tuple[str, int]]] = {
    "Corridor 1 - Fall Line Freeway": [
        ("SR", 540), ("US", 80), ("SR", 22), ("SR", 49), ("SR", 26),
        ("SR", 96),
    ],
    "Corridor 2 - Savannah River Parkway": [
        ("SR", 520), ("US", 25), ("SR", 121),
    ],
    "Corridor 3 - Heartland Flyer / US 27": [
        ("US", 27), ("SR", 1),
    ],
    "Corridor 4 - Golden Isles Parkway": [
        ("US", 82), ("SR", 520), ("US", 84),
    ],
    "Corridor 5 - US 1": [
        ("US", 1), ("SR", 4),
    ],
    "Corridor 6 - US 441 / SR 15": [
        ("US", 441), ("SR", 15),
    ],
    "Corridor 7 - SR 515 / US 76": [
        ("SR", 515), ("US", 76), ("SR", 5),
    ],
    "Corridor 8 - SR 365 / US 23": [
        ("SR", 365), ("US", 23),
    ],
    "Corridor 9 - US 301 / SR 73": [
        ("US", 301), ("SR", 73),
    ],
    "Corridor 10 - SR 21": [
        ("SR", 21),
    ],
    "Corridor 11 - SR 400": [
        ("SR", 400),
    ],
    "Corridor 12 - SR 316": [
        ("SR", 316),
    ],
    "Corridor 13 - US 280 / SR 30": [
        ("US", 280), ("SR", 30),
    ],
    "Corridor 14 - US 341 / SR 27": [
        ("US", 341), ("SR", 27),
    ],
    "Corridor 15 - SR 3 / US 19 / US 41": [
        ("SR", 3), ("US", 19), ("US", 41),
    ],
    "Corridor 16 - US 17 / SR 25": [
        ("US", 17), ("SR", 25),
    ],
    "Corridor 17 - SR 369 / SR 60": [
        ("SR", 369), ("SR", 60),
    ],
    "Corridor 18 - US 78 / SR 10": [
        ("US", 78), ("SR", 10),
    ],
    "Corridor 19 - US 129 / SR 11": [
        ("US", 129), ("SR", 11),
    ],
    "Truck Access 1 - SR 402 / Spur 5 (Port of Savannah)": [
        ("SR", 402), ("SP", 5), ("SR", 307),
    ],
    "Truck Access 2 - US 80 Savannah": [
        ("US", 80),
    ],
    "Truck Access 3 - SR 3 Truck Route": [
        ("SR", 3),
    ],
}

# ROUTE_TYPE_GDOT suffix types that represent the same base route.
# E.g., SR 1 Connector has ROUTE_TYPE_GDOT='CN', BASE_ROUTE_NUMBER=1.
_SUFFIX_TYPES = {"SP", "BU", "CN", "BY", "LP", "AL"}

# Map from GRIP route type notation to GDOT ROUTE_TYPE_GDOT codes.
_GRIP_TYPE_TO_GDOT = {
    "I": {"I"},
    "US": {"US"},
    "SR": {"SR"},
    "SP": {"SP"},
}


def _build_grip_route_set() -> dict[tuple[str, int], list[str]]:
    """Build a (gdot_type, route_number) -> [corridor_names] lookup."""
    lookup: dict[tuple[str, int], list[str]] = {}
    for corridor_name, routes in GRIP_CORRIDORS.items():
        for grip_type, number in routes:
            gdot_types = _GRIP_TYPE_TO_GDOT.get(grip_type, {grip_type})
            for gt in gdot_types:
                key = (gt, number)
                lookup.setdefault(key, []).append(corridor_name)
            for suffix in _SUFFIX_TYPES:
                key = (suffix, number)
                lookup.setdefault(key, []).append(corridor_name)
    return lookup


def apply_grip_enrichment(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Flag segments on GRIP corridors.

    Adds IS_GRIP_CORRIDOR (bool) and GRIP_CORRIDOR_NAME (corridor name(s)).
    Uses spatial overlay if grip_corridors.geojson exists, otherwise
    attribute-based matching on ROUTE_TYPE_GDOT + BASE_ROUTE_NUMBER.
    """
    enriched = gdf.copy()
    enriched["IS_GRIP_CORRIDOR"] = False
    enriched["GRIP_CORRIDOR_NAME"] = None

    if GRIP_GEOJSON.exists():
        return _apply_grip_spatial(enriched)

    return _apply_grip_attribute(enriched)


def _apply_grip_spatial(enriched: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Flag segments using a spatial overlay with downloaded GRIP GIS data."""
    LOGGER.info("Loading GRIP GIS data from %s", GRIP_GEOJSON)
    grip = gpd.read_file(GRIP_GEOJSON, engine="pyogrio")

    if grip.empty:
        LOGGER.warning("GRIP GeoJSON is empty — falling back to attribute matching")
        return _apply_grip_attribute(enriched)

    seg_crs = enriched.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot run GRIP spatial overlay")
        return _apply_grip_attribute(enriched)

    if grip.crs != seg_crs:
        grip = grip.to_crs(seg_crs)

    grip_union = grip.geometry.buffer(50).union_all()

    state_mask = pd.Series(True, index=enriched.index)
    if "ROUTE_TYPE_GDOT" in enriched.columns:
        state_mask = ~enriched["ROUTE_TYPE_GDOT"].isin({"CR", "CS"})

    candidates = enriched.loc[state_mask]
    hits = candidates[candidates.geometry.intersects(grip_union)]

    for idx in hits.index:
        enriched.at[idx, "IS_GRIP_CORRIDOR"] = True

    LOGGER.info("GRIP spatial overlay: %d segments flagged", len(hits))
    return enriched


def _apply_grip_attribute(enriched: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Flag segments using attribute-based GRIP corridor route lookup."""
    if "ROUTE_TYPE_GDOT" not in enriched.columns or "BASE_ROUTE_NUMBER" not in enriched.columns:
        LOGGER.warning(
            "Missing ROUTE_TYPE_GDOT or BASE_ROUTE_NUMBER columns — "
            "cannot apply GRIP attribute matching"
        )
        return enriched

    route_lookup = _build_grip_route_set()
    LOGGER.info(
        "GRIP attribute matching: %d route keys from %d corridors",
        len(route_lookup), len(GRIP_CORRIDORS),
    )

    flagged = 0
    for idx in enriched.index:
        rt = enriched.at[idx, "ROUTE_TYPE_GDOT"]
        brn = enriched.at[idx, "BASE_ROUTE_NUMBER"]

        if pd.isna(rt) or pd.isna(brn):
            continue

        try:
            brn_int = int(brn)
        except (ValueError, TypeError):
            continue

        key = (str(rt).strip(), brn_int)
        corridors = route_lookup.get(key)
        if corridors:
            enriched.at[idx, "IS_GRIP_CORRIDOR"] = True
            enriched.at[idx, "GRIP_CORRIDOR_NAME"] = "; ".join(sorted(set(corridors)))
            flagged += 1

    LOGGER.info("GRIP attribute matching: %d segments flagged", flagged)

    corridor_counts: dict[str, int] = {}
    named = enriched.loc[enriched["GRIP_CORRIDOR_NAME"].notna(), "GRIP_CORRIDOR_NAME"]
    for val in named:
        for name in str(val).split("; "):
            corridor_counts[name] = corridor_counts.get(name, 0) + 1

    for name, count in sorted(corridor_counts.items()):
        LOGGER.info("  %s: %d segments", name, count)

    return enriched
