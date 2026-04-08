"""Enrich Georgia roadway segments with RNHP GDOT service data.

Current enrichment layers:
- SpeedZone OnSystem: posted speed limits for state highway routes

Downloads are cached under 01-Raw-Data/GA_RDWY_INV/GDOT_GPAS/rnhp_enrichment/.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from route_verification import (
    _clean_text,
    _feature_collection_to_gdf,
    _fetch_arcgis_features,
    _fetch_arcgis_object_ids,
    _intervals_overlap,
    _round_milepoint,
)

LOGGER = logging.getLogger(__name__)

MILEPOINT_TOLERANCE = 1e-4
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "02-Data-Staging" / "config" / "rnhp_enrichment_sources.json"

CONFIG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
LAYER_CONFIG = CONFIG["layers"]

ENRICHMENT_COLUMNS = [
    "SPEED_LIMIT",
    "IS_SCHOOL_ZONE",
    "SPEED_LIMIT_SOURCE",
]


def _local_path(layer_key: str) -> Path:
    return PROJECT_ROOT / LAYER_CONFIG[layer_key]["local_geojson"]


def fetch_enrichment_layer(layer_key: str, refresh: bool = False) -> gpd.GeoDataFrame:
    """Load an enrichment layer from local snapshot or live RNHP service."""

    spec = LAYER_CONFIG[layer_key]
    local_path = _local_path(layer_key)

    if local_path.exists() and not refresh:
        LOGGER.info("Loading enrichment snapshot: %s", local_path)
        return gpd.read_file(local_path, engine="pyogrio")

    LOGGER.info("Fetching enrichment layer from RNHP: %s", layer_key)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    object_ids = _fetch_arcgis_object_ids(spec["service_url"])
    gdf = _fetch_arcgis_features(spec["service_url"], object_ids)

    if gdf.empty:
        LOGGER.warning("Enrichment layer %s returned no features", layer_key)
        return gdf

    gdf.to_file(local_path, driver="GeoJSON", engine="pyogrio")
    LOGGER.info("Wrote enrichment snapshot: %s (%d features)", local_path, len(gdf))
    return gdf


def _normalize_speed_zones(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Normalize SpeedZone OnSystem into a lookup-ready DataFrame."""

    if gdf.empty:
        return pd.DataFrame(columns=[
            "ROUTE_ID_BASE", "FROM_MP", "TO_MP",
            "SPEED_LIMIT", "IS_SCHOOL_ZONE",
        ])

    spec = LAYER_CONFIG["speed_zone_on_system"]
    df = gdf.copy()

    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]

    route_id_field = spec["route_id_field"].upper()
    from_field = spec["from_field"].upper()
    to_field = spec["to_field"].upper()

    if route_id_field not in df.columns:
        LOGGER.warning("Speed zone layer missing %s field", route_id_field)
        return pd.DataFrame()

    active_col = "RECORD_STATUS_CD"
    if active_col in df.columns:
        before = len(df)
        df = df[df[active_col].astype(str).str.strip().str.upper() == "ACTV"]
        LOGGER.info("Filtered to active speed zones: %d -> %d", before, len(df))

    result = pd.DataFrame()
    result["ROUTE_ID_BASE"] = df[route_id_field].astype(str).str.strip().str.upper().str[:13]
    result["FROM_MP"] = df[from_field].map(_round_milepoint) if from_field in df.columns else None
    result["TO_MP"] = df[to_field].map(_round_milepoint) if to_field in df.columns else None
    result["SPEED_LIMIT"] = pd.to_numeric(
        df["SPEED_LIMIT_CD"].astype(str).str.strip(), errors="coerce"
    ).astype("Int64")
    result["IS_SCHOOL_ZONE"] = (
        df["IS_SCHOOL_ZONE_CD"].astype(str).str.strip().str.upper() == "Y"
        if "IS_SCHOOL_ZONE_CD" in df.columns
        else False
    )

    result = result.dropna(subset=["ROUTE_ID_BASE", "SPEED_LIMIT"])
    result = result[result["ROUTE_ID_BASE"].str.len() >= 13]

    return result


def _build_speed_zone_lookup(
    speed_zones: pd.DataFrame,
) -> dict[str, list[dict[str, Any]]]:
    """Build a ROUTE_ID_BASE -> list of speed zone records lookup."""

    lookup: dict[str, list[dict[str, Any]]] = {}
    if speed_zones.empty:
        return lookup

    for route_base, group in speed_zones.groupby("ROUTE_ID_BASE", sort=False):
        lookup[str(route_base)] = group.to_dict("records")

    return lookup


def _match_speed_zone(
    row: pd.Series,
    lookup: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    """Find the best-covering speed zone for a segment."""

    route_id = _clean_text(row.get("ROUTE_ID")).upper()
    if len(route_id) < 13:
        return None

    route_base = route_id[:13]
    candidates = lookup.get(route_base, [])
    if not candidates:
        return None

    seg_from = _round_milepoint(row.get("FROM_MILEPOINT"))
    seg_to = _round_milepoint(row.get("TO_MILEPOINT"))

    best_match = None
    best_overlap = -1.0

    for record in candidates:
        ref_from = record.get("FROM_MP")
        ref_to = record.get("TO_MP")

        # Skip records with missing milepoints — can't verify overlap
        if ref_from is None or ref_to is None:
            continue
        if seg_from is None or seg_to is None:
            continue

        overlap = min(float(seg_to), float(ref_to)) - max(float(seg_from), float(ref_from))
        if overlap > -MILEPOINT_TOLERANCE and overlap > best_overlap:
            best_overlap = overlap
            best_match = record

    return best_match


def apply_speed_zone_enrichment(
    gdf: gpd.GeoDataFrame,
    refresh: bool = False,
) -> gpd.GeoDataFrame:
    """Apply SpeedZone OnSystem data to state highway segments."""

    enriched = gdf.copy()
    enriched["SPEED_LIMIT"] = pd.array([pd.NA] * len(enriched), dtype="Int64")
    enriched["IS_SCHOOL_ZONE"] = False
    enriched["SPEED_LIMIT_SOURCE"] = None

    try:
        raw = fetch_enrichment_layer("speed_zone_on_system", refresh=refresh)
    except Exception as exc:
        LOGGER.warning("Speed zone enrichment unavailable: %s", exc)
        return enriched

    speed_zones = _normalize_speed_zones(raw)
    if speed_zones.empty:
        LOGGER.warning("No usable speed zone records after normalization")
        return enriched

    lookup = _build_speed_zone_lookup(speed_zones)
    LOGGER.info(
        "Speed zone lookup: %d route keys, %d total records",
        len(lookup),
        sum(len(v) for v in lookup.values()),
    )

    state_highway_mask = enriched["PARSED_SYSTEM_CODE"].astype(str) == "1"
    candidate_indices = enriched.index[state_highway_mask]
    match_count = 0
    school_zone_count = 0

    for idx in candidate_indices:
        row = enriched.loc[idx]
        match = _match_speed_zone(row, lookup)
        if match is None:
            continue

        enriched.at[idx, "SPEED_LIMIT"] = match["SPEED_LIMIT"]
        enriched.at[idx, "IS_SCHOOL_ZONE"] = match.get("IS_SCHOOL_ZONE", False)
        enriched.at[idx, "SPEED_LIMIT_SOURCE"] = "gdot_speed_zone_on_system"
        match_count += 1
        if match.get("IS_SCHOOL_ZONE", False):
            school_zone_count += 1

    LOGGER.info(
        "Speed zone matches: %d segments (including %d school zones)",
        match_count,
        school_zone_count,
    )

    return enriched


def apply_rnhp_enrichment(
    gdf: gpd.GeoDataFrame,
    refresh: bool = False,
) -> gpd.GeoDataFrame:
    """Apply all RNHP enrichment layers to roadway segments."""

    enriched = apply_speed_zone_enrichment(gdf, refresh=refresh)
    return enriched


def write_enrichment_summary(gdf: pd.DataFrame) -> None:
    """Write enrichment coverage summary."""

    speed_limit_filled = int(gdf["SPEED_LIMIT"].notna().sum())
    school_zones = int(gdf["IS_SCHOOL_ZONE"].fillna(False).sum())

    speed_dist: dict[str, int] = {}
    if speed_limit_filled > 0:
        speed_dist = {
            str(k): int(v)
            for k, v in gdf["SPEED_LIMIT"].dropna().value_counts().sort_index().items()
        }

    summary = {
        "segment_count": int(len(gdf)),
        "speed_limit_coverage": speed_limit_filled,
        "school_zone_segments": school_zones,
        "speed_limit_distribution": speed_dist,
        "speed_limit_source_counts": {
            str(k): int(v)
            for k, v in gdf["SPEED_LIMIT_SOURCE"].value_counts(dropna=False).items()
        },
    }

    output_path = (
        PROJECT_ROOT / "02-Data-Staging" / "config" / "rnhp_enrichment_summary.json"
    )
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote RNHP enrichment summary to %s", output_path)
