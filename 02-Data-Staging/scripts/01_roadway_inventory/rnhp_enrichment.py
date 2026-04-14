"""Enrich Georgia roadway segments with RNHP GDOT service data.

Current enrichment layers:
- SpeedZone OnSystem: posted speed limits for state highway routes
- SpeedZone OffSystem: posted speed limits for non-state-highway roads

Downloads are cached under 01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from route_verification import (
    _fetch_arcgis_features,
    _fetch_arcgis_object_ids,
)
from utils import _clean_text, _round_milepoint

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
        if overlap > MILEPOINT_TOLERANCE and overlap > best_overlap:
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


def _normalize_off_system_speed_zones(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize SpeedZone OffSystem into a spatially-joinable GeoDataFrame.

    Layer 9 has no FROM/TO_MILEPOINT_STATEWIDE fields, so all records use
    spatial matching.  The ``match_method`` column is always ``"spatial"``.
    """

    if gdf.empty:
        return gpd.GeoDataFrame(columns=[
            "SPEED_LIMIT", "IS_SCHOOL_ZONE", "match_method", "geometry",
        ])

    df = gdf.copy()
    geom_col = df.geometry.name
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    # Restore geometry column name after uppercasing
    if geom_col.upper() in df.columns and geom_col.upper() != geom_col:
        df = df.rename(columns={geom_col.upper(): geom_col})
    df = df.set_geometry(geom_col)

    active_col = "RECORD_STATUS_CD"
    if active_col in df.columns:
        before = len(df)
        df = df[df[active_col].astype(str).str.strip().str.upper() == "ACTV"]
        LOGGER.info("Filtered to active off-system speed zones: %d -> %d", before, len(df))

    result = gpd.GeoDataFrame(geometry=df.geometry, crs=df.crs)

    result["SPEED_LIMIT"] = pd.to_numeric(
        df["SPEED_LIMIT_CD"].astype(str).str.strip(), errors="coerce"
    ).astype("Int64")
    result["IS_SCHOOL_ZONE"] = (
        df["IS_SCHOOL_ZONE_CD"].astype(str).str.strip().str.upper() == "Y"
        if "IS_SCHOOL_ZONE_CD" in df.columns
        else False
    )

    result = result.dropna(subset=["SPEED_LIMIT"])
    result["match_method"] = "spatial"

    LOGGER.info("Off-system speed zones normalized: %d records (all spatial)", len(result))

    return result


def _spatial_match_off_system(
    segments: gpd.GeoDataFrame,
    speed_zones: gpd.GeoDataFrame,
) -> dict[Any, dict[str, Any]]:
    """Spatially match off-system speed zones to roadway segments.

    Returns a dict of segment index -> best speed-zone record.  When a segment
    intersects multiple zones the one with the longest intersection is chosen.
    """

    spatial_zones = speed_zones[speed_zones["match_method"] == "spatial"].copy()
    if spatial_zones.empty:
        return {}

    if segments.crs is None or spatial_zones.crs is None:
        LOGGER.warning("Cannot spatially match without CRS on both inputs")
        return {}
    if segments.crs != spatial_zones.crs:
        spatial_zones = spatial_zones.to_crs(segments.crs)

    # Drop records with null/empty geometry before joining
    spatial_zones = spatial_zones[~spatial_zones.geometry.is_empty & spatial_zones.geometry.notna()]
    segments = segments[~segments.geometry.is_empty & segments.geometry.notna()]
    if segments.empty:
        return {}

    LOGGER.info(
        "Running spatial join: %d segments x %d off-system speed zones",
        len(segments), len(spatial_zones),
    )

    joined = gpd.sjoin(
        segments[["geometry"]],
        spatial_zones[["geometry", "SPEED_LIMIT", "IS_SCHOOL_ZONE"]],
        how="inner",
        predicate="intersects",
    )

    if joined.empty:
        LOGGER.info("Spatial join produced no matches")
        return {}

    results: dict[Any, dict[str, Any]] = {}
    MIN_OVERLAP = 0.0  # require positive overlap (reject point-touches)

    for seg_idx, group in joined.groupby(joined.index):
        best_limit = None
        best_school = False
        best_overlap = MIN_OVERLAP
        seg_geom = segments.loc[seg_idx, "geometry"]
        for _, row in group.iterrows():
            zone_geom = spatial_zones.loc[row["index_right"], "geometry"]
            try:
                if zone_geom.geom_type in ("Point", "MultiPoint"):
                    overlap = seg_geom.length if seg_geom.intersects(zone_geom) else 0.0
                else:
                    overlap = seg_geom.intersection(zone_geom).length
            except Exception:
                overlap = 0.0
            if overlap > best_overlap:
                best_overlap = overlap
                best_limit = row["SPEED_LIMIT"]
                best_school = row.get("IS_SCHOOL_ZONE", False)
        if best_limit is not None:
            results[seg_idx] = {
                "SPEED_LIMIT": best_limit,
                "IS_SCHOOL_ZONE": best_school,
            }

    LOGGER.info("Spatial matching produced %d segment matches", len(results))
    return results


def apply_off_system_speed_zone_enrichment(
    gdf: gpd.GeoDataFrame,
    refresh: bool = False,
) -> gpd.GeoDataFrame:
    """Apply SpeedZone OffSystem data to segments not already speed-enriched.

    Layer 9 has no milepoint fields, so all matching is spatial.  Only
    non-state-highway segments (``PARSED_SYSTEM_CODE != "1"``) without an
    existing ``SPEED_LIMIT`` are candidates.
    """

    enriched = gdf.copy()

    try:
        raw = fetch_enrichment_layer("speed_zone_off_system", refresh=refresh)
    except Exception as exc:
        LOGGER.warning("Off-system speed zone enrichment unavailable: %s", exc)
        return enriched

    off_system = _normalize_off_system_speed_zones(raw)
    if off_system.empty:
        LOGGER.warning("No usable off-system speed zone records after normalization")
        return enriched

    already_filled = enriched["SPEED_LIMIT"].notna()
    off_system_mask = enriched["PARSED_SYSTEM_CODE"].astype(str) != "1"
    unfilled_mask = ~already_filled & off_system_mask
    unfilled_segments = enriched.loc[unfilled_mask]
    if unfilled_segments.empty:
        LOGGER.info("No unfilled off-system segments to match — skipping off-system enrichment")
        return enriched

    spatial_matches = _spatial_match_off_system(unfilled_segments, off_system)
    spatial_count = 0
    school_count = 0
    for idx, match in spatial_matches.items():
        enriched.at[idx, "SPEED_LIMIT"] = match["SPEED_LIMIT"]
        enriched.at[idx, "IS_SCHOOL_ZONE"] = match.get("IS_SCHOOL_ZONE", False)
        enriched.at[idx, "SPEED_LIMIT_SOURCE"] = "gdot_speed_zone_off_system"
        spatial_count += 1
        if match.get("IS_SCHOOL_ZONE", False):
            school_count += 1

    LOGGER.info(
        "Off-system spatial matches: %d segments (including %d school zones)",
        spatial_count, school_count,
    )

    return enriched


def apply_rnhp_enrichment(
    gdf: gpd.GeoDataFrame,
    refresh: bool = False,
) -> gpd.GeoDataFrame:
    """Apply all RNHP enrichment layers to roadway segments."""

    enriched = apply_speed_zone_enrichment(gdf, refresh=refresh)
    enriched = apply_off_system_speed_zone_enrichment(enriched, refresh=refresh)
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
        PROJECT_ROOT / "02-Data-Staging" / "reports" / "rnhp_enrichment_summary.json"
    )
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote RNHP enrichment summary to %s", output_path)
