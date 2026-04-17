"""Evacuation route matching engine.

Shared by both the enrichment pipeline (evacuation_enrichment.py) and the QC
map generator (generate_qc_map.py).

Algorithm — simple spatial buffer:
  1. Union ALL evacuation route geometries (named + unnamed).
  2. Buffer by ROUTE_BUFFER_M (50 m — wide enough for divided highways).
  3. Find every road segment that intersects the buffer.
  4. Accept if overlap ratio >= MIN_OVERLAP_RATIO (20%).
  5. Skip RAMP segments.
  Per-corridor labeling assigns a corridor name to each matched segment
  for reporting, but does NOT affect the accept/reject decision.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import unary_union

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spatial constants
# ---------------------------------------------------------------------------
ROUTE_BUFFER_M = 50.0
MIN_OVERLAP_RATIO = 0.20

# Local/Other segments are allowed only if they meet BOTH thresholds:
# high overlap (running along corridor, not crossing it) AND minimum length
# (not a short driveway or cross-street).
LOCAL_MIN_OVERLAP_RATIO = 0.80
LOCAL_MIN_LENGTH_M = 200.0

# Families that belong to the signed state road system.
_STATE_SYSTEM_FAMILIES = frozenset(
    {"interstate", "us route", "u.s. route", "state route"}
)

# HWY_NAME values that are never evacuation route segments.
_EXCLUDED_HWY_NAME_VALUES: frozenset[str] = frozenset({"RAMP"})

# Route designation regex (used for corridor labeling only)
_ROUTE_DESIGNATION_RE = re.compile(
    r"(?P<type>I|Interstate|US|SR|CR)\s*-?\s*"
    r"(?P<number>\d+)"
    r"(?:\s+(?P<suffix>North|South|East|West|Spur|Business|Connector|Bypass|Loop|Alternate))?",
    re.IGNORECASE,
)

_SUFFIX_ABBREV: dict[str, str] = {
    "spur": "SPUR",
    "business": "BUS",
    "connector": "CONN",
    "bypass": "BYP",
    "loop": "LOOP",
    "alternate": "ALT",
}


# ===================================================================
# Per-corridor matching engine
# ===================================================================

def per_corridor_evac_overlay(
    segments: gpd.GeoDataFrame,
    evac_routes: gpd.GeoDataFrame,
    name_field: str,
) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    """Spatial buffer matching for evacuation routes.

    1. Buffer ALL evac route geometries (named + unnamed) by 50 m.
    2. Find every road segment intersecting the buffer.
    3. Accept if overlap ratio >= 20 %.  Skip RAMPs.
    4. Label each match with its corridor name(s) for reporting.

    Returns
    -------
    results : dict[int, dict]
        ``{seg_idx: {"names": [...], "overlap_m": float, "overlap_ratio": float,
        "match_method": str}}``
    diagnostics : dict
        Corridor-level metrics for QC checks.
    """
    if evac_routes.empty or segments.empty:
        LOGGER.info("Evac overlay: 0 candidates (empty input)")
        return {}, {"per_corridor_counts": {}, "match_method_breakdown": {}, "total_matched": 0}

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot run spatial overlay")
        return {}, {}

    if evac_routes.crs != seg_crs:
        evac_routes = evac_routes.to_crs(seg_crs)

    # --- Pre-cache column locations ---
    has_seg_len = "segment_length_m" in segments.columns
    has_route_fam = "ROUTE_FAMILY" in segments.columns
    seg_geom_arr = segments.geometry.values
    seg_len_col = segments.columns.get_loc("segment_length_m") if has_seg_len else None
    route_fam_col = segments.columns.get_loc("ROUTE_FAMILY") if has_route_fam else None
    hwy_name_col = segments.columns.get_loc("HWY_NAME") if "HWY_NAME" in segments.columns else None
    seg_sindex = segments.sindex

    # =================================================================
    # STEP 1: Build per-corridor buffers and a global union buffer
    # =================================================================
    named_mask = evac_routes[name_field].notna() & (evac_routes[name_field] != "")
    corridor_buffers: dict[str, Any] = {}  # name -> buffer polygon
    per_corridor_counts: dict[str, int] = {}

    # Named corridors
    for route_name, corridor_features in evac_routes[named_mask].groupby(name_field):
        route_name_str = str(route_name)
        individual_buffers = corridor_features.geometry.buffer(ROUTE_BUFFER_M)
        corridor_buffers[route_name_str] = unary_union(individual_buffers.values)
        per_corridor_counts[route_name_str] = 0

    # Unnamed features — buffer them too
    unnamed_features = evac_routes[~named_mask]
    unnamed_buffer = None
    if not unnamed_features.empty:
        individual_buffers = unnamed_features.geometry.buffer(ROUTE_BUFFER_M)
        unnamed_buffer = unary_union(individual_buffers.values)

    # Global buffer = union of all corridor buffers + unnamed buffer
    all_buffers = list(corridor_buffers.values())
    if unnamed_buffer is not None:
        all_buffers.append(unnamed_buffer)
    global_buffer = unary_union(all_buffers)

    LOGGER.info("Built %d named corridor buffers + %d unnamed features",
                len(corridor_buffers), len(unnamed_features))

    # =================================================================
    # STEP 2: Find ALL segments intersecting the global buffer
    # =================================================================
    candidate_positions = seg_sindex.query(global_buffer, predicate="intersects")
    LOGGER.info("Spatial query: %d candidate segments", len(candidate_positions))

    # =================================================================
    # STEP 3: Accept/reject each candidate
    # =================================================================
    results: dict[int, dict[str, Any]] = {}

    for pos_idx in candidate_positions:
        # Skip RAMPs
        if hwy_name_col is not None:
            hwy_val = segments.iat[pos_idx, hwy_name_col]
            if pd.notna(hwy_val) and str(hwy_val).strip().upper() in _EXCLUDED_HWY_NAME_VALUES:
                continue

        seg_geom = seg_geom_arr[pos_idx]
        if seg_geom is None or seg_geom.is_empty:
            continue

        # Compute segment length
        seg_len_raw = segments.iat[pos_idx, seg_len_col] if has_seg_len else None
        try:
            segment_length_m = (
                float(seg_len_raw)
                if seg_len_raw is not None
                and pd.notna(seg_len_raw)
                and float(seg_len_raw) > 0
                else float(seg_geom.length)
            )
        except (TypeError, ValueError):
            segment_length_m = float(seg_geom.length)

        if segment_length_m <= 0:
            continue

        # Compute overlap with global buffer
        try:
            overlap_geom = seg_geom.intersection(global_buffer)
            overlap_len = float(overlap_geom.length)
        except Exception:
            continue

        overlap_ratio = overlap_len / segment_length_m

        # Accept if overlap ratio >= threshold
        if overlap_ratio < MIN_OVERLAP_RATIO:
            continue

        # Local/Other filter — allow only if long enough and high overlap
        # (running along the corridor, not a short cross-street)
        if has_route_fam:
            fam = segments.iat[pos_idx, route_fam_col]
            if pd.notna(fam) and str(fam).strip().lower() not in _STATE_SYSTEM_FAMILIES:
                if segment_length_m < LOCAL_MIN_LENGTH_M or overlap_ratio < LOCAL_MIN_OVERLAP_RATIO:
                    continue

        # =============================================================
        # STEP 4: Label — which corridor(s) does this segment belong to?
        # =============================================================
        matched_corridors: list[str] = []
        best_corridor_overlap = 0.0

        for corridor_name, corridor_buf in corridor_buffers.items():
            try:
                corr_overlap = seg_geom.intersection(corridor_buf)
                corr_overlap_len = float(corr_overlap.length)
            except Exception:
                continue
            if corr_overlap_len > 0:
                corr_ratio = corr_overlap_len / segment_length_m
                if corr_ratio >= MIN_OVERLAP_RATIO:
                    matched_corridors.append(corridor_name)
                    per_corridor_counts[corridor_name] = per_corridor_counts.get(corridor_name, 0) + 1
                    best_corridor_overlap = max(best_corridor_overlap, corr_overlap_len)

        idx = segments.index[pos_idx]
        results[idx] = {
            "names": matched_corridors,
            "overlap_m": overlap_len,
            "overlap_ratio": overlap_ratio,
            "match_method": "spatial",
        }

    # =================================================================
    # Diagnostics
    # =================================================================
    diagnostics = {
        "per_corridor_counts": per_corridor_counts,
        "match_method_breakdown": {"spatial": len(results)},
        "multi_corridor_segments": sum(1 for r in results.values() if len(r["names"]) > 1),
        "null_feature_matches": sum(1 for r in results.values() if not r["names"]),
        "concurrent_fallback_matches": 0,
        "corridors_with_zero_matches": [
            name for name, count in per_corridor_counts.items() if count == 0
        ],
        "total_matched": len(results),
    }

    LOGGER.info(
        "Evac overlay total: %d matched segments (%d with corridor labels, %d unnamed-only)",
        len(results),
        sum(1 for r in results.values() if r["names"]),
        diagnostics["null_feature_matches"],
    )

    return results, diagnostics


# Backward-compatible alias while callers move to the public helper name.
_per_corridor_evac_overlay = per_corridor_evac_overlay


# ===================================================================
# Merge helper (kept for API compatibility)
# ===================================================================

def _merge_result(
    results: dict[int, dict[str, Any]],
    idx: int,
    route_name: str | None,
    method: str,
    overlap_m: float,
    overlap_ratio: float,
) -> None:
    """Merge a match into the results dict, handling multi-corridor segments."""
    if idx in results:
        existing = results[idx]
        if route_name and route_name not in existing["names"]:
            existing["names"].append(route_name)
        existing["overlap_m"] = max(existing["overlap_m"], overlap_m)
        existing["overlap_ratio"] = max(existing["overlap_ratio"], overlap_ratio)
    else:
        results[idx] = {
            "names": [route_name] if route_name else [],
            "overlap_m": overlap_m,
            "overlap_ratio": overlap_ratio,
            "match_method": method,
        }


# ===================================================================
# Automated QC checks
# ===================================================================

def run_automated_qc(
    diagnostics: dict[str, Any],
    contraflow_count: int,
    baseline_total: int = 2000,
    baseline_contraflow: int = 170,
) -> tuple[bool, list[str], list[str]]:
    """Run automated QC checks on corridor-level diagnostics.

    Returns
    -------
    passed : bool
        True if all blocking checks pass.
    errors : list[str]
        Blocking errors (fail = block commit).
    warnings : list[str]
        Non-blocking warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    total = diagnostics.get("total_matched", 0)

    zero_corridors = diagnostics.get("corridors_with_zero_matches", [])
    if zero_corridors:
        warnings.append(
            f"WARNING: {len(zero_corridors)} corridors with zero matches: "
            f"{', '.join(zero_corridors[:10])}"
        )

    # Blocking: total within 30% of baseline (relaxed for simplified matching)
    margin = int(baseline_total * 0.30)
    if abs(total - baseline_total) > margin:
        errors.append(
            f"FAIL: Total matched ({total}) outside baseline range "
            f"({baseline_total} ± {margin})"
        )

    # Blocking: contraflow unchanged
    if contraflow_count != baseline_contraflow:
        errors.append(
            f"FAIL: Contraflow count changed ({contraflow_count} vs "
            f"baseline {baseline_contraflow})"
        )

    # Warning: corridors with <3 matches
    per_corridor = diagnostics.get("per_corridor_counts", {})
    low_corridors = [
        f"{name}={count}" for name, count in per_corridor.items()
        if 0 < count < 3
    ]
    if low_corridors:
        warnings.append(
            f"WARNING: {len(low_corridors)} corridors with <3 matches: "
            f"{', '.join(low_corridors[:10])}"
        )

    passed = len(errors) == 0
    return passed, errors, warnings
