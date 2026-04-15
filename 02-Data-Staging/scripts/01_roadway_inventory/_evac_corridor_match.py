"""Per-corridor evacuation route matching engine.

Shared by both the enrichment pipeline (evacuation_enrichment.py) and the QC
map generator (generate_qc_map.py).  Extracting matching logic into a single
module prevents drift between the two copies.

Algorithm overview — three passes:
  1. Named corridors: HWY_NAME prefix match + spatial overlap
  2. Concurrent fallback: unmatched state-system segments inside any corridor
  3. Null-name features: geometry-differenced residuals, spatial only
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import unary_union

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spatial constants (must stay in sync with evacuation_enrichment.py)
# ---------------------------------------------------------------------------
ROUTE_BUFFER_M = 30.0
SHORT_SEGMENT_MAX_M = 400.0
SHORT_SEGMENT_MIN_RATIO = 0.40
NORMAL_MIN_OVERLAP_M = 150.0
NORMAL_MIN_RATIO = 0.20
MEGA_SEGMENT_LENGTH_M = 10_000.0
MEGA_MIN_OVERLAP_M = 200.0
MEGA_MIN_RATIO = 0.50
MAX_ALIGNMENT_ANGLE_DEG = 30.0
MIN_INSIDE_CORRIDOR_RATIO = 0.10

# ---------------------------------------------------------------------------
# Suffix abbreviation map (GDOT HWY_NAME conventions)
# ---------------------------------------------------------------------------
_SUFFIX_ABBREV: dict[str, str] = {
    "spur": "SPUR",
    "business": "BUS",
    "connector": "CONN",
    "bypass": "BYP",
    "loop": "LOOP",
    "alternate": "ALT",
}

# ---------------------------------------------------------------------------
# Manual name map for unparseable evac names
# ---------------------------------------------------------------------------
_MANUAL_NAME_MAP: dict[str, dict[str, Any]] = {
    "Liberty Expy": {"hwy_patterns": ["US-19", "US-82"], "hpms_contains": "Liberty"},
    "Ocean Hwy": {"hwy_patterns": ["US-17"], "hpms_contains": "Ocean Hwy"},
}

# Families that belong to the signed state road system.
_STATE_SYSTEM_FAMILIES = frozenset(
    {"interstate", "us route", "u.s. route", "state route"}
)

# Match method precedence (higher = stronger)
_METHOD_PRECEDENCE = {
    "hwy_name+spatial": 4,
    "hpms+spatial": 3,
    "concurrent+spatial": 2,
    "spatial_only": 1,
}

# Route designation regex
_ROUTE_DESIGNATION_RE = re.compile(
    r"(?P<type>I|Interstate|US|SR|CR)\s*-?\s*"
    r"(?P<number>\d+)"
    r"(?:\s+(?P<suffix>North|South|East|West|Spur|Business|Connector|Bypass|Loop|Alternate))?",
    re.IGNORECASE,
)


# ===================================================================
# Geometry helpers
# ===================================================================

def _line_azimuth(geom):
    """Azimuth (radians) from first to last coordinate of a line geometry."""
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, MultiLineString):
        geom = max(geom.geoms, key=lambda g: g.length)
    if not isinstance(geom, LineString):
        return None
    coords = list(geom.coords)
    if len(coords) < 2:
        return None
    dx = coords[-1][0] - coords[0][0]
    dy = coords[-1][1] - coords[0][1]
    if abs(dx) < 1e-10 and abs(dy) < 1e-10:
        return None
    return math.atan2(dx, dy)


def _alignment_angle_deg(az1, az2):
    """Minimum angle (degrees) between two azimuths, direction-independent."""
    if az1 is None or az2 is None:
        return None
    diff = abs(az1 - az2) % math.pi
    return math.degrees(min(diff, math.pi - diff))


# ===================================================================
# Pattern building (Phase 1)
# ===================================================================

def _build_hwy_patterns(route_name: str) -> list[str]:
    """Convert an evac ROUTE_NAME into HWY_NAME prefix patterns.

    Examples::

        "SR 26"           -> ["SR-26"]
        "I 75 North"      -> ["I-75"]
        "I 16 Spur"       -> ["I-16 SPUR"]
        "SR 21 Business"  -> ["SR-21 BUS"]
        "SR 300 Connector" -> ["SR-300 CONN"]
        "SR 1/US 27"      -> ["SR-1", "US-27"]
        "CR 780"          -> ["CR-780"]
    """
    if not isinstance(route_name, str):
        return []
    normalized = re.sub(r"\s+", " ", route_name).strip()
    if not normalized:
        return []

    patterns: list[str] = []
    for part in normalized.split("/"):
        m = _ROUTE_DESIGNATION_RE.search(part.strip())
        if m is None:
            continue
        raw_type = m.group("type").upper()
        if raw_type in ("I", "INTERSTATE"):
            route_type = "I"
        elif raw_type == "US":
            route_type = "US"
        elif raw_type == "SR":
            route_type = "SR"
        elif raw_type == "CR":
            route_type = "CR"
        else:
            continue
        number = m.group("number")
        suffix_raw = m.group("suffix")

        # Directional suffixes (North/South/East/West) are stripped
        if suffix_raw and suffix_raw.lower() in _SUFFIX_ABBREV:
            abbrev = _SUFFIX_ABBREV[suffix_raw.lower()]
            patterns.append(f"{route_type}-{number} {abbrev}")
        else:
            patterns.append(f"{route_type}-{number}")

    return patterns


def _normalize_hpms(val) -> str:
    """Uppercase and strip semicolons/extra whitespace for HPMS matching."""
    return re.sub(r"[;\s]+", " ", str(val)).strip().upper()


def _build_hwy_prefix_index(
    segments: gpd.GeoDataFrame,
) -> dict[str, list[int]]:
    """Map HWY_NAME values and base prefixes to segment positional indices.

    Each HWY_NAME is indexed under its full value AND its base prefix
    (the part before the first space).  For example, ``"SR-300 CONN"``
    is indexed under both ``"SR-300 CONN"`` and ``"SR-300"``.
    """
    index: dict[str, list[int]] = {}
    hwy_col = segments["HWY_NAME"]
    for pos_idx in range(len(segments)):
        name = hwy_col.iat[pos_idx]
        if pd.isna(name):
            continue
        name = str(name)
        index.setdefault(name, []).append(pos_idx)
        base = name.split()[0] if " " in name else name
        if base != name:
            index.setdefault(base, []).append(pos_idx)
    return index


# ===================================================================
# Acceptance logic (exact copy from current code — do not change)
# ===================================================================

def _accept_overlap(
    overlap_len: float,
    overlap_ratio: float,
    segment_length_m: float,
    overlap_geom,
    route_geom,
    seg_geom,
    buffer_m: float,
) -> bool:
    """Apply tiered acceptance thresholds and angular alignment check.

    Returns True if the segment-corridor overlap passes all filters.
    """
    is_short = segment_length_m < SHORT_SEGMENT_MAX_M
    is_mega = segment_length_m > MEGA_SEGMENT_LENGTH_M

    # Tiered overlap thresholds
    if is_short:
        accepted = overlap_ratio >= SHORT_SEGMENT_MIN_RATIO
    elif is_mega:
        accepted = (
            overlap_len >= MEGA_MIN_OVERLAP_M
            and overlap_ratio >= MEGA_MIN_RATIO
        )
        if not accepted and overlap_len >= NORMAL_MIN_OVERLAP_M:
            accepted = True
    else:
        accepted = (
            overlap_len >= NORMAL_MIN_OVERLAP_M
            and overlap_ratio >= NORMAL_MIN_RATIO
        )

    # Angular alignment (skip for short segments)
    if accepted and not is_short and overlap_geom is not None:
        try:
            route_section = route_geom.intersection(seg_geom.buffer(buffer_m))
        except Exception:
            route_section = None
        seg_az = _line_azimuth(overlap_geom)
        route_az = _line_azimuth(route_section)
        angle = _alignment_angle_deg(seg_az, route_az)
        if angle is not None and angle > MAX_ALIGNMENT_ANGLE_DEG:
            accepted = False

    return accepted


# ===================================================================
# Segment evaluation helper
# ===================================================================

def _get_segment_length(segments: gpd.GeoDataFrame, idx: int) -> float:
    """Get segment length in meters, falling back to geometry length."""
    if "segment_length_m" in segments.columns:
        raw = segments.at[idx, "segment_length_m"]
        try:
            val = float(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            pass
    return float(segments.at[idx, "geometry"].length)


# ===================================================================
# Per-corridor matching engine (Phase 2)
# ===================================================================

def _per_corridor_evac_overlay(
    segments: gpd.GeoDataFrame,
    evac_routes: gpd.GeoDataFrame,
    name_field: str,
) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    """Three-pass per-corridor matching for evacuation routes.

    Returns
    -------
    results : dict[int, dict]
        ``{seg_idx: {"names": [...], "overlap_m": float, "overlap_ratio": float,
        "match_method": str}}``
    diagnostics : dict
        Corridor-level metrics for automated QC checks.
    """
    if evac_routes.empty or segments.empty:
        LOGGER.info("Per-corridor evac overlay: 0 candidates (empty input)")
        return {}, {"per_corridor_counts": {}, "match_method_breakdown": {}}

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot run spatial overlay")
        return {}, {}

    if evac_routes.crs != seg_crs:
        evac_routes = evac_routes.to_crs(seg_crs)

    # --- ONE-TIME SETUP ---
    LOGGER.info("Building HWY_NAME prefix index...")
    hwy_index = _build_hwy_prefix_index(segments)
    seg_sindex = segments.sindex

    # Pre-cache segment lengths and ROUTE_FAMILY for fast lookup
    has_seg_len = "segment_length_m" in segments.columns
    has_route_fam = "ROUTE_FAMILY" in segments.columns
    has_hpms = "HPMS_ROUTE_NAME" in segments.columns
    seg_geom_arr = segments.geometry.values  # numpy array of geometries

    results: dict[int, dict[str, Any]] = {}
    # corridor_name -> (corridor_buffer, corridor_line_geom)
    corridor_data: dict[str, tuple] = {}
    per_corridor_counts: dict[str, int] = {}
    method_counts: dict[str, int] = {
        "hwy_name+spatial": 0,
        "hpms+spatial": 0,
        "concurrent+spatial": 0,
        "spatial_only": 0,
    }
    multi_corridor_segments = 0

    # =========================================================
    # PASS 1: NAMED CORRIDORS
    # =========================================================
    named_mask = evac_routes[name_field].notna()
    named_routes = evac_routes[named_mask]

    if not named_routes.empty:
        LOGGER.info("Pass 1: processing %d named corridor groups...",
                     named_routes[name_field].nunique())

        for route_name, corridor_features in named_routes.groupby(name_field):
            route_name_str = str(route_name)
            per_corridor_counts.setdefault(route_name_str, 0)

            # PERF: buffer each feature individually, then union the polygons.
            # This avoids the expensive LineString union_all() which can take
            # 400+ seconds per corridor.
            individual_buffers = corridor_features.geometry.buffer(ROUTE_BUFFER_M)
            corridor_buffer = unary_union(individual_buffers.values)

            # Keep raw line geometry as MultiLineString for alignment checks
            # (just collect, don't union — collecting is O(n), union is O(n²))
            line_parts = []
            for g in corridor_features.geometry:
                if isinstance(g, MultiLineString):
                    line_parts.extend(g.geoms)
                elif isinstance(g, LineString):
                    line_parts.append(g)
            corridor_line_geom = MultiLineString(line_parts) if line_parts else None

            corridor_data[route_name_str] = (corridor_buffer, corridor_line_geom)

            # Determine matching strategy
            if route_name_str in _MANUAL_NAME_MAP:
                manual = _MANUAL_NAME_MAP[route_name_str]
                hwy_pats = manual["hwy_patterns"]
                hpms_substr = manual.get("hpms_contains", "")
                method = "hpms+spatial"

                candidate_positions: set[int] = set()
                for pat in hwy_pats:
                    candidate_positions.update(hwy_index.get(pat, []))

                # HPMS scan: only check spatial hits (not all 245K segments)
                if hpms_substr and has_hpms:
                    spatial_hits_for_hpms = set(
                        seg_sindex.query(corridor_buffer, predicate="intersects")
                    )
                    hpms_upper = hpms_substr.upper()
                    hpms_col = segments["HPMS_ROUTE_NAME"]
                    for pos_idx in spatial_hits_for_hpms:
                        hpms_val = hpms_col.iat[pos_idx]
                        if pd.notna(hpms_val) and hpms_upper in _normalize_hpms(hpms_val):
                            candidate_positions.add(pos_idx)
            else:
                patterns = _build_hwy_patterns(route_name_str)
                if not patterns:
                    continue
                method = "hwy_name+spatial"

                candidate_positions = set()
                for pat in patterns:
                    candidate_positions.update(hwy_index.get(pat, []))

            # Spatial filter
            spatial_hits = set(seg_sindex.query(corridor_buffer, predicate="intersects"))
            matched_positions = candidate_positions & spatial_hits

            corridor_match_count = 0
            for pos_idx in matched_positions:
                idx = segments.index[pos_idx]
                seg_geom = seg_geom_arr[pos_idx]
                seg_len_raw = segments.iat[pos_idx, segments.columns.get_loc("segment_length_m")] if has_seg_len else None
                try:
                    segment_length_m = float(seg_len_raw) if seg_len_raw is not None and pd.notna(seg_len_raw) and float(seg_len_raw) > 0 else float(seg_geom.length)
                except (TypeError, ValueError):
                    segment_length_m = float(seg_geom.length)

                try:
                    overlap_geom = seg_geom.intersection(corridor_buffer)
                    overlap_len = float(overlap_geom.length)
                except Exception:
                    continue

                overlap_ratio = overlap_len / segment_length_m if segment_length_m > 0 else 0.0

                accepted = _accept_overlap(
                    overlap_len, overlap_ratio, segment_length_m,
                    overlap_geom, corridor_line_geom, seg_geom, ROUTE_BUFFER_M,
                )
                if not accepted:
                    continue

                # Corridor proximity post-filter (per-corridor)
                # overlap_geom.length == inside_len (intersection already computed)
                inside_ratio = overlap_len / float(seg_geom.length) if seg_geom.length > 0 else 0.0
                if inside_ratio < MIN_INSIDE_CORRIDOR_RATIO:
                    continue

                corridor_match_count += 1
                _merge_result(
                    results, idx, route_name_str, method,
                    overlap_len, overlap_ratio,
                )

            per_corridor_counts[route_name_str] = corridor_match_count
            LOGGER.info(
                "Corridor '%s': %d matched (%d attr candidates, %d spatial hits, method=%s)",
                route_name_str, corridor_match_count,
                len(candidate_positions), len(spatial_hits), method,
            )

    # =========================================================
    # PASS 2: CONCURRENT ROUTE FALLBACK
    # =========================================================
    if corridor_data:
        LOGGER.info("Pass 2: concurrent route fallback...")

        # Build a list of (name, buffer, line_geom) for iteration
        corridor_list = [
            (name, buf, line_geom)
            for name, (buf, line_geom) in corridor_data.items()
        ]
        all_corridor_buffer = unary_union([buf for _, buf, _ in corridor_list])
        all_spatial_hits = set(seg_sindex.query(all_corridor_buffer, predicate="intersects"))

        matched_indices = set(results.keys())
        unmatched_positions: list[tuple[int, Any]] = []
        for pos_idx in all_spatial_hits:
            idx = segments.index[pos_idx]
            if idx in matched_indices:
                continue
            if has_route_fam:
                fam = segments.iat[pos_idx, segments.columns.get_loc("ROUTE_FAMILY")]
                if pd.notna(fam) and str(fam).strip().lower() not in _STATE_SYSTEM_FAMILIES:
                    continue
            unmatched_positions.append((pos_idx, idx))

        LOGGER.info(
            "Concurrent fallback: %d unmatched state-system segments to evaluate",
            len(unmatched_positions),
        )

        # Pre-filter: for each unmatched segment, find which corridor buffers
        # it intersects using per-corridor spatial queries (avoid testing all
        # corridors for every segment).
        # Build a mapping: pos_idx -> list of corridor indices that contain it
        from shapely import STRtree
        corridor_buffers_list = [buf for _, buf, _ in corridor_list]
        corridor_tree = STRtree(corridor_buffers_list)

        concurrent_matched = 0
        for pos_idx, idx in unmatched_positions:
            seg_geom = seg_geom_arr[pos_idx]
            seg_len_raw = segments.iat[pos_idx, segments.columns.get_loc("segment_length_m")] if has_seg_len else None
            try:
                segment_length_m = float(seg_len_raw) if seg_len_raw is not None and pd.notna(seg_len_raw) and float(seg_len_raw) > 0 else float(seg_geom.length)
            except (TypeError, ValueError):
                segment_length_m = float(seg_geom.length)

            # Find which corridors this segment intersects
            nearby_corridor_idxs = corridor_tree.query(seg_geom, predicate="intersects")

            best_overlap_len = 0.0
            best_overlap_ratio = 0.0
            best_overlap_geom = None
            best_corridor_line = None
            best_corridor_buffer = None
            best_corridor_name = None

            for ci in nearby_corridor_idxs:
                cname, buf, line_geom = corridor_list[ci]
                try:
                    ov = seg_geom.intersection(buf)
                    ov_len = float(ov.length)
                except Exception:
                    continue
                if ov_len > best_overlap_len:
                    best_overlap_len = ov_len
                    best_overlap_ratio = ov_len / segment_length_m if segment_length_m > 0 else 0.0
                    best_overlap_geom = ov
                    best_corridor_buffer = buf
                    best_corridor_line = line_geom
                    best_corridor_name = cname

            if best_overlap_len <= 0 or best_corridor_line is None:
                continue

            accepted = _accept_overlap(
                best_overlap_len, best_overlap_ratio, segment_length_m,
                best_overlap_geom, best_corridor_line, seg_geom, ROUTE_BUFFER_M,
            )
            if not accepted:
                continue

            inside_ratio = best_overlap_len / float(seg_geom.length) if seg_geom.length > 0 else 0.0
            if inside_ratio < MIN_INSIDE_CORRIDOR_RATIO:
                continue

            concurrent_matched += 1
            per_corridor_counts[best_corridor_name] = per_corridor_counts.get(best_corridor_name, 0) + 1
            _merge_result(
                results, idx, best_corridor_name, "concurrent+spatial",
                best_overlap_len, best_overlap_ratio,
            )

        LOGGER.info("Concurrent fallback matched %d segments", concurrent_matched)

    # =========================================================
    # PASS 3: NULL-NAME FEATURES
    # =========================================================
    null_features = evac_routes[~named_mask]
    null_matched = 0
    if not null_features.empty:
        LOGGER.info("Pass 3: %d null-name features...", len(null_features))

        # Build named corridor union for differencing (from buffered polygons)
        named_corridor_buffers = [buf for _, (buf, _) in corridor_data.items()]
        named_union = unary_union(named_corridor_buffers) if named_corridor_buffers else None

        for _, feature in null_features.iterrows():
            feat_geom = feature.geometry
            if feat_geom is None or feat_geom.is_empty:
                continue

            # Difference against named corridor buffers
            if named_union is not None:
                try:
                    residual = feat_geom.difference(named_union)
                except Exception:
                    residual = feat_geom
            else:
                residual = feat_geom

            if residual is None or residual.is_empty or residual.length < 100:
                continue

            residual_buffer = residual.buffer(ROUTE_BUFFER_M)
            spatial_hits = set(seg_sindex.query(residual_buffer, predicate="intersects"))

            matched_indices = set(results.keys())
            for pos_idx in spatial_hits:
                idx = segments.index[pos_idx]
                if idx in matched_indices:
                    continue

                if has_route_fam:
                    fam = segments.iat[pos_idx, segments.columns.get_loc("ROUTE_FAMILY")]
                    if pd.notna(fam) and str(fam).strip().lower() not in _STATE_SYSTEM_FAMILIES:
                        continue

                seg_geom = seg_geom_arr[pos_idx]
                seg_len_raw = segments.iat[pos_idx, segments.columns.get_loc("segment_length_m")] if has_seg_len else None
                try:
                    segment_length_m = float(seg_len_raw) if seg_len_raw is not None and pd.notna(seg_len_raw) and float(seg_len_raw) > 0 else float(seg_geom.length)
                except (TypeError, ValueError):
                    segment_length_m = float(seg_geom.length)

                try:
                    overlap_geom = seg_geom.intersection(residual_buffer)
                    overlap_len = float(overlap_geom.length)
                except Exception:
                    continue

                overlap_ratio = overlap_len / segment_length_m if segment_length_m > 0 else 0.0

                accepted = _accept_overlap(
                    overlap_len, overlap_ratio, segment_length_m,
                    overlap_geom, residual, seg_geom, ROUTE_BUFFER_M,
                )
                if not accepted:
                    continue

                inside_ratio = overlap_len / float(seg_geom.length) if seg_geom.length > 0 else 0.0
                if inside_ratio < MIN_INSIDE_CORRIDOR_RATIO:
                    continue

                null_matched += 1
                _merge_result(
                    results, idx, None, "spatial_only",
                    overlap_len, overlap_ratio,
                )

        LOGGER.info("Null-feature pass matched %d segments", null_matched)

    # =========================================================
    # Count match methods
    # =========================================================
    for match in results.values():
        mm = match.get("match_method", "spatial_only")
        method_counts[mm] = method_counts.get(mm, 0) + 1
        names = match.get("names", [])
        if len(names) > 1:
            multi_corridor_segments += 1

    diagnostics = {
        "per_corridor_counts": per_corridor_counts,
        "match_method_breakdown": method_counts,
        "multi_corridor_segments": multi_corridor_segments,
        "null_feature_matches": null_matched,
        "concurrent_fallback_matches": method_counts.get("concurrent+spatial", 0),
        "corridors_with_zero_matches": [
            name for name, count in per_corridor_counts.items() if count == 0
        ],
        "total_matched": len(results),
    }

    LOGGER.info(
        "Per-corridor evac overlay total: %d matched segments "
        "(hwy_name+spatial=%d, hpms+spatial=%d, concurrent+spatial=%d, spatial_only=%d)",
        len(results),
        method_counts["hwy_name+spatial"],
        method_counts["hpms+spatial"],
        method_counts["concurrent+spatial"],
        method_counts["spatial_only"],
    )

    return results, diagnostics


# ===================================================================
# Merge helper
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
        # Union corridor names
        if route_name:
            if route_name not in existing["names"]:
                existing["names"].append(route_name)
        # Keep max overlap metrics
        existing["overlap_m"] = max(existing["overlap_m"], overlap_m)
        existing["overlap_ratio"] = max(existing["overlap_ratio"], overlap_ratio)
        # Precedence for match method
        existing_prec = _METHOD_PRECEDENCE.get(existing["match_method"], 0)
        new_prec = _METHOD_PRECEDENCE.get(method, 0)
        if new_prec > existing_prec:
            existing["match_method"] = method
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
    baseline_total: int = 1682,
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

    # Blocking: corridors with zero matches (warn-only for short corridors
    # whose segments are typically covered by overlapping longer corridors)
    zero_corridors = diagnostics.get("corridors_with_zero_matches", [])
    if zero_corridors:
        warnings.append(
            f"WARNING: {len(zero_corridors)} corridors with zero matches: "
            f"{', '.join(zero_corridors[:10])}"
        )

    # Blocking: total within 10% of baseline
    margin = int(baseline_total * 0.10)
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

    # Warning: concurrent >10% of total
    concurrent = diagnostics.get("concurrent_fallback_matches", 0)
    if total > 0 and concurrent / total > 0.10:
        warnings.append(
            f"WARNING: concurrent+spatial matches ({concurrent}) are "
            f"{concurrent/total:.0%} of total — may indicate HWY_NAME "
            f"matching is too restrictive"
        )

    # Warning: spatial_only >5%
    mm = diagnostics.get("match_method_breakdown", {})
    spatial_only = mm.get("spatial_only", 0)
    if total > 0 and spatial_only / total > 0.05:
        warnings.append(
            f"WARNING: spatial_only matches ({spatial_only}) are "
            f"{spatial_only/total:.0%} of total"
        )

    passed = len(errors) == 0
    return passed, errors, warnings
