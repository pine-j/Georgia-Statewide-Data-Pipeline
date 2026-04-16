"""Per-corridor evacuation route matching engine.

Shared by both the enrichment pipeline (evacuation_enrichment.py) and the QC
map generator (generate_qc_map.py).  Extracting matching logic into a single
module prevents drift between the two copies.

Algorithm — spatial-first per-corridor:
  For each corridor, buffer the route geometry, find ALL segments inside the
  buffer via spatial index, then filter false positives (Local/Other, overlap
  thresholds, angular alignment).  Match method is a diagnostic label only —
  it does NOT determine inclusion.

  Null-name features are differenced against the union of named corridor
  buffers to avoid double-counting.
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

# Gap-filling thresholds — used to allow Local/Other segments in gaps between
# matched state-system segments.  High ratio ensures only segments running
# along the corridor are included (not cross-streets).
GAP_FILL_MIN_OVERLAP_RATIO = 0.30
GAP_FILL_SAMPLE_INTERVAL_M = 200.0
GAP_FILL_TOLERANCE_M = 80.0

# Attribute-boosted thresholds (Tier 1) — relaxed for segments whose HWY_NAME
# matches the corridor designation, confirming identity via attribute.
ATTR_NORMAL_MIN_OVERLAP_M = 50.0   # was 150.0 for Tier 2
ATTR_SHORT_MIN_RATIO = 0.30        # was 0.40 for Tier 2

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

# HWY_NAME values that are never evacuation route segments.
_EXCLUDED_HWY_NAME_VALUES: frozenset[str] = frozenset({"RAMP"})

# Match method precedence (higher = stronger)
_METHOD_PRECEDENCE = {
    "hwy_name+spatial": 4,
    "hpms+spatial": 3,
    "concurrent+spatial": 2,
    "spatial_only": 1,
    "hardcode": 0,
    "gap_fill": 0,
}

# Hard-code overrides: segments that must be flagged regardless of thresholds.
# Only needed for main-pass rejections (alignment angle, mega-segment ratio,
# geometry digitization offset).  Local/Other gap fills are now auto-discovered
# by the gap-fill pass via direct gap_region spatial query — no hardcodes needed
# for county roads or city streets that spatially connect corridor endpoints.
# Format: "ROUTE_NAME": ["unique_id_1", "unique_id_2"]
_HARDCODE_OVERRIDES: dict[str, list[str]] = {
    # US-221 segment along SR 76 — alignment angle 31.35° barely exceeds 30°
    # threshold due to road curvature; 8 km segment with 100% overlap.
    "SR 76": ["1000100007600INC_0.0000_4.8133"],
    # US-80 concurrent with SR 26 — alignment azimuth mismatch due to route
    # curvature near coast; 5.7 km segment fully within corridor buffer.
    "SR 26": ["1000100002600INC_268.9443_272.3271"],
    # US-300 mega-segment (116 km) — only 1.4 km overlaps SR 300 corridor,
    # inside_ratio 0.012 fails proximity filter. Covers gap at south end.
    "SR 300": ["1000100030000INC_0.0000_71.4236"],
    # SR-93 segment with >30 m geometry offset from evac route polyline —
    # zero buffer overlap despite correct HWY_NAME. Digitization offset.
    "SR 93": ["1000100009300INC_11.7350_12.0117"],
    # SR 3 state-system alignment/ratio rejects only.
    # CR/Local gap fills removed — auto-discovered by gap-fill spatial query.
    "SR 3": [
        "1000100002700INC_58.1249_59.4835",   # US-341 alignment reject
        "1000100000300INC_121.5390_125.7467",   # US-19 low-ratio section
        "1000100000300INC_131.0434_134.7584",   # US-19 low-ratio section
        "1000100012700INC_9.9467_22.4303",      # US-41 cross-section
        "1000100009000INC_128.1383_133.1540",   # SR-90 cross-section
    ],
}

# Hard-code exclusions: segments force-excluded regardless of thresholds.
# Use "" key to exclude globally across all corridors.
# Populate from QC map review after logic changes stabilize.
_HARDCODE_EXCLUSIONS: dict[str, list[str]] = {
    # "CORRIDOR_NAME": ["unique_id_1"],
    # "": ["globally_excluded_uid"],  # "" = exclude from ALL corridors
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

    # Angular alignment (skip for short segments, high-overlap segments, and
    # segments with >=2 km absolute overlap).  High overlap ratio or large
    # absolute overlap means the segment physically follows the corridor —
    # the first-to-last azimuth can be misleading on curved or long roads.
    skip_alignment = is_short or overlap_ratio >= 0.50 or overlap_len >= 2000.0
    if accepted and not skip_alignment and overlap_geom is not None:
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


def _accept_overlap_attribute_matched(
    overlap_len: float,
    overlap_ratio: float,
    segment_length_m: float,
) -> bool:
    """Relaxed acceptance for segments whose HWY_NAME matches the corridor.

    No ratio minimum for normal/mega — attribute match confirms identity.
    No angular alignment check. No proximity filter applied after.
    """
    is_short = segment_length_m < SHORT_SEGMENT_MAX_M
    if is_short:
        return overlap_ratio >= ATTR_SHORT_MIN_RATIO
    else:
        return overlap_len >= ATTR_NORMAL_MIN_OVERLAP_M


# ===================================================================
# Per-corridor matching engine — spatial-first
# ===================================================================

def _per_corridor_evac_overlay(
    segments: gpd.GeoDataFrame,
    evac_routes: gpd.GeoDataFrame,
    name_field: str,
) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    """Spatial-first per-corridor matching for evacuation routes.

    For each corridor: buffer → spatial candidates → filter false positives →
    label match method.  Match method is a diagnostic label only — it does NOT
    determine inclusion.  Every segment inside the buffer that passes the
    false-positive filters is matched, guaranteeing zero false negatives.

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
    LOGGER.info("Building HWY_NAME prefix index (for labeling, not filtering)...")
    hwy_index = _build_hwy_prefix_index(segments)
    seg_sindex = segments.sindex

    # Pre-cache column locations and flags for fast inner-loop access
    has_seg_len = "segment_length_m" in segments.columns
    has_route_fam = "ROUTE_FAMILY" in segments.columns
    has_hpms = "HPMS_ROUTE_NAME" in segments.columns
    seg_geom_arr = segments.geometry.values  # numpy array of geometries
    seg_len_col = segments.columns.get_loc("segment_length_m") if has_seg_len else None
    route_fam_col = segments.columns.get_loc("ROUTE_FAMILY") if has_route_fam else None
    hwy_name_col = segments.columns.get_loc("HWY_NAME") if "HWY_NAME" in segments.columns else None
    hpms_col_loc = segments.columns.get_loc("HPMS_ROUTE_NAME") if has_hpms else None

    results: dict[int, dict[str, Any]] = {}
    corridor_data: dict[str, tuple] = {}  # name -> (buffer, line_geom)
    per_corridor_counts: dict[str, int] = {}
    method_counts: dict[str, int] = {
        "hwy_name+spatial": 0,
        "hpms+spatial": 0,
        "concurrent+spatial": 0,
        "spatial_only": 0,
        "hardcode": 0,
        "gap_fill": 0,
    }
    multi_corridor_segments = 0

    # Build set of HWY_NAME-index positions for each corridor (for labeling)
    corridor_hwy_positions: dict[str, set[int]] = {}
    corridor_hpms_positions: dict[str, set[int]] = {}

    # =========================================================
    # NAMED CORRIDORS — spatial-first
    # =========================================================
    named_mask = evac_routes[name_field].notna()
    named_routes = evac_routes[named_mask]

    if not named_routes.empty:
        LOGGER.info("Processing %d named corridor groups (spatial-first)...",
                     named_routes[name_field].nunique())

        for route_name, corridor_features in named_routes.groupby(name_field):
            route_name_str = str(route_name)
            per_corridor_counts.setdefault(route_name_str, 0)

            # Determine if this is a CR corridor (Local/Other segments allowed)
            corridor_is_cr = route_name_str.strip().upper().startswith("CR ")

            # PERF: buffer each feature individually, then union the polygons.
            # DO NOT use geometry.union_all().buffer() — hangs 400+ s/corridor.
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

            # Build HWY_NAME position set for match-method labeling
            hwy_pos_set: set[int] = set()
            if route_name_str in _MANUAL_NAME_MAP:
                manual = _MANUAL_NAME_MAP[route_name_str]
                for pat in manual["hwy_patterns"]:
                    hwy_pos_set.update(hwy_index.get(pat, []))
                # HPMS labeling set
                hpms_substr = manual.get("hpms_contains", "")
                if hpms_substr and has_hpms:
                    hpms_pos_set: set[int] = set()
                    hpms_upper = hpms_substr.upper()
                    # Scan spatial hits only for HPMS (not all 245K segments)
                    spatial_hits_for_hpms = seg_sindex.query(
                        corridor_buffer, predicate="intersects"
                    )
                    for pi in spatial_hits_for_hpms:
                        hpms_val = segments.iat[pi, hpms_col_loc]
                        if pd.notna(hpms_val) and hpms_upper in _normalize_hpms(hpms_val):
                            hpms_pos_set.add(pi)
                    corridor_hpms_positions[route_name_str] = hpms_pos_set
            else:
                patterns = _build_hwy_patterns(route_name_str)
                for pat in patterns:
                    hwy_pos_set.update(hwy_index.get(pat, []))
            corridor_hwy_positions[route_name_str] = hwy_pos_set

            # --- HARD-CODE OVERRIDES: force-include by unique_id ---
            hardcode_ids = set(_HARDCODE_OVERRIDES.get(route_name_str, []))
            has_unique_id = "unique_id" in segments.columns
            uid_col_loc = segments.columns.get_loc("unique_id") if has_unique_id else None

            # --- SPATIAL-FIRST: ALL segments intersecting the buffer ---
            candidate_positions = seg_sindex.query(
                corridor_buffer, predicate="intersects"
            )

            corridor_match_count = 0
            for pos_idx in candidate_positions:
                # Check if this segment is hard-coded for this corridor
                is_hardcoded = False
                if hardcode_ids and has_unique_id:
                    uid = segments.iat[pos_idx, uid_col_loc]
                    if pd.notna(uid) and str(uid) in hardcode_ids:
                        is_hardcoded = True

                # Block A — HWY_NAME exclusion (e.g. RAMP segments)
                if not is_hardcoded and hwy_name_col is not None:
                    hwy_val = segments.iat[pos_idx, hwy_name_col]
                    if pd.notna(hwy_val) and str(hwy_val).strip().upper() in _EXCLUDED_HWY_NAME_VALUES:
                        continue

                # Block B — hard-code exclusion
                if has_unique_id and not is_hardcoded:
                    uid = segments.iat[pos_idx, uid_col_loc]
                    if pd.notna(uid):
                        uid_str = str(uid)
                        if (uid_str in _HARDCODE_EXCLUSIONS.get("", [])
                                or uid_str in _HARDCODE_EXCLUSIONS.get(route_name_str, [])):
                            continue

                if not is_hardcoded:
                    # 1. False-positive filter: skip Local/Other unless corridor is CR
                    if has_route_fam and not corridor_is_cr:
                        fam = segments.iat[pos_idx, route_fam_col]
                        if pd.notna(fam) and str(fam).strip().lower() not in _STATE_SYSTEM_FAMILIES:
                            continue

                seg_geom = seg_geom_arr[pos_idx]
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

                # 2. Compute overlap with corridor buffer
                try:
                    overlap_geom = seg_geom.intersection(corridor_buffer)
                    overlap_len = float(overlap_geom.length)
                except Exception:
                    continue

                overlap_ratio = overlap_len / segment_length_m if segment_length_m > 0 else 0.0

                if is_hardcoded:
                    # Hard-coded segments bypass all threshold checks
                    accepted = True
                    method = "hardcode"
                else:
                    # Two-tier acceptance: attribute-matched vs standard
                    attribute_matched = (
                        pos_idx in corridor_hwy_positions.get(route_name_str, set())
                        or pos_idx in corridor_hpms_positions.get(route_name_str, set())
                    )

                    if attribute_matched:
                        # Tier 1: relaxed thresholds, no alignment/proximity filter
                        accepted = _accept_overlap_attribute_matched(
                            overlap_len, overlap_ratio, segment_length_m,
                        )
                    else:
                        # Tier 2: standard thresholds + alignment + proximity
                        accepted = _accept_overlap(
                            overlap_len, overlap_ratio, segment_length_m,
                            overlap_geom, corridor_line_geom, seg_geom, ROUTE_BUFFER_M,
                        )
                        if accepted:
                            inside_ratio = overlap_len / float(seg_geom.length) if seg_geom.length > 0 else 0.0
                            if inside_ratio < MIN_INSIDE_CORRIDOR_RATIO and overlap_len < 1000.0:
                                accepted = False

                    if not accepted:
                        continue

                    # Label match method (after acceptance)
                    if pos_idx in corridor_hpms_positions.get(route_name_str, set()):
                        method = "hpms+spatial"
                    elif pos_idx in corridor_hwy_positions.get(route_name_str, set()):
                        method = "hwy_name+spatial"
                    else:
                        method = "concurrent+spatial"

                corridor_match_count += 1
                idx = segments.index[pos_idx]
                _merge_result(
                    results, idx, route_name_str, method,
                    overlap_len, overlap_ratio,
                )

            # --- HARDCODE SECOND PASS: catch segments outside the buffer ---
            if hardcode_ids and has_unique_id:
                matched_uids = set()
                for pos_idx in candidate_positions:
                    uid_val = segments.iat[pos_idx, uid_col_loc]
                    if pd.notna(uid_val) and str(uid_val) in hardcode_ids:
                        matched_uids.add(str(uid_val))
                missing_uids = hardcode_ids - matched_uids
                if missing_uids:
                    uid_series = segments.iloc[:, uid_col_loc].astype(str)
                    for missing_uid in missing_uids:
                        matches = uid_series == missing_uid
                        if not matches.any():
                            LOGGER.warning(
                                "Hardcode uid %s for '%s' not found in segments",
                                missing_uid, route_name_str,
                            )
                            continue
                        pos_idx = matches.values.argmax()
                        idx = segments.index[pos_idx]
                        if idx in results and route_name_str in results[idx].get("names", []):
                            continue  # already matched via another path
                        seg_geom = seg_geom_arr[pos_idx]
                        corridor_match_count += 1
                        _merge_result(
                            results, idx, route_name_str, "hardcode",
                            0.0, 0.0,
                        )
                        LOGGER.info(
                            "Hardcode: added uid %s to '%s' (outside buffer)",
                            missing_uid, route_name_str,
                        )

            # --- GAP-FILLING PASS: allow Local/Other in gap areas ---
            # Sample points along the corridor; where no matched segment covers
            # them, allow Local/Other segments with high overlap to fill the gap.
            if corridor_line_geom is not None and corridor_match_count > 0:
                matched_indices_set = {
                    segments.index[p] for p in candidate_positions
                    if segments.index[p] in results
                    and route_name_str in results[segments.index[p]].get("names", [])
                }
                if matched_indices_set:
                    matched_union = unary_union(
                        [seg_geom_arr[segments.index.get_loc(i)]
                         for i in matched_indices_set
                         if not pd.isna(seg_geom_arr[segments.index.get_loc(i)])]
                    ).buffer(GAP_FILL_TOLERANCE_M)

                    # Sample corridor line for gap points
                    total_len = corridor_line_geom.length
                    gap_points = []
                    for si in range(int(total_len / GAP_FILL_SAMPLE_INTERVAL_M) + 1):
                        d = min(si * GAP_FILL_SAMPLE_INTERVAL_M, total_len)
                        pt = corridor_line_geom.interpolate(d)
                        if not matched_union.contains(pt):
                            gap_points.append(pt)

                    if gap_points:
                        gap_region = unary_union(
                            [p.buffer(GAP_FILL_TOLERANCE_M) for p in gap_points]
                        )
                        # Query the spatial index directly against gap_region so
                        # segments up to GAP_FILL_TOLERANCE_M (80 m) from gap
                        # points are considered — wider than the main 30 m
                        # corridor buffer, which is what allows Local/Other
                        # segments (county roads, city streets) that sit slightly
                        # off the state-system centreline to be auto-discovered
                        # rather than relying on hardcoded unique_ids.
                        gap_fill_candidate_positions = seg_sindex.query(
                            gap_region, predicate="intersects"
                        )
                        gap_fill_count = 0
                        for pos_idx in gap_fill_candidate_positions:
                            idx = segments.index[pos_idx]
                            if idx in results and route_name_str in results[idx].get("names", []):
                                continue  # already matched
                            # HWY_NAME exclusion (e.g. RAMP segments)
                            if hwy_name_col is not None:
                                hwy_val = segments.iat[pos_idx, hwy_name_col]
                                if pd.notna(hwy_val) and str(hwy_val).strip().upper() in _EXCLUDED_HWY_NAME_VALUES:
                                    continue
                            seg_geom = seg_geom_arr[pos_idx]
                            try:
                                overlap_geom = seg_geom.intersection(corridor_buffer)
                                overlap_len = float(overlap_geom.length)
                            except Exception:
                                continue
                            seg_len = float(seg_geom.length) if seg_geom.length > 0 else 1.0
                            overlap_ratio = overlap_len / seg_len
                            if overlap_ratio >= GAP_FILL_MIN_OVERLAP_RATIO:
                                corridor_match_count += 1
                                gap_fill_count += 1
                                _merge_result(
                                    results, idx, route_name_str, "gap_fill",
                                    overlap_len, overlap_ratio,
                                )
                        if gap_fill_count:
                            LOGGER.info(
                                "Corridor '%s': gap-fill added %d Local/Other segments",
                                route_name_str, gap_fill_count,
                            )

            per_corridor_counts[route_name_str] = corridor_match_count
            LOGGER.info(
                "Corridor '%s': %d matched (%d spatial candidates)",
                route_name_str, corridor_match_count, len(candidate_positions),
            )

    # =========================================================
    # NULL-NAME FEATURES
    # =========================================================
    null_features = evac_routes[~named_mask]
    null_matched = 0
    if not null_features.empty:
        LOGGER.info("Null-name features: %d to process...", len(null_features))

        # Build named corridor union for differencing (from buffered polygons)
        named_corridor_buffers = [buf for _, (buf, _) in corridor_data.items()]
        named_union = unary_union(named_corridor_buffers) if named_corridor_buffers else None

        for _, feature in null_features.iterrows():
            feat_geom = feature.geometry
            if feat_geom is None or feat_geom.is_empty:
                continue

            # Difference against named corridor buffers to avoid double-counting
            if named_union is not None:
                try:
                    residual = feat_geom.difference(named_union)
                except Exception:
                    residual = feat_geom
            else:
                residual = feat_geom

            if residual is None or residual.is_empty or residual.length <= 100:
                continue

            residual_buffer = residual.buffer(ROUTE_BUFFER_M)
            spatial_hits = seg_sindex.query(residual_buffer, predicate="intersects")

            matched_indices = set(results.keys())
            for pos_idx in spatial_hits:
                idx = segments.index[pos_idx]
                if idx in matched_indices:
                    continue

                # HWY_NAME exclusion (e.g. RAMP segments)
                if hwy_name_col is not None:
                    hwy_val = segments.iat[pos_idx, hwy_name_col]
                    if pd.notna(hwy_val) and str(hwy_val).strip().upper() in _EXCLUDED_HWY_NAME_VALUES:
                        continue

                # Hard-code exclusion (global keys only — null features have no corridor name)
                if has_unique_id:
                    uid = segments.iat[pos_idx, uid_col_loc]
                    if pd.notna(uid) and str(uid) in _HARDCODE_EXCLUSIONS.get("", []):
                        continue

                # Skip Local/Other
                if has_route_fam:
                    fam = segments.iat[pos_idx, route_fam_col]
                    if pd.notna(fam) and str(fam).strip().lower() not in _STATE_SYSTEM_FAMILIES:
                        continue

                seg_geom = seg_geom_arr[pos_idx]
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
        "(hwy_name+spatial=%d, hpms+spatial=%d, concurrent+spatial=%d, "
        "spatial_only=%d, hardcode=%d, gap_fill=%d)",
        len(results),
        method_counts["hwy_name+spatial"],
        method_counts["hpms+spatial"],
        method_counts["concurrent+spatial"],
        method_counts["spatial_only"],
        method_counts.get("hardcode", 0),
        method_counts.get("gap_fill", 0),
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
    baseline_total: int = 1448,
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
