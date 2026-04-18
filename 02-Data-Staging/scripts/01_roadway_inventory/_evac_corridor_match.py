"""Evacuation route matching engine.

Shared by both the enrichment pipeline (evacuation_enrichment.py) and the QC
map generator (generate_qc_map.py).

Algorithm — per-corridor spatial buffer + auto-discovered attribute filter:
  1. For each named evac corridor (and each unnamed feature treated as its own
     corridor), build a loose (ROUTE_BUFFER_M) and a tight (DISCOVERY_BUFFER_M)
     buffer.
  2. Auto-discover the HWY_NAME values that follow each corridor: any segment
     whose centerline lies tightly within the tight buffer (>= DISCOVERY_MIN_RATIO
     overlap, >= DISCOVERY_MIN_LENGTH_M long) contributes its HWY_NAME to the
     corridor's valid_prefixes set. This captures concurrent designations
     (e.g., US-19 on SR 3, US-341 on SR 27) without a hardcoded map.
  3. Each named corridor's valid_prefixes is additionally seeded with the
     designation parsed from the corridor name itself (e.g., "SR 3" -> {"SR-3"}).
  4. Main pass: a segment is flagged for a corridor only if (a) it overlaps the
     corridor's loose buffer at >= MIN_OVERLAP_RATIO AND (b) its HWY_NAME is in
     the corridor's valid_prefixes (state-system segments) OR it passes the
     strict Local/Other filter (>= LOCAL_MIN_OVERLAP_RATIO, >= LOCAL_MIN_LENGTH_M).
     AND (c) the segment passes the angular alignment check — the segment's
     azimuth must be within MAX_ALIGNMENT_ANGLE_DEG of the corridor's local
     azimuth, unless the segment is short (<ALIGNMENT_SKIP_LENGTH_M, where
     azimuth is unreliable) or the overlap is strong enough to prove the
     segment runs along the corridor (ratio > ALIGNMENT_SKIP_RATIO OR
     absolute overlap > ALIGNMENT_SKIP_ABS_OVERLAP_M).
  5. Unnamed evac route features are each treated as an individual corridor
     (rather than merged into one anonymous blob). This tightens the
     attribute filter — a segment whose HWY_NAME is only in some OTHER
     unnamed feature's valid_prefixes no longer passes. Segments matched
     only to unnamed features get SEC_EVAC_ROUTE_NAME=null.
  6. Mega-segments (>MEGA_SEGMENT_LENGTH_M) must meet a stricter overlap
     ratio (>= MEGA_MIN_RATIO). A 12 km segment with 25% overlap has 9 km
     of unrelated road outside the corridor — flagging it paints the whole
     length red when only a fraction actually follows the evac route.
  7. RAMP segments are always skipped.
"""

from __future__ import annotations

import logging
import math
import re
from collections import defaultdict
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

# Local/Other (off-system) segments gate: must run along the corridor, not
# cross it. Strict length + overlap avoids short driveways and cross-streets.
LOCAL_MIN_OVERLAP_RATIO = 0.80
LOCAL_MIN_LENGTH_M = 200.0

# Designation auto-discovery: segments whose centerlines lie tightly within
# the corridor are near-certain to carry the corridor. Their HWY_NAMEs become
# the "valid" set for that corridor. Accept EITHER a high ratio (segment is
# mostly inside the tight buffer) OR a long absolute overlap (segment tracks
# the corridor for hundreds of meters even if it extends past it). The
# absolute threshold recovers concurrent designations on corridors whose
# source features are shorter than the road segments they cover (e.g., SR 17
# with 1.1 km features crossing 3.3 km US-80 segments).
DISCOVERY_BUFFER_M = 20.0
DISCOVERY_MIN_RATIO = 0.70
DISCOVERY_MIN_ABS_OVERLAP_M = 300.0
DISCOVERY_MIN_LENGTH_M = 300.0

# Angular alignment check: a segment that briefly crosses a corridor at a
# large angle (e.g., a 600 m cross-street catching 20% overlap at 90°) is a
# false positive. We require the segment's azimuth to be within
# MAX_ALIGNMENT_ANGLE_DEG of the corridor's local azimuth. The check is
# skipped for:
#   (a) Segments shorter than ALIGNMENT_SKIP_LENGTH_M (azimuth unreliable
#       on short geometry, and short segments that sit entirely inside a
#       corridor are exactly what we want to flag).
#   (b) Segments whose overlap ratio exceeds ALIGNMENT_SKIP_RATIO or whose
#       absolute overlap exceeds ALIGNMENT_SKIP_ABS_OVERLAP_M — in either
#       case the segment is already proven to run along the corridor.
MAX_ALIGNMENT_ANGLE_DEG = 30.0
ALIGNMENT_SKIP_LENGTH_M = 400.0
ALIGNMENT_SKIP_RATIO = 0.50
ALIGNMENT_SKIP_ABS_OVERLAP_M = 2000.0
ALIGNMENT_SAMPLE_BUFFER_M = 60.0

# Mega-segment gate: a segment longer than MEGA_SEGMENT_LENGTH_M with a
# moderate overlap ratio is painting a long stretch of unrelated road red.
# Require a strong ratio or a very large absolute overlap for such segments.
MEGA_SEGMENT_LENGTH_M = 10_000.0
MEGA_MIN_RATIO = 0.50

# Families that belong to the signed state road system.
_STATE_SYSTEM_FAMILIES = frozenset(
    {"interstate", "us route", "u.s. route", "state route"}
)

# HWY_NAME values that are never evacuation route segments.
_EXCLUDED_HWY_NAME_VALUES: frozenset[str] = frozenset({"RAMP"})

# Route designation regex — parses corridor names like "SR 3", "I 75 North",
# "US 341 Spur" into HWY_NAME-style prefixes ("SR-3", "I-75", "US-341 SPUR").
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

_DIRECTIONAL_SUFFIXES = frozenset({"north", "south", "east", "west"})

# Sentinel label used internally for the "anonymous corridor" built from
# unnamed evacuation route features. These features still represent real
# corridors (I-95, I-16 mainline, US-17, US-301, etc. in the GDOT source),
# they just have a null ROUTE_NAME. Matches against this group are flagged
# but labeled with None so they show up as unnamed on the QC map.
_UNNAMED_CORRIDOR = "__UNNAMED__"


def _parse_corridor_prefixes(route_name: str) -> list[str]:
    """Parse a corridor name into HWY_NAME-style prefixes.

    Examples::

        "SR 26"            -> ["SR-26"]
        "I 75 North"       -> ["I-75"]
        "I 16 Spur"        -> ["I-16 SPUR"]
        "SR 1/US 27"       -> ["SR-1", "US-27"]
        "Liberty Expy"     -> []
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
            rt = "I"
        elif raw_type == "US":
            rt = "US"
        elif raw_type == "SR":
            rt = "SR"
        elif raw_type == "CR":
            rt = "CR"
        else:
            continue
        number = m.group("number")
        suffix_raw = m.group("suffix")
        if suffix_raw and suffix_raw.lower() in _SUFFIX_ABBREV:
            patterns.append(f"{rt}-{number} {_SUFFIX_ABBREV[suffix_raw.lower()]}")
        else:
            patterns.append(f"{rt}-{number}")
    return patterns


def _clean_hwy_name(raw: Any) -> str | None:
    """Normalize a HWY_NAME value for valid-prefix comparison."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip().upper()
    if not s:
        return None
    return s


def _line_azimuth(geom) -> float | None:
    """Azimuth (radians) from first to last coordinate of a line geometry.

    For MultiLineString, uses the longest constituent line.
    """
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, MultiLineString):
        try:
            geom = max(geom.geoms, key=lambda g: g.length)
        except ValueError:
            return None
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


def _alignment_angle_deg(az1: float | None, az2: float | None) -> float | None:
    """Minimum angle (degrees) between two azimuths, direction-independent."""
    if az1 is None or az2 is None:
        return None
    diff = abs(az1 - az2) % math.pi
    return math.degrees(min(diff, math.pi - diff))


# ===================================================================
# Per-corridor matching engine
# ===================================================================

def per_corridor_evac_overlay(
    segments: gpd.GeoDataFrame,
    evac_routes: gpd.GeoDataFrame,
    name_field: str,
) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    """Per-corridor matching with auto-discovered attribute filter."""
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

    def _get_seg_len(pos_idx: int, seg_geom) -> float:
        """Return segment length in meters (falling back to geometry.length)."""
        if has_seg_len:
            raw = segments.iat[pos_idx, seg_len_col]
            try:
                v = float(raw) if raw is not None and pd.notna(raw) else 0.0
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
        return float(seg_geom.length)

    def _is_ramp(pos_idx: int) -> bool:
        if hwy_name_col is None:
            return False
        hwy_val = segments.iat[pos_idx, hwy_name_col]
        if pd.isna(hwy_val):
            return False
        return str(hwy_val).strip().upper() in _EXCLUDED_HWY_NAME_VALUES

    # =================================================================
    # STEP 1: Build per-corridor loose + tight buffers
    # Named corridors are built from all features sharing a ROUTE_NAME.
    # Unnamed features are each treated as their own corridor (labelled
    # with the _UNNAMED_CORRIDOR sentinel but kept separate internally) —
    # this avoids the permissive union valid_prefixes that previously let
    # a segment pass because its HWY_NAME was valid for SOME other
    # unnamed feature elsewhere in the state.
    # =================================================================
    named_mask = evac_routes[name_field].notna() & (evac_routes[name_field] != "")
    named_routes = evac_routes[named_mask]
    unnamed_features = evac_routes[~named_mask].reset_index(drop=True)
    unnamed_count = int(len(unnamed_features))

    corridor_loose: dict[str, Any] = {}
    corridor_tight: dict[str, Any] = {}
    corridor_geom: dict[str, Any] = {}  # raw source geometry for azimuth checks
    corridor_public_name: dict[str, str | None] = {}  # sentinel -> reported name
    per_corridor_counts: dict[str, int] = {}

    for route_name, corridor_features in named_routes.groupby(name_field):
        route_name_str = str(route_name)
        loose = unary_union(corridor_features.geometry.buffer(ROUTE_BUFFER_M).values)
        tight = unary_union(corridor_features.geometry.buffer(DISCOVERY_BUFFER_M).values)
        corridor_loose[route_name_str] = loose
        corridor_tight[route_name_str] = tight
        corridor_geom[route_name_str] = unary_union(corridor_features.geometry.values)
        corridor_public_name[route_name_str] = route_name_str
        per_corridor_counts[route_name_str] = 0

    # Each unnamed feature gets its own corridor with its own valid_prefixes.
    # A stable internal key avoids collisions; the public label stays None
    # so downstream reporting continues to treat it as an unnamed match.
    for i, feat in unnamed_features.iterrows():
        key = f"{_UNNAMED_CORRIDOR}#{int(i)}"
        corridor_loose[key] = feat.geometry.buffer(ROUTE_BUFFER_M)
        corridor_tight[key] = feat.geometry.buffer(DISCOVERY_BUFFER_M)
        corridor_geom[key] = feat.geometry
        corridor_public_name[key] = None

    global_loose = unary_union(list(corridor_loose.values())) if corridor_loose else None

    LOGGER.info(
        "Built %d named corridors + %d unnamed per-feature corridors",
        len(named_routes[name_field].unique()) if not named_routes.empty else 0,
        unnamed_count,
    )

    if global_loose is None:
        return {}, {"per_corridor_counts": {}, "match_method_breakdown": {}, "total_matched": 0}

    # =================================================================
    # STEP 2: Auto-discover valid HWY_NAMEs per corridor (tight buffer)
    # =================================================================
    valid_prefixes: dict[str, set[str]] = defaultdict(set)

    # Seed with parsed designations from corridor names. Unnamed-feature
    # corridors have no name to parse, so they only get prefixes from
    # auto-discovery below.
    for corridor_key, public_name in corridor_public_name.items():
        if public_name is None:
            continue
        for p in _parse_corridor_prefixes(public_name):
            valid_prefixes[corridor_key].add(p.upper())

    # Auto-discover from tight-buffer matches.
    for corridor_name, tight_buf in corridor_tight.items():
        cand_pos = seg_sindex.query(tight_buf, predicate="intersects")
        for pos_idx in cand_pos:
            if _is_ramp(pos_idx):
                continue
            seg_geom = seg_geom_arr[pos_idx]
            if seg_geom is None or seg_geom.is_empty:
                continue
            seg_len = _get_seg_len(pos_idx, seg_geom)
            if seg_len < DISCOVERY_MIN_LENGTH_M:
                continue
            try:
                overlap_len = float(seg_geom.intersection(tight_buf).length)
            except Exception:
                continue
            if (
                overlap_len / seg_len < DISCOVERY_MIN_RATIO
                and overlap_len < DISCOVERY_MIN_ABS_OVERLAP_M
            ):
                continue
            hwy_clean = _clean_hwy_name(segments.iat[pos_idx, hwy_name_col]) if hwy_name_col is not None else None
            if hwy_clean:
                valid_prefixes[corridor_name].add(hwy_clean)

    LOGGER.info(
        "Auto-discovered valid HWY_NAMEs — corridors with >=1 prefix: %d / %d",
        sum(1 for s in valid_prefixes.values() if s),
        len(corridor_loose),
    )

    # =================================================================
    # STEP 3: Main matching pass — per-corridor with attribute filter
    # =================================================================
    candidate_positions = seg_sindex.query(global_loose, predicate="intersects")
    LOGGER.info("Spatial query: %d candidate segments", len(candidate_positions))

    results: dict[int, dict[str, Any]] = {}
    attribute_rejects = 0
    alignment_rejects = 0
    mega_rejects = 0

    for pos_idx in candidate_positions:
        if _is_ramp(pos_idx):
            continue

        seg_geom = seg_geom_arr[pos_idx]
        if seg_geom is None or seg_geom.is_empty:
            continue

        seg_len = _get_seg_len(pos_idx, seg_geom)
        if seg_len <= 0:
            continue

        # Segment attributes
        fam_val = segments.iat[pos_idx, route_fam_col] if has_route_fam else None
        fam_lc = (
            str(fam_val).strip().lower() if fam_val is not None and pd.notna(fam_val) else ""
        )
        is_state_system = fam_lc in _STATE_SYSTEM_FAMILIES
        hwy_clean = _clean_hwy_name(segments.iat[pos_idx, hwy_name_col]) if hwy_name_col is not None else None

        # Azimuth is precomputed once per segment — corridor-side azimuth is
        # computed lazily inside the loop because it depends on the corridor.
        seg_az = _line_azimuth(seg_geom) if seg_len >= ALIGNMENT_SKIP_LENGTH_M else None

        matched_corridors: list[str] = []
        best_overlap_m = 0.0
        best_overlap_ratio = 0.0
        any_spatial_pass_rejected_by_attr = False
        rejected_by_alignment = False
        rejected_by_mega = False

        for corridor_key, corridor_buf in corridor_loose.items():
            try:
                co_overlap = seg_geom.intersection(corridor_buf)
                co_overlap_len = float(co_overlap.length)
            except Exception:
                continue
            if co_overlap_len <= 0:
                continue

            co_ratio = co_overlap_len / seg_len
            if co_ratio < MIN_OVERLAP_RATIO:
                continue

            # Local/Other gate — must run along the corridor, not cross it.
            if not is_state_system:
                if seg_len < LOCAL_MIN_LENGTH_M or co_ratio < LOCAL_MIN_OVERLAP_RATIO:
                    continue

            # Attribute filter — state-system segments must match a HWY_NAME
            # that auto-discovery identified for this corridor.
            if is_state_system:
                cvp = valid_prefixes.get(corridor_key) or set()
                if cvp and (hwy_clean is None or hwy_clean not in cvp):
                    any_spatial_pass_rejected_by_attr = True
                    continue
                # If cvp is empty (unparseable corridor with no tight matches),
                # fall through to pure spatial — do not filter.

            # Mega-segment gate — a >10 km segment needs a strong ratio to
            # avoid painting unrelated road red just because a fraction of
            # it follows the corridor.
            if seg_len > MEGA_SEGMENT_LENGTH_M and co_ratio < MEGA_MIN_RATIO:
                rejected_by_mega = True
                continue

            # Angular alignment gate — reject segments that cross the
            # corridor at a sharp angle. Skip the check for short segments
            # (unreliable azimuth) and for segments whose overlap already
            # proves they run along the corridor.
            overlap_is_proving = (
                co_ratio > ALIGNMENT_SKIP_RATIO
                or co_overlap_len > ALIGNMENT_SKIP_ABS_OVERLAP_M
            )
            if (
                seg_len >= ALIGNMENT_SKIP_LENGTH_M
                and not overlap_is_proving
                and seg_az is not None
            ):
                try:
                    corridor_section = corridor_geom[corridor_key].intersection(
                        seg_geom.buffer(ALIGNMENT_SAMPLE_BUFFER_M)
                    )
                except Exception:
                    corridor_section = None
                corridor_az = _line_azimuth(corridor_section)
                angle = _alignment_angle_deg(seg_az, corridor_az)
                if angle is not None and angle > MAX_ALIGNMENT_ANGLE_DEG:
                    rejected_by_alignment = True
                    continue

            matched_corridors.append(corridor_key)
            public_name = corridor_public_name.get(corridor_key)
            if public_name is not None:
                per_corridor_counts[public_name] = per_corridor_counts.get(public_name, 0) + 1
            best_overlap_m = max(best_overlap_m, co_overlap_len)
            best_overlap_ratio = max(best_overlap_ratio, co_ratio)

        if not matched_corridors:
            if any_spatial_pass_rejected_by_attr:
                attribute_rejects += 1
            if rejected_by_alignment:
                alignment_rejects += 1
            if rejected_by_mega:
                mega_rejects += 1
            continue

        # Separate named matches from unnamed-feature matches. If any named
        # corridor matched, drop the unnamed labels (cleaner reporting).
        # Otherwise, the segment is unnamed-only — emit an empty name list.
        output_names: list[str] = []
        seen: set[str] = set()
        for key in matched_corridors:
            name = corridor_public_name.get(key)
            if name and name not in seen:
                output_names.append(name)
                seen.add(name)

        idx = segments.index[pos_idx]
        results[idx] = {
            "names": output_names,
            "overlap_m": best_overlap_m,
            "overlap_ratio": best_overlap_ratio,
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
        "attribute_rejects": attribute_rejects,
        "alignment_rejects": alignment_rejects,
        "mega_rejects": mega_rejects,
        "unnamed_features_count": unnamed_count,
        "corridors_with_zero_matches": [
            name for name, count in per_corridor_counts.items() if count == 0
        ],
        "total_matched": len(results),
    }

    LOGGER.info(
        "Evac overlay total: %d matched (rejects — attr=%d, alignment=%d, mega=%d)",
        len(results),
        attribute_rejects,
        alignment_rejects,
        mega_rejects,
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
