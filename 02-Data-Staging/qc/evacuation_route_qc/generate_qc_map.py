from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, box

# Add the enrichment scripts directory to sys.path so we can import the
# shared corridor-matching module.
_ENRICHMENT_DIR = str(Path(__file__).resolve().parents[2] / "scripts" / "01_roadway_inventory")
if _ENRICHMENT_DIR not in sys.path:
    sys.path.insert(0, _ENRICHMENT_DIR)
from _evac_corridor_match import (
    per_corridor_evac_overlay,
    run_automated_qc,
    ROUTE_BUFFER_M as _CORRIDOR_BUFFER_M,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = Path(__file__).resolve().parent
ROADWAY_PATH = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"
ROADWAY_LAYER = "roadway_segments"
RAW_GDOT_EOC_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "GDOT_EOC"
EVAC_PATH = RAW_GDOT_EOC_DIR / "ga_evac_routes.geojson"
CONTRAFLOW_PATH = RAW_GDOT_EOC_DIR / "ga_contraflow_routes.geojson"

ROADWAY_CRS = "EPSG:32617"
WEB_CRS = "EPSG:4326"
# Tiered overlap thresholds — length-adaptive to avoid filtering out short
# segments that sit entirely within an evacuation corridor.
#   Short  (<400 m):   ratio >= 40 % only  (no absolute minimum)
#   Normal (400 m–10 km): overlap >= 150 m AND ratio >= 20 %
#   Mega   (>10 km):   overlap >= 200 m AND ratio >= 50 %
#                       OR clipped-to-corridor portion meets normal thresholds
SHORT_SEGMENT_MAX_M = 400.0
SHORT_SEGMENT_MIN_RATIO = 0.40
NORMAL_MIN_OVERLAP_M = 150.0
NORMAL_MIN_RATIO = 0.20
MEGA_SEGMENT_LENGTH_M = 10_000.0
MEGA_MIN_OVERLAP_M = 200.0
MEGA_MIN_RATIO = 0.50
MATCH_BUFFER_M = 30.0
MAX_ALIGNMENT_ANGLE_DEG = 30.0
# Contraflow-specific spatial parameters — tighter than general evacuation
# routes because contraflow applies only to specific Interstate lanes and
# both geometries (GDOT Interstates + contraflow polylines) are high-quality.
CONTRAFLOW_BUFFER_M = 15.0
CONTRAFLOW_MIN_OVERLAP_M = 200.0
CONTRAFLOW_MIN_RATIO = 0.30
CONTRAFLOW_SHORT_MIN_RATIO = 0.60
CONTRAFLOW_MAX_ALIGNMENT_ANGLE_DEG = 20.0
CONTRAFLOW_MIN_INSIDE_CORRIDOR_RATIO = 0.25
CONTEXT_SAMPLE_SIZE = 5000
RANDOM_STATE = 42
ROADWAY_COLUMNS = [
    "unique_id",
    "HWY_NAME",
    "ROUTE_FAMILY",
    "ROUTE_TYPE_GDOT",
    "BASE_ROUTE_NUMBER",
    "AADT",
    "DISTRICT_NAME",
    "COUNTY_NAME",
    "segment_length_m",
    "ROUTE_ID",
    "FROM_MILEPOINT",
    "TO_MILEPOINT",
    "geometry",
]
CONTEXT_COLUMNS = ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "geometry"]


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


def load_routes(path: Path, target_crs: str) -> gpd.GeoDataFrame:
    routes = gpd.read_file(path)
    if routes.crs is None:
        raise ValueError(f"{path} is missing a CRS.")
    return routes.to_crs(target_crs)


def load_roadway_subset(bbox: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    roads = gpd.read_file(
        ROADWAY_PATH,
        layer=ROADWAY_LAYER,
        columns=ROADWAY_COLUMNS,
        bbox=bbox,
    )
    if roads.crs is None:
        roads = roads.set_crs(ROADWAY_CRS)
    elif roads.crs.to_string() != ROADWAY_CRS:
        raise ValueError(f"Unexpected roadway CRS: {roads.crs}")
    return roads


def summarize_route_families(frame: gpd.GeoDataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    counts = frame["ROUTE_FAMILY"].fillna("Unknown").value_counts().sort_index()
    return {str(key): int(value) for key, value in counts.items()}


_STATE_SYSTEM_FAMILIES = frozenset({"interstate", "us route", "u.s. route", "state route"})

_SUFFIX_TYPE_MAP = {
    "spur": "SP",
    "business": "BU",
    "connector": "CN",
    "bypass": "BY",
    "loop": "LP",
    "alternate": "AL",
}

_ROUTE_DESIGNATION_RE = re.compile(
    r"(?P<type>I|Interstate|US|SR|CR)\s*-?\s*"
    r"(?P<number>\d+)"
    r"(?:\s+(?P<suffix>North|South|East|West|Spur|Business|Connector|Bypass|Loop|Alternate))?",
    re.IGNORECASE,
)


def _parse_route_designations(
    route_name: str,
) -> list[tuple[str, int, str | None]]:
    """Parse evacuation ROUTE_NAME into (route_type, number, suffix) tuples."""
    if not isinstance(route_name, str):
        return []
    normalized = re.sub(r"\s+", " ", route_name).strip()
    if not normalized:
        return []
    results: list[tuple[str, int, str | None]] = []
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
        number = int(m.group("number"))
        suffix_raw = m.group("suffix")
        suffix: str | None = None
        if suffix_raw and suffix_raw.lower() in _SUFFIX_TYPE_MAP:
            suffix = suffix_raw.capitalize()
        results.append((route_type, number, suffix))
    return results


# Mirror sanity gates. Mirroring INC->DEC purely on ROUTE_ID base amplifies
# any INC false positive into a DEC false positive (and in cases where a
# ROUTE_ID base covers a concurrent bypass that diverges from the mainline,
# the DEC segment can land on a completely different road). Two gates:
#   - INC match must be a confident corridor follower: the matcher's
#     match_portion must be >= MIRROR_MIN_INC_PORTION. Clipped partials
#     that only brushed the corridor briefly shouldn't be amplified.
#   - DEC geometry must actually lie near a polyline for one of the
#     matched corridor names (distance <= MIRROR_MAX_DEC_OFFSET_M). Rejects
#     DEC partners that diverge onto a different alignment.
MIRROR_MIN_INC_PORTION = 0.50
MIRROR_MAX_DEC_OFFSET_M = 75.0

# Hardcoded concurrent-corridor force-flags. Where the official evac
# polyline for a state-route corridor runs on a US route through a
# stretch of counties (because the signed state route is actually
# concurrent with the US route there), the matcher sometimes misses
# individual segments — most often DEC-direction lanes on divided
# highways whose INC partner didn't match cleanly or whose
# auto-discovered HWY_NAME prefix failed the strict gates. Rather than
# keep loosening thresholds (which reintroduces cross-street FPs),
# we force-flag the concurrent HWY_NAME within the concurrent-county
# set for the specific corridor. Each entry = (corridor_name, {
# HWY_NAME: {counties}}).
FORCED_CONCURRENT_FLAGS: list[tuple[str, dict[str, set[str]]]] = [
    # SR 3 runs concurrently with US-19 from Taylor County south
    # through Schley/Sumter/Lee/Dougherty/Mitchell — the canonical
    # Macon-to-Albany evac spine.
    ("SR 3", {
        "US-19": {"Taylor", "Macon", "Marion", "Schley", "Sumter",
                   "Lee", "Dougherty", "Mitchell", "Upson"},
    }),
]


# Corridor-proximity post-filter. After matcher + mirror, a flagged
# segment's rendered geometry must lie mostly within POST_FILTER_BUFFER_M
# of one of its matched corridor polylines. Drops residual FPs where a
# segment barely clips the polyline at an intersection but trails off
# onto a cross-street for most of its length — a cross-street that
# shares a HWY_NAME with an auto-discovered concurrent designation
# (e.g. SR-19 running south from a US-80/SR-26 intersection) lands in
# the flagged set because attribute filtering alone cannot distinguish
# "on-corridor" from "crossing-corridor".
# Tuned at POST_FILTER_BUFFER_M = 40 m / ratio >= 0.70 so the
# rendered geometry must truly hug the polyline — a segment whose
# samples drift past 40 m for more than 30 % of its length is on a
# different road than the corridor it's attributed to.
POST_FILTER_BUFFER_M = 40.0
POST_FILTER_MIN_RATIO = 0.70


# Hard cap applied to ALL flagged segments regardless of match_method.
# User's rule: "anything outside the buffer stays out." Every sample
# along the rendered geometry must lie within GLOBAL_HARD_CAP_BUFFER_M
# of a polyline for ONE of its matched corridors. A single sample
# beyond the cap drops the segment. 100 m accommodates DEC-lane
# offsets on the widest-median divided highways while still catching
# cross-street stubs, bypass trails, and mis-attributed long segments.
GLOBAL_HARD_CAP_BUFFER_M = 100.0


def _global_hard_cap(
    roads: gpd.GeoDataFrame,
    evac_matches: dict[int, dict],
    evac_routes: gpd.GeoDataFrame,
    name_field: str = "ROUTE_NAME",
) -> dict[int, dict]:
    """Drop any flagged segment with any sample farther than
    ``GLOBAL_HARD_CAP_BUFFER_M`` from a polyline for one of its matched
    corridors. Unnamed-only matches are tested against the full
    polyline union.
    """
    if not evac_matches:
        return evac_matches
    from shapely.ops import unary_union
    name_to_polyline: dict[str, object] = {}
    if name_field in evac_routes.columns:
        for name, grp in evac_routes.groupby(name_field):
            if not isinstance(name, str) or not name:
                continue
            geoms = [g for g in grp.geometry.values if g is not None and not g.is_empty]
            if geoms:
                name_to_polyline[name] = unary_union(geoms)
    all_polyline = None
    try:
        all_polyline = unary_union(
            [g for g in evac_routes.geometry.values if g is not None and not g.is_empty]
        )
    except Exception:
        pass

    dropped = 0
    for idx in list(evac_matches.keys()):
        match = evac_matches[idx]
        names = [n for n in match.get("names", []) if n]
        geom = match.get("clipped_geom") or roads.at[idx, "geometry"]
        if geom is None or geom.is_empty:
            continue
        try:
            seg_len = float(geom.length)
        except Exception:
            continue
        if seg_len <= 0:
            continue
        try:
            samples = [geom.interpolate(seg_len * i / 10.0) for i in range(11)]
        except Exception:
            samples = [geom.representative_point()]
        # Pick the best matched-corridor polyline (lowest max-sample-dist).
        passes = False
        targets = [name_to_polyline.get(n) for n in names if name_to_polyline.get(n) is not None]
        if not targets and all_polyline is not None:
            targets = [all_polyline]
        for poly in targets:
            max_d = 0.0
            bad = False
            for s in samples:
                d = s.distance(poly)
                if d > GLOBAL_HARD_CAP_BUFFER_M:
                    bad = True
                    break
                if d > max_d:
                    max_d = d
            if not bad:
                passes = True
                break
        if not passes:
            del evac_matches[idx]
            dropped += 1
    print(
        f"Global hard-cap: dropped {dropped} segments "
        f"(any sample > {GLOBAL_HARD_CAP_BUFFER_M:.0f} m from matched corridor polyline)"
    )
    return evac_matches


# Forced entries must still actually follow the corridor polyline.
# A US-19 bypass segment around Ellaville is nominally in Schley County
# but physically sits ~500 m east of the polyline (which runs through
# downtown on CS-552/556). Require the forced segment to hug the
# polyline: >= FORCED_MIN_SAMPLE_FRAC of 11 samples within
# FORCED_MAX_SAMPLE_DIST_M of the corridor's polyline.
FORCED_MAX_SAMPLE_DIST_M = 75.0
FORCED_MIN_SAMPLE_FRAC = 0.70


def _apply_forced_concurrent_flags(
    roads: gpd.GeoDataFrame,
    evac_matches: dict[int, dict],
    evac_routes: gpd.GeoDataFrame,
    name_field: str = "ROUTE_NAME",
) -> dict[int, dict]:
    """Force-flag (HWY_NAME, COUNTY_NAME) tuples listed in
    ``FORCED_CONCURRENT_FLAGS`` as matched to the specified corridor —
    provided the segment actually hugs the corridor polyline.

    Used to close gaps where a state-route corridor's official polyline
    runs concurrently with a US route and the matcher's thresholds miss
    individual directional segments. Bypass alignments that share the
    HWY_NAME but are physically offset from the polyline (e.g. US-19
    bypass around Ellaville vs. the SR 3 polyline through downtown)
    are rejected by the sample-distance gate.
    """
    if not FORCED_CONCURRENT_FLAGS:
        return evac_matches
    hwy_col = roads.columns.get_loc("HWY_NAME") if "HWY_NAME" in roads.columns else None
    cty_col = roads.columns.get_loc("COUNTY_NAME") if "COUNTY_NAME" in roads.columns else None
    if hwy_col is None or cty_col is None:
        return evac_matches

    from shapely.ops import unary_union
    name_to_polyline: dict[str, object] = {}
    if name_field in evac_routes.columns:
        for name, grp in evac_routes.groupby(name_field):
            if not isinstance(name, str) or not name:
                continue
            geoms = [g for g in grp.geometry.values if g is not None and not g.is_empty]
            if geoms:
                name_to_polyline[name] = unary_union(geoms)

    added = 0
    rejected_by_distance = 0
    for corridor_name, rules in FORCED_CONCURRENT_FLAGS:
        polyline = name_to_polyline.get(corridor_name)
        if polyline is None:
            continue
        for hwy, counties in rules.items():
            for pos in range(len(roads)):
                h = roads.iat[pos, hwy_col]
                c = roads.iat[pos, cty_col]
                if h != hwy or c not in counties:
                    continue
                idx = roads.index[pos]
                geom = roads.at[idx, "geometry"]
                if geom is None or geom.is_empty:
                    continue
                try:
                    seg_len = float(geom.length)
                except Exception:
                    continue
                if seg_len <= 0:
                    continue
                try:
                    samples = [geom.interpolate(seg_len * i / 10.0) for i in range(11)]
                except Exception:
                    continue
                near = sum(1 for s in samples if s.distance(polyline) <= FORCED_MAX_SAMPLE_DIST_M)
                if near / len(samples) < FORCED_MIN_SAMPLE_FRAC:
                    rejected_by_distance += 1
                    continue
                if idx in evac_matches:
                    existing = evac_matches[idx]
                    if corridor_name not in existing.get("names", []):
                        existing.setdefault("names", []).append(corridor_name)
                    continue
                evac_matches[idx] = {
                    "names": [corridor_name],
                    "overlap_m": 0.0,
                    "overlap_ratio": 0.0,
                    "match_method": "concurrent_forced",
                    "clipped_geom": geom,
                    "match_portion": 1.0,
                }
                added += 1
    print(
        f"Forced concurrent-corridor flags: added {added} segments "
        f"(rejected {rejected_by_distance} for being > {FORCED_MAX_SAMPLE_DIST_M:.0f} m "
        f"from corridor polyline)"
    )
    return evac_matches


def _mirror_inc_to_dec(
    roads: gpd.GeoDataFrame,
    evac_matches: dict[int, dict],
    evac_routes: gpd.GeoDataFrame,
    name_field: str = "ROUTE_NAME",
) -> dict[int, dict]:
    """Copy INC-direction matches onto unflagged DEC partners by ROUTE_ID
    base + milepoint overlap. Mirrors ``apply_direction_mirror_evac`` so
    the QC map matches what the pipeline delivers.

    The DEC mirror entry reuses the INC match's names/overlap metadata
    but carries the DEC segment's own geometry (clipped_geom = DEC line)
    so the rendered red overlay tracks the DEC alignment, not the INC
    one 30-50 m away.

    Two FP gates (see MIRROR_MIN_INC_PORTION, MIRROR_MAX_DEC_OFFSET_M):
      - Skip INC matches whose match_portion is below the threshold.
      - Skip DEC candidates whose own geometry is farther than the
        threshold from any matched corridor's polyline.
    """
    if not evac_matches or "ROUTE_ID" not in roads.columns:
        return evac_matches

    # Per-name polyline union for the DEC-distance gate. Uses the raw
    # evacuation polyline geometries keyed by ROUTE_NAME so we can check
    # each mirror candidate against the specific corridors the INC
    # partner matched.
    from shapely.ops import unary_union
    name_to_polyline: dict[str, object] = {}
    if name_field in evac_routes.columns:
        for name, grp in evac_routes.groupby(name_field):
            if not isinstance(name, str) or not name:
                continue
            geoms = [g for g in grp.geometry.values if g is not None and not g.is_empty]
            if geoms:
                name_to_polyline[name] = unary_union(geoms)

    MILEPOINT_TOLERANCE = 0.01
    route_id_col = roads.columns.get_loc("ROUTE_ID")
    from_mp_col = roads.columns.get_loc("FROM_MILEPOINT") if "FROM_MILEPOINT" in roads.columns else None
    to_mp_col = roads.columns.get_loc("TO_MILEPOINT") if "TO_MILEPOINT" in roads.columns else None
    if from_mp_col is None or to_mp_col is None:
        return evac_matches

    def _mp(pos: int, col: int) -> float:
        v = roads.iat[pos, col]
        try:
            return float(v) if v is not None and pd.notna(v) else 0.0
        except (TypeError, ValueError):
            return 0.0

    # Build INC lookup: route_base -> list of (from_mp, to_mp, idx, match).
    # Skip INC matches whose match_portion is too low — those are partial
    # / clipped matches that shouldn't be amplified onto their DEC partner.
    inc_lookup: dict[str, list[tuple[float, float, int, dict]]] = {}
    for idx, match in evac_matches.items():
        if float(match.get("match_portion", 1.0)) < MIRROR_MIN_INC_PORTION:
            continue
        pos = roads.index.get_loc(idx)
        rid = roads.iat[pos, route_id_col]
        if not isinstance(rid, str) or not rid.endswith("INC"):
            continue
        base = rid[:-3]
        inc_lookup.setdefault(base, []).append(
            (_mp(pos, from_mp_col), _mp(pos, to_mp_col), idx, match)
        )

    if not inc_lookup:
        return evac_matches

    mirrored = 0
    rejected_by_distance = 0
    for pos in range(len(roads)):
        rid = roads.iat[pos, route_id_col]
        if not isinstance(rid, str) or not rid.endswith("DEC"):
            continue
        dec_idx = roads.index[pos]
        if dec_idx in evac_matches:
            continue
        base = rid[:-3]
        candidates = inc_lookup.get(base)
        if not candidates:
            continue
        dec_from = _mp(pos, from_mp_col)
        dec_to = _mp(pos, to_mp_col)
        best_overlap = -1.0
        best_match: dict | None = None
        for inc_from, inc_to, _, m in candidates:
            ov = min(dec_to, inc_to) - max(dec_from, inc_from)
            if ov > best_overlap:
                best_overlap = ov
                best_match = m
        if best_match is None or best_overlap < -MILEPOINT_TOLERANCE:
            continue
        dec_geom = roads.at[dec_idx, "geometry"]
        # Polyline-proximity gate: DEC must actually lie near one of the
        # matched corridor polylines. Avoids painting DEC segments that
        # diverge onto a different physical road even though their
        # ROUTE_ID base and milepoint range match an INC evac segment.
        names = list(best_match.get("names", []))
        dec_ok = False
        for name in names:
            poly = name_to_polyline.get(name)
            if poly is None:
                continue
            try:
                if dec_geom.distance(poly) <= MIRROR_MAX_DEC_OFFSET_M:
                    dec_ok = True
                    break
            except Exception:
                continue
        # Unnamed-only matches (names empty / no matched-name polyline) —
        # skip the distance gate, mirror through. These are rare and
        # correspond to unnamed-corridor matches where name lookup is not
        # meaningful.
        if names and not dec_ok and any(n in name_to_polyline for n in names):
            rejected_by_distance += 1
            continue
        evac_matches[dec_idx] = {
            "names": names,
            "overlap_m": float(best_match.get("overlap_m", 0.0)),
            "overlap_ratio": float(best_match.get("overlap_ratio", 0.0)),
            "match_method": "direction_mirror",
            "clipped_geom": dec_geom,
            "match_portion": 1.0,
        }
        mirrored += 1

    print(
        f"Direction mirror: filled {mirrored} DEC segments from INC partners "
        f"(rejected {rejected_by_distance} for DEC > {MIRROR_MAX_DEC_OFFSET_M:.0f} m "
        f"from any matched corridor polyline)"
    )
    return evac_matches


def _post_filter_corridor_proximity(
    roads: gpd.GeoDataFrame,
    evac_matches: dict[int, dict],
    evac_routes: gpd.GeoDataFrame,
    name_field: str = "ROUTE_NAME",
) -> dict[int, dict]:
    """Drop flagged entries whose rendered geometry lies mostly outside
    the matched corridor polyline.

    Sample-distance gate (fast): 11 points along the segment; if fewer
    than POST_FILTER_MIN_RATIO lie within POST_FILTER_BUFFER_M of any
    matched corridor's polyline, drop. Cleans up residual FPs where a
    bypass alignment shares a ROUTE_ID or concurrent designation with
    the corridor and the clipped_geom retains a length that trails off
    onto a different road.
    """
    if not evac_matches:
        return evac_matches
    from shapely.ops import unary_union
    name_to_polyline: dict[str, object] = {}
    if name_field in evac_routes.columns:
        for name, grp in evac_routes.groupby(name_field):
            if not isinstance(name, str) or not name:
                continue
            geoms = [g for g in grp.geometry.values if g is not None and not g.is_empty]
            if geoms:
                name_to_polyline[name] = unary_union(geoms)

    # For unnamed-only matches (corridor name is null because the source
    # polyline feature has no ROUTE_NAME), test against the union of ALL
    # polyline features — the segment must still be near SOME polyline to
    # be a legitimate match.
    all_polyline = None
    try:
        all_polyline = unary_union(
            [g for g in evac_routes.geometry.values if g is not None and not g.is_empty]
        )
    except Exception:
        pass

    dropped = 0
    for idx in list(evac_matches.keys()):
        match = evac_matches[idx]
        # direction_mirror entries are intentionally ~30-50 m offset from
        # the polyline (opposing lane of a divided highway). They have
        # their own proximity gate (MIRROR_MAX_DEC_OFFSET_M) and would
        # be falsely dropped by this post-filter's tighter 40 m / 70 %
        # threshold. Skip.
        if match.get("match_method") == "direction_mirror":
            continue
        names = [n for n in match.get("names", []) if n]
        geom = match.get("clipped_geom") or roads.at[idx, "geometry"]
        if geom is None or geom.is_empty:
            continue
        try:
            seg_len = float(geom.length)
        except Exception:
            continue
        if seg_len <= 0:
            continue
        # Sample 11 points along the rendered geometry.
        try:
            samples = [geom.interpolate(seg_len * i / 10.0) for i in range(11)]
        except Exception:
            samples = [geom.representative_point()]
        passes = False
        # Test against each named corridor's polyline first.
        for name in names:
            poly = name_to_polyline.get(name)
            if poly is None:
                continue
            near = sum(1 for s in samples if s.distance(poly) <= POST_FILTER_BUFFER_M)
            if near / len(samples) >= POST_FILTER_MIN_RATIO:
                passes = True
                break
        # Unnamed-only match (no named corridor hit or names were all
        # unknown in the polyline set) — fall back to the full-polyline
        # union. This catches cross-street FPs that slipped in via the
        # unnamed-corridor path.
        if not passes and not names and all_polyline is not None:
            near = sum(1 for s in samples if s.distance(all_polyline) <= POST_FILTER_BUFFER_M)
            if near / len(samples) >= POST_FILTER_MIN_RATIO:
                passes = True
        if not passes:
            del evac_matches[idx]
            dropped += 1
    print(
        f"Corridor proximity post-filter: dropped {dropped} segments "
        f"(< {POST_FILTER_MIN_RATIO:.0%} of samples within {POST_FILTER_BUFFER_M:.0f} m "
        f"of any matched corridor polyline)"
    )
    return evac_matches


_LOCAL_ROUTE_TYPES = frozenset({"CR", "CS"})


def _attribute_prefilter_mask(
    segments: gpd.GeoDataFrame,
    designations: list[tuple[str, int, str | None]],
) -> pd.Series:
    """Build boolean mask excluding Local/Other (CR/CS) segments.

    CR/CS segments are re-included when a parsed designation explicitly
    targets them (e.g., CR 780 is a legitimate evac route).
    """
    if "ROUTE_TYPE_GDOT" not in segments.columns:
        return pd.Series(True, index=segments.index)

    local_designations = [
        (rt, num, sfx) for rt, num, sfx in designations if rt in _LOCAL_ROUTE_TYPES
    ]
    mask = ~segments["ROUTE_TYPE_GDOT"].isin(_LOCAL_ROUTE_TYPES)
    if local_designations and "BASE_ROUTE_NUMBER" in segments.columns:
        for route_type, number, _suffix in local_designations:
            mask |= (
                (segments["ROUTE_TYPE_GDOT"] == route_type)
                & (segments["BASE_ROUTE_NUMBER"] == number)
            )
    return mask


def _specific_designation_mask(
    segments: gpd.GeoDataFrame,
    designations: list[tuple[str, int, str | None]],
) -> pd.Series:
    """Build boolean mask for segments matching a specific designation."""
    mask = pd.Series(False, index=segments.index)
    has_hwy = "HWY_NAME" in segments.columns
    has_rt = "ROUTE_TYPE_GDOT" in segments.columns
    has_brn = "BASE_ROUTE_NUMBER" in segments.columns
    for route_type, number, suffix in designations:
        if route_type == "I":
            if has_hwy:
                if suffix:
                    suffix_upper = suffix.upper()
                    prefix = f"I-{number}"
                    mask |= (
                        segments["HWY_NAME"].str.startswith(prefix, na=False)
                        & segments["HWY_NAME"].str.upper().str.contains(
                            suffix_upper, na=False
                        )
                    )
                else:
                    prefix = f"I-{number}"
                    mask |= segments["HWY_NAME"].str.startswith(prefix, na=False)
        else:
            if has_rt and has_brn:
                type_set = {route_type}
                if suffix:
                    gdot_suffix = _SUFFIX_TYPE_MAP.get(suffix.lower())
                    if gdot_suffix:
                        type_set.add(gdot_suffix)
                mask |= (
                    segments["ROUTE_TYPE_GDOT"].isin(type_set)
                    & (segments["BASE_ROUTE_NUMBER"] == number)
                )
    return mask


def _parse_expected_family(route_name: str) -> str | None:
    """Infer roadway family from an evacuation route name."""
    if not isinstance(route_name, str):
        return None
    normalized = re.sub(r"\s+", " ", route_name).strip().lower()
    if not normalized:
        return None
    if normalized.startswith(("i ", "i-", "interstate")):
        return "Interstate"
    if normalized.startswith(("us ", "us-")):
        return "US Route"
    if normalized.startswith(("sr ", "sr-", "state route")):
        return "State Route"
    return None


def flag_matches(
    roads: gpd.GeoDataFrame,
    routes: gpd.GeoDataFrame,
    keep_columns: list[str],
    *,
    route_name_field: str | None = None,
    enforce_route_family: bool = False,
    interstate_only: bool = False,
    attribute_prefilter: bool = False,
    buffer_m: float | None = None,
    min_overlap_m: float | None = None,
    min_ratio: float | None = None,
    short_min_ratio: float | None = None,
    max_alignment_deg: float | None = None,
) -> gpd.GeoDataFrame:
    # Resolve per-call threshold overrides (fall back to module-level defaults)
    eff_buffer_m = buffer_m if buffer_m is not None else MATCH_BUFFER_M
    eff_min_overlap_m = min_overlap_m if min_overlap_m is not None else NORMAL_MIN_OVERLAP_M
    eff_min_ratio = min_ratio if min_ratio is not None else NORMAL_MIN_RATIO
    eff_short_min_ratio = short_min_ratio if short_min_ratio is not None else SHORT_SEGMENT_MIN_RATIO
    eff_max_alignment_deg = max_alignment_deg if max_alignment_deg is not None else MAX_ALIGNMENT_ANGLE_DEG

    eligible = roads
    if interstate_only and "ROUTE_FAMILY" in roads.columns:
        eligible = roads.loc[roads["ROUTE_FAMILY"] == "Interstate"]
    elif attribute_prefilter and route_name_field and route_name_field in routes.columns:
        all_designations: list[tuple[str, int, str | None]] = []
        for name in routes[route_name_field].dropna():
            all_designations.extend(_parse_route_designations(str(name)))
        if all_designations:
            attr_mask = _attribute_prefilter_mask(roads, all_designations)
            eligible = roads.loc[attr_mask]
            print(
                f"Attribute prefilter: {attr_mask.sum()} of {len(roads)} "
                f"segments eligible"
            )
    if eligible.empty:
        empty = roads.iloc[0:0][keep_columns + ["geometry"]].copy()
        empty["overlap_m"] = pd.Series(dtype="float64")
        empty["overlap_ratio"] = pd.Series(dtype="float64")
        return empty

    routes_indexed = routes[["geometry"]].copy()
    if route_name_field and route_name_field in routes.columns:
        routes_indexed[route_name_field] = routes[route_name_field].values
    routes_indexed = routes_indexed.reset_index(drop=True).rename_axis("route_idx").reset_index()
    original_route_geoms = routes_indexed.set_index("route_idx").geometry
    buffered = routes_indexed.copy()
    buffered["geometry"] = buffered.geometry.buffer(eff_buffer_m)
    candidates = gpd.sjoin(
        eligible,
        buffered[["route_idx", "geometry"] + ([route_name_field] if route_name_field and route_name_field in buffered.columns else [])],
        how="inner",
        predicate="intersects",
    )

    if candidates.empty:
        empty = roads.iloc[0:0][keep_columns + ["geometry"]].copy()
        empty["overlap_m"] = pd.Series(dtype="float64")
        empty["overlap_ratio"] = pd.Series(dtype="float64")
        return empty

    buffered_geoms = buffered.set_index("route_idx").geometry
    accepted_rows = []
    for seg_idx, group in candidates.groupby(candidates.index):
        seg_geom = eligible.loc[seg_idx, "geometry"]
        seg_length = (
            float(eligible.loc[seg_idx, "segment_length_m"])
            if "segment_length_m" in eligible.columns and pd.notna(eligible.loc[seg_idx, "segment_length_m"])
            else float(seg_geom.length)
        )
        if seg_length <= 0:
            seg_length = float(seg_geom.length)

        seg_family = None
        if "ROUTE_FAMILY" in eligible.columns:
            raw = eligible.loc[seg_idx, "ROUTE_FAMILY"]
            seg_family = str(raw).strip() if pd.notna(raw) else None

        best_overlap_m = 0.0
        best_overlap_ratio = 0.0
        for _, row in group.iterrows():
            if enforce_route_family and route_name_field and route_name_field in row.index:
                rn = row[route_name_field]
                if pd.notna(rn):
                    expected = _parse_expected_family(str(rn))
                    if expected is not None:
                        seg_fam_lower = seg_family.lower() if seg_family else ""
                        if seg_fam_lower not in _STATE_SYSTEM_FAMILIES:
                            continue

            corridor = buffered_geoms.loc[row["route_idx"]]
            try:
                overlap_geom = seg_geom.intersection(corridor)
                overlap_len = float(overlap_geom.length)
            except Exception:
                overlap_geom = None
                overlap_len = 0.0

            overlap_ratio = overlap_len / seg_length if seg_length > 0 else 0.0
            is_short = seg_length < SHORT_SEGMENT_MAX_M
            is_mega = seg_length > MEGA_SEGMENT_LENGTH_M
            if is_short:
                accepted = overlap_ratio >= eff_short_min_ratio
            elif is_mega:
                accepted = (
                    overlap_len >= MEGA_MIN_OVERLAP_M
                    and overlap_ratio >= MEGA_MIN_RATIO
                )
                # Fallback: accept mega-segments whose clipped-to-corridor
                # portion meets the normal absolute threshold.
                if not accepted and overlap_len >= eff_min_overlap_m:
                    accepted = True
            else:
                accepted = (
                    overlap_len >= eff_min_overlap_m
                    and overlap_ratio >= eff_min_ratio
                )

            if accepted and not is_short and overlap_geom is not None:
                route_geom = original_route_geoms.loc[row["route_idx"]]
                try:
                    route_section = route_geom.intersection(
                        seg_geom.buffer(eff_buffer_m)
                    )
                except Exception:
                    route_section = None
                seg_az = _line_azimuth(overlap_geom)
                route_az = _line_azimuth(route_section)
                angle = _alignment_angle_deg(seg_az, route_az)
                if angle is not None and angle > eff_max_alignment_deg:
                    accepted = False

            if not accepted:
                continue

            best_overlap_m = max(best_overlap_m, overlap_len)
            best_overlap_ratio = max(best_overlap_ratio, overlap_ratio)

        if best_overlap_m > 0:
            uid = eligible.loc[seg_idx, "unique_id"]
            accepted_rows.append({
                "unique_id": uid,
                "overlap_m": best_overlap_m,
                "overlap_ratio": best_overlap_ratio,
            })

    if not accepted_rows:
        empty = roads.iloc[0:0][keep_columns + ["geometry"]].copy()
        empty["overlap_m"] = pd.Series(dtype="float64")
        empty["overlap_ratio"] = pd.Series(dtype="float64")
        return empty

    accepted_df = pd.DataFrame(accepted_rows)
    flagged = eligible.merge(accepted_df, on="unique_id", how="inner")
    out_cols = [c for c in keep_columns if c in flagged.columns] + ["overlap_m", "overlap_ratio", "geometry"]
    return flagged[out_cols].sort_values("unique_id").reset_index(drop=True)


def build_context_layer(
    roads: gpd.GeoDataFrame,
    flagged_ids: set[str],
    bbox: tuple[float, float, float, float],
) -> gpd.GeoDataFrame:
    bbox_polygon = box(*bbox)
    candidates = roads.loc[
        ~roads["unique_id"].isin(flagged_ids) & roads.geometry.intersects(bbox_polygon),
        CONTEXT_COLUMNS,
    ].copy()
    if len(candidates) > CONTEXT_SAMPLE_SIZE:
        candidates = candidates.sample(n=CONTEXT_SAMPLE_SIZE, random_state=RANDOM_STATE)
    return candidates.sort_values("unique_id").reset_index(drop=True)


def export_geojson(frame: gpd.GeoDataFrame, path: Path) -> None:
    export_frame = frame.to_crs(WEB_CRS)
    export_frame.to_file(path, driver="GeoJSON")


def html_template(summary: dict[str, object]) -> str:
    summary_json = json.dumps(summary, indent=2)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GDOT Evacuation Route QC Map</title>
  <link
    rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
    crossorigin=""
  >
  <style>
    html, body, #map {{
      height: 100%;
      margin: 0;
      font-family: Arial, sans-serif;
    }}
    .title-banner {{
      position: absolute;
      top: 12px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 1000;
      background: rgba(255, 255, 255, 0.95);
      padding: 10px 14px;
      border-radius: 6px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
      font-size: 18px;
      font-weight: 700;
    }}
    .info-box {{
      background: rgba(255, 255, 255, 0.95);
      padding: 12px 14px;
      border-radius: 6px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
      line-height: 1.4;
      min-width: 240px;
    }}
    .info-box h3 {{
      margin: 0 0 8px;
      font-size: 15px;
    }}
    .info-box .metric {{
      margin-bottom: 6px;
      font-size: 13px;
    }}
    .info-box .breakdown-title {{
      margin: 10px 0 4px;
      font-size: 13px;
      font-weight: 700;
    }}
    .info-box ul {{
      margin: 0;
      padding-left: 18px;
      font-size: 12px;
    }}
    .leaflet-popup-content {{
      min-width: 220px;
    }}
  </style>
</head>
<body>
  <div class="title-banner">GDOT Evacuation Route QC Map</div>
  <div id="map"></div>
  <script>
    const QC_SUMMARY = {summary_json};
  </script>
  <script
    src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
    crossorigin=""
  ></script>
  <script>
    const map = L.map('map');
    window._leafletMap = map;
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const infoControl = L.control({{ position: 'topright' }});
    infoControl.onAdd = function() {{
      const div = L.DomUtil.create('div', 'info-box');
      const evacBreakdown = Object.entries(QC_SUMMARY.evac_route_family_breakdown)
        .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
        .join('');
      const contraBreakdown = Object.entries(QC_SUMMARY.contraflow_route_family_breakdown)
        .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
        .join('');
      const matchMethodBreakdown = QC_SUMMARY.evac_match_method_breakdown
        ? Object.entries(QC_SUMMARY.evac_match_method_breakdown)
            .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
            .join('')
        : '';
      const corridorCounts = QC_SUMMARY.per_corridor_counts
        ? Object.entries(QC_SUMMARY.per_corridor_counts)
            .sort((a, b) => b[1] - a[1])
            .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
            .join('')
        : '';
      div.innerHTML = `
        <h3>QC Summary</h3>
        <div class="metric"><strong>Total evacuation flagged:</strong> ${{QC_SUMMARY.total_evac_flagged}}</div>
        <div class="metric"><strong>Total contraflow flagged:</strong> ${{QC_SUMMARY.total_contraflow_flagged}}</div>
        ${{QC_SUMMARY.evac_routes_null_name_excluded ? `<div class="metric" style="color:#888"><em>Unnamed evac route features excluded from blue layer: ${{QC_SUMMARY.evac_routes_null_name_excluded}}</em></div>` : ''}}
        <div class="breakdown-title">Evacuation by ROUTE_FAMILY</div>
        <ul>${{evacBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Contraflow by ROUTE_FAMILY</div>
        <ul>${{contraBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Evac Match Method</div>
        <ul>${{matchMethodBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Per-Corridor Counts</div>
        <ul style="max-height:200px;overflow-y:auto">${{corridorCounts || '<li>None</li>'}}</ul>
      `;
      return div;
    }};
    infoControl.addTo(map);

    function formatValue(value) {{
      if (value === null || value === undefined || value === '') {{
        return 'N/A';
      }}
      return value;
    }}

    function makePopup(properties, fields) {{
      return fields
        .map(([label, field]) => `<div><strong>${{label}}:</strong> ${{formatValue(properties[field])}}</div>`)
        .join('');
    }}

    function styleFactory(style) {{
      return function() {{
        return style;
      }};
    }}

    Promise.all([
      fetch('network_context.geojson').then((r) => r.json()),
      fetch('evac_routes_official.geojson').then((r) => r.json()),
      fetch('contraflow_routes_official.geojson').then((r) => r.json()),
      fetch('network_evac_flagged.geojson').then((r) => r.json()),
      fetch('network_contraflow_flagged.geojson').then((r) => r.json())
    ]).then(([contextData, evacRoutesData, contraRoutesData, evacFlaggedData, contraFlaggedData]) => {{
      const contextLayer = L.geoJSON(contextData, {{
        style: styleFactory({{ color: '#999999', weight: 1, opacity: 0.4 }})
      }});

      const evacRoutesLayer = L.geoJSON(evacRoutesData, {{
        style: styleFactory({{ color: '#2196F3', weight: 3, opacity: 0.6 }}),
        onEachFeature: (feature, layer) => {{
          layer.bindPopup(makePopup(feature.properties, [['ROUTE_NAME', 'ROUTE_NAME']]));
        }}
      }});

      const contraRoutesLayer = L.geoJSON(contraRoutesData, {{
        style: styleFactory({{ color: '#9C27B0', weight: 5, opacity: 0.8 }}),
        onEachFeature: (feature, layer) => {{
          layer.bindPopup(makePopup(feature.properties, [['TITLE', 'TITLE'], ['DESCRIPTION', 'DESCRIPTION']]));
        }}
      }});

      const evacFlaggedLayer = L.geoJSON(evacFlaggedData, {{
        style: styleFactory({{ color: '#d32f2f', weight: 5, opacity: 0.9 }}),
        onEachFeature: (feature, layer) => {{
          const p = feature.properties;
          const ratio = p.overlap_ratio != null ? (p.overlap_ratio * 100).toFixed(1) + '%' : 'N/A';
          const overlapM = p.overlap_m != null ? Math.round(p.overlap_m) + ' m' : 'N/A';
          const portion = p.match_portion != null ? (p.match_portion * 100).toFixed(1) + '%' : 'N/A';
          layer.bindPopup(
            makePopup(p, [
              ['HWY_NAME', 'HWY_NAME'],
              ['ROUTE_FAMILY', 'ROUTE_FAMILY'],
              ['Evac Route', 'SEC_EVAC_ROUTE_NAME'],
              ['AADT', 'AADT'],
              ['COUNTY_NAME', 'COUNTY_NAME'],
              ['DISTRICT_NAME', 'DISTRICT_NAME'],
              ['Match Method', 'match_method']
            ]) + `<div><strong>Overlap:</strong> ${{overlapM}} (${{ratio}})</div><div><strong>Match portion (clipped/seg):</strong> ${{portion}}</div>`
          );
        }}
      }});

      const contraFlaggedLayer = L.geoJSON(contraFlaggedData, {{
        style: styleFactory({{ color: '#f57c00', weight: 4, opacity: 0.8 }}),
        onEachFeature: (feature, layer) => {{
          const p = feature.properties;
          const ratio = p.overlap_ratio != null ? (p.overlap_ratio * 100).toFixed(1) + '%' : 'N/A';
          const overlapM = p.overlap_m != null ? Math.round(p.overlap_m) + ' m' : 'N/A';
          layer.bindPopup(
            makePopup(p, [
              ['HWY_NAME', 'HWY_NAME'],
              ['ROUTE_FAMILY', 'ROUTE_FAMILY']
            ]) + `<div><strong>Overlap:</strong> ${{overlapM}} (${{ratio}})</div>`
          );
        }}
      }});

      const overlays = {{
        'Road Network (context)': contextLayer,
        'GDOT Evacuation Routes (official)': evacRoutesLayer,
        'GDOT Contraflow Routes (official)': contraRoutesLayer,
        'Flagged Segments (evacuation)': evacFlaggedLayer,
        'Flagged Segments (contraflow)': contraFlaggedLayer
      }};

      evacFlaggedLayer.addTo(map);

      L.control.layers(null, overlays, {{ collapsed: false }}).addTo(map);
      const bounds = evacFlaggedLayer.getBounds();
      if (bounds.isValid()) {{
        map.fitBounds(bounds.pad(0.08));
      }} else {{
        map.setView([32.5, -83.5], 7);
      }}

      // --- Per-corridor dropdown filter ---
      const corridorControl = L.control({{ position: 'topleft' }});
      corridorControl.onAdd = function() {{
        const div = L.DomUtil.create('div', 'info-box');
        div.style.marginTop = '60px';
        const label = L.DomUtil.create('div', '', div);
        label.innerHTML = '<strong style="font-size:13px">Filter by Corridor</strong>';
        const select = L.DomUtil.create('select', '', div);
        select.id = 'corridor-filter';
        select.style.cssText = 'width:100%;margin-top:4px;padding:3px;font-size:13px';
        select.innerHTML = '<option value="">All corridors</option>';

        const corridorNames = new Set();
        evacFlaggedLayer.eachLayer(layer => {{
          const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME;
          if (name) {{
            name.split('; ').forEach(n => corridorNames.add(n));
          }}
        }});
        [...corridorNames].sort().forEach(name => {{
          select.innerHTML += `<option value="${{name}}">${{name}}</option>`;
        }});

        select.onchange = function() {{
          filterByCorridor(this.value);
        }};
        L.DomEvent.disableClickPropagation(div);
        return div;
      }};
      corridorControl.addTo(map);

      window.filterByCorridor = function(corridorName) {{
        // Filter flagged segments (red)
        evacFlaggedLayer.eachLayer(layer => {{
          const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
          const visible = !corridorName || name.includes(corridorName);
          if (visible) {{
            layer.setStyle({{ opacity: 0.9, weight: 5 }});
          }} else {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }}
        }});

        // Filter official evac routes (blue)
        evacRoutesLayer.eachLayer(layer => {{
          const rn = layer.feature.properties.ROUTE_NAME || '';
          const visible = !corridorName || rn === corridorName;
          if (visible) {{
            layer.setStyle({{ opacity: 0.6, weight: 3 }});
          }} else {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }}
        }});

        // Filter contraflow routes (purple) — hide when filtering
        contraRoutesLayer.eachLayer(layer => {{
          if (corridorName) {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }} else {{
            layer.setStyle({{ opacity: 0.8, weight: 5 }});
          }}
        }});

        // Filter contraflow flagged segments (orange) — hide when filtering
        contraFlaggedLayer.eachLayer(layer => {{
          if (corridorName) {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }} else {{
            layer.setStyle({{ opacity: 0.8, weight: 4 }});
          }}
        }});

        if (corridorName) {{
          const cbounds = L.latLngBounds();
          evacFlaggedLayer.eachLayer(layer => {{
            const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
            if (name.includes(corridorName)) {{
              cbounds.extend(layer.getBounds());
            }}
          }});
          // Also include the official route bounds
          evacRoutesLayer.eachLayer(layer => {{
            const rn = layer.feature.properties.ROUTE_NAME || '';
            if (rn === corridorName) {{
              cbounds.extend(layer.getBounds());
            }}
          }});
          if (cbounds.isValid()) {{
            map.fitBounds(cbounds.pad(0.1));
          }}
          document.querySelector('.title-banner').textContent =
            `GDOT Evacuation Route QC — ${{corridorName}}`;
          contextLayer.remove();
        }} else {{
          const allBounds = evacRoutesLayer.getBounds();
          if (allBounds.isValid()) map.fitBounds(allBounds.pad(0.08));
          document.querySelector('.title-banner').textContent =
            'GDOT Evacuation Route QC Map';
          contextLayer.addTo(map);
        }}
      }};
    }}).catch((error) => {{
      console.error(error);
      alert(`Failed to load one or more QC layers: ${{error.message}}`);
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    evac_routes = load_routes(EVAC_PATH, ROADWAY_CRS)
    contraflow_routes = load_routes(CONTRAFLOW_PATH, ROADWAY_CRS)

    evac_bounds = evac_routes.total_bounds
    contraflow_bounds = contraflow_routes.total_bounds
    roadway_bounds = (
        min(evac_bounds[0], contraflow_bounds[0]),
        min(evac_bounds[1], contraflow_bounds[1]),
        max(evac_bounds[2], contraflow_bounds[2]),
        max(evac_bounds[3], contraflow_bounds[3]),
    )
    roads = load_roadway_subset(roadway_bounds)

    evac_keep_cols = ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "AADT", "DISTRICT_NAME", "COUNTY_NAME"]

    # --- Per-corridor evacuation matching via shared module ---
    evac_matches, evac_diagnostics = per_corridor_evac_overlay(
        roads, evac_routes, name_field="ROUTE_NAME",
    )

    # --- Direction mirror: INC->DEC on divided highways ---
    # Mirror the pipeline's apply_direction_mirror_evac so the QC map
    # reflects what the final webapp delivery will have. Divided highways
    # carry INC and DEC ROUTE_ID pairs; the matcher typically flags only
    # one direction because the opposing lane's geometry is offset 30-50 m
    # from the polyline. Here we copy an INC match onto its DEC partner
    # (matched by route_base + milepoint overlap) using the DEC's own
    # segment geometry so the gap on the QC map closes.
    evac_matches = _mirror_inc_to_dec(roads, evac_matches, evac_routes)

    # --- Corridor-proximity post-filter ---
    # Drop any flagged segment whose rendered geometry lies mostly outside
    # the buffered polyline of any of its matched corridors. Cleans up
    # residual FPs where long or diverging segments make it through the
    # matcher/mirror gates but visually trace a different alignment.
    evac_matches = _post_filter_corridor_proximity(roads, evac_matches, evac_routes)

    # --- Forced concurrent-corridor flags ---
    # Closes residual FN gaps where the matcher couldn't reach a
    # DEC-direction segment or failed auto-discovery on a concurrent
    # US-route designation. Applied LAST so the proximity post-filter
    # doesn't strip force-flagged entries.
    evac_matches = _apply_forced_concurrent_flags(roads, evac_matches, evac_routes)

    # --- Global hard cap: every rendered segment must stay inside
    #     GLOBAL_HARD_CAP_BUFFER_M of ITS matched corridor polyline for
    #     the entire length. Drops any segment with any sample farther
    #     than the cap. User rule: "anything outside the buffer stays
    #     out."
    evac_matches = _global_hard_cap(roads, evac_matches, evac_routes)

    # Convert results dict to GeoDataFrame for export. Use the clipped-to-
    # corridor geometry so partial-follow overshoots render only the in-
    # corridor portion — the original full-segment geometry is only used
    # as a fallback when the matcher didn't produce a clip.
    if evac_matches:
        rows = []
        for idx, match in evac_matches.items():
            row = {col: roads.at[idx, col] for col in evac_keep_cols if col in roads.columns}
            row["geometry"] = match.get("clipped_geom") or roads.at[idx, "geometry"]
            row["overlap_m"] = match["overlap_m"]
            row["overlap_ratio"] = match["overlap_ratio"]
            row["match_method"] = match["match_method"]
            row["match_portion"] = match.get("match_portion", 1.0)
            row["SEC_EVAC_ROUTE_NAME"] = "; ".join(sorted(set(match["names"]))) if match["names"] else None
            rows.append(row)
        evac_flagged = gpd.GeoDataFrame(rows, crs=roads.crs)
        evac_flagged = evac_flagged.sort_values("unique_id").reset_index(drop=True)
    else:
        evac_flagged = gpd.GeoDataFrame(
            columns=evac_keep_cols + ["overlap_m", "overlap_ratio", "match_method", "match_portion", "SEC_EVAC_ROUTE_NAME", "geometry"]
        )

    # Print per-corridor diagnostics
    print(f"\n=== Per-Corridor Diagnostics ===")
    print(f"Total matched: {evac_diagnostics.get('total_matched', 0)}")
    print(f"Match methods: {json.dumps(evac_diagnostics.get('match_method_breakdown', {}), indent=2)}")
    print(f"Multi-corridor segments: {evac_diagnostics.get('multi_corridor_segments', 0)}")
    print(f"Concurrent fallback: {evac_diagnostics.get('concurrent_fallback_matches', 0)}")
    print(f"Null-feature matches: {evac_diagnostics.get('null_feature_matches', 0)}")
    zero_corridors = evac_diagnostics.get("corridors_with_zero_matches", [])
    if zero_corridors:
        print(f"ALERT — corridors with zero matches: {', '.join(zero_corridors)}")
    per_corridor = evac_diagnostics.get("per_corridor_counts", {})
    low = {k: v for k, v in per_corridor.items() if 0 < v < 3}
    if low:
        print(f"WARNING — corridors with <3 matches: {low}")

    contraflow_flagged = flag_matches(
        roads,
        contraflow_routes,
        ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "AADT", "DISTRICT_NAME", "COUNTY_NAME"],
        route_name_field="TITLE",
        interstate_only=True,
        buffer_m=CONTRAFLOW_BUFFER_M,
        min_overlap_m=CONTRAFLOW_MIN_OVERLAP_M,
        min_ratio=CONTRAFLOW_MIN_RATIO,
        short_min_ratio=CONTRAFLOW_SHORT_MIN_RATIO,
        max_alignment_deg=CONTRAFLOW_MAX_ALIGNMENT_ANGLE_DEG,
    )

    # Corridor proximity post-filter for contraflow
    if not contraflow_flagged.empty:
        contra_corridor = contraflow_routes.geometry.buffer(CONTRAFLOW_BUFFER_M).union_all()
        inside_lens = contraflow_flagged.geometry.intersection(contra_corridor).length
        seg_lens = contraflow_flagged.geometry.length
        inside_ratios = inside_lens / seg_lens.replace(0, 1)
        before = len(contraflow_flagged)
        contraflow_flagged = contraflow_flagged[inside_ratios >= CONTRAFLOW_MIN_INSIDE_CORRIDOR_RATIO].reset_index(drop=True)
        removed = before - len(contraflow_flagged)
        if removed > 0:
            print(f"Contraflow corridor proximity filter removed {removed} segments (kept {len(contraflow_flagged)})")

    flagged_ids = set(evac_flagged["unique_id"]).union(set(contraflow_flagged["unique_id"]))
    context = build_context_layer(roads, flagged_ids, tuple(evac_bounds))

    evac_routes_export = evac_routes.drop(columns=["_designations"], errors="ignore")
    # Label null-name features so they are visible on the QC map
    null_name_mask = evac_routes_export["ROUTE_NAME"].isna() | (evac_routes_export["ROUTE_NAME"] == "")
    null_name_count = int(null_name_mask.sum())
    if null_name_count:
        evac_routes_export.loc[null_name_mask, "ROUTE_NAME"] = "(unnamed)"
        print(f"Including {null_name_count} null-name evac route features on QC map blue layer as '(unnamed)'")
    export_geojson(evac_routes_export, OUTPUT_DIR / "evac_routes_official.geojson")
    export_geojson(contraflow_routes, OUTPUT_DIR / "contraflow_routes_official.geojson")
    export_geojson(evac_flagged, OUTPUT_DIR / "network_evac_flagged.geojson")
    export_geojson(contraflow_flagged, OUTPUT_DIR / "network_contraflow_flagged.geojson")
    export_geojson(context, OUTPUT_DIR / "network_context.geojson")

    match_method_breakdown = {}
    if "match_method" in evac_flagged.columns:
        counts = evac_flagged["match_method"].fillna("unknown").value_counts()
        match_method_breakdown = {str(k): int(v) for k, v in counts.items()}

    summary = {
        "total_evac_flagged": int(len(evac_flagged)),
        "total_contraflow_flagged": int(len(contraflow_flagged)),
        "evac_routes_null_name_excluded": null_name_count,
        "evac_route_family_breakdown": summarize_route_families(evac_flagged),
        "contraflow_route_family_breakdown": summarize_route_families(contraflow_flagged),
        "evac_match_method_breakdown": match_method_breakdown,
        "per_corridor_counts": evac_diagnostics.get("per_corridor_counts", {}),
    }
    (OUTPUT_DIR / "index.html").write_text(html_template(summary), encoding="utf-8")

    # Run automated QC checks
    passed, qc_errors, qc_warnings = run_automated_qc(
        evac_diagnostics, int(len(contraflow_flagged)),
    )
    for msg in qc_errors:
        print(msg)
    for msg in qc_warnings:
        print(msg)
    if passed:
        print("Automated QC checks PASSED")
    else:
        print("Automated QC checks FAILED — review errors above")

    report = {
        "evac_routes_official": int(len(evac_routes)),
        "contraflow_routes_official": int(len(contraflow_routes)),
        "network_evac_flagged": int(len(evac_flagged)),
        "network_contraflow_flagged": int(len(contraflow_flagged)),
        "network_context": int(len(context)),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
