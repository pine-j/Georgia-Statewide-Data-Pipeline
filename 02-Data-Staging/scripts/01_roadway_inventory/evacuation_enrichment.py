"""Enrich Georgia roadway segments with hurricane evacuation route flags.

Raw layers live under 01-Raw-Data/Roadway-Inventory/GDOT_EOC/. Fetch them
with 01-Raw-Data/Roadway-Inventory/scripts/download_evacuation_routes.py
before running the pipeline.

Source layers (GDOT EOC Response):
- Layer 7: GDOT Hurricane Evacuation Routes (268 polylines)
- Layer 8: GDOT Hurricane Evacuation Routes - Contraflow Route (12 polylines)

Adds columns:
- SEC_EVAC: Boolean flag - True when the segment overlaps an evacuation route
- SEC_EVAC_CONTRAFLOW: Boolean flag - True when the segment overlaps a contraflow route
- SEC_EVAC_ROUTE_NAME: Evacuation route name(s) matched to the segment
- SEC_EVAC_SOURCE: Source attribution for the evacuation flag
- SEC_EVAC_OVERLAP_M: Maximum accepted overlap length in meters
- SEC_EVAC_OVERLAP_RATIO: Maximum accepted overlap ratio
"""

from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import substring, unary_union

from _evac_corridor_match import per_corridor_evac_overlay

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_GDOT_EOC_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "GDOT_EOC"
EVAC_SNAPSHOT_DIR = PROJECT_ROOT / "02-Data-Staging" / "tests" / "snapshots"


def _evac_snapshot(label: str, gdf: pd.DataFrame) -> None:
    """Persist a per-stage snapshot of evac flags for the regression fixture.

    Writes a lightweight parquet containing only the columns the fixture
    inspects, keyed by (HWY_NAME, COUNTY_NAME, ROUTE_ID). The test runner
    uses these to localize which stage regressed a case.
    """
    try:
        EVAC_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        cols = [c for c in (
            "HWY_NAME", "COUNTY_NAME", "ROUTE_ID", "ROUTE_FAMILY",
            "FROM_MILEPOINT", "TO_MILEPOINT",
            "SEC_EVAC", "SEC_EVAC_CONTRAFLOW", "SEC_EVAC_ROUTE_NAME",
            "SEC_EVAC_OVERLAP_M", "SEC_EVAC_OVERLAP_RATIO", "SEC_EVAC_MATCH_PORTION",
        ) if c in gdf.columns]
        snap = pd.DataFrame(gdf[cols]).copy()
        snap["_stage"] = label
        out = EVAC_SNAPSHOT_DIR / f"{label}.parquet"
        snap.to_parquet(out, index=True)
        LOGGER.info("Evac snapshot: wrote %s (%d rows)", out.name, len(snap))
    except Exception as exc:
        LOGGER.warning("Evac snapshot %s failed: %s", label, exc)

EVAC_ROUTES_GEOJSON = RAW_GDOT_EOC_DIR / "ga_evac_routes.geojson"
CONTRAFLOW_ROUTES_GEOJSON = RAW_GDOT_EOC_DIR / "ga_contraflow_routes.geojson"

ENRICHMENT_COLUMNS = [
    "SEC_EVAC",
    "SEC_EVAC_CONTRAFLOW",
    "SEC_EVAC_ROUTE_NAME",
    "SEC_EVAC_SOURCE",
    "SEC_EVAC_MATCH_METHOD",
    "SEC_EVAC_OVERLAP_M",
    "SEC_EVAC_OVERLAP_RATIO",
    "SEC_EVAC_MATCH_PORTION",
]

# Segment splitting — when a matched segment's in-corridor portion covers
# <SPLIT_SKIP_PORTION of its length, the segment is cut into inside/outside
# child rows so downstream consumers (webapp, RAPTOR) see SEC_EVAC=True
# only on the geometry that actually follows the corridor. Pieces shorter
# than SPLIT_MIN_PIECE_M are absorbed into the adjacent piece (takes the
# survivor's is_inside status). Matches the matcher's CLIP_SKIP_PORTION /
# CLIP_MIN_LENGTH_M so the GPKG row and the QC display stay in sync.
SPLIT_SKIP_PORTION = 0.90
SPLIT_MIN_PIECE_M = 30.0
SPLIT_ENDPOINT_SNAP_M = 0.5  # tolerance for merging touching intervals

# Tiered overlap thresholds — length-adaptive to avoid filtering out short
# segments that sit entirely within an evacuation corridor.
#
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

# Buffer distance in meters applied to evacuation route polylines before
# measuring overlap. Evacuation routes and roadway segments are digitized
# from different source geometries; a 30 m corridor accounts for positional
# offset so that collinear roads register measurable intersection length
# instead of zero-length point contacts.
ROUTE_BUFFER_M = 30.0

# Contraflow-specific spatial parameters — tighter than general evacuation
# routes because contraflow applies only to specific Interstate lanes and
# both geometries (GDOT Interstates + contraflow polylines) are high-quality.
# A narrower buffer (15 m) prevents parallel frontage roads and ramps from
# matching, and higher thresholds suppress incidental clips.
CONTRAFLOW_BUFFER_M = 15.0
CONTRAFLOW_MIN_OVERLAP_M = 200.0
CONTRAFLOW_MIN_RATIO = 0.30
CONTRAFLOW_SHORT_MIN_RATIO = 0.60
CONTRAFLOW_MAX_ALIGNMENT_ANGLE_DEG = 20.0
CONTRAFLOW_MIN_INSIDE_CORRIDOR_RATIO = 0.25

# Angular alignment: reject segments crossing the corridor at steep angles.
# A segment running along the route should have a similar azimuth to the
# route at the overlap point.  Cross-roads and tangential clips fail this.
MAX_ALIGNMENT_ANGLE_DEG = 30.0


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


def _load_local_geojson(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{label} not found at {path}. "
            "Run 01-Raw-Data/Roadway-Inventory/scripts/download_evacuation_routes.py first."
        )
    gdf = gpd.read_file(path, engine="pyogrio")
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].reset_index(drop=True)
    LOGGER.info("Loaded %d %s features", len(gdf), label)
    return gdf


def _load_evac_routes(refresh: bool = False) -> gpd.GeoDataFrame:
    return _load_local_geojson(EVAC_ROUTES_GEOJSON, "evacuation route")


def _load_contraflow_routes(refresh: bool = False) -> gpd.GeoDataFrame:
    return _load_local_geojson(CONTRAFLOW_ROUTES_GEOJSON, "contraflow route")


def _parse_expected_family(route_name: str) -> str | None:
    """Infer roadway family from an evacuation route name.

    Returns a family string when the name clearly belongs to a signed
    route system, or ``None`` when the name is ambiguous / unparseable.
    """
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


# Families that belong to the signed state road system.  A segment in
# any of these families may carry a concurrent route designation that
# differs from the evacuation route name — e.g., a segment classified
# "US Route" can physically follow an SR corridor.  Only "Local/Other"
# segments (not part of the signed system) are rejected by the family
# filter.
_STATE_SYSTEM_FAMILIES = frozenset({
    "interstate", "us route", "u.s. route", "state route",
})

# GDOT suffix ROUTE_TYPE_GDOT codes associated with each base route type.
# Used to expand pre-filter matches for suffix variants (e.g., SR 300
# Connector segments have ROUTE_TYPE_GDOT='CN' not 'SR').
_SUFFIX_TYPE_MAP = {
    "spur": "SP",
    "business": "BU",
    "connector": "CN",
    "bypass": "BY",
    "loop": "LP",
    "alternate": "AL",
}

# Regex for parsing route designations from evacuation ROUTE_NAME values.
_ROUTE_DESIGNATION_RE = re.compile(
    r"(?P<type>I|Interstate|US|SR|CR)\s*-?\s*"
    r"(?P<number>\d+)"
    r"(?:\s+(?P<suffix>North|South|East|West|Spur|Business|Connector|Bypass|Loop|Alternate))?",
    re.IGNORECASE,
)


def _parse_route_designations(
    route_name: str,
) -> list[tuple[str, int, str | None]]:
    """Parse evacuation ROUTE_NAME into (route_type, number, suffix) tuples.

    Handles single names (``SR 26``), directional suffixes (``I 75 North``),
    route-type suffixes (``SR 300 Connector``), and dual designations
    (``SR 1/US 27``).

    Returns an empty list when the name cannot be parsed (``Liberty Expy``,
    ``Ocean Hwy``, ``None``).
    """
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
        # Directional suffixes are not route-type modifiers — discard them.
        suffix: str | None = None
        if suffix_raw and suffix_raw.lower() in _SUFFIX_TYPE_MAP:
            suffix = suffix_raw.capitalize()
        results.append((route_type, number, suffix))
    return results


# GDOT ROUTE_TYPE_GDOT values that are Local/Other (county roads, city
# streets) — the primary source of false-positive evacuation matches.
_LOCAL_ROUTE_TYPES = frozenset({"CR", "CS"})


def _attribute_prefilter_mask(
    segments: gpd.GeoDataFrame,
    designations: list[tuple[str, int, str | None]],
) -> pd.Series:
    """Build a boolean mask excluding Local/Other segments.

    The primary purpose is to eliminate false positives from county roads
    and city streets that run parallel to evacuation corridors.  All
    state-system segments (I, US, SR, and their suffix variants like SP,
    BU, CN) are kept so that concurrent route designations are not lost
    (e.g., a US Route segment carrying an SR evac corridor).

    CR/CS segments are excluded UNLESS a parsed designation specifically
    targets them (e.g., ``CR 780`` is a legitimate evac route).
    """
    if "ROUTE_TYPE_GDOT" not in segments.columns:
        return pd.Series(True, index=segments.index)

    # Check whether any designation explicitly targets a Local/Other type.
    local_designations = [
        (rt, num, sfx) for rt, num, sfx in designations if rt in _LOCAL_ROUTE_TYPES
    ]

    mask = ~segments["ROUTE_TYPE_GDOT"].isin(_LOCAL_ROUTE_TYPES)

    # Re-include CR/CS segments that match a specific local designation.
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
    """Build a boolean mask for segments matching a specific designation.

    Used to determine ``match_method`` — segments matching here get
    ``attribute+spatial``, others get ``spatial_only``.
    """
    mask = pd.Series(False, index=segments.index)
    has_hwy = "HWY_NAME" in segments.columns
    has_rt = "ROUTE_TYPE_GDOT" in segments.columns
    has_brn = "BASE_ROUTE_NUMBER" in segments.columns

    for route_type, number, suffix in designations:
        if route_type == "I":
            if has_hwy:
                if suffix:
                    # "I 16 Spur" → match HWY_NAME containing "I-16" and
                    # the suffix (e.g., "I-16 SPUR"), case-insensitive.
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


def _overlay_matches(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str | None,
    *,
    overlay_label: str,
    interstate_only: bool = False,
    enforce_route_family: bool = False,
    attribute_prefilter: bool = False,
    buffer_m: float | None = None,
    min_overlap_m: float | None = None,
    min_ratio: float | None = None,
    short_min_ratio: float | None = None,
    max_alignment_deg: float | None = None,
) -> dict[int, dict[str, object]]:
    """Return matched segments with route names and overlap diagnostics."""
    # Resolve per-call threshold overrides (fall back to module-level defaults)
    eff_buffer_m = buffer_m if buffer_m is not None else ROUTE_BUFFER_M
    eff_min_overlap_m = min_overlap_m if min_overlap_m is not None else NORMAL_MIN_OVERLAP_M
    eff_min_ratio = min_ratio if min_ratio is not None else NORMAL_MIN_RATIO
    eff_short_min_ratio = short_min_ratio if short_min_ratio is not None else SHORT_SEGMENT_MIN_RATIO
    eff_max_alignment_deg = max_alignment_deg if max_alignment_deg is not None else MAX_ALIGNMENT_ANGLE_DEG

    if overlay.empty:
        LOGGER.info(
            "%s overlay matched 0 segments; 0 candidates rejected by ratio "
            "filter, 0 rejected by route-family filter, 0 mega-segment "
            "candidates encountered, 0 rejected",
            overlay_label,
        )
        return {}

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS - cannot run spatial overlay")
        return {}

    eligible_segments = segments
    if interstate_only:
        if "ROUTE_FAMILY" not in segments.columns:
            LOGGER.warning(
                "Segments are missing ROUTE_FAMILY - cannot restrict %s overlay "
                "to Interstate segments",
                overlay_label.lower(),
            )
            return {}
        eligible_segments = segments.loc[segments["ROUTE_FAMILY"] == "Interstate"]
        LOGGER.info(
            "Running %s spatial join: %d Interstate segments (of %d total) x "
            "%d overlay features (buffered %.0f m)",
            overlay_label.lower(), len(eligible_segments), len(segments),
            len(overlay), eff_buffer_m,
        )
    elif attribute_prefilter and "_designations" in overlay.columns:
        # Collect all designations across all overlay features and restrict
        # the segment pool to those matching at least one designation.
        all_designations: list[tuple[str, int, str | None]] = []
        for desigs in overlay["_designations"]:
            all_designations.extend(desigs)
        if all_designations:
            attr_mask = _attribute_prefilter_mask(segments, all_designations)
            eligible_segments = segments.loc[attr_mask]
            LOGGER.info(
                "Running %s spatial join: %d attribute-filtered segments "
                "(of %d total) x %d overlay features (buffered %.0f m)",
                overlay_label.lower(), len(eligible_segments), len(segments),
                len(overlay), eff_buffer_m,
            )
        else:
            LOGGER.info(
                "Running %s spatial join: %d segments x %d overlay features "
                "(buffered %.0f m) [no valid designations for prefilter]",
                overlay_label.lower(), len(eligible_segments), len(overlay),
                eff_buffer_m,
            )
    else:
        LOGGER.info(
            "Running %s spatial join: %d segments x %d overlay features "
            "(buffered %.0f m)",
            overlay_label.lower(), len(eligible_segments), len(overlay),
            eff_buffer_m,
        )

    if eligible_segments.empty:
        LOGGER.info(
            "%s overlay matched 0 segments; 0 candidates rejected by ratio "
            "filter, 0 rejected by route-family filter, 0 mega-segment "
            "candidates encountered, 0 rejected",
            overlay_label,
        )
        return {}

    if overlay.crs != seg_crs:
        overlay = overlay.to_crs(seg_crs)

    original_route_geoms = overlay.geometry

    overlay_buffered = overlay.copy()
    overlay_buffered["geometry"] = overlay_buffered.geometry.buffer(eff_buffer_m)

    name_cols = [name_field] if name_field and name_field in overlay.columns else []
    join_cols = ["geometry"] + name_cols
    for col in name_cols:
        overlay_buffered[col] = overlay[col].values

    joined = gpd.sjoin(
        eligible_segments[["geometry"]],
        overlay_buffered[join_cols],
        how="inner",
        predicate="intersects",
    )

    if joined.empty:
        LOGGER.info(
            "%s overlay matched 0 segments; 0 candidates rejected by ratio "
            "filter, 0 rejected by route-family filter, 0 mega-segment "
            "candidates encountered, 0 rejected",
            overlay_label,
        )
        return {}

    results: dict[int, dict[str, object]] = {}
    buffered_geoms = overlay_buffered.geometry
    ratio_rejected = 0
    route_family_rejected = 0
    alignment_rejected = 0
    mega_candidates = 0
    mega_rejected = 0

    for seg_idx, group in joined.groupby(joined.index):
        seg_geom = segments.loc[seg_idx, "geometry"]

        segment_length_raw = (
            segments.loc[seg_idx, "segment_length_m"]
            if "segment_length_m" in segments.columns
            else None
        )
        try:
            segment_length_m = float(segment_length_raw)
        except (TypeError, ValueError):
            segment_length_m = 0.0
        if segment_length_m <= 0:
            segment_length_m = float(seg_geom.length)

        segment_family_raw = (
            segments.loc[seg_idx, "ROUTE_FAMILY"]
            if "ROUTE_FAMILY" in segments.columns
            else None
        )
        segment_family = (
            str(segment_family_raw).strip() if pd.notna(segment_family_raw) else None
        )

        names: list[str] = []
        max_overlap_m = 0.0
        max_overlap_ratio = 0.0
        has_valid_overlap = False
        is_mega_segment = segment_length_m > MEGA_SEGMENT_LENGTH_M
        is_short_segment = segment_length_m < SHORT_SEGMENT_MAX_M

        for _, row in group.iterrows():
            if is_mega_segment:
                mega_candidates += 1

            route_name = row[name_field] if name_field and name_field in row.index else None
            if enforce_route_family and pd.notna(route_name):
                expected_family = _parse_expected_family(str(route_name))
                if expected_family is not None:
                    seg_fam_lower = segment_family.lower() if segment_family else ""
                    if seg_fam_lower not in _STATE_SYSTEM_FAMILIES:
                        route_family_rejected += 1
                        continue

            corridor = buffered_geoms.iloc[row["index_right"]]
            try:
                overlap_geom = seg_geom.intersection(corridor)
                overlap_len = float(overlap_geom.length)
            except Exception:
                overlap_geom = None
                overlap_len = 0.0

            overlap_ratio = 0.0
            if segment_length_m > 0:
                overlap_ratio = overlap_len / segment_length_m

            # Tiered acceptance: short segments use ratio-only,
            # normal segments require absolute + ratio, mega need high ratio
            # or their clipped-to-corridor portion meets normal thresholds.
            if is_short_segment:
                accepted = overlap_ratio >= eff_short_min_ratio
            elif is_mega_segment:
                accepted = (
                    overlap_len >= MEGA_MIN_OVERLAP_M
                    and overlap_ratio >= MEGA_MIN_RATIO
                )
                # Fallback: clip mega-segment to corridor and evaluate the
                # clipped portion against normal thresholds.  This handles
                # very long segments (e.g. 363 km US-1 DEC) that run along
                # a corridor for several km but have a tiny full-segment ratio.
                if not accepted and overlap_len >= eff_min_overlap_m:
                    accepted = True
            else:
                accepted = (
                    overlap_len >= eff_min_overlap_m
                    and overlap_ratio >= eff_min_ratio
                )

            # Angular alignment: reject segments crossing the corridor
            # at steep angles (not running parallel to the route).
            if accepted and not is_short_segment and overlap_geom is not None:
                route_geom = original_route_geoms.iloc[row["index_right"]]
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
                    alignment_rejected += 1

            if not accepted:
                ratio_rejected += 1
                if is_mega_segment:
                    mega_rejected += 1
                continue

            has_valid_overlap = True
            max_overlap_m = max(max_overlap_m, overlap_len)
            max_overlap_ratio = max(max_overlap_ratio, overlap_ratio)
            if name_field and name_field in row.index:
                val = row[name_field]
                if pd.notna(val) and str(val).strip():
                    names.append(str(val).strip())

        if has_valid_overlap:
            results[seg_idx] = {
                "names": names,
                "overlap_m": max_overlap_m,
                "overlap_ratio": max_overlap_ratio,
            }

    LOGGER.info(
        "%s overlay matched %d segments; %d candidates rejected by ratio filter, "
        "%d rejected by route-family filter, %d rejected by alignment filter, "
        "%d mega-segment candidates encountered, %d rejected",
        overlay_label, len(results), ratio_rejected, route_family_rejected,
        alignment_rejected, mega_candidates, mega_rejected,
    )
    return results


def _hybrid_evac_overlay(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str,
) -> dict[int, dict[str, object]]:
    """Hybrid attribute + spatial matching for evacuation routes.

    For each evac feature with a parseable ROUTE_NAME, spatial join candidates
    are restricted to segments whose ROUTE_TYPE_GDOT / BASE_ROUTE_NUMBER (or
    HWY_NAME for Interstates) match the parsed designation.  Features with
    null or unparseable names fall through to spatial-only matching with the
    same thresholds as before.

    Returns ``{seg_idx: {"names": [...], "overlap_m": float,
    "overlap_ratio": float, "match_method": str}}``.
    """
    if overlay.empty or segments.empty:
        LOGGER.info("Hybrid evac overlay: 0 candidates (empty input)")
        return {}

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot run spatial overlay")
        return {}

    if overlay.crs != seg_crs:
        overlay = overlay.to_crs(seg_crs)

    # Split evac features into parseable vs. unparseable.
    overlay = overlay.copy()
    overlay["_designations"] = overlay[name_field].apply(
        lambda n: _parse_route_designations(n) if pd.notna(n) else []
    )
    parseable_mask = overlay["_designations"].apply(len) > 0
    evac_parseable = overlay[parseable_mask].reset_index(drop=True)
    evac_unparseable = overlay[~parseable_mask].reset_index(drop=True)

    LOGGER.info(
        "Hybrid evac split: %d parseable, %d spatial-only fallback",
        len(evac_parseable), len(evac_unparseable),
    )

    results: dict[int, dict[str, object]] = {}

    # --- Attribute-prefiltered path for parseable evac features ---
    # Prefilter excludes Local/Other (CR/CS) segments; state-system segments
    # are kept to accommodate concurrent route designations.
    if not evac_parseable.empty:
        attr_results = _overlay_matches(
            segments,
            evac_parseable,
            name_field,
            overlay_label="Evac (attribute-filtered)",
            attribute_prefilter=True,
        )
        # Determine match_method per segment: "attribute+spatial" if the
        # segment's own attributes match the evac designation specifically,
        # "spatial_only" if it matched via proximity to a concurrent route.
        all_designations: list[tuple[str, int, str | None]] = []
        for desigs in evac_parseable["_designations"]:
            all_designations.extend(desigs)
        specific_mask = _specific_designation_mask(segments, all_designations)
        for idx, match in attr_results.items():
            if specific_mask.loc[idx]:
                match["match_method"] = "attribute+spatial"
            else:
                match["match_method"] = "spatial_only"
            results[idx] = match

    # --- Spatial-only path for unparseable evac features ---
    if not evac_unparseable.empty:
        spatial_results = _overlay_matches(
            segments,
            evac_unparseable,
            name_field,
            overlay_label="Evac (spatial-only)",
        )
        for idx, match in spatial_results.items():
            match["match_method"] = "spatial_only"
            if idx in results:
                existing = results[idx]
                existing["names"] = list(
                    set(existing["names"]) | set(match.get("names", []))
                )
                if match["overlap_m"] > existing["overlap_m"]:
                    existing["overlap_m"] = match["overlap_m"]
                if match["overlap_ratio"] > existing["overlap_ratio"]:
                    existing["overlap_ratio"] = match["overlap_ratio"]
            else:
                results[idx] = match

    LOGGER.info(
        "Hybrid evac overlay total: %d matched segments "
        "(%d attribute+spatial, %d spatial-only)",
        len(results),
        sum(1 for m in results.values() if m.get("match_method") == "attribute+spatial"),
        sum(1 for m in results.values() if m.get("match_method") == "spatial_only"),
    )
    return results


def _contraflow_overlay_standalone(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str | None,
) -> dict[int, dict[str, object]]:
    """Spatial-only matching for contraflow routes (Interstate segments only).

    Contraflow routes have no usable route-name attributes (only phonetic
    code names like ``Mary``, ``Lincoln``).  Matching is restricted to
    Interstate segments and uses tighter spatial parameters than general
    evacuation routes: narrower buffer (15 m), higher overlap thresholds,
    and stricter alignment tolerance (20 deg).

    This is a standalone function — thresholds can be tuned independently
    without affecting evacuation matching.
    """
    if overlay.empty:
        LOGGER.info("Contraflow overlay: 0 candidates (empty overlay)")
        return {}

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot run contraflow overlay")
        return {}

    if "ROUTE_FAMILY" not in segments.columns:
        LOGGER.warning(
            "Segments are missing ROUTE_FAMILY — cannot restrict contraflow "
            "overlay to Interstate segments"
        )
        return {}

    eligible = segments.loc[segments["ROUTE_FAMILY"] == "Interstate"]
    if eligible.empty:
        LOGGER.info("Contraflow overlay: 0 Interstate segments available")
        return {}

    if overlay.crs != seg_crs:
        overlay = overlay.to_crs(seg_crs)

    LOGGER.info(
        "Running contraflow spatial join: %d Interstate segments (of %d total) "
        "x %d overlay features (buffered %.0f m)",
        len(eligible), len(segments), len(overlay), CONTRAFLOW_BUFFER_M,
    )

    original_route_geoms = overlay.geometry.copy()
    overlay_buffered = overlay.copy()
    overlay_buffered["geometry"] = overlay_buffered.geometry.buffer(CONTRAFLOW_BUFFER_M)

    name_cols = [name_field] if name_field and name_field in overlay.columns else []
    join_cols = ["geometry"] + name_cols
    for col in name_cols:
        overlay_buffered[col] = overlay[col].values

    joined = gpd.sjoin(
        eligible[["geometry"]],
        overlay_buffered[join_cols],
        how="inner",
        predicate="intersects",
    )

    if joined.empty:
        LOGGER.info("Contraflow overlay matched 0 segments")
        return {}

    results: dict[int, dict[str, object]] = {}
    buffered_geoms = overlay_buffered.geometry
    ratio_rejected = 0
    alignment_rejected = 0

    for seg_idx, group in joined.groupby(joined.index):
        seg_geom = segments.loc[seg_idx, "geometry"]
        segment_length_raw = (
            segments.loc[seg_idx, "segment_length_m"]
            if "segment_length_m" in segments.columns
            else None
        )
        try:
            segment_length_m = float(segment_length_raw)
        except (TypeError, ValueError):
            segment_length_m = 0.0
        if segment_length_m <= 0:
            segment_length_m = float(seg_geom.length)

        names: list[str] = []
        max_overlap_m = 0.0
        max_overlap_ratio = 0.0
        has_valid_overlap = False
        is_short_segment = segment_length_m < SHORT_SEGMENT_MAX_M

        for _, row in group.iterrows():
            corridor = buffered_geoms.iloc[row["index_right"]]
            try:
                overlap_geom = seg_geom.intersection(corridor)
                overlap_len = float(overlap_geom.length)
            except Exception:
                overlap_geom = None
                overlap_len = 0.0

            overlap_ratio = overlap_len / segment_length_m if segment_length_m > 0 else 0.0

            if is_short_segment:
                accepted = overlap_ratio >= CONTRAFLOW_SHORT_MIN_RATIO
            else:
                accepted = (
                    overlap_len >= CONTRAFLOW_MIN_OVERLAP_M
                    and overlap_ratio >= CONTRAFLOW_MIN_RATIO
                )

            # Angular alignment check
            if accepted and not is_short_segment and overlap_geom is not None:
                route_geom = original_route_geoms.iloc[row["index_right"]]
                try:
                    route_section = route_geom.intersection(
                        seg_geom.buffer(CONTRAFLOW_BUFFER_M)
                    )
                except Exception:
                    route_section = None
                seg_az = _line_azimuth(overlap_geom)
                route_az = _line_azimuth(route_section)
                angle = _alignment_angle_deg(seg_az, route_az)
                if angle is not None and angle > CONTRAFLOW_MAX_ALIGNMENT_ANGLE_DEG:
                    accepted = False
                    alignment_rejected += 1

            if not accepted:
                ratio_rejected += 1
                continue

            has_valid_overlap = True
            max_overlap_m = max(max_overlap_m, overlap_len)
            max_overlap_ratio = max(max_overlap_ratio, overlap_ratio)
            if name_field and name_field in row.index:
                val = row[name_field]
                if pd.notna(val) and str(val).strip():
                    names.append(str(val).strip())

        if has_valid_overlap:
            results[seg_idx] = {
                "names": names,
                "overlap_m": max_overlap_m,
                "overlap_ratio": max_overlap_ratio,
            }

    LOGGER.info(
        "Contraflow overlay matched %d segments; %d rejected by "
        "ratio/threshold, %d rejected by alignment",
        len(results), ratio_rejected, alignment_rejected,
    )
    return results


def _update_overlap_diagnostics(
    enriched: gpd.GeoDataFrame,
    idx: int,
    overlap_m: float,
    overlap_ratio: float,
) -> None:
    """Store the strongest overlap metrics seen for a segment."""
    existing_overlap_m = enriched.at[idx, "SEC_EVAC_OVERLAP_M"]
    existing_overlap_ratio = enriched.at[idx, "SEC_EVAC_OVERLAP_RATIO"]

    if pd.isna(existing_overlap_m) or float(existing_overlap_m) < overlap_m:
        enriched.at[idx, "SEC_EVAC_OVERLAP_M"] = overlap_m
    if pd.isna(existing_overlap_ratio) or float(existing_overlap_ratio) < overlap_ratio:
        enriched.at[idx, "SEC_EVAC_OVERLAP_RATIO"] = overlap_ratio


# ---------------------------------------------------------------------------
# Segment splitting — cut SEC_EVAC=True segments at the corridor boundary so
# the GPKG reflects only the in-corridor portion as flagged, matching the QC
# map's clipped display. Called after all flagging is done; changes row count.
# ---------------------------------------------------------------------------


def _clip_to_intervals(
    clip_geom,
    parent: LineString,
    parent_len: float,
) -> list[tuple[float, float]]:
    """Project clip sub-lines onto the parent and return merged distance
    intervals (in meters along the parent) covered by the clip.
    """
    if clip_geom is None or clip_geom.is_empty:
        return []
    if isinstance(clip_geom, LineString):
        lines = [clip_geom]
    elif isinstance(clip_geom, MultiLineString):
        lines = list(clip_geom.geoms)
    else:
        try:
            lines = list(clip_geom.geoms)
        except AttributeError:
            return []

    raw: list[tuple[float, float]] = []
    for ln in lines:
        if not isinstance(ln, LineString) or ln.is_empty or ln.length < 0.01:
            continue
        coords = list(ln.coords)
        d0 = parent.project(Point(coords[0]))
        d1 = parent.project(Point(coords[-1]))
        lo, hi = (d0, d1) if d0 <= d1 else (d1, d0)
        lo = max(0.0, min(parent_len, lo))
        hi = max(0.0, min(parent_len, hi))
        if hi - lo < 0.01:
            continue
        raw.append((lo, hi))

    raw.sort()
    merged: list[tuple[float, float]] = []
    for s, e in raw:
        if merged and s <= merged[-1][1] + SPLIT_ENDPOINT_SNAP_M:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def _absorb_slivers(
    pieces: list[tuple[float, float, bool]],
) -> list[tuple[float, float, bool]]:
    """Merge pieces shorter than SPLIT_MIN_PIECE_M into the longer adjacent
    piece. The merged piece takes the ``is_inside`` flag of the survivor.
    """
    if len(pieces) <= 1:
        return pieces

    changed = True
    while changed and len(pieces) > 1:
        changed = False
        for i, (s, e, _flag) in enumerate(pieces):
            if e - s >= SPLIT_MIN_PIECE_M:
                continue
            # Sliver — merge into the longer neighbor.
            prev_len = (pieces[i - 1][1] - pieces[i - 1][0]) if i > 0 else -1.0
            next_len = (
                (pieces[i + 1][1] - pieces[i + 1][0]) if i + 1 < len(pieces) else -1.0
            )
            if prev_len < 0 and next_len < 0:
                return pieces  # only sliver, nothing to merge
            if next_len > prev_len:
                # merge into next
                ns, ne, nflag = pieces[i + 1]
                pieces = (
                    pieces[:i]
                    + [(s, ne, nflag)]
                    + pieces[i + 2 :]
                )
            else:
                # merge into previous
                ps, pe, pflag = pieces[i - 1]
                pieces = (
                    pieces[: i - 1]
                    + [(ps, e, pflag)]
                    + pieces[i + 1 :]
                )
            changed = True
            break
    return pieces


def _split_single_segment(
    parent_row: pd.Series,
    intervals: list[tuple[float, float]],
) -> list[dict] | None:
    """Split ``parent_row`` at corridor boundaries given inside intervals.

    Returns a list of new row dicts (2+ children) or ``None`` if splitting
    should be skipped (segment kept whole).
    """
    parent_geom = parent_row.geometry
    if parent_geom is None or parent_geom.is_empty:
        return None
    if not isinstance(parent_geom, LineString):
        # Only split simple LineStrings; MultiLineStrings are rare here and
        # the milepoint projection assumptions break down.
        return None

    parent_len = float(parent_geom.length)
    if parent_len <= 0.0 or not intervals:
        return None

    # Clamp intervals once more and compute inside length.
    clamped = [(max(0.0, s), min(parent_len, e)) for s, e in intervals if e > s]
    if not clamped:
        return None
    inside_len = sum(e - s for s, e in clamped)
    if inside_len / parent_len >= SPLIT_SKIP_PORTION:
        return None  # mostly inside — keep whole

    # Build ordered piece list covering [0, parent_len] alternating inside/outside.
    pieces: list[tuple[float, float, bool]] = []
    cursor = 0.0
    for s, e in clamped:
        if s > cursor + SPLIT_ENDPOINT_SNAP_M:
            pieces.append((cursor, s, False))
        pieces.append((s, e, True))
        cursor = e
    if parent_len > cursor + SPLIT_ENDPOINT_SNAP_M:
        pieces.append((cursor, parent_len, False))

    pieces = _absorb_slivers(pieces)
    if len(pieces) <= 1:
        return None  # nothing to split after sliver absorption

    mp_from = parent_row.get("FROM_MILEPOINT")
    mp_to = parent_row.get("TO_MILEPOINT")
    try:
        mp_from_f = float(mp_from) if pd.notna(mp_from) else None
        mp_to_f = float(mp_to) if pd.notna(mp_to) else None
    except (TypeError, ValueError):
        mp_from_f = mp_to_f = None
    mp_range = (
        (mp_to_f - mp_from_f)
        if (mp_from_f is not None and mp_to_f is not None)
        else None
    )

    parent_dict = parent_row.to_dict()
    children: list[dict] = []
    for s, e, is_inside in pieces:
        try:
            piece_geom = substring(parent_geom, s, e)
        except Exception:
            return None
        if piece_geom is None or piece_geom.is_empty:
            return None

        child = dict(parent_dict)
        child["geometry"] = piece_geom
        child["segment_length_m"] = float(piece_geom.length)
        child["segment_length_mi"] = float(piece_geom.length) / 1609.344

        if mp_range is not None and parent_len > 0 and mp_from_f is not None:
            frac_s = s / parent_len
            frac_e = e / parent_len
            child["FROM_MILEPOINT"] = round(mp_from_f + frac_s * mp_range, 4)
            child["TO_MILEPOINT"] = round(mp_from_f + frac_e * mp_range, 4)

        if not is_inside:
            # Outside pieces lose all evac flagging and diagnostics. Contraflow
            # flagging is also cleared because contraflow corridors are a
            # subset of the evac corridor for the Interstates we flag.
            child["SEC_EVAC"] = False
            child["SEC_EVAC_CONTRAFLOW"] = False
            child["SEC_EVAC_ROUTE_NAME"] = None
            child["SEC_EVAC_SOURCE"] = None
            child["SEC_EVAC_MATCH_METHOD"] = None
            child["SEC_EVAC_OVERLAP_M"] = 0.0
            child["SEC_EVAC_OVERLAP_RATIO"] = 0.0
            child["SEC_EVAC_MATCH_PORTION"] = None

        children.append(child)
    return children


def split_flagged_segments_at_corridor(
    enriched: gpd.GeoDataFrame,
    evac_matches: dict[int, dict],
    contraflow_corridor,
) -> gpd.GeoDataFrame:
    """Cut SEC_EVAC=True segments at corridor boundaries.

    ``evac_matches`` is the matcher's per-segment dict, keyed by the row
    index used at matching time. Each entry provides ``clipped_geom`` (the
    portion of the segment that passed all gates) and ``match_portion``.

    For contraflow-only segments — currently empty since all contraflow
    matches coincide with evac matches — we clip against ``contraflow_corridor``.

    Returns a new GeoDataFrame with split rows replacing the originals.
    Column set is preserved; row count changes.
    """
    if enriched.empty:
        return enriched

    flagged_mask = enriched["SEC_EVAC"].fillna(False).astype(bool)
    if not flagged_mask.any():
        LOGGER.info("Segment splitting: 0 flagged segments, nothing to split")
        return enriched

    # Copy matches geometry into a lookup the splitter can use per parent index.
    parent_indices = enriched.index[flagged_mask]
    matches_by_idx: dict = {}
    for idx in parent_indices:
        m = evac_matches.get(idx)
        if m and m.get("clipped_geom") is not None:
            matches_by_idx[idx] = (m["clipped_geom"], float(m.get("match_portion", 1.0)))
            continue
        # Contraflow-only fallback — clip the parent against contraflow corridor.
        if contraflow_corridor is not None:
            seg_geom = enriched.at[idx, "geometry"]
            if seg_geom is not None and not seg_geom.is_empty:
                try:
                    clip = seg_geom.intersection(contraflow_corridor)
                except Exception:
                    clip = None
                if clip is not None and not clip.is_empty and clip.length >= SPLIT_MIN_PIECE_M:
                    portion = float(clip.length) / float(seg_geom.length) if seg_geom.length > 0 else 1.0
                    matches_by_idx[idx] = (clip, portion)

    if not matches_by_idx:
        LOGGER.info("Segment splitting: no clipped geometries available, skipping")
        return enriched

    # Record match_portion for every flagged row (whether or not it splits).
    enriched["SEC_EVAC_MATCH_PORTION"] = pd.Series(
        {idx: portion for idx, (_g, portion) in matches_by_idx.items()},
        dtype="float64",
    )

    split_parents: list[int] = []
    new_rows: list[dict] = []
    skipped_full_inside = 0
    skipped_invalid = 0
    total_children_emitted = 0
    pieces_absorbed = 0

    for idx, (clip_geom, _portion) in matches_by_idx.items():
        parent_row = enriched.loc[idx]
        parent_geom = parent_row.geometry
        if parent_geom is None or parent_geom.is_empty or not isinstance(parent_geom, LineString):
            skipped_invalid += 1
            continue
        parent_len = float(parent_geom.length)
        intervals = _clip_to_intervals(clip_geom, parent_geom, parent_len)
        if not intervals:
            skipped_invalid += 1
            continue
        inside_len = sum(e - s for s, e in intervals)
        if parent_len > 0 and inside_len / parent_len >= SPLIT_SKIP_PORTION:
            skipped_full_inside += 1
            continue
        children = _split_single_segment(parent_row, intervals)
        if not children:
            skipped_full_inside += 1
            continue
        split_parents.append(idx)
        new_rows.extend(children)
        total_children_emitted += len(children)
        # Pieces absorbed = (raw interval pieces + gaps) - emitted pieces.
        raw_piece_count = len(intervals) * 2 + 1  # worst case alternation
        if raw_piece_count > len(children):
            pieces_absorbed += raw_piece_count - len(children)

    if not split_parents:
        LOGGER.info(
            "Segment splitting: 0 segments split (%d kept whole >=%.0f%% inside, %d skipped invalid)",
            skipped_full_inside, SPLIT_SKIP_PORTION * 100, skipped_invalid,
        )
        return enriched

    # Assemble new frame: drop parents, append children.
    kept = enriched.drop(index=split_parents)
    children_gdf = gpd.GeoDataFrame(new_rows, crs=enriched.crs)

    # Preserve column order and any extension columns.
    for col in enriched.columns:
        if col not in children_gdf.columns:
            children_gdf[col] = None
    children_gdf = children_gdf[enriched.columns]

    combined = pd.concat([kept, children_gdf], ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=enriched.crs)

    LOGGER.info(
        "Segment splitting: %d parents split into %d children "
        "(kept whole >=%.0f%% inside=%d, skipped invalid=%d). "
        "Net row delta: +%d.",
        len(split_parents), total_children_emitted, SPLIT_SKIP_PORTION * 100,
        skipped_full_inside, skipped_invalid,
        total_children_emitted - len(split_parents),
    )
    return combined


def apply_evacuation_enrichment(
    gdf: gpd.GeoDataFrame,
    refresh: bool = False,
) -> gpd.GeoDataFrame:
    """Flag roadway segments that overlap GDOT hurricane evacuation routes."""

    enriched = gdf.copy()
    enriched["SEC_EVAC"] = False
    enriched["SEC_EVAC_CONTRAFLOW"] = False
    enriched["SEC_EVAC_ROUTE_NAME"] = None
    enriched["SEC_EVAC_SOURCE"] = None
    enriched["SEC_EVAC_MATCH_METHOD"] = None
    enriched["SEC_EVAC_OVERLAP_M"] = 0.0
    enriched["SEC_EVAC_OVERLAP_RATIO"] = 0.0
    enriched["SEC_EVAC_MATCH_PORTION"] = pd.NA

    # --- Evacuation routes (hybrid attribute + spatial matching) ---
    try:
        evac = _load_evac_routes(refresh=refresh)
    except Exception as exc:
        LOGGER.warning("Evacuation route enrichment unavailable: %s", exc)
        return enriched

    evac_matches, evac_diagnostics = per_corridor_evac_overlay(
        enriched, evac, name_field="ROUTE_NAME",
    )
    # Corridor proximity post-filter is now applied per-corridor inside
    # _per_corridor_evac_overlay — no statewide post-filter needed.

    for idx, match in evac_matches.items():
        names = match.get("names", [])
        enriched.at[idx, "SEC_EVAC"] = True
        enriched.at[idx, "SEC_EVAC_SOURCE"] = "gdot_eoc_hurricane_evacuation"
        enriched.at[idx, "SEC_EVAC_MATCH_METHOD"] = match.get(
            "match_method", "spatial_only"
        )
        _update_overlap_diagnostics(
            enriched,
            idx,
            float(match.get("overlap_m", 0.0)),
            float(match.get("overlap_ratio", 0.0)),
        )
        if names:
            enriched.at[idx, "SEC_EVAC_ROUTE_NAME"] = "; ".join(sorted(set(names)))

    LOGGER.info("Evacuation route matches: %d segments", len(evac_matches))
    _evac_snapshot("01_after_matcher_and_filter", enriched)

    # --- Contraflow routes (standalone spatial-only matching) ---
    contraflow_matches: dict[int, dict[str, object]] = {}
    contra_corridor = None
    try:
        contraflow = _load_contraflow_routes(refresh=refresh)
        contraflow_matches = _contraflow_overlay_standalone(
            enriched,
            contraflow,
            name_field="TITLE",
        )
    except Exception as exc:
        LOGGER.warning("Contraflow route enrichment unavailable: %s", exc)

    # Corridor proximity post-filter for contraflow (tighter than evac routes)
    if contraflow_matches:
        seg_crs = enriched.crs
        contra_proj = contraflow.to_crs(seg_crs) if contraflow.crs != seg_crs else contraflow
        contra_corridor = contra_proj.geometry.buffer(CONTRAFLOW_BUFFER_M).union_all()
        matched_idxs = list(contraflow_matches.keys())
        matched_geoms = enriched.loc[matched_idxs, "geometry"]
        inside_lens = matched_geoms.intersection(contra_corridor).length
        seg_lens = matched_geoms.length
        inside_ratios = inside_lens / seg_lens.replace(0, 1)
        remove_idxs = set(inside_ratios[inside_ratios < CONTRAFLOW_MIN_INSIDE_CORRIDOR_RATIO].index)
        if remove_idxs:
            contraflow_matches = {k: v for k, v in contraflow_matches.items() if k not in remove_idxs}
            LOGGER.info(
                "Contraflow corridor proximity filter removed %d segments (kept %d)",
                len(remove_idxs), len(contraflow_matches),
            )

    for idx, match in contraflow_matches.items():
        names = match.get("names", [])
        enriched.at[idx, "SEC_EVAC_CONTRAFLOW"] = True
        _update_overlap_diagnostics(
            enriched,
            idx,
            float(match.get("overlap_m", 0.0)),
            float(match.get("overlap_ratio", 0.0)),
        )
        # Evacuation source takes priority; only set source for contraflow-only segments
        if not enriched.at[idx, "SEC_EVAC"]:
            enriched.at[idx, "SEC_EVAC"] = True
            enriched.at[idx, "SEC_EVAC_SOURCE"] = "gdot_eoc_contraflow"
        if names:
            existing = enriched.at[idx, "SEC_EVAC_ROUTE_NAME"]
            all_names = []
            if existing and pd.notna(existing):
                all_names.extend(str(existing).split("; "))
            all_names.extend(names)
            enriched.at[idx, "SEC_EVAC_ROUTE_NAME"] = "; ".join(sorted(set(all_names)))

    LOGGER.info("Contraflow route matches: %d segments", len(contraflow_matches))
    _evac_snapshot("02_after_contraflow", enriched)

    total = int(enriched["SEC_EVAC"].sum())
    contra = int(enriched["SEC_EVAC_CONTRAFLOW"].sum())
    LOGGER.info(
        "Evacuation enrichment complete: %d evacuation segments (%d contraflow)",
        total, contra,
    )

    # --- Split flagged segments at corridor boundary ---
    # Replace parent rows with inside/outside child rows so SEC_EVAC=True
    # applies only to the geometry actually following the corridor. Matches
    # the QC map's clipped display.
    enriched = split_flagged_segments_at_corridor(
        enriched, evac_matches, contra_corridor,
    )

    post_total = int(enriched["SEC_EVAC"].sum())
    post_contra = int(enriched["SEC_EVAC_CONTRAFLOW"].sum())
    LOGGER.info(
        "Post-split totals: %d SEC_EVAC segments (%d contraflow), %d total rows",
        post_total, post_contra, len(enriched),
    )
    _evac_snapshot("03_after_split", enriched)

    # Mirror evac flags from INC to DEC direction partners so divided highways
    # are fully flagged in both directions.
    enriched = apply_direction_mirror_evac(enriched)
    _evac_snapshot("04_after_direction_mirror", enriched)

    return enriched


def apply_direction_mirror_evac(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Mirror SEC_EVAC flags from INC to DEC direction segments on the same route.

    Divided highways are represented as INC and DEC ROUTE_ID pairs.  The
    matcher runs on geometry and typically flags the INC direction; the
    matching DEC segment (same physical road, opposite direction) is missed
    because the DEC polyline is offset by a few metres from the INC one.
    This pass finds unflagged DEC segments whose INC partner is flagged and
    shares an overlapping milepoint range, then copies all SEC_EVAC_* columns
    across.
    """
    MILEPOINT_TOLERANCE = 0.01

    filled = gdf.copy()

    dec_unflagged = filled[
        (filled["ROUTE_ID"].str.endswith("DEC", na=False))
        & (~filled["SEC_EVAC"].fillna(False).astype(bool))
    ]

    if dec_unflagged.empty:
        LOGGER.info("Evac direction mirror: no unflagged DEC segments found")
        return filled

    inc_flagged = filled[
        (filled["ROUTE_ID"].str.endswith("INC", na=False))
        & (filled["SEC_EVAC"].fillna(False).astype(bool))
    ]

    if inc_flagged.empty:
        LOGGER.info("Evac direction mirror: no flagged INC segments found")
        return filled

    # Build lookup: route_base -> [(from_mp, to_mp, idx), ...]
    inc_lookup: dict[str, list[tuple[float, float, int]]] = {}
    for idx, row in inc_flagged.iterrows():
        route_base = str(row["ROUTE_ID"])[:-3]
        from_mp = float(row["FROM_MILEPOINT"]) if pd.notna(row["FROM_MILEPOINT"]) else 0.0
        to_mp = float(row["TO_MILEPOINT"]) if pd.notna(row["TO_MILEPOINT"]) else 0.0
        inc_lookup.setdefault(route_base, []).append((from_mp, to_mp, idx))

    evac_cols = [
        "SEC_EVAC", "SEC_EVAC_CONTRAFLOW", "SEC_EVAC_ROUTE_NAME",
        "SEC_EVAC_SOURCE", "SEC_EVAC_MATCH_METHOD",
        "SEC_EVAC_OVERLAP_M", "SEC_EVAC_OVERLAP_RATIO", "SEC_EVAC_MATCH_PORTION",
    ]
    mirror_count = 0
    for dec_idx, dec_row in dec_unflagged.iterrows():
        route_base = str(dec_row["ROUTE_ID"])[:-3]
        candidates = inc_lookup.get(route_base, [])
        if not candidates:
            continue
        dec_from = float(dec_row["FROM_MILEPOINT"]) if pd.notna(dec_row["FROM_MILEPOINT"]) else 0.0
        dec_to = float(dec_row["TO_MILEPOINT"]) if pd.notna(dec_row["TO_MILEPOINT"]) else 0.0

        best_overlap = -1.0
        best_inc_idx = None
        for inc_from, inc_to, inc_idx in candidates:
            overlap = min(dec_to, inc_to) - max(dec_from, inc_from)
            if overlap > best_overlap:
                best_overlap = overlap
                best_inc_idx = inc_idx

        if best_inc_idx is None or best_overlap < -MILEPOINT_TOLERANCE:
            continue

        inc_row = filled.loc[best_inc_idx]
        for col in evac_cols:
            if col in filled.columns and col in inc_row.index:
                filled.at[dec_idx, col] = inc_row[col]
        mirror_count += 1

    LOGGER.info("Evac direction mirror: filled %d DEC segments from INC partners", mirror_count)
    return filled


def write_evacuation_summary(gdf: pd.DataFrame) -> None:
    """Write evacuation enrichment summary report."""
    reports_dir = PROJECT_ROOT / "02-Data-Staging" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    evac_count = int(gdf["SEC_EVAC"].sum()) if "SEC_EVAC" in gdf.columns else 0
    contra_count = int(gdf["SEC_EVAC_CONTRAFLOW"].sum()) if "SEC_EVAC_CONTRAFLOW" in gdf.columns else 0

    route_names: dict[str, int] = {}
    if "SEC_EVAC_ROUTE_NAME" in gdf.columns:
        named = gdf.loc[gdf["SEC_EVAC_ROUTE_NAME"].notna(), "SEC_EVAC_ROUTE_NAME"]
        for val in named:
            for name in str(val).split("; "):
                name = name.strip()
                if name:
                    route_names[name] = route_names.get(name, 0) + 1

    summary = {
        "segment_count": int(len(gdf)),
        "evacuation_segments": evac_count,
        "contraflow_segments": contra_count,
        "evacuation_route_names": dict(sorted(route_names.items(), key=lambda x: -x[1])),
    }

    output_path = reports_dir / "evacuation_enrichment_summary.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote evacuation enrichment summary to %s", output_path)
