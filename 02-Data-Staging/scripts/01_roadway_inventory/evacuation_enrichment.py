"""Enrich Georgia roadway segments with hurricane evacuation route flags.

Downloads are cached under 02-Data-Staging/spatial/.

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
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"

EVAC_ROUTES_GEOJSON = SPATIAL_DIR / "ga_evac_routes.geojson"
CONTRAFLOW_ROUTES_GEOJSON = SPATIAL_DIR / "ga_contraflow_routes.geojson"

EOC_SERVICE_URL = (
    "https://rnhp.dot.ga.gov/hosting/rest/services/EOC/EOC_RESPONSE_LAYERS/MapServer"
)
EVAC_LAYER_URL = f"{EOC_SERVICE_URL}/7"
CONTRAFLOW_LAYER_URL = f"{EOC_SERVICE_URL}/8"

ENRICHMENT_COLUMNS = [
    "SEC_EVAC",
    "SEC_EVAC_CONTRAFLOW",
    "SEC_EVAC_ROUTE_NAME",
    "SEC_EVAC_SOURCE",
    "SEC_EVAC_OVERLAP_M",
    "SEC_EVAC_OVERLAP_RATIO",
]

# Minimum overlap length in meters to count as a match.
# Set at 200 m based on validation against the staged network:
#   - Local/Other segments have median overlap of only 32 m (intersection crossings)
#   - State-system routes have median overlap of 190-441 m (genuine collinear overlap)
#   - 200 m eliminates 97.6% of Local/Other false positives while keeping all
#     meaningful Interstate/US/State Route overlaps
MIN_OVERLAP_M = 200.0
MIN_OVERLAP_RATIO = 0.25
MEGA_SEGMENT_LENGTH_M = 10_000.0
MEGA_SEGMENT_MIN_RATIO = 0.50

# Buffer distance in meters applied to evacuation route polylines before
# measuring overlap. Evacuation routes and roadway segments are digitized
# from different source geometries; a 30 m corridor accounts for positional
# offset so that collinear roads register measurable intersection length
# instead of zero-length point contacts.
ROUTE_BUFFER_M = 30.0


def _download_geojson(url: str, dest: Path) -> None:
    """Download a full layer as GeoJSON from an ArcGIS MapServer."""
    from urllib.request import Request, urlopen

    query_url = (
        f"{url}/query?where=1%3D1&outFields=*"
        f"&f=geojson&returnGeometry=true"
    )
    LOGGER.info("Downloading evacuation layer: %s", query_url)
    req = Request(query_url, headers={"User-Agent": "Georgia-Pipeline-ETL"})
    with urlopen(req, timeout=120) as resp:
        data = resp.read()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    fc = json.loads(data)
    LOGGER.info("Downloaded %d features to %s", len(fc.get("features", [])), dest)


def _load_evac_routes(refresh: bool = False) -> gpd.GeoDataFrame:
    """Load hurricane evacuation routes from local cache or live service."""
    if not EVAC_ROUTES_GEOJSON.exists() or refresh:
        _download_geojson(EVAC_LAYER_URL, EVAC_ROUTES_GEOJSON)

    gdf = gpd.read_file(EVAC_ROUTES_GEOJSON, engine="pyogrio")
    # Drop records with null/empty geometry
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].reset_index(drop=True)
    LOGGER.info("Loaded %d evacuation route features", len(gdf))
    return gdf


def _load_contraflow_routes(refresh: bool = False) -> gpd.GeoDataFrame:
    """Load contraflow routes from local cache or live service."""
    if not CONTRAFLOW_ROUTES_GEOJSON.exists() or refresh:
        _download_geojson(CONTRAFLOW_LAYER_URL, CONTRAFLOW_ROUTES_GEOJSON)

    gdf = gpd.read_file(CONTRAFLOW_ROUTES_GEOJSON, engine="pyogrio")
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].reset_index(drop=True)
    LOGGER.info("Loaded %d contraflow route features", len(gdf))
    return gdf


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


def _overlay_matches(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str | None,
    *,
    overlay_label: str,
    interstate_only: bool = False,
    enforce_route_family: bool = False,
) -> dict[int, dict[str, object]]:
    """Return matched segments with route names and overlap diagnostics."""
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
            len(overlay), ROUTE_BUFFER_M,
        )
    else:
        LOGGER.info(
            "Running %s spatial join: %d segments x %d overlay features "
            "(buffered %.0f m)",
            overlay_label.lower(), len(eligible_segments), len(overlay),
            ROUTE_BUFFER_M,
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

    overlay_buffered = overlay.copy()
    overlay_buffered["geometry"] = overlay_buffered.geometry.buffer(ROUTE_BUFFER_M)

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

        for _, row in group.iterrows():
            if is_mega_segment:
                mega_candidates += 1

            route_name = row[name_field] if name_field and name_field in row.index else None
            if enforce_route_family and pd.notna(route_name):
                expected_family = _parse_expected_family(str(route_name))
                if expected_family is not None:
                    seg_fam_lower = segment_family.lower() if segment_family else ""
                    # State-system segments (Interstate / US Route / State Route)
                    # may carry concurrent designations, so only reject
                    # Local/Other segments that don't belong to the signed
                    # route system.
                    if seg_fam_lower not in _STATE_SYSTEM_FAMILIES:
                        route_family_rejected += 1
                        continue

            corridor = buffered_geoms.iloc[row["index_right"]]
            try:
                overlap_len = float(seg_geom.intersection(corridor).length)
            except Exception:
                overlap_len = 0.0

            overlap_ratio = 0.0
            if segment_length_m > 0:
                overlap_ratio = overlap_len / segment_length_m

            min_ratio = (
                MEGA_SEGMENT_MIN_RATIO if is_mega_segment else MIN_OVERLAP_RATIO
            )
            if overlap_len < MIN_OVERLAP_M or overlap_ratio < min_ratio:
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
        "%d rejected by route-family filter, %d mega-segment candidates "
        "encountered, %d rejected",
        overlay_label, len(results), ratio_rejected, route_family_rejected,
        mega_candidates, mega_rejected,
    )
    return results


def _spatial_overlay(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str | None,
) -> dict[int, dict[str, object]]:
    """Return segment indices that overlap evacuation routes."""
    return _overlay_matches(
        segments,
        overlay,
        name_field,
        overlay_label="Evacuation route",
        enforce_route_family=True,
    )


def _contraflow_overlay(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str | None,
) -> dict[int, dict[str, object]]:
    """Return Interstate segment indices that overlap contraflow routes."""
    return _overlay_matches(
        segments,
        overlay,
        name_field,
        overlay_label="Contraflow route",
        interstate_only=True,
    )


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
    enriched["SEC_EVAC_OVERLAP_M"] = 0.0
    enriched["SEC_EVAC_OVERLAP_RATIO"] = 0.0

    # --- Evacuation routes ---
    try:
        evac = _load_evac_routes(refresh=refresh)
    except Exception as exc:
        LOGGER.warning("Evacuation route enrichment unavailable: %s", exc)
        return enriched

    evac_matches = _spatial_overlay(enriched, evac, name_field="ROUTE_NAME")
    for idx, match in evac_matches.items():
        names = match.get("names", [])
        enriched.at[idx, "SEC_EVAC"] = True
        enriched.at[idx, "SEC_EVAC_SOURCE"] = "gdot_eoc_hurricane_evacuation"
        _update_overlap_diagnostics(
            enriched,
            idx,
            float(match.get("overlap_m", 0.0)),
            float(match.get("overlap_ratio", 0.0)),
        )
        if names:
            enriched.at[idx, "SEC_EVAC_ROUTE_NAME"] = "; ".join(sorted(set(names)))

    LOGGER.info("Evacuation route matches: %d segments", len(evac_matches))

    # --- Contraflow routes (optional - failure here does not abort evacuation flags) ---
    contraflow_matches: dict[int, dict[str, object]] = {}
    try:
        contraflow = _load_contraflow_routes(refresh=refresh)
        contraflow_matches = _contraflow_overlay(
            enriched,
            contraflow,
            name_field="TITLE",
        )
    except Exception as exc:
        LOGGER.warning("Contraflow route enrichment unavailable: %s", exc)

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

    total = int(enriched["SEC_EVAC"].sum())
    contra = int(enriched["SEC_EVAC_CONTRAFLOW"].sum())
    LOGGER.info(
        "Evacuation enrichment complete: %d evacuation segments (%d contraflow)",
        total, contra,
    )

    return enriched


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
