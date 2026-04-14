"""Enrich Georgia roadway segments with hurricane evacuation route flags.

Downloads are cached under 02-Data-Staging/spatial/.

Source layers (GDOT EOC Response):
- Layer 7: GDOT Hurricane Evacuation Routes (268 polylines)
- Layer 8: GDOT Hurricane Evacuation Routes - Contraflow Route (12 polylines)

Adds columns:
- SEC_EVAC: Boolean flag — True when the segment overlaps an evacuation route
- SEC_EVAC_CONTRAFLOW: Boolean flag — True when the segment overlaps a contraflow route
- SEC_EVAC_ROUTE_NAME: Evacuation route name(s) matched to the segment
- SEC_EVAC_SOURCE: Source attribution for the evacuation flag
"""

from __future__ import annotations

import json
import logging
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
]

# Minimum overlap length in meters to count as a match.
# Set at 200 m based on validation against the staged network:
#   - Local/Other segments have median overlap of only 32 m (intersection crossings)
#   - State-system routes have median overlap of 190-441 m (genuine collinear overlap)
#   - 200 m eliminates 97.6% of Local/Other false positives while keeping all
#     meaningful Interstate/US/State Route overlaps
MIN_OVERLAP_M = 200.0

# Buffer distance in meters applied to evacuation route polylines before
# measuring overlap.  Evacuation routes and roadway segments are digitized
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


def _spatial_overlay(
    segments: gpd.GeoDataFrame,
    overlay: gpd.GeoDataFrame,
    name_field: str | None,
) -> dict[int, list[str]]:
    """Return segment indices that overlap the overlay, with route names.

    Reprojects the overlay to match the segments' CRS, buffers the overlay
    polylines into corridor polygons (ROUTE_BUFFER_M), then spatial-joins
    and measures how much of each road segment falls inside the corridor.
    The buffer is necessary because the evacuation routes and roadway
    segments are digitized from different source geometries — without it,
    polyline-to-polyline intersection returns zero-length point contacts.
    """
    if overlay.empty:
        return {}

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot run spatial overlay")
        return {}

    # Reproject overlay to match segments
    if overlay.crs != seg_crs:
        overlay = overlay.to_crs(seg_crs)

    # Buffer routes into corridor polygons for measurable intersection
    overlay_buffered = overlay.copy()
    overlay_buffered["geometry"] = overlay_buffered.geometry.buffer(ROUTE_BUFFER_M)

    LOGGER.info(
        "Running evacuation spatial join: %d segments x %d overlay features "
        "(buffered %d m)",
        len(segments), len(overlay), ROUTE_BUFFER_M,
    )

    name_cols = [name_field] if name_field and name_field in overlay.columns else []
    join_cols = ["geometry"] + name_cols
    # Carry name fields from the original overlay onto the buffered version
    for col in name_cols:
        overlay_buffered[col] = overlay[col].values

    joined = gpd.sjoin(
        segments[["geometry"]],
        overlay_buffered[join_cols],
        how="inner",
        predicate="intersects",
    )

    if joined.empty:
        LOGGER.info("Spatial join produced no matches")
        return {}

    # Measure how much of each road segment falls inside the buffered corridor
    results: dict[int, list[str]] = {}
    buffered_geoms = overlay_buffered.geometry
    for seg_idx, group in joined.groupby(joined.index):
        seg_geom = segments.loc[seg_idx, "geometry"]
        names: list[str] = []
        has_valid_overlap = False
        for _, row in group.iterrows():
            corridor = buffered_geoms.iloc[row["index_right"]]
            try:
                overlap_len = seg_geom.intersection(corridor).length
            except Exception:
                overlap_len = 0.0
            if overlap_len >= MIN_OVERLAP_M:
                has_valid_overlap = True
                if name_field and name_field in row.index:
                    val = row[name_field]
                    if pd.notna(val) and str(val).strip():
                        names.append(str(val).strip())
        if has_valid_overlap:
            results[seg_idx] = names

    LOGGER.info("Spatial overlay matched %d segments", len(results))
    return results


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

    # --- Evacuation routes ---
    try:
        evac = _load_evac_routes(refresh=refresh)
    except Exception as exc:
        LOGGER.warning("Evacuation route enrichment unavailable: %s", exc)
        return enriched

    evac_matches = _spatial_overlay(enriched, evac, name_field="ROUTE_NAME")
    for idx, names in evac_matches.items():
        enriched.at[idx, "SEC_EVAC"] = True
        enriched.at[idx, "SEC_EVAC_SOURCE"] = "gdot_eoc_hurricane_evacuation"
        if names:
            enriched.at[idx, "SEC_EVAC_ROUTE_NAME"] = "; ".join(sorted(set(names)))

    LOGGER.info("Evacuation route matches: %d segments", len(evac_matches))

    # --- Contraflow routes (optional — failure here does not abort evacuation flags) ---
    contraflow_matches: dict[int, list[str]] = {}
    try:
        contraflow = _load_contraflow_routes(refresh=refresh)
        contraflow_matches = _spatial_overlay(enriched, contraflow, name_field="TITLE")
    except Exception as exc:
        LOGGER.warning("Contraflow route enrichment unavailable: %s", exc)

    for idx, names in contraflow_matches.items():
        enriched.at[idx, "SEC_EVAC_CONTRAFLOW"] = True
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
