"""Flag roadway segments within 10-mile NRC Emergency Planning Zones.

Georgia has two nuclear power plants:
  - Plant Vogtle (Burke County): 33.1417°N, 81.7586°W — 4 reactors
  - Plant Hatch  (Appling County): 31.9342°N, 82.3444°W — 2 reactors

The NRC standard plume-exposure EPZ is a 10-mile (~16,093 m) radius.
All state-system route segments whose geometry intersects the EPZ buffer
are flagged IS_NUCLEAR_EPZ_ROUTE = True.
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
EPZ_GEOJSON_PATH = (
    PROJECT_ROOT / "01-Raw-Data" / "connectivity" / "nuclear_epz" / "epz_buffers.geojson"
)

PLANTS = [
    {"name": "Plant Vogtle", "lat": 33.1417, "lon": -81.7586, "county": "Burke"},
    {"name": "Plant Hatch", "lat": 31.9342, "lon": -82.3444, "county": "Appling"},
]

EPZ_RADIUS_MILES = 10
EPZ_RADIUS_METERS = EPZ_RADIUS_MILES * 1609.344  # 16,093.44 m

TARGET_CRS = "EPSG:32617"
WGS84 = "EPSG:4326"


def build_epz_buffers() -> gpd.GeoDataFrame:
    """Create 10-mile EPZ buffer polygons around each nuclear plant."""
    points = gpd.GeoDataFrame(
        PLANTS,
        geometry=[Point(p["lon"], p["lat"]) for p in PLANTS],
        crs=WGS84,
    )
    projected = points.to_crs(TARGET_CRS)
    projected["geometry"] = projected.geometry.buffer(EPZ_RADIUS_METERS)
    LOGGER.info(
        "Built EPZ buffers: %d plants, %.1f mile radius (%.0f m)",
        len(projected), EPZ_RADIUS_MILES, EPZ_RADIUS_METERS,
    )
    return projected


def write_epz_buffers_geojson(
    epz: gpd.GeoDataFrame,
    output_path: Path = EPZ_GEOJSON_PATH,
) -> Path:
    """Write EPZ buffers to GeoJSON in WGS84 for external inspection."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    epz.to_crs(WGS84).to_file(output_path, driver="GeoJSON")
    LOGGER.info("Saved EPZ buffer geometries to %s", output_path)
    return output_path


def apply_nuclear_epz_enrichment(
    gdf: gpd.GeoDataFrame,
    *,
    write_buffers: bool = False,
) -> gpd.GeoDataFrame:
    """Flag segments within nuclear plant EPZs.

    Adds IS_NUCLEAR_EPZ_ROUTE (bool) and NUCLEAR_EPZ_PLANT (plant name).
    Only flags state-system segments (not CR/CS county/city streets).
    """
    enriched = gdf.copy()
    enriched["IS_NUCLEAR_EPZ_ROUTE"] = False
    enriched["NUCLEAR_EPZ_PLANT"] = None

    epz = build_epz_buffers()

    seg_crs = enriched.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot flag nuclear EPZ routes")
        return enriched

    if str(epz.crs) != str(seg_crs):
        epz = epz.to_crs(seg_crs)

    state_system_mask = pd.Series(True, index=enriched.index)
    if "ROUTE_TYPE_GDOT" in enriched.columns:
        state_system_mask = ~enriched["ROUTE_TYPE_GDOT"].isin({"CR", "CS"})

    state_segments = enriched.loc[state_system_mask]
    if state_segments.empty:
        LOGGER.info("Nuclear EPZ: no state-system segments to check")
        return enriched

    total_flagged = 0
    for _, plant_row in epz.iterrows():
        plant_name = plant_row["name"]
        buffer_geom = plant_row.geometry

        candidates = state_segments[state_segments.geometry.intersects(buffer_geom)]
        for idx in candidates.index:
            enriched.at[idx, "IS_NUCLEAR_EPZ_ROUTE"] = True
            existing = enriched.at[idx, "NUCLEAR_EPZ_PLANT"]
            if existing and pd.notna(existing):
                enriched.at[idx, "NUCLEAR_EPZ_PLANT"] = f"{existing}; {plant_name}"
            else:
                enriched.at[idx, "NUCLEAR_EPZ_PLANT"] = plant_name

        LOGGER.info(
            "Nuclear EPZ %s: %d state-system segments flagged",
            plant_name, len(candidates),
        )
        total_flagged += len(candidates)

    unique_flagged = int(enriched["IS_NUCLEAR_EPZ_ROUTE"].sum())
    LOGGER.info(
        "Nuclear EPZ enrichment complete: %d segments flagged "
        "(%d total hits across %d plants)",
        unique_flagged, total_flagged, len(PLANTS),
    )

    if write_buffers:
        write_epz_buffers_geojson(epz)

    return enriched
