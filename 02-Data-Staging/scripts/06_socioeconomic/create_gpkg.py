"""Create a GeoPackage (demographics.gpkg) with spatial layers.

Layers written (all EPSG:32617 — UTM Zone 17N):
- tract_aggregated_blocks - Tract geometry with aggregated 2020 block totals
- block_groups     — ACS 5-Year block groups with socioeconomic attributes
- tracts           — Tract boundaries
- opportunity_zones — QOZ tracts with designation attributes
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = REPO_ROOT / "01-Raw-Data" / "demographics"
CLEAN_DIR = REPO_ROOT / "02-Data-Staging" / "tables" / "demographics"
OUT_DIR = REPO_ROOT / "03-Processed-Data" / "demographics"

TARGET_CRS = "EPSG:32617"
STATE_FIPS = "13"


def _find_shapefile(directory: Path) -> Path | None:
    """Find the first .shp file in a directory tree."""
    shapefiles = list(directory.rglob("*.shp"))
    return shapefiles[0] if shapefiles else None


def _load_tiger_layer(subfolder: str) -> gpd.GeoDataFrame | None:
    """Load a TIGER shapefile from the tiger_shapefiles subfolder."""
    layer_dir = RAW_DIR / "tiger_shapefiles" / subfolder
    if not layer_dir.exists():
        logger.warning("TIGER directory not found: %s", layer_dir)
        return None

    shp = _find_shapefile(layer_dir)
    if shp is None:
        logger.warning("No .shp found in %s", layer_dir)
        return None

    gdf = gpd.read_file(shp)
    gdf = gdf.to_crs(TARGET_CRS)
    return gdf


def create_gpkg() -> Path:
    """Build demographics.gpkg with all spatial layers."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    gpkg_path = OUT_DIR / "demographics.gpkg"

    layers_written = 0

    # --- Block Groups (ACS joined) ---
    acs_gpkg = CLEAN_DIR / "acs_block_groups.gpkg"
    if acs_gpkg.exists():
        bg = gpd.read_file(acs_gpkg)
        bg = bg.to_crs(TARGET_CRS)
        bg.to_file(gpkg_path, layer="block_groups", driver="GPKG")
        logger.info("Wrote block_groups layer: %d features", len(bg))
        layers_written += 1
    else:
        # Fall back to raw TIGER + CSV join
        bg_gdf = _load_tiger_layer("block_groups")
        if bg_gdf is not None:
            bg_gdf.to_file(gpkg_path, layer="block_groups", driver="GPKG")
            logger.info("Wrote block_groups layer (geometry only): %d features", len(bg_gdf))
            layers_written += 1

    # --- Tracts ---
    tracts = _load_tiger_layer("tracts")
    if tracts is not None:
        mode = "a" if layers_written > 0 else "w"
        tracts.to_file(gpkg_path, layer="tracts", driver="GPKG", mode=mode)
        logger.info("Wrote tracts layer: %d features", len(tracts))
        layers_written += 1

    # --- Tract-Aggregated Block Totals ---
    # Blocks don't have a separate TIGER download in our pipeline (too large),
    # so we create a lightweight centroid layer from decennial data if available
    decennial_csv = CLEAN_DIR / "decennial_blocks.csv"
    if decennial_csv.exists() and tracts is not None:
        dec = pd.read_csv(decennial_csv, dtype=str, low_memory=False)
        # Assign blocks to tracts via GEOID prefix match
        dec["TRACT_GEOID"] = dec["GEOID"].str[:11]
        # For spatial representation, use tract geometry as a proxy
        dec_tracts = tracts[["GEOID", "geometry"]].rename(columns={"GEOID": "TRACT_GEOID"})
        dec_merged = dec_tracts.merge(
            dec.groupby("TRACT_GEOID").agg(
                total_pop=("P1_001N", lambda x: pd.to_numeric(x, errors="coerce").sum()),
                total_housing=("H1_001N", lambda x: pd.to_numeric(x, errors="coerce").sum()),
                block_count=("GEOID", "count"),
            ).reset_index(),
            on="TRACT_GEOID",
            how="inner",
        )
        dec_gdf = gpd.GeoDataFrame(dec_merged, crs=TARGET_CRS)
        dec_gdf.to_file(gpkg_path, layer="tract_aggregated_blocks", driver="GPKG", mode="a")
        logger.info("Wrote tract_aggregated_blocks layer: %d features", len(dec_gdf))
        layers_written += 1

    # --- Opportunity Zones ---
    oz_csv = CLEAN_DIR / "opportunity_zones.csv"
    if oz_csv.exists() and tracts is not None:
        oz = pd.read_csv(oz_csv, dtype=str)
        if "TRACT_GEOID" in oz.columns:
            oz_tracts = tracts[tracts["GEOID"].isin(oz["TRACT_GEOID"])].copy()
            oz_tracts = oz_tracts.merge(oz, left_on="GEOID", right_on="TRACT_GEOID", how="left")
            oz_tracts.to_file(gpkg_path, layer="opportunity_zones", driver="GPKG", mode="a")
            logger.info("Wrote opportunity_zones layer: %d features", len(oz_tracts))
            layers_written += 1

    if layers_written == 0:
        logger.warning("No layers written — ensure raw data is downloaded first")
    else:
        logger.info("GeoPackage created at %s with %d layers", gpkg_path, layers_written)

    return gpkg_path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    create_gpkg()


if __name__ == "__main__":
    main()
