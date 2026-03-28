"""Normalize Georgia Roadway Inventory data.

Loads the full GDB, cleans column names, parses RCLINK into components,
builds unique IDs, computes segment geometry length, reprojects to
EPSG:32617, and exports cleaned CSV and GeoPackage.
"""

import json
import logging
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "GA_RDWY_INV"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
CLEANED_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned"

TARGET_CRS = "EPSG:32617"


def find_gdb(raw_dir: Path) -> Path:
    """Locate the .gdb directory under the raw data folder."""
    gdb_dirs = list(raw_dir.rglob("*.gdb"))
    if not gdb_dirs:
        raise FileNotFoundError(
            f"No .gdb directory found under {raw_dir}. Run download.py first."
        )
    return gdb_dirs[0]


def load_config(name: str) -> dict:
    """Load a JSON config file from the config directory."""
    path = CONFIG_DIR / name
    return json.loads(path.read_text())


def clean_column_names(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Standardize column names: uppercase, strip whitespace, replace spaces."""
    gdf.columns = [
        col.strip().upper().replace(" ", "_") if col != "geometry" else col
        for col in gdf.columns
    ]
    return gdf


def parse_rclink(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Parse RCLINK into county_code, route_type, route_number, suffix, direction.

    RCLINK format is typically: CCCTRNNNNSD
    where:
        CCC  = 3-digit county code
        T    = route type (system code)
        R    = reserved / route qualifier
        NNNN = route number
        S    = suffix
        D    = direction (0=both, 1=increasing, 2=decreasing)

    The exact format may vary; this parser handles common patterns.
    """
    if "RCLINK" not in gdf.columns:
        logger.warning("RCLINK column not found; skipping parse")
        return gdf

    def _parse(rclink: str) -> dict:
        if pd.isna(rclink) or not isinstance(rclink, str):
            return {
                "COUNTY_CODE": None,
                "ROUTE_TYPE": None,
                "ROUTE_NUMBER": None,
                "ROUTE_SUFFIX": None,
                "ROUTE_DIRECTION": None,
            }

        rclink = rclink.strip()

        # Try standard format: first 3 chars = county, next 1 = system,
        # then route number, suffix, direction
        match = re.match(
            r"^(\d{3})(\d)(\d?)(\d{4,5})(\w?)(\d?)$", rclink
        )
        if match:
            return {
                "COUNTY_CODE": match.group(1),
                "ROUTE_TYPE": match.group(2),
                "ROUTE_NUMBER": match.group(4),
                "ROUTE_SUFFIX": match.group(5) or None,
                "ROUTE_DIRECTION": match.group(6) or None,
            }

        # Fallback: extract what we can
        county = rclink[:3] if len(rclink) >= 3 else None
        remainder = rclink[3:] if len(rclink) > 3 else ""
        return {
            "COUNTY_CODE": county,
            "ROUTE_TYPE": remainder[:1] if len(remainder) >= 1 else None,
            "ROUTE_NUMBER": remainder[1:] if len(remainder) > 1 else None,
            "ROUTE_SUFFIX": None,
            "ROUTE_DIRECTION": None,
        }

    parsed = gdf["RCLINK"].apply(_parse).apply(pd.Series)
    for col in parsed.columns:
        if col not in gdf.columns:
            gdf[col] = parsed[col]

    return gdf


def build_unique_id(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Build unique_id from ROUTE_ID, FROM_MEASURE, TO_MEASURE."""
    route_col = "ROUTE_ID" if "ROUTE_ID" in gdf.columns else "RCLINK"

    from_col = None
    to_col = None
    for candidate in ["FROM_MEASURE", "FROMMEASURE", "BEG_MP", "FROM_MP"]:
        if candidate in gdf.columns:
            from_col = candidate
            break
    for candidate in ["TO_MEASURE", "TOMEASURE", "END_MP", "TO_MP"]:
        if candidate in gdf.columns:
            to_col = candidate
            break

    if from_col and to_col:
        gdf["unique_id"] = (
            gdf[route_col].astype(str)
            + "_"
            + gdf[from_col].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "NA")
            + "_"
            + gdf[to_col].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "NA")
        )
    else:
        logger.warning(
            "Could not find FROM/TO measure columns. "
            "Using index-based unique_id as fallback."
        )
        gdf["unique_id"] = (
            gdf[route_col].astype(str) + "_" + gdf.index.astype(str)
        )

    return gdf


def compute_segment_length(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Compute segment length in meters from geometry (after reprojection)."""
    if gdf.geometry is not None and not gdf.geometry.is_empty.all():
        gdf["segment_length_m"] = gdf.geometry.length
    else:
        gdf["segment_length_m"] = np.nan
    return gdf


def main() -> None:
    """Run the normalization workflow."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    gdb_path = find_gdb(RAW_DIR)
    logger.info("Loading GDB: %s", gdb_path)

    # Load the primary layer (typically the first/largest)
    gdf = gpd.read_file(gdb_path, engine="pyogrio", use_arrow=True)
    logger.info("Loaded %d rows, %d columns", len(gdf), len(gdf.columns))
    logger.info("Original CRS: %s", gdf.crs)

    # Clean column names
    gdf = clean_column_names(gdf)
    logger.info("Columns after cleaning: %s", list(gdf.columns))

    # Parse RCLINK
    gdf = parse_rclink(gdf)

    # Build unique ID
    gdf = build_unique_id(gdf)

    # Reproject to target CRS
    if gdf.crs is not None:
        gdf = gdf.to_crs(TARGET_CRS)
        logger.info("Reprojected to %s", TARGET_CRS)
    else:
        logger.warning("No CRS set on data; cannot reproject")

    # Compute segment length (after reprojection so units are meters)
    gdf = compute_segment_length(gdf)

    # Export cleaned CSV (no geometry)
    CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = CLEANED_DIR / "roadway_inventory_cleaned.csv"
    gdf.drop(columns=["geometry"], errors="ignore").to_csv(csv_path, index=False)
    logger.info("Wrote cleaned CSV: %s (%d rows)", csv_path, len(gdf))

    # Export GeoPackage
    SPATIAL_DIR.mkdir(parents=True, exist_ok=True)
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    gdf.to_file(gpkg_path, layer="roadway_segments", driver="GPKG", engine="pyogrio")
    logger.info("Wrote GeoPackage: %s", gpkg_path)

    logger.info("Normalization complete.")


if __name__ == "__main__":
    main()
