"""Georgia Roadway Inventory data loader for RAPTOR pipeline.

Loads Georgia GDOT roadway inventory data from the processed SQLite database
(tabular) and GeoPackage (geometry), applies standard filtering for
RAPTOR analysis, and provides the data as a GeoDataFrame.
"""

import json
import logging
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "GA_RDWY_INV"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"

TARGET_CRS = "EPSG:32617"
HISTORICAL_AADT_YEARS = list(range(2010, 2021))


class RoadwayData:
    """Load and filter Georgia roadway inventory data for RAPTOR analysis.

    Reads tabular data from roadway_inventory.db and geometry from
    base_network.gpkg (or the original GDB as fallback). Filters to
    State Highway Routes (SYSTEM_CODE=1) by default and optionally
    by GDOT district.

    Attributes:
        GA_RDWY_INV: GeoDataFrame of filtered roadway segments.
        district_id: Optional district filter (1-7).
    """

    def __init__(self, district_id: int | None = None):
        self.GA_RDWY_INV: gpd.GeoDataFrame | None = None
        self.district_id = district_id

        # RAPTOR-relevant columns to retain
        self.COLUMNS_TO_KEEP = [
            "unique_id",
            "RCLINK",
            "ROUTE_ID",
            "COUNTY_CODE",
            "COUNTY_NAME",
            "DISTRICT",
            "DISTRICT_NAME",
            "DISTRICT_LABEL",
            "SYSTEM_CODE",
            "SYSTEM_CODE_LABEL",
            "FUNCTION_TYPE",
            "FUNCTION_TYPE_LABEL",
            "FUNCTIONAL_CLASS",
            "FUNCTIONAL_CLASS_LABEL",
            "ROUTE_TYPE",
            "ROUTE_TYPE_LABEL",
            "ROUTE_NUMBER",
            "ROUTE_SUFFIX",
            "ROUTE_DIRECTION",
            "ROUTE_DIRECTION_LABEL",
            "NUM_LANES",
            "SURFACE_WIDTH",
            "SURFACE_TYPE",
            "SURFACE_TYPE_LABEL",
            "MEDIAN_TYPE",
            "MEDIAN_TYPE_LABEL",
            "MEDIAN_WIDTH",
            "SHOULDER_TYPE",
            "SHOULDER_TYPE_LABEL",
            "SHOULDER_WIDTH",
            "FACILITY_TYPE",
            "FACILITY_TYPE_LABEL",
            "SPEED_LIMIT",
            "AADT",
            "AADT_2024",
            "AADT_YEAR",
            "TRUCK_AADT",
            "TRUCK_PCT",
            "K_FACTOR",
            "D_FACTOR",
            "TERRAIN",
            "URBAN_CODE",
            "URBAN_CODE_LABEL",
            "NHS_IND",
            "NHS_IND_LABEL",
            "STRAHNET",
            "ACCESS_CONTROL",
            "current_aadt_covered",
            "historical_aadt_years_available",
            "segment_length_m",
            "geometry",
        ]
        self.COLUMNS_TO_KEEP.extend([f"AADT_{year}" for year in HISTORICAL_AADT_YEARS])
        self.COLUMNS_TO_KEEP.extend([f"TRUCK_AADT_{year}" for year in HISTORICAL_AADT_YEARS])
        self.COLUMNS_TO_KEEP.extend([f"TRUCK_PCT_{year}" for year in HISTORICAL_AADT_YEARS])

    @staticmethod
    def _normalize_numeric_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Normalize numeric filter columns loaded from GeoPackage storage."""
        for column in ["SYSTEM_CODE", "DISTRICT"]:
            if column in gdf.columns:
                gdf[column] = pd.to_numeric(gdf[column], errors="coerce")
        return gdf

    def _load_from_db(self) -> pd.DataFrame:
        """Load tabular data from SQLite database."""
        db_path = DB_DIR / "roadway_inventory.db"
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database not found: {db_path}. "
                "Run the ETL pipeline (normalize.py, create_db.py) first."
            )

        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql("SELECT * FROM segments", conn)
            logger.info("Loaded %d rows from database", len(df))
            return df
        finally:
            conn.close()

    def _load_geometry(self) -> gpd.GeoDataFrame:
        """Load geometry from GeoPackage or original GDB as fallback."""
        gpkg_path = SPATIAL_DIR / "base_network.gpkg"

        if gpkg_path.exists():
            gdf = gpd.read_file(
                gpkg_path,
                layer="roadway_segments",
                engine="pyogrio",
                use_arrow=True,
            )
            logger.info("Loaded geometry from GeoPackage: %d features", len(gdf))
            return gdf

        # Fallback to original GDB
        gdb_dirs = list(RAW_DIR.rglob("*.gdb"))
        if gdb_dirs:
            gdf = gpd.read_file(gdb_dirs[0], engine="pyogrio", use_arrow=True)
            logger.info("Loaded geometry from GDB: %d features", len(gdf))
            return gdf

        raise FileNotFoundError(
            "No geometry source found. Expected base_network.gpkg or .gdb file."
        )

    def load_data(self) -> None:
        """Load and filter roadway inventory data for RAPTOR analysis.

        Merges tabular data from the database with geometry, filters to
        State Highway Routes, applies optional district filter, selects
        RAPTOR-relevant columns, and reprojects to the target CRS.
        """
        # Load geometry source (has both tabular + geometry)
        gdf = self._load_geometry()
        gdf = self._normalize_numeric_columns(gdf)

        logger.info("Source CRS: %s", gdf.crs)

        # Filter to State Highway Routes (SYSTEM_CODE = 1)
        if "SYSTEM_CODE" in gdf.columns:
            original_count = len(gdf)
            gdf = gdf[gdf["SYSTEM_CODE"] == 1].copy()
            logger.info(
                "Filtered to State Highway Routes: %d -> %d segments",
                original_count,
                len(gdf),
            )
        else:
            logger.warning("SYSTEM_CODE column not found; skipping system filter")

        # Filter by district if specified
        if self.district_id is not None and "DISTRICT" in gdf.columns:
            original_count = len(gdf)
            gdf = gdf[gdf["DISTRICT"] == self.district_id].copy()
            logger.info(
                "Filtered to District %d: %d -> %d segments",
                self.district_id,
                original_count,
                len(gdf),
            )

        # Select only RAPTOR-relevant columns that exist in the data
        available_cols = [c for c in self.COLUMNS_TO_KEEP if c in gdf.columns]
        missing_cols = [c for c in self.COLUMNS_TO_KEEP if c not in gdf.columns]
        if missing_cols:
            logger.info("Columns not available in data: %s", missing_cols)
        gdf = gdf[available_cols].copy()

        # Sort by route for consistent ordering
        sort_cols = [c for c in ["ROUTE_ID", "RCLINK"] if c in gdf.columns]
        if sort_cols:
            gdf = gdf.sort_values(by=sort_cols).reset_index(drop=True)

        # Reproject to target CRS
        if gdf.crs is not None:
            gdf = gdf.to_crs(TARGET_CRS)
            logger.info("Reprojected to %s", TARGET_CRS)

        self.GA_RDWY_INV = gdf
        logger.info("Loaded %d roadway segments for RAPTOR analysis", len(gdf))

    def clear_data(self) -> None:
        """Release roadway data from memory."""
        self.GA_RDWY_INV = None
        logger.info("Cleared roadway data")
