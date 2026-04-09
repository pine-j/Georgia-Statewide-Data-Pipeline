"""Georgia Roadway Inventory data loader for RAPTOR pipeline.

Loads Georgia GDOT roadway inventory data from the processed SQLite database
(tabular) and GeoPackage (geometry), applies standard filtering for
RAPTOR analysis, and provides the data as a GeoDataFrame.
"""

import logging
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"

TARGET_CRS = "EPSG:32617"


class RoadwayData:
    """Load and filter Georgia roadway inventory data for RAPTOR analysis.

    Reads tabular data from roadway_inventory.db and geometry from
    base_network.gpkg (or the original GDB as fallback). Filters to
    State Highway Routes (SYSTEM_CODE=1) by default and optionally
    by GDOT district.

    Attributes:
        Roadway_Inventory: GeoDataFrame of filtered roadway segments.
        district_id: Optional district filter (1-7).
    """

    def __init__(self, district_id: int | None = None):
        self.Roadway_Inventory: gpd.GeoDataFrame | None = None
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
            "BASE_ROUTE_NUMBER",
            "ROUTE_SUFFIX",
            "ROUTE_SUFFIX_LABEL",
            "ROUTE_DIRECTION",
            "ROUTE_DIRECTION_LABEL",
            "ROUTE_FAMILY",
            "ROUTE_FAMILY_DETAIL",
            "ROUTE_FAMILY_CONFIDENCE",
            "ROUTE_FAMILY_SOURCE",
            "ROUTE_TYPE_GDOT",
            "ROUTE_TYPE_GDOT_LABEL",
            "HWY_NAME",
            "SIGNED_INTERSTATE_FLAG",
            "SIGNED_US_ROUTE_FLAG",
            "SIGNED_STATE_ROUTE_FLAG",
            "SIGNED_ROUTE_FAMILY_PRIMARY",
            "SIGNED_ROUTE_FAMILY_ALL",
            "SIGNED_ROUTE_VERIFY_SOURCE",
            "SIGNED_ROUTE_VERIFY_METHOD",
            "SIGNED_ROUTE_VERIFY_CONFIDENCE",
            "SIGNED_ROUTE_VERIFY_SCORE",
            "SIGNED_ROUTE_VERIFY_NOTES",
            "NUM_LANES",
            "LANE_WIDTH",
            "SURFACE_WIDTH",
            "SURFACE_TYPE",
            "SURFACE_TYPE_LABEL",
            "MEDIAN_TYPE",
            "MEDIAN_TYPE_LABEL",
            "MEDIAN_WIDTH",
            "SHOULDER_TYPE",
            "SHOULDER_TYPE_LABEL",
            "SHOULDER_WIDTH_L",
            "SHOULDER_WIDTH_R",
            "SHOULDER_WIDTH",
            "OWNERSHIP",
            "OWNERSHIP_LABEL",
            "FACILITY_TYPE",
            "FACILITY_TYPE_LABEL",
            "SPEED_LIMIT",
            "AADT",
            "AADT_2024",
            "AADT_2024_OFFICIAL",
            "AADT_2024_SOURCE",
            "AADT_2024_CONFIDENCE",
            "AADT_2024_FILL_METHOD",
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
            "STRAHNET_LABEL",
            "ACCESS_CONTROL",
            "current_aadt_official_covered",
            "current_aadt_covered",
            "HPMS_ROUTE_NAME",
            "HPMS_ROUTE_NUMBER",
            "HPMS_IRI",
            "HPMS_PSR",
            "HPMS_RUTTING",
            "HPMS_CRACKING_PCT",
            "HPMS_ACCESS_CONTROL",
            "HPMS_TERRAIN_TYPE",
            "VMT",
            "TruckVMT",
            "FUTURE_AADT",
            "FUTURE_AADT_2044",
            "FUTURE_AADT_2044_SOURCE",
            "FUTURE_AADT_2044_CONFIDENCE",
            "FROM_MILEPOINT",
            "TO_MILEPOINT",
            "segment_length_m",
            "segment_length_mi",
            "geometry",
        ]
        # Historical AADT columns (2010-2020) have been removed from the pipeline output.
        # Raw source files are retained in 01-Raw-Data/ for future use if needed.

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
        """Load geometry from GeoPackage or the official road inventory GDB."""
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

        gdb_dirs = sorted(RAW_DIR.rglob("Road_Inventory*.gdb"))
        for gdb_path in gdb_dirs:
            layer_names = [layer_name for layer_name, _ in pyogrio.list_layers(gdb_path)]
            route_layers = [
                layer_name
                for layer_name in layer_names
                if layer_name.startswith("GA_") and layer_name.endswith("_Routes")
            ]
            if not route_layers:
                continue

            route_layer = sorted(route_layers)[0]
            gdf = gpd.read_file(
                gdb_path,
                layer=route_layer,
                engine="pyogrio",
                use_arrow=True,
            )
            logger.info(
                "Loaded geometry from %s layer %s: %d features",
                gdb_path.name,
                route_layer,
                len(gdf),
            )
            return gdf

        raise FileNotFoundError(
            (
                "No geometry source found. Expected "
                f"{gpkg_path} or a Road_Inventory*.gdb containing a GA_*_Routes layer."
            )
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

        self.Roadway_Inventory = gdf
        logger.info("Loaded %d roadway segments for RAPTOR analysis", len(gdf))

    def clear_data(self) -> None:
        """Release roadway data from memory."""
        self.Roadway_Inventory = None
        logger.info("Cleared roadway data")
