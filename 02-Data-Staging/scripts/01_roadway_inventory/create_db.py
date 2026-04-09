"""Create the SQLite database and verify the GeoPackage exists.

Reads the normalized CSV, creates roadway_inventory.db with a `segments` table
(tabular only), builds indexes for fast lookups, writes a load_summary
metadata table, and verifies geometry is already available in base_network.gpkg.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TABLES_DIR = PROJECT_ROOT / "02-Data-Staging" / "tables"
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"
SQLITE_TEMP_DIR = PROJECT_ROOT / ".tmp" / "sqlite_runtime"

INDEX_COLUMNS = [
    "ROUTE_ID",
    "GDOT_District",
    "COUNTY_ID",
    "F_SYSTEM",
    "SYSTEM_CODE",
    "unique_id",
]


def configure_local_sqlite_temp() -> None:
    """Force SQLite temp files into the workspace on Windows."""
    SQLITE_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    sqlite_temp = str(SQLITE_TEMP_DIR.resolve())
    os.environ["TMP"] = sqlite_temp
    os.environ["TEMP"] = sqlite_temp
    os.environ["TMPDIR"] = sqlite_temp


def find_staged_table_csv() -> Path:
    """Locate the normalized roadway inventory CSV."""
    csv_path = TABLES_DIR / "roadway_inventory_cleaned.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Normalized CSV not found at {csv_path}. Run normalize.py first."
        )
    return csv_path


def find_gpkg() -> Path:
    """Locate the base_network GeoPackage."""
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    if not gpkg_path.exists():
        raise FileNotFoundError(
            f"GeoPackage not found at {gpkg_path}. Run normalize.py first."
        )
    return gpkg_path


def create_segments_table(db_path: Path, df: pd.DataFrame) -> None:
    """Create the segments table and populate it from the DataFrame."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA journal_mode=MEMORY")
        conn.execute("PRAGMA synchronous=OFF")
        df.to_sql("segments", conn, if_exists="replace", index=False)
        logger.info("Created 'segments' table with %d rows", len(df))

        # Create indexes on key columns
        for col in INDEX_COLUMNS:
            if col in df.columns:
                idx_name = f"idx_segments_{col.lower()}"
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON segments ({col})"
                )
                logger.info("  Created index: %s", idx_name)
            else:
                logger.warning("  Column %s not found, skipping index", col)

        # Create composite index for common query patterns
        available_composite = [c for c in ["GDOT_District", "SYSTEM_CODE"] if c in df.columns]
        if len(available_composite) == 2:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_segments_district_system "
                "ON segments (GDOT_District, SYSTEM_CODE)"
            )

        conn.commit()
    finally:
        conn.close()


def create_load_summary(db_path: Path, df: pd.DataFrame) -> None:
    """Create a load_summary metadata table."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA journal_mode=MEMORY")
        conn.execute("PRAGMA synchronous=OFF")
        summary = {
            "load_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": json.dumps(list(df.columns)),
        }

        # Add per-column null counts
        null_counts = df.isnull().sum()
        summary["null_count_json"] = json.dumps(
            {col: int(cnt) for col, cnt in null_counts.items() if cnt > 0}
        )

        # District distribution if available
        if "GDOT_District" in df.columns:
            dist_counts = df["GDOT_District"].value_counts().to_dict()
            summary["district_distribution"] = json.dumps(
                {str(k): int(v) for k, v in dist_counts.items()}
            )

        summary_df = pd.DataFrame([summary])
        summary_df.to_sql("load_summary", conn, if_exists="replace", index=False)
        logger.info("Created 'load_summary' table")

        conn.commit()
    finally:
        conn.close()


def main() -> None:
    """Run the database creation workflow."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    configure_local_sqlite_temp()

    csv_path = find_staged_table_csv()
    logger.info("Reading normalized CSV: %s", csv_path)
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception:
        logger.info("Retrying CSV read with default memory settings")
        df = pd.read_csv(csv_path)
    logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    # Create database
    DB_DIR.mkdir(parents=True, exist_ok=True)
    db_path = DB_DIR / "roadway_inventory.db"
    logger.info("Creating database: %s", db_path)

    create_segments_table(db_path, df)
    create_load_summary(db_path, df)

    # Verify GeoPackage exists (already created by normalize.py)
    gpkg_path = find_gpkg()
    logger.info("Verified GeoPackage exists: %s", gpkg_path)

    # Log summary stats
    conn = sqlite3.connect(db_path)
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
        logger.info("Database ready: %d segments, indexes on %s", row_count, INDEX_COLUMNS)
    finally:
        conn.close()

    logger.info("Database creation complete.")


if __name__ == "__main__":
    main()
