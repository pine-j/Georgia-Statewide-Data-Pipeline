"""Create a SQLite database (socioeconomic.db) from normalized demographic data.

Reads normalized CSVs and GeoPackages from ``02-Data-Staging/tables/demographics/``
and writes them into a single SQLite database with indexed FIPS columns for
efficient querying.

Tables created:
- decennial_blocks
- acs_block_groups
- lodes_wac
- lodes_rac
- economic_census
- opb_projections
- opportunity_zones
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
CLEAN_DIR = REPO_ROOT / "02-Data-Staging" / "tables" / "demographics"
DB_DIR = REPO_ROOT / "03-Processed-Data" / "demographics"

# Map of table name -> (source file, FIPS index columns)
TABLE_SPEC: dict[str, tuple[str, list[str]]] = {
    "decennial_blocks": ("decennial_blocks.csv", ["GEOID", "COUNTY_FIPS"]),
    "acs_block_groups": ("acs_block_groups.csv", ["GEOID", "COUNTY_FIPS"]),
    "lodes_wac": ("lodes_wac.csv", ["w_geocode", "COUNTY_FIPS", "BLOCK_GROUP_GEOID"]),
    "lodes_rac": ("lodes_rac.csv", ["h_geocode", "COUNTY_FIPS", "BLOCK_GROUP_GEOID"]),
    "economic_census": ("economic_census.csv", ["COUNTY_FIPS"]),
    "opb_projections": ("opb_projections.csv", []),
    "opportunity_zones": ("opportunity_zones.csv", ["TRACT_GEOID", "COUNTY_FIPS"]),
}


def _load_source(filename: str) -> pd.DataFrame | None:
    """Load a normalized CSV or GeoPackage (dropping geometry for SQLite)."""
    csv_path = CLEAN_DIR / filename
    gpkg_path = csv_path.with_suffix(".gpkg")

    if csv_path.exists():
        return pd.read_csv(csv_path, dtype=str, low_memory=False)

    if gpkg_path.exists():
        try:
            import geopandas as gpd
            gdf = gpd.read_file(gpkg_path)
            return pd.DataFrame(gdf.drop(columns="geometry", errors="ignore"))
        except Exception:
            logger.warning("Could not read %s as GeoPackage", gpkg_path)
            return None

    logger.warning("Source not found: %s", csv_path)
    return None


def create_database() -> Path:
    """Create socioeconomic.db from normalized demographic files."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    db_path = DB_DIR / "socioeconomic.db"

    # Remove existing database to rebuild cleanly
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    tables_created = 0

    for table_name, (filename, index_cols) in TABLE_SPEC.items():
        df = _load_source(filename)
        if df is None or df.empty:
            logger.warning("Skipping %s — no data", table_name)
            continue

        df.to_sql(table_name, conn, index=False, if_exists="replace")
        logger.info("Created table %s with %d rows", table_name, len(df))

        # Create indexes on FIPS columns
        for col in index_cols:
            if col in df.columns:
                idx_name = f"idx_{table_name}_{col}"
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({col})"
                )
                logger.info("  Index %s created", idx_name)

        tables_created += 1

    conn.execute("ANALYZE")
    conn.close()

    logger.info(
        "Database created at %s with %d tables", db_path, tables_created
    )
    return db_path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    create_database()


if __name__ == "__main__":
    main()
