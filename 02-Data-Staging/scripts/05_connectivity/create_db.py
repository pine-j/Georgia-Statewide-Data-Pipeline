"""Create a SQLite database from cleaned connectivity datasets.

Reads cleaned GeoJSON files from 02-Data-Staging/cleaned/connectivity/ and
writes them into 02-Data-Staging/databases/connectivity.db with appropriate
indexes for each table.
"""

import logging
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEAN_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned" / "connectivity"
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
DB_PATH = DB_DIR / "connectivity.db"

# Mapping from cleaned file stem to database table name
TABLE_MAP = {
    "priority_routes": "priority_routes",
    "nevi_corridors": "nevi_corridors",
    "alt_fuel_stations": "alt_fuel_stations",
    "airports": "airports",
    "seaports": "seaports",
    "universities": "universities",
    "military_bases": "military_bases",
    "national_parks": "national_parks",
    "rail_facilities": "rail_facilities",
    "freight_generators": "freight_generators",
}

# Indexes to create: (table, column(s))
INDEXES = [
    ("priority_routes", "priority_level"),
    ("alt_fuel_stations", "fuel_type_code"),
    ("airports", "state"),
    ("seaports", "state_post"),
    ("universities", "state"),
]


def _load_cleaned(name: str) -> pd.DataFrame | None:
    """Load a cleaned GeoJSON, drop geometry, and return a DataFrame."""
    path = CLEAN_DIR / f"{name}.geojson"
    if not path.exists():
        log.warning("  %s not found – skipping.", path.name)
        return None
    gdf = gpd.read_file(path)
    # Store WKT geometry so the table is queryable without GIS tools
    gdf["geom_wkt"] = gdf.geometry.to_wkt()
    df = pd.DataFrame(gdf.drop(columns="geometry"))
    return df


def main() -> None:
    """Build connectivity.db."""
    DB_DIR.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
        log.info("Removed existing %s", DB_PATH.name)

    conn = sqlite3.connect(str(DB_PATH))
    log.info("Creating %s ...", DB_PATH)

    for stem, table in TABLE_MAP.items():
        df = _load_cleaned(stem)
        if df is None:
            continue
        df.to_sql(table, conn, index=False, if_exists="replace")
        log.info("  Table %-25s  %d rows", table, len(df))

    # Create indexes
    cur = conn.cursor()
    for table, col in INDEXES:
        idx_name = f"idx_{table}_{col}"
        try:
            cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{col}")')
            log.info("  Index %s", idx_name)
        except sqlite3.OperationalError:
            log.warning("  Could not create index %s (column may not exist).", idx_name)

    conn.commit()
    conn.close()
    log.info("Database complete: %s", DB_PATH)


if __name__ == "__main__":
    main()
