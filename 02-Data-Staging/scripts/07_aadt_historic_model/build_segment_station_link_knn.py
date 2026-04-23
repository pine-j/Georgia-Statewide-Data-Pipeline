"""Orchestrator: build the segment_station_link_knn table (k=5).

Reads segments from base_network.gpkg and stations from historic_stations,
computes k=5 nearest stations per segment-year, writes to SQLite.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_segment_station_link import load_stations
from segment_station_link_knn import KNN_K, KNN_LINK_COLUMNS, build_knn_link_rows

logger = logging.getLogger(__name__)

GPKG_PATH = _SCRIPTS.parents[1] / "spatial" / "base_network.gpkg"
DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
KNN_TABLE = "segment_station_link_knn"
YEARS = list(range(2015, 2025))


def _load_segment_midpoints() -> pd.DataFrame:
    logger.info("Reading segments layer from %s", GPKG_PATH)
    gdf = gpd.read_file(
        GPKG_PATH, layer="roadway_segments", engine="pyogrio", columns=["unique_id", "ROUTE_ID"]
    )
    mids = gdf.geometry.interpolate(0.5, normalized=True)
    return pd.DataFrame(
        {
            "unique_id": gdf["unique_id"].astype("string").to_numpy(),
            "ROUTE_ID": gdf["ROUTE_ID"].astype("string").to_numpy(),
            "mid_x_m": mids.x.to_numpy(),
            "mid_y_m": mids.y.to_numpy(),
        }
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    logger.info("Loading segment midpoints...")
    segments = _load_segment_midpoints()
    logger.info("  %d segments", len(segments))

    logger.info("Loading stations...")
    stations = load_stations(DB_PATH, YEARS)
    logger.info("  %d station rows", len(stations))

    logger.info("Building k=%d NN link rows...", KNN_K)
    link_df = build_knn_link_rows(segments, stations, years=YEARS, k=KNN_K)
    logger.info("  %d link rows built", len(link_df))

    expected = len(segments) * len(YEARS) * KNN_K
    logger.info("  Expected: %d, Got: %d", expected, len(link_df))

    logger.info("Writing to %s.%s ...", DB_PATH.name, KNN_TABLE)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(f"DROP TABLE IF EXISTS {KNN_TABLE}")
    link_df.to_sql(KNN_TABLE, conn, if_exists="replace", index=False)
    conn.execute(f"CREATE INDEX idx_{KNN_TABLE}_uid_year ON {KNN_TABLE}(unique_id, year)")
    conn.execute(f"CREATE INDEX idx_{KNN_TABLE}_year_rank ON {KNN_TABLE}(year, k_rank)")
    conn.execute(f"CREATE INDEX idx_{KNN_TABLE}_tc ON {KNN_TABLE}(nearest_tc_number)")
    conn.commit()

    total = conn.execute(f"SELECT COUNT(*) FROM {KNN_TABLE}").fetchone()[0]
    logger.info("  %d rows in table", total)

    for year in YEARS:
        yr_cnt = conn.execute(f"SELECT COUNT(*) FROM {KNN_TABLE} WHERE year = {year}").fetchone()[0]
        median_d = conn.execute(
            f"SELECT AVG(station_distance_m) FROM {KNN_TABLE} WHERE year = {year} AND k_rank = 1"
        ).fetchone()[0]
        logger.info("  Year %d: %d rows, k=1 mean dist %.0fm", year, yr_cnt, median_d or 0)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
