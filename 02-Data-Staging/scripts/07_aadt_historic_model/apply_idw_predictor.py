"""Apply IDW predictor to produce AADT_2021 for all segments.

Reads k-NN links for 2021, joins to station AADTs, runs IDW prediction,
and writes results to the segments table.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from idw_predictor import CUTOFF_M, predict_idw

logger = logging.getLogger(__name__)

DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
DOCS_PATH = _SCRIPTS.parents[1] / "docs"

YEAR = 2021
COL_MAP = {
    "AADT_MODELED": "AADT_2021_MODELED",
    "AADT_NEIGHBOR_MIN": "AADT_2021_NEIGHBOR_MIN",
    "AADT_NEIGHBOR_MAX": "AADT_2021_NEIGHBOR_MAX",
    "AADT_CONFIDENCE": "AADT_2021_CONFIDENCE",
    "AADT_SOURCE": "AADT_2021_SOURCE",
    "AADT_NEAREST_STATION_DIST_M": "AADT_2021_NEAREST_STATION_DIST_M",
    "AADT_NEAREST_STATION_TC": "AADT_2021_NEAREST_STATION_TC",
    "AADT_N_STATIONS_USED": "AADT_2021_N_STATIONS_USED",
}

COL_TYPES = {
    "AADT_2021_MODELED": "INTEGER",
    "AADT_2021_NEIGHBOR_MIN": "INTEGER",
    "AADT_2021_NEIGHBOR_MAX": "INTEGER",
    "AADT_2021_CONFIDENCE": "TEXT",
    "AADT_2021_SOURCE": "TEXT",
    "AADT_2021_NEAREST_STATION_DIST_M": "REAL",
    "AADT_2021_NEAREST_STATION_TC": "TEXT",
    "AADT_2021_N_STATIONS_USED": "INTEGER",
}


def _write_idw_to_segments(conn: sqlite3.Connection, result: pd.DataFrame, col_map: dict, col_types: dict) -> None:
    """Write IDW results to the segments table using year-specific column names."""
    result = result.rename(columns=col_map)

    existing = {r[1] for r in conn.execute("PRAGMA table_info(segments)").fetchall()}
    for col_name, col_type in col_types.items():
        if col_name not in existing:
            conn.execute(f"ALTER TABLE segments ADD COLUMN {col_name} {col_type}")

    for col_name in col_types:
        conn.execute(f"UPDATE segments SET {col_name} = NULL")

    for _, row in result.iterrows():
        uid = row["unique_id"]
        sets = []
        params = []
        for col_name in col_types:
            val = row.get(col_name)
            if pd.notna(val):
                sets.append(f"{col_name} = ?")
                params.append(val if not isinstance(val, (np.integer, np.floating)) else val.item())
            else:
                sets.append(f"{col_name} = NULL")
        if sets:
            params.append(uid)
            conn.execute(f"UPDATE segments SET {', '.join(sets)} WHERE unique_id = ?", params)

    conn.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    logger.info("Loading %d k-NN links...", YEAR)
    knn = pd.read_sql(
        f"SELECT unique_id, k_rank, nearest_tc_number, station_distance_m, same_route_flag "
        f"FROM segment_station_link_knn WHERE year = {YEAR}",
        conn,
    )
    logger.info("  %d k-NN rows", len(knn))

    logger.info("Loading %d stations...", YEAR)
    stations = pd.read_sql(
        f"SELECT tc_number, aadt FROM historic_stations WHERE year = {YEAR}",
        conn,
    )
    stations = stations.drop_duplicates(subset=["tc_number"], keep="first")
    logger.info("  %d unique stations", len(stations))

    logger.info("Running IDW prediction...")
    result = predict_idw(knn, stations)
    logger.info("  %d segments processed", len(result))

    if len(result) == 0:
        logger.warning("  No segments processed — empty result.")
        conn.close()
        return

    predicted = result["AADT_MODELED"].notna().sum()
    null = result["AADT_MODELED"].isna().sum()
    logger.info("  Predicted: %d (%.1f%%)", predicted, predicted / len(result) * 100)
    logger.info("  NULL (beyond cutoff): %d (%.1f%%)", null, null / len(result) * 100)

    high = (result["AADT_CONFIDENCE"] == "high").sum()
    medium = (result["AADT_CONFIDENCE"] == "medium").sum()
    none_conf = (result["AADT_CONFIDENCE"] == "none").sum()
    logger.info("  High: %d, Medium: %d, None: %d", high, medium, none_conf)

    logger.info("Writing columns to segments table...")
    _write_idw_to_segments(conn, result, COL_MAP, COL_TYPES)

    verify = conn.execute("SELECT COUNT(*) FROM segments WHERE AADT_2021_MODELED IS NOT NULL").fetchone()[0]
    logger.info("  Verified: %d segments with AADT_2021_MODELED", verify)

    seg_count = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
    logger.info("  Total segments: %d (unchanged)", seg_count)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
