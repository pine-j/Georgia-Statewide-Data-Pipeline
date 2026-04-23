"""FC 6-7 gap-fill for 2022/2023 using IDW.

Fills AADT_{2022,2023}_LOCAL_FILL on FC 6-7 segments where HPMS is NULL,
using the same IDW approach as the 2021 predictor.
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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    fc67_null = {}
    for year in [2022, 2023]:
        fc67_null[year] = pd.read_sql(
            f"SELECT unique_id FROM segments "
            f"WHERE FUNCTIONAL_CLASS IN (6, 7) AND AADT_{year}_HPMS IS NULL",
            conn,
        )["unique_id"].tolist()
        logger.info("%d: %d FC 6-7 segments with NULL HPMS", year, len(fc67_null[year]))

    for year in [2022, 2023]:
        logger.info("\nProcessing %d...", year)
        target_uids = set(fc67_null[year])

        if not target_uids:
            logger.info("  No target segments, skipping.")
            continue

        knn = pd.read_sql(
            f"SELECT unique_id, k_rank, nearest_tc_number, station_distance_m, same_route_flag "
            f"FROM segment_station_link_knn WHERE year = {year}",
            conn,
        )
        knn = knn[knn["unique_id"].isin(target_uids)]
        logger.info("  %d k-NN rows for target segments", len(knn))

        stations = pd.read_sql(
            f"SELECT tc_number, aadt FROM historic_stations WHERE year = {year}",
            conn,
        )
        stations = stations.drop_duplicates(subset=["tc_number"], keep="first")
        logger.info("  %d unique stations", len(stations))

        result = predict_idw(knn, stations)
        predicted = result["AADT_MODELED"].notna().sum()
        logger.info("  %d predicted, %d NULL", predicted, len(result) - predicted)

        fill_col = f"AADT_{year}_LOCAL_FILL"
        conf_col = f"AADT_{year}_LOCAL_FILL_CONFIDENCE"

        for col_name, col_type in [(fill_col, "INTEGER"), (conf_col, "TEXT")]:
            existing = {r[1] for r in conn.execute("PRAGMA table_info(segments)").fetchall()}
            if col_name not in existing:
                conn.execute(f"ALTER TABLE segments ADD COLUMN {col_name} {col_type}")

        conn.execute(f"UPDATE segments SET {fill_col} = NULL, {conf_col} = NULL")

        for _, row in result.iterrows():
            if pd.notna(row["AADT_MODELED"]):
                conn.execute(
                    f"UPDATE segments SET {fill_col} = ?, {conf_col} = ? WHERE unique_id = ?",
                    (int(row["AADT_MODELED"]), row["AADT_CONFIDENCE"], row["unique_id"]),
                )

        conn.commit()

        filled = conn.execute(f"SELECT COUNT(*) FROM segments WHERE {fill_col} IS NOT NULL").fetchone()[0]
        logger.info("  %s: %d rows written", fill_col, filled)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
