"""Apply IDW predictor to produce per-year AADT + truck AADT for all segments.

Reads k-NN links per year, joins to station AADTs, runs IDW prediction,
and writes results to the segments table. Supports any year range.
"""

from __future__ import annotations

import argparse
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

DEFAULT_YEARS = list(range(2015, 2025))


def _col_map(year: int) -> dict[str, str]:
    return {
        "AADT_MODELED": f"AADT_{year}_MODELED",
        "AADT_NEIGHBOR_MIN": f"AADT_{year}_NEIGHBOR_MIN",
        "AADT_NEIGHBOR_MAX": f"AADT_{year}_NEIGHBOR_MAX",
        "AADT_CONFIDENCE": f"AADT_{year}_CONFIDENCE",
        "AADT_SOURCE": f"AADT_{year}_SOURCE",
        "AADT_NEAREST_STATION_DIST_M": f"AADT_{year}_NEAREST_STATION_DIST_M",
        "AADT_NEAREST_STATION_TC": f"AADT_{year}_NEAREST_STATION_TC",
        "AADT_N_STATIONS_USED": f"AADT_{year}_N_STATIONS_USED",
    }


def _col_types(year: int) -> dict[str, str]:
    return {
        f"AADT_{year}_MODELED": "INTEGER",
        f"AADT_{year}_NEIGHBOR_MIN": "INTEGER",
        f"AADT_{year}_NEIGHBOR_MAX": "INTEGER",
        f"AADT_{year}_CONFIDENCE": "TEXT",
        f"AADT_{year}_SOURCE": "TEXT",
        f"AADT_{year}_NEAREST_STATION_DIST_M": "REAL",
        f"AADT_{year}_NEAREST_STATION_TC": "TEXT",
        f"AADT_{year}_N_STATIONS_USED": "INTEGER",
    }


def _truck_col_types(year: int) -> dict[str, str]:
    return {f"TRUCK_AADT_{year}_MODELED": "INTEGER"}


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


def _run_year(conn: sqlite3.Connection, year: int) -> None:
    """Run total-AADT IDW for a single year and write to segments."""
    logger.info("--- Year %d: total AADT ---", year)
    knn = pd.read_sql(
        "SELECT unique_id, k_rank, nearest_tc_number, station_distance_m, same_route_flag "
        f"FROM segment_station_link_knn WHERE year = {year}",
        conn,
    )
    if knn.empty:
        logger.warning("  No k-NN rows for year %d — skipping.", year)
        return

    stations = pd.read_sql(
        f"SELECT tc_number, aadt FROM historic_stations WHERE year = {year}",
        conn,
    )
    stations = stations.drop_duplicates(subset=["tc_number"], keep="first")

    result = predict_idw(knn, stations)
    if result.empty:
        return

    predicted = result["AADT_MODELED"].notna().sum()
    logger.info("  Predicted: %d / %d segments", predicted, len(result))

    col_map = _col_map(year)
    col_types = _col_types(year)
    _write_idw_to_segments(conn, result, col_map, col_types)


def _run_truck_year(conn: sqlite3.Connection, year: int) -> None:
    """Run truck-AADT IDW for a single year and write to segments."""
    logger.info("--- Year %d: truck AADT ---", year)
    knn = pd.read_sql(
        "SELECT unique_id, k_rank, nearest_tc_number, station_distance_m, same_route_flag "
        f"FROM segment_station_link_knn WHERE year = {year}",
        conn,
    )
    if knn.empty:
        return

    stations = pd.read_sql(
        f"SELECT tc_number, "
        f"COALESCE(single_unit_aadt, 0) + COALESCE(combo_unit_aadt, 0) AS aadt "
        f"FROM historic_stations WHERE year = {year} "
        f"AND (single_unit_aadt IS NOT NULL OR combo_unit_aadt IS NOT NULL)",
        conn,
    )
    stations = stations.drop_duplicates(subset=["tc_number"], keep="first")

    if stations.empty:
        logger.warning("  No truck AADT data for year %d — skipping.", year)
        return

    result = predict_idw(knn, stations)
    if result.empty:
        return

    predicted = result["AADT_MODELED"].notna().sum()
    logger.info("  Truck predicted: %d / %d segments", predicted, len(result))

    truck_result = result[["unique_id", "AADT_MODELED"]].rename(
        columns={"AADT_MODELED": f"TRUCK_AADT_{year}_MODELED"}
    )
    truck_col_types = _truck_col_types(year)

    existing = {r[1] for r in conn.execute("PRAGMA table_info(segments)").fetchall()}
    for col_name, col_type in truck_col_types.items():
        if col_name not in existing:
            conn.execute(f"ALTER TABLE segments ADD COLUMN {col_name} {col_type}")

    for col_name in truck_col_types:
        conn.execute(f"UPDATE segments SET {col_name} = NULL")

    for _, row in truck_result.iterrows():
        uid = row["unique_id"]
        val = row[f"TRUCK_AADT_{year}_MODELED"]
        if pd.notna(val):
            v = int(val) if isinstance(val, (np.integer, np.floating)) else val
            conn.execute(
                f"UPDATE segments SET TRUCK_AADT_{year}_MODELED = ? WHERE unique_id = ?",
                (v, uid),
            )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years", nargs="+", type=int, default=DEFAULT_YEARS,
    )
    parser.add_argument("--skip-truck", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    for year in args.years:
        _run_year(conn, year)
        if not args.skip_truck:
            _run_truck_year(conn, year)

    seg_count = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
    logger.info("Total segments: %d (unchanged)", seg_count)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
