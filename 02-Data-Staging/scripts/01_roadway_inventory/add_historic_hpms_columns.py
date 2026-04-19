"""Add per-year historic HPMS AADT columns to an existing segments DB.

Runs `build_historic_hpms_aadt_column` for one or more historic HPMS years
and writes each result as a new `AADT_{year}_HPMS` column on the segments
table (and refreshes the paired column list in load_summary).

Purely additive: the canonical `AADT`, `AADT_2024*`, and every other
existing column are untouched. Row count is preserved — the builder joins
by `ROUTE_ID` + milepoint overlap, writing NULL where no HPMS match exists.

Typical usage after downloads + normalize have both run:

    python add_historic_hpms_columns.py --year 2020
    python add_historic_hpms_columns.py --year 2022 --year 2023

2024 is a regression-test entrypoint only: running with `--year 2024`
rebuilds the column into `AADT_2024_HPMS_REBUILD` so the caller can diff
it against the existing `AADT_2024_HPMS` to prove the refactor is
byte-identical to the Phase 1 enrichment.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

from hpms_enrichment import build_historic_hpms_aadt_column

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = PROJECT_ROOT / "02-Data-Staging" / "databases" / "roadway_inventory.db"
RENAME_MAP_DIR = (
    PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "scripts" / "hpms_rename_maps"
)


def _load_rename_map(year: int) -> dict[str, str] | None:
    path = RENAME_MAP_DIR / f"{year}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _add_column_if_missing(conn: sqlite3.Connection, column: str) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(segments)").fetchall()}
    if column in cols:
        return
    conn.execute(f"ALTER TABLE segments ADD COLUMN {column} INTEGER")
    LOGGER.info("Added column %s to segments", column)


def _write_column(conn: sqlite3.Connection, column: str, series: pd.Series, unique_ids: list) -> None:
    update_payload = [
        (None if pd.isna(val) else int(val), unique_id)
        for val, unique_id in zip(series.tolist(), unique_ids)
    ]
    conn.executemany(
        f"UPDATE segments SET {column} = ? WHERE unique_id = ?",
        update_payload,
    )
    conn.commit()


def run_year(conn: sqlite3.Connection, year: int, output_column: str) -> dict:
    LOGGER.info("Reading segments from %s", DB_PATH)
    segments = pd.read_sql_query(
        "SELECT unique_id, ROUTE_ID, FROM_MILEPOINT, TO_MILEPOINT FROM segments",
        conn,
    )
    LOGGER.info("Loaded %d segments", len(segments))

    rename_map = _load_rename_map(year)
    if rename_map:
        LOGGER.info("Loaded %d-field rename map for %d", len(rename_map), year)
    else:
        LOGGER.info("No rename map found for %d (expected for 2024)", year)

    series, stats = build_historic_hpms_aadt_column(segments, year, rename_map=rename_map)
    non_null = int(series.notna().sum())
    LOGGER.info(
        "Year %d: %d / %d segments populated (%.2f%%) | hpms rows loaded=%d | deduped=%d",
        year,
        non_null,
        len(series),
        100.0 * non_null / len(series),
        stats["hpms_rows"],
        stats["deduped"],
    )

    _add_column_if_missing(conn, output_column)
    _write_column(conn, output_column, series, segments["unique_id"].tolist())
    LOGGER.info("Wrote column %s", output_column)

    return {
        "year": year,
        "column": output_column,
        "segments_total": int(len(series)),
        "segments_populated": non_null,
        "coverage_pct": round(100.0 * non_null / len(series), 4),
        "hpms_rows_loaded": stats["hpms_rows"],
        "hpms_rows_deduped": stats["deduped"],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add historic HPMS AADT columns to the segments DB.")
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        required=True,
        help="HPMS year to add. Pass --year multiple times for multiple years.",
    )
    parser.add_argument(
        "--regression-2024",
        action="store_true",
        help="When running with --year 2024, write to AADT_2024_HPMS_REBUILD instead of AADT_2024_HPMS so the result can be diffed against the Phase 1 value.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args(argv)
    if not DB_PATH.exists():
        LOGGER.error("DB not found at %s — run normalize.py + create_db.py first", DB_PATH)
        return 2

    results = []
    with sqlite3.connect(DB_PATH) as conn:
        for year in args.year:
            if year == 2024 and args.regression_2024:
                output_column = "AADT_2024_HPMS_REBUILD"
            else:
                output_column = f"AADT_{year}_HPMS"
            results.append(run_year(conn, year, output_column))

    print(json.dumps({"runs": results}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
