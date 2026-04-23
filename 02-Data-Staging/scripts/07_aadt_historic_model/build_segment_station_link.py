"""Orchestrator pass: build the `segment_station_link` SQLite table.

Plan reference: `aadt-modeling-scoped-2020-2024.md` Step 4.

Inputs:
- `base_network.gpkg` layer `roadway_segments` (geometry in EPSG:32617).
- `historic_stations` (populated by `stage_historic_stations.py`).

Output: `segment_station_link` in the same SQLite DB.
One row per `segments.unique_id` × year ∈ {2020, 2021, 2022, 2023, 2024}.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from segment_station_link import LINK_COLUMNS, build_link_rows  # noqa: E402

logger = logging.getLogger(__name__)

PROJECT_MAIN = Path(__file__).resolve().parents[3]
GPKG_PATH = PROJECT_MAIN / "02-Data-Staging/spatial/base_network.gpkg"
STAGED_DB = PROJECT_MAIN / "02-Data-Staging/databases/roadway_inventory.db"

LINK_TABLE = "segment_station_link"
DEFAULT_YEARS = [2020, 2021, 2022, 2023, 2024]


def load_segment_midpoints() -> pd.DataFrame:
    logger.info("Reading segments layer from %s", GPKG_PATH)
    gdf = gpd.read_file(
        GPKG_PATH, layer="roadway_segments", engine="pyogrio", columns=["unique_id", "ROUTE_ID"]
    )
    logger.info("Loaded %d segments", len(gdf))
    # Compute midpoint in the native CRS (metric). Shapely interpolate at
    # normalized position 0.5 gives the midpoint along the line.
    mids = gdf.geometry.interpolate(0.5, normalized=True)
    df = pd.DataFrame(
        {
            "unique_id": gdf["unique_id"].astype("string").to_numpy(),
            "ROUTE_ID": gdf["ROUTE_ID"].astype("string").to_numpy(),
            "mid_x_m": mids.x.to_numpy(),
            "mid_y_m": mids.y.to_numpy(),
        }
    )
    # For fallback/debug: also compute lat/lon midpoints.
    # The segment_station_link impl will project back only if needed; we
    # provide metric coords directly so no reprojection happens in the
    # hot path.
    return df


def load_stations(db_path: Path, years: list[int]) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(
            "SELECT year, tc_number, latitude, longitude FROM historic_stations "
            f"WHERE year IN ({','.join(str(y) for y in years)})",
            con,
        )
    finally:
        con.close()
    # Drop rows with NULL coords — they cannot anchor a nearest match.
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    logger.info("Loaded %d non-null-coord station rows across years", len(df))
    return df


def ingest_link(db_path: Path, link_df: pd.DataFrame) -> dict:
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA foreign_keys = OFF")
        con.execute(f"DROP TABLE IF EXISTS {LINK_TABLE}")
        link_df.to_sql(LINK_TABLE, con, if_exists="replace", index=False)
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{LINK_TABLE}_uid_year ON {LINK_TABLE}(unique_id, year)")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{LINK_TABLE}_year_tc ON {LINK_TABLE}(year, nearest_tc_number)")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{LINK_TABLE}_year ON {LINK_TABLE}(year)")
        con.commit()

        total = con.execute(f"SELECT COUNT(*) FROM {LINK_TABLE}").fetchone()[0]
        per_year_rows = con.execute(
            f"SELECT year, COUNT(*) AS n, AVG(station_distance_m) AS mean_m, "
            f"SUM(same_route_flag) AS same_route_count "
            f"FROM {LINK_TABLE} GROUP BY year ORDER BY year"
        ).fetchall()
    finally:
        con.close()
    return {
        "row_count": int(total),
        "per_year_stats": {
            int(year): {
                "row_count": int(n),
                "mean_station_distance_m": float(mean_m) if mean_m is not None else None,
                "same_route_count": int(sr),
            }
            for year, n, mean_m, sr in per_year_rows
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=DEFAULT_YEARS,
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "_scratch/segment_station_link_report.json",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    segments = load_segment_midpoints()
    stations = load_stations(STAGED_DB, args.years)

    logger.info("Building link rows for %d segments × %d years", len(segments), len(args.years))
    link = build_link_rows(segments=segments, stations=stations, years=args.years)
    logger.info("Link rows built: %d", len(link))

    ingest_summary = ingest_link(STAGED_DB, link)
    logger.info("Ingest summary: %s", ingest_summary)

    per_year_stats = (
        link.groupby("year")
        .agg(
            row_count=("unique_id", "size"),
            median_dist_m=("station_distance_m", "median"),
            mean_dist_m=("station_distance_m", "mean"),
            max_dist_m=("station_distance_m", "max"),
            same_route_rate=("same_route_flag", "mean"),
        )
        .reset_index()
    )
    final = {
        "run_token": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "ingest_summary": ingest_summary,
        "per_year_stats": per_year_stats.to_dict(orient="records"),
    }
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(final, indent=2, default=str))
    logger.info("Wrote report to %s", args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
