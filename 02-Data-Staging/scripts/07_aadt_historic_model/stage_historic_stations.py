"""Orchestrator pass: stage `historic_stations` from 4 xlsx + 1 GDB source.

Plan reference: `aadt-modeling-scoped-2020-2024.md` §Prerequisite #2 and
§Fan-out point A.

Flow:
1. For each xlsx year ∈ {2020, 2021, 2022, 2023}, extract the file from
   `Traffic_Historical.zip`, run `historic_stations_loader.load_station_xlsx`,
   write `_scratch/historic_stations/{year}.parquet`.
2. Read `TRAFFIC_Data_2024.gdb` layer `TRAFFIC_DataYear2024`, normalize to
   the same schema, write `_scratch/historic_stations/2024.parquet`.
3. Open the staged SQLite DB, `DROP TABLE IF EXISTS historic_stations`,
   recreate, and `INSERT` the 5-parquet UNION in a single transaction.
4. Emit a staging summary with per-year row counts and distribution of
   `statistics_type`.

Deviation from plan §Fan-out A: the 4 parallel sub-agents are collapsed
into a sequential orchestrator loop. Each year still writes to its own
per-year parquet and the SQLite ingest is still a single-writer
transaction, so the lock-contention concern the plan guards against is
unchanged. Parallelism savings on ~25k-row xlsx reads are not material.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from historic_stations_loader import (
    HISTORIC_STATIONS_COLUMNS,
    SCHEMA_2020_2021_TEXT_LATLONG,
    SCHEMA_2022_2023_NUMERIC_LATLONG,
    load_station_xlsx,
)

logger = logging.getLogger(__name__)

PROJECT_MAIN = Path("D:/Jacobs/Georgia-Statewide-Data-Pipeline")
TRAFFIC_ZIP = PROJECT_MAIN / "01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip"
TRAFFIC_2024_GDB = PROJECT_MAIN / "01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_2024_Geodatabase/TRAFFIC_Data_2024.gdb"
STAGED_DB = PROJECT_MAIN / "02-Data-Staging/databases/roadway_inventory.db"

# Per-year xlsx paths inside Traffic_Historical.zip, verified 2026-04-19.
# Row-count targets are the inventory §1 expectations.
XLSX_SOURCES: dict[int, dict[str, object]] = {
    2020: {
        "zip_path": "2010_thr_2023_Published_Traffic/2020_Published_Traffic/2020_annualized_statistics.xlsx",
        "schema_variant": SCHEMA_2020_2021_TEXT_LATLONG,
        "source_tag": "xlsx:2020_annualized_statistics.xlsx",
        "expected_rows": 25889,
    },
    2021: {
        "zip_path": "2010_thr_2023_Published_Traffic/2021_Published_Traffic/2021 annualized_statistics (85).xlsx",
        "schema_variant": SCHEMA_2020_2021_TEXT_LATLONG,
        "source_tag": "xlsx:2021 annualized_statistics (85).xlsx",
        "expected_rows": 25966,
    },
    2022: {
        "zip_path": "2010_thr_2023_Published_Traffic/2022_Published_Traffic/2022 annualized_statistics.xlsx",
        "schema_variant": SCHEMA_2022_2023_NUMERIC_LATLONG,
        "source_tag": "xlsx:2022 annualized_statistics.xlsx",
        "expected_rows": 25668,
    },
    2023: {
        "zip_path": "2010_thr_2023_Published_Traffic/2023_Published_Traffic/annualized_statistics_2023.xlsx",
        "schema_variant": SCHEMA_2022_2023_NUMERIC_LATLONG,
        "source_tag": "xlsx:annualized_statistics_2023.xlsx",
        "expected_rows": 25714,
    },
}

GDB_SOURCE_TAG = "gdb:TRAFFIC_DataYear2024"
GDB_LAYER = "TRAFFIC_DataYear2024"
# Inventory §2024 recap: ~15,796 Actual rows; total row count here includes
# Estimated and Calculated rows emitted by the same layer.
EXPECTED_2024_ACTUAL_MIN = 15000

HISTORIC_STATIONS_TABLE = "historic_stations"


def extract_xlsx(zip_path: Path, member: str, dest: Path) -> Path:
    logger.info("Extracting %s from %s", member, zip_path.name)
    with zipfile.ZipFile(zip_path) as archive:
        try:
            data = archive.read(member)
        except KeyError as exc:
            raise FileNotFoundError(
                f"xlsx member {member!r} not present in {zip_path}"
            ) from exc
    dest.write_bytes(data)
    return dest


def stage_year_xlsx(year: int, scratch_dir: Path) -> dict:
    spec = XLSX_SOURCES[year]
    with tempfile.TemporaryDirectory() as tmp:
        xlsx_tmp = Path(tmp) / f"{year}.xlsx"
        extract_xlsx(TRAFFIC_ZIP, str(spec["zip_path"]), xlsx_tmp)
        df = load_station_xlsx(
            xlsx_path=xlsx_tmp,
            year=year,
            schema_variant=str(spec["schema_variant"]),
            source_tag=str(spec["source_tag"]),
        )

    out_path = scratch_dir / f"{year}.parquet"
    df.to_parquet(out_path, index=False)

    stats_type_counts = df["statistics_type"].value_counts(dropna=False).to_dict()
    report = {
        "year": year,
        "status": "SUCCESS",
        "row_count_written": int(len(df)),
        "row_count_expected": int(spec["expected_rows"]),
        "row_count_match": int(len(df)) == int(spec["expected_rows"]),
        "null_count_lat": int(df["latitude"].isna().sum()),
        "null_count_stats_type": int(df["statistics_type"].isna().sum()),
        "stats_type_distribution": {str(k): int(v) for k, v in stats_type_counts.items()},
        "parquet_path": str(out_path),
        "source": str(spec["source_tag"]),
    }
    logger.info("Staged year %d: %s", year, report)
    return report


def stage_2024_gdb(scratch_dir: Path) -> dict:
    """Read `TRAFFIC_DataYear2024` and normalize to historic_stations schema.

    2024 rows carry `TC_NUMBER`, `TC_Latitude`, `TC_Longitude`, `AADT`,
    `Statistics_Type`, `Single_Unit_AADT`, `Combo_Unit_AADT`, `K_Factor`,
    `D_Factor`, `Traffic_Class`, `Future_AADT` directly on the layer (plus
    the road-inventory FC which is off-layer). Option A deviation: we do
    NOT push these through normalize.py / the `segments` table; they flow
    straight from the GDB into historic_stations.
    """

    logger.info("Reading %s layer %s", TRAFFIC_2024_GDB, GDB_LAYER)
    raw = gpd.read_file(
        TRAFFIC_2024_GDB,
        layer=GDB_LAYER,
        engine="pyogrio",
        ignore_geometry=True,
    )

    def _to_int(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    def _to_float(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").astype("float64")

    # The GDB's `F_SYSTEM` is the integer functional class on the same
    # layer. `Station Type` (Short Term / CCS) is not present on the 2024
    # traffic layer, so we leave it NULL for 2024 rows — the xlsx years
    # carry it and that is the pool the model will learn from.
    df = pd.DataFrame(
        {
            "year": [2024] * len(raw),
            "tc_number": raw["TC_NUMBER"].astype("string"),
            "latitude": _to_float(raw["TC_Latitude"]),
            "longitude": _to_float(raw["TC_Longitude"]),
            "aadt": _to_int(raw["AADTRound"] if "AADTRound" in raw.columns else raw["AADT"]),
            "statistics_type": raw["Statistics_Type"].astype("string"),
            "single_unit_aadt": _to_int(raw.get("Single_Unit_AADT")),
            "combo_unit_aadt": _to_int(raw.get("Combo_Unit_AADT")),
            "k_factor": _to_float(raw.get("K_Factor")),
            "d_factor": _to_float(raw.get("D_Factor")),
            "functional_class": _to_int(raw["F_SYSTEM"]) if "F_SYSTEM" in raw.columns else pd.Series([pd.NA] * len(raw), dtype="Int64"),
            "station_type": pd.Series([pd.NA] * len(raw), dtype="string"),
            "traffic_class": raw["Traffic_Class"].astype("string") if "Traffic_Class" in raw.columns else pd.Series([pd.NA] * len(raw), dtype="string"),
            "future_aadt": _to_int(raw.get("Future_AADT")),
            "source": [GDB_SOURCE_TAG] * len(raw),
        }
    )
    df = df[HISTORIC_STATIONS_COLUMNS]

    out_path = scratch_dir / "2024.parquet"
    df.to_parquet(out_path, index=False)

    stats_type_counts = df["statistics_type"].value_counts(dropna=False).to_dict()
    report = {
        "year": 2024,
        "status": "SUCCESS",
        "row_count_written": int(len(df)),
        "stats_type_distribution": {str(k): int(v) for k, v in stats_type_counts.items()},
        "null_count_lat": int(df["latitude"].isna().sum()),
        "parquet_path": str(out_path),
        "source": GDB_SOURCE_TAG,
    }
    actual_count = stats_type_counts.get("Actual", 0)
    if int(actual_count) < EXPECTED_2024_ACTUAL_MIN:
        report["warning"] = (
            f"2024 Actual count {actual_count} below expected floor "
            f"{EXPECTED_2024_ACTUAL_MIN} (inventory §2024 recap)"
        )
    logger.info("Staged 2024: %s", report)
    return report


def ingest_parquets(scratch_dir: Path, db_path: Path, year_reports: list[dict]) -> dict:
    """Single-writer UNION-ALL ingest into `historic_stations`."""

    parquets = [Path(r["parquet_path"]) for r in year_reports if r["status"] == "SUCCESS"]
    frames = [pd.read_parquet(p) for p in parquets]
    union = pd.concat(frames, axis=0, ignore_index=True)
    union = union[HISTORIC_STATIONS_COLUMNS]

    if len(union) == 0:
        raise RuntimeError("No rows to ingest into historic_stations")

    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA foreign_keys = OFF")
        con.execute("BEGIN")
        con.execute(f"DROP TABLE IF EXISTS {HISTORIC_STATIONS_TABLE}")
        union.to_sql(HISTORIC_STATIONS_TABLE, con, if_exists="replace", index=False)
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{HISTORIC_STATIONS_TABLE}_year "
            f"ON {HISTORIC_STATIONS_TABLE}(year)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{HISTORIC_STATIONS_TABLE}_tc "
            f"ON {HISTORIC_STATIONS_TABLE}(tc_number)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{HISTORIC_STATIONS_TABLE}_year_tc "
            f"ON {HISTORIC_STATIONS_TABLE}(year, tc_number)"
        )
        con.commit()

        row_count = con.execute(
            f"SELECT COUNT(*) FROM {HISTORIC_STATIONS_TABLE}"
        ).fetchone()[0]
        per_year = dict(
            con.execute(
                f"SELECT year, COUNT(*) FROM {HISTORIC_STATIONS_TABLE} "
                f"GROUP BY year ORDER BY year"
            ).fetchall()
        )
    finally:
        con.close()

    return {
        "status": "SUCCESS",
        "row_count": int(row_count),
        "per_year_counts": {int(k): int(v) for k, v in per_year.items()},
        "expected_total": int(sum(int(r["row_count_written"]) for r in year_reports)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scratch-dir",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "_scratch/historic_stations",
        help="Where per-year parquets are written before ingest.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "_scratch/historic_stations/_staging_report.json",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2020, 2021, 2022, 2023, 2024],
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    scratch = args.scratch_dir
    scratch.mkdir(parents=True, exist_ok=True)

    year_reports: list[dict] = []
    for year in args.years:
        if year == 2024:
            year_reports.append(stage_2024_gdb(scratch))
        else:
            year_reports.append(stage_year_xlsx(year, scratch))

    ingest_summary = ingest_parquets(scratch, STAGED_DB, year_reports)

    final = {
        "run_token": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "year_reports": year_reports,
        "ingest_summary": ingest_summary,
    }
    args.report_path.write_text(json.dumps(final, indent=2))
    logger.info("Wrote staging report to %s", args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
