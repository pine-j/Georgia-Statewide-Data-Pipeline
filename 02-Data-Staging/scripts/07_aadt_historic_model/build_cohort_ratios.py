"""Orchestrator pass: build 5 cohort-ratio versions and write to SQLite.

Plan reference: `aadt-modeling-scoped-2020-2024.md` Step 5.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cohort_ratios import (  # noqa: E402
    COHORT_RATIO_VERSIONS,
    MIN_COHORT_SIZE_FOR_RATIO,
    build_all_versions,
)

logger = logging.getLogger(__name__)

PROJECT_MAIN = Path("D:/Jacobs/Georgia-Statewide-Data-Pipeline")
STAGED_DB = PROJECT_MAIN / "02-Data-Staging/databases/roadway_inventory.db"
COHORT_TABLE = "cohort_ratios"


def load_segments() -> pd.DataFrame:
    con = sqlite3.connect(STAGED_DB)
    try:
        df = pd.read_sql(
            "SELECT unique_id, FUNCTIONAL_CLASS, URBAN_CODE, DISTRICT, "
            "AADT_2020_HPMS, AADT_2022_HPMS, AADT_2023_HPMS, AADT_2024_HPMS "
            "FROM segments",
            con,
        )
    finally:
        con.close()
    return df


def ingest_cohort_table(db_path: Path, df: pd.DataFrame) -> dict:
    con = sqlite3.connect(db_path)
    try:
        con.execute(f"DROP TABLE IF EXISTS {COHORT_TABLE}")
        df.to_sql(COHORT_TABLE, con, if_exists="replace", index=False)
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{COHORT_TABLE}_key "
            f"ON {COHORT_TABLE}(version, fc_bin, urban_rural, district)"
        )
        con.commit()
        row_count = con.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}").fetchone()[0]
        per_version = dict(
            con.execute(
                f"SELECT version, COUNT(*) FROM {COHORT_TABLE} GROUP BY version ORDER BY version"
            ).fetchall()
        )
    finally:
        con.close()
    return {"row_count": int(row_count), "per_version": {k: int(v) for k, v in per_version.items()}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "_scratch/cohort_ratios_report.json",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    segments = load_segments()
    logger.info("Loaded %d segments", len(segments))

    ratios = build_all_versions(segments)
    logger.info("Built %d cohort-ratio rows (5 versions)", len(ratios))

    ingest_summary = ingest_cohort_table(STAGED_DB, ratios)
    logger.info("Ingest: %s", ingest_summary)

    # Per-version diagnostics.
    full = ratios[ratios["version"] == "full"]
    cohort_summary = {
        "total_cohorts": int(len(full)),
        "cohorts_with_fallback": int(full["cohort_fallback_used"].sum()),
        "cohorts_with_both_ratios": int(
            (full["cohort_ratio_2020_to_2022"].notna() & full["cohort_ratio_2020_to_2024"].notna()).sum()
        ),
        "cohorts_by_fc_bin": full["fc_bin"].value_counts(dropna=False).to_dict(),
    }
    final = {
        "run_token": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "min_cohort_size": int(MIN_COHORT_SIZE_FOR_RATIO),
        "versions": list(COHORT_RATIO_VERSIONS),
        "ingest_summary": ingest_summary,
        "full_version_summary": cohort_summary,
    }
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(final, indent=2, default=str))
    logger.info("Wrote report to %s", args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
