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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    logger.info("Loading 2021 k-NN links...")
    knn = pd.read_sql(
        "SELECT unique_id, k_rank, nearest_tc_number, station_distance_m, same_route_flag "
        "FROM segment_station_link_knn WHERE year = 2021",
        conn,
    )
    logger.info("  %d k-NN rows", len(knn))

    logger.info("Loading 2021 stations...")
    stations = pd.read_sql(
        "SELECT tc_number, aadt FROM historic_stations WHERE year = 2021",
        conn,
    )
    stations = stations.drop_duplicates(subset=["tc_number"], keep="first")
    logger.info("  %d unique stations", len(stations))

    logger.info("Running IDW prediction...")
    result = predict_idw(knn, stations)
    logger.info("  %d segments processed", len(result))

    predicted = result["AADT_2021_MODELED"].notna().sum()
    null = result["AADT_2021_MODELED"].isna().sum()
    logger.info("  Predicted: %d (%.1f%%)", predicted, predicted / len(result) * 100)
    logger.info("  NULL (beyond cutoff): %d (%.1f%%)", null, null / len(result) * 100)

    high = (result["AADT_2021_CONFIDENCE"] == "high").sum()
    medium = (result["AADT_2021_CONFIDENCE"] == "medium").sum()
    none_conf = (result["AADT_2021_CONFIDENCE"] == "none").sum()
    logger.info("  High confidence: %d (%.1f%%)", high, high / len(result) * 100)
    logger.info("  Medium confidence: %d (%.1f%%)", medium, medium / len(result) * 100)
    logger.info("  No prediction: %d (%.1f%%)", none_conf, none_conf / len(result) * 100)

    logger.info("Writing columns to segments table...")
    new_cols = [
        ("AADT_2021_MODELED", "INTEGER"),
        ("AADT_2021_P10", "INTEGER"),
        ("AADT_2021_P90", "INTEGER"),
        ("AADT_2021_CONFIDENCE", "TEXT"),
        ("AADT_2021_SOURCE", "TEXT"),
        ("AADT_2021_NEAREST_STATION_DIST_M", "REAL"),
        ("AADT_2021_NEAREST_STATION_TC", "TEXT"),
        ("AADT_2021_N_STATIONS_USED", "INTEGER"),
    ]

    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(segments)").fetchall()}
    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE segments ADD COLUMN {col_name} {col_type}")

    for col_name, _ in new_cols:
        conn.execute(f"UPDATE segments SET {col_name} = NULL")

    for _, row in result.iterrows():
        uid = row["unique_id"]
        sets = []
        params = []
        for col_name, _ in new_cols:
            val = row[col_name]
            if pd.notna(val):
                sets.append(f"{col_name} = ?")
                params.append(val if not isinstance(val, (np.integer, np.floating)) else val.item())
            else:
                sets.append(f"{col_name} = NULL")
        if sets:
            params.append(uid)
            conn.execute(
                f"UPDATE segments SET {', '.join(sets)} WHERE unique_id = ?",
                params,
            )

    conn.commit()

    verify = conn.execute(
        "SELECT COUNT(*) FROM segments WHERE AADT_2021_MODELED IS NOT NULL"
    ).fetchone()[0]
    logger.info("  Verified: %d segments with AADT_2021_MODELED", verify)

    seg_count = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
    logger.info("  Total segments: %d (unchanged)", seg_count)

    _write_report(result, conn)

    conn.close()
    logger.info("Done.")


def _write_report(result: pd.DataFrame, conn: sqlite3.Connection) -> None:
    predicted = result[result["AADT_2021_MODELED"].notna()]
    lines = [
        "# AADT 2021 IDW Prediction Report",
        "",
        "## Method",
        "",
        "Inverse-distance-weighted interpolation of k=5 nearest 2021 stations.",
        f"Cutoff: {CUTOFF_M}m. Segments beyond cutoff get NULL.",
        "",
        "## Coverage",
        "",
        f"| Metric | Count | % |",
        f"|--------|------:|--:|",
        f"| Total segments | {len(result):,} | 100% |",
        f"| Predicted | {len(predicted):,} | {len(predicted)/len(result)*100:.1f}% |",
        f"| NULL (beyond cutoff) | {len(result) - len(predicted):,} | {(len(result) - len(predicted))/len(result)*100:.1f}% |",
        "",
        "## Confidence tiers",
        "",
        f"| Tier | Count | % |",
        f"|------|------:|--:|",
    ]
    for tier in ["high", "medium", "none"]:
        cnt = (result["AADT_2021_CONFIDENCE"] == tier).sum()
        lines.append(f"| {tier} | {cnt:,} | {cnt/len(result)*100:.1f}% |")

    if len(predicted) > 0:
        lines += [
            "",
            "## Predicted AADT distribution",
            "",
            f"- Min: {predicted['AADT_2021_MODELED'].min():,.0f}",
            f"- Median: {predicted['AADT_2021_MODELED'].median():,.0f}",
            f"- Mean: {predicted['AADT_2021_MODELED'].mean():,.0f}",
            f"- Max: {predicted['AADT_2021_MODELED'].max():,.0f}",
            "",
            "## Stations used per prediction",
            "",
            f"- Median: {predicted['AADT_2021_N_STATIONS_USED'].median():.0f}",
            f"- Mean: {predicted['AADT_2021_N_STATIONS_USED'].mean():.1f}",
        ]

    fc_stats = pd.read_sql(
        "SELECT FUNCTIONAL_CLASS, "
        "COUNT(*) as total, "
        "SUM(CASE WHEN AADT_2021_MODELED IS NOT NULL THEN 1 ELSE 0 END) as predicted "
        "FROM segments GROUP BY FUNCTIONAL_CLASS ORDER BY FUNCTIONAL_CLASS",
        conn,
    )
    lines += ["", "## Coverage by FC", "", "| FC | Total | Predicted | % |", "|------|------:|----------:|---:|"]
    for _, row in fc_stats.iterrows():
        fc = row["FUNCTIONAL_CLASS"]
        fc_label = f"{int(fc)}" if pd.notna(fc) else "NULL"
        pct = row["predicted"] / row["total"] * 100 if row["total"] > 0 else 0
        lines.append(f"| {fc_label} | {int(row['total']):,} | {int(row['predicted']):,} | {pct:.1f}% |")

    lines.append("")
    report_path = DOCS_PATH / "aadt_v2_idw_prediction.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report written to %s", report_path)


if __name__ == "__main__":
    main()
