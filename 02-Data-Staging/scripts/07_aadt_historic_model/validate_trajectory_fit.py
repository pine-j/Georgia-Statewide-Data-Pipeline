"""Trajectory-fit holdout validation (Fan-out B).

For each Tier 0 segment, fit the quadratic on 3 of the 4 HPMS anchors
and predict the held-out year. Compute MAPE per fold. The two folds
requested by the plan are hide-2022 and hide-2023.

Plan reference: `aadt-modeling-scoped-2020-2024.md` Step 6 and
§Trajectory-fit validation.

Gate: MAPE ≤ 10% on ≥80% of Tier 0 segments per fold; median APE ≤ 8%.
Hard-stop: MAPE > 25% on either fold → abandon trajectory fit.

Deviation from plan §Fan-out B: the two parallel sub-agents are
collapsed into a single sequential orchestrator script. The folds are
independent arithmetic on disjoint slices of the same table; no shared
writes. Parallelism saves ~seconds and adds the risk of two agents
drifting on schema interpretation. A single script guarantees
consistency.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cohort_ratios import fc_bin_for, urban_rural_for  # noqa: E402
from trajectory_fit import (  # noqa: E402
    clamp_prediction,
    fit_segment_quadratic_log,
    is_tier0_eligible,
)

logger = logging.getLogger(__name__)

PROJECT_MAIN = Path("D:/Jacobs/Georgia-Statewide-Data-Pipeline")
STAGED_DB = PROJECT_MAIN / "02-Data-Staging/databases/roadway_inventory.db"
REPORT_PATH = Path(__file__).resolve().parents[3] / "02-Data-Staging/docs/aadt_trajectory_fit_validation.md"

GATE_MAPE_WITHIN_10PCT_SHARE_MIN = 0.80
GATE_MEDIAN_APE_MAX = 0.08  # 8 percent
GATE_MAPE_HARDSTOP = 0.25  # 25 percent


def load_tier0_segments() -> pd.DataFrame:
    con = sqlite3.connect(STAGED_DB)
    try:
        df = pd.read_sql(
            "SELECT unique_id, FUNCTIONAL_CLASS, URBAN_CODE, DISTRICT, COUNTY_ID, "
            "AADT_2020_HPMS, AADT_2022_HPMS, AADT_2023_HPMS, AADT_2024_HPMS "
            "FROM segments "
            "WHERE AADT_2020_HPMS IS NOT NULL AND AADT_2022_HPMS IS NOT NULL "
            "  AND (AADT_2023_HPMS IS NOT NULL OR AADT_2024_HPMS IS NOT NULL)",
            con,
        )
    finally:
        con.close()
    mask = df.apply(
        lambda row: is_tier0_eligible(
            row["AADT_2020_HPMS"], row["AADT_2022_HPMS"], row["AADT_2023_HPMS"], row["AADT_2024_HPMS"]
        ),
        axis=1,
    )
    df = df[mask].reset_index(drop=True)
    df["fc_bin"] = df["FUNCTIONAL_CLASS"].map(fc_bin_for)
    df["urban_rural"] = df["URBAN_CODE"].map(urban_rural_for)
    return df


def _covid_widened(row: pd.Series) -> bool:
    fc = row.get("FUNCTIONAL_CLASS")
    ur = row.get("urban_rural")
    if fc is None or (isinstance(fc, float) and math.isnan(fc)):
        return False
    return ur == "urban" and int(fc) in (1, 2, 3)


def predict_held_out_year(row: pd.Series, holdout_year: int) -> tuple[float, float] | None:
    """Return (predicted_log_aadt, truth_aadt) for the held-out year.

    If the segment lacks that year's HPMS value, returns None.
    """

    truth = row[f"AADT_{holdout_year}_HPMS"]
    if truth is None or (isinstance(truth, float) and math.isnan(truth)):
        return None

    # Build anchors from the non-holdout years that have values.
    anchors: dict[int, float] = {}
    for year in (2020, 2022, 2023, 2024):
        if year == holdout_year:
            continue
        v = row[f"AADT_{year}_HPMS"]
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        anchors[year] = float(v)

    # Need at least 3 anchors for a quadratic.
    if len(anchors) < 3:
        return None

    years = sorted(anchors.keys())
    values = [anchors[y] for y in years]
    coeffs = fit_segment_quadratic_log(years, values)
    raw_pred = float(np.exp(np.polyval(coeffs, holdout_year)))

    # Clamp against 2020 and the nearest available post-COVID anchor that
    # isn't the holdout year. In production (predict 2021), this is 2022.
    # In hide-2022 validation, substitute 2023 or 2024 — using the true
    # 2022 value as the clamp anchor would be a leak.
    a2020 = float(row["AADT_2020_HPMS"])
    if holdout_year == 2022:
        post_covid_anchor = anchors.get(2023) or anchors.get(2024)
    else:
        post_covid_anchor = anchors.get(2022)
    if post_covid_anchor is None:
        post_covid_anchor = a2020
    clamped = clamp_prediction(
        raw_pred, a2020, post_covid_anchor, covid_widened=_covid_widened(row)
    )
    return (clamped, float(truth))


def run_fold(segments: pd.DataFrame, holdout_year: int) -> dict:
    pairs: list[tuple[str, float, float, str | None]] = []
    for _, row in segments.iterrows():
        result = predict_held_out_year(row, holdout_year)
        if result is None:
            continue
        pred, truth = result
        pairs.append((row["unique_id"], pred, truth, row["fc_bin"]))

    if not pairs:
        return {"holdout_year": holdout_year, "segment_count": 0, "status": "EMPTY"}

    df = pd.DataFrame(pairs, columns=["unique_id", "pred", "truth", "fc_bin"])
    df["ape"] = (df["pred"] - df["truth"]).abs() / df["truth"]
    df["within_10pct"] = (df["ape"] <= 0.10).astype(int)

    mape = float(df["ape"].mean())
    median_ape = float(df["ape"].median())
    p95_ape = float(df["ape"].quantile(0.95))
    within_10pct_share = float(df["within_10pct"].mean())

    slice_by_fc = (
        df.groupby("fc_bin", dropna=False)
        .agg(count=("ape", "size"), mape=("ape", "mean"), median_ape=("ape", "median"))
        .reset_index()
    )

    gate_met = (
        mape <= GATE_MAPE_HARDSTOP
        and median_ape <= GATE_MEDIAN_APE_MAX
        and within_10pct_share >= GATE_MAPE_WITHIN_10PCT_SHARE_MIN
    )
    hardstop = mape > GATE_MAPE_HARDSTOP

    return {
        "holdout_year": holdout_year,
        "segment_count": int(len(df)),
        "mape": mape,
        "median_ape": median_ape,
        "p95_ape": p95_ape,
        "within_10pct_share": within_10pct_share,
        "gate_met": bool(gate_met),
        "hardstop_triggered": bool(hardstop),
        "fc_bin_slice": slice_by_fc.to_dict(orient="records"),
    }


def _write_report(hide_2022: dict, hide_2023: dict) -> None:
    lines: list[str] = []
    lines.append("# AADT trajectory-fit validation (Tier 0)")
    lines.append("")
    lines.append(
        "Holdout validation for the Tier 0 shape-preserving quadratic "
        "(log AADT). Each segment in Tier 0 is re-fit using 3 of its 4 "
        "HPMS anchors, and the held-out year is predicted; absolute "
        "percentage error is computed vs the true HPMS value."
    )
    lines.append("")
    lines.append("## Gates")
    lines.append("")
    lines.append(f"- MAPE ≤ {GATE_MAPE_HARDSTOP*100:.0f}%  — hardstop")
    lines.append(f"- Median APE ≤ {GATE_MEDIAN_APE_MAX*100:.0f}%")
    lines.append(f"- Within-10% share ≥ {GATE_MAPE_WITHIN_10PCT_SHARE_MIN*100:.0f}%")
    lines.append("")

    for label, result in [("Hide-2022 fold", hide_2022), ("Hide-2023 fold", hide_2023)]:
        lines.append(f"## {label}")
        lines.append("")
        if result.get("status") == "EMPTY":
            lines.append("No Tier 0 segments have a value for this holdout year — fold skipped.")
            lines.append("")
            continue
        lines.append(f"- Segment count: {result['segment_count']:,}")
        lines.append(f"- MAPE: {result['mape']*100:.2f}%")
        lines.append(f"- Median APE: {result['median_ape']*100:.2f}%")
        lines.append(f"- P95 APE: {result['p95_ape']*100:.2f}%")
        lines.append(f"- Within 10%: {result['within_10pct_share']*100:.2f}%")
        lines.append(f"- Gate met: {'YES' if result['gate_met'] else 'NO'}")
        lines.append(f"- Hardstop triggered: {'YES' if result['hardstop_triggered'] else 'NO'}")
        lines.append("")
        lines.append("| FC bin | Count | MAPE | Median APE |")
        lines.append("|---|---|---|---|")
        for s in result["fc_bin_slice"]:
            fb = s["fc_bin"] if s["fc_bin"] is not None else "NULL"
            lines.append(
                f"| {fb} | {s['count']:,} | {s['mape']*100:.2f}% | {s['median_ape']*100:.2f}% |"
            )
        lines.append("")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=REPORT_PATH,
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    tier0 = load_tier0_segments()
    logger.info("Tier 0 segments (eligible via HPMS anchors): %d", len(tier0))

    hide_2022 = run_fold(tier0, 2022)
    hide_2023 = run_fold(tier0, 2023)
    logger.info("Hide-2022: %s", hide_2022)
    logger.info("Hide-2023: %s", hide_2023)

    _write_report(hide_2022, hide_2023)
    logger.info("Wrote report to %s", args.report_path)

    scratch = PROJECT_MAIN / "_scratch_trajectory_validation.json"  # debug only
    (PROJECT_MAIN.parent / "_scratch_trajectory_validation.json").parent.mkdir(parents=True, exist_ok=True)
    return 0 if not (hide_2022.get("hardstop_triggered") or hide_2023.get("hardstop_triggered")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
