"""Apply synthetic classifier + hard-outlier flags to the segments table.

Adds columns:
  AADT_{2020,2022,2023,2024}_HPMS_SYNTHETIC  (0/1/NULL)
  AADT_{2022,2023}_HPMS_HARD_OUTLIER         (0/1/NULL)

Writes QC report to 02-Data-Staging/docs/aadt_v2_synthetic_classifier.md.
"""

from __future__ import annotations

import sqlite3
import sys
import textwrap
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from synthetic_classifier import (
    HPMS_YEARS,
    classify_hard_outliers,
    classify_synthetic,
)

DB_PATH = SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
DOCS_PATH = SCRIPTS.parents[1] / "docs"


def _load_segments(conn: sqlite3.Connection) -> pd.DataFrame:
    hpms_cols = ", ".join(f"AADT_{y}_HPMS" for y in HPMS_YEARS)
    sql = f"SELECT unique_id, FUNCTIONAL_CLASS, COUNTY_ID, {hpms_cols} FROM segments"
    return pd.read_sql(sql, conn)


def _write_column(conn: sqlite3.Connection, df: pd.DataFrame, col: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE segments ADD COLUMN {col} INTEGER")
    except sqlite3.OperationalError:
        pass

    updates = df[["unique_id", col]].copy()
    non_null = updates[col].notna()

    cur.execute(f"UPDATE segments SET {col} = NULL")

    rows = updates.loc[non_null, ["unique_id", col]].values.tolist()
    cur.executemany(
        f"UPDATE segments SET {col} = ? WHERE unique_id = ?",
        [(int(r[1]), r[0]) for r in rows],
    )
    return len(rows)


def _sanity_check_actuals(
    conn: sqlite3.Connection, df: pd.DataFrame, year: int
) -> list[dict]:
    """Check: no known Actual station lands on a synthetic-flagged segment."""
    syn_col = f"AADT_{year}_HPMS_SYNTHETIC"
    flagged_ids = set(df.loc[df[syn_col] == 1, "unique_id"])
    if not flagged_ids:
        return []

    station_links = pd.read_sql(
        f"SELECT unique_id, nearest_tc_number FROM segment_station_link WHERE year = {year}",
        conn,
    )
    actuals = pd.read_sql(
        f"SELECT tc_number, aadt FROM historic_stations WHERE year = {year} AND statistics_type = 'Actual'",
        conn,
    )
    linked = station_links.merge(actuals, left_on="nearest_tc_number", right_on="tc_number")
    overlap = linked[linked["unique_id"].isin(flagged_ids)]

    hpms_col = f"AADT_{year}_HPMS"
    seg_vals = df.set_index("unique_id")[hpms_col]
    violations = []
    for _, row in overlap.iterrows():
        hpms_val = seg_vals.get(row["unique_id"])
        if hpms_val is not None and not (isinstance(hpms_val, float) and np.isnan(hpms_val)):
            if abs(row["aadt"] - hpms_val) < 1:
                violations.append({
                    "unique_id": row["unique_id"],
                    "tc_number": row["tc_number"],
                    "aadt_actual": row["aadt"],
                    "hpms_value": hpms_val,
                })
    return violations


def _spot_check_synthetic(df: pd.DataFrame, year: int, value: int, n: int = 20) -> pd.DataFrame:
    """Spot-check n random rows flagged synthetic for a given value."""
    syn_col = f"AADT_{year}_HPMS_SYNTHETIC"
    hpms_col = f"AADT_{year}_HPMS"
    pool = df[(df[syn_col] == 1) & (df[hpms_col] == value)]
    sample = pool.sample(n=min(n, len(pool)), random_state=42)
    return sample[["unique_id", "FUNCTIONAL_CLASS", "COUNTY_ID", hpms_col]]


def _build_report(
    df: pd.DataFrame,
    flag_rates: dict[int, dict],
    spot_checks: dict[tuple[int, int], pd.DataFrame],
    actual_violations: dict[int, list[dict]],
    hard_outlier_stats: dict[int, dict],
) -> str:
    lines = [
        "# AADT v2 Synthetic Classifier Report",
        "",
        "## Classifier rule",
        "",
        "A segment's `AADT_{year}_HPMS` is flagged `SYNTHETIC = 1` iff **both** hold:",
        "1. The AADT integer value repeats >= 500 times within (FC, year)",
        "2. `FUNCTIONAL_CLASS` is 6 or 7",
        "",
        "FC 1-5 rows are never flagged (0 by construction). NULL HPMS -> NULL flag.",
        "",
        "The FC 6-7 scope restriction makes a separate county-spread predicate unnecessary:",
        "within FC 6-7, 500+ exact-integer repeats is sufficient evidence of FHWA default fill.",
        "FC 4 corridor carry-forwards (the false-positive concern) are excluded by the FC restriction.",
        "",
        "## Flag rates by (FC, year)",
        "",
        "| Year | FC | Total non-null | Synthetic | Empirical | Synthetic % |",
        "|------|----|--------------:|----------:|----------:|------------:|",
    ]

    for year in HPMS_YEARS:
        rates = flag_rates[year]
        for fc_key in sorted(rates.keys()):
            r = rates[fc_key]
            lines.append(
                f"| {year} | {fc_key} | {r['total']:,} | {r['synthetic']:,} | {r['empirical']:,} | {r['pct']:.1f}% |"
            )

    lines += ["", "## Sanity check 1: spot-check synthetic-flagged rows", ""]

    for (year, value), sample in spot_checks.items():
        lines.append(f"### Year {year}, value {value} ({len(sample)} sampled rows)")
        lines.append("")
        if sample.empty:
            lines.append("No rows to sample.")
        else:
            fc_vals = sorted(set(int(x) for x in sample["FUNCTIONAL_CLASS"].dropna()))
            all_fc67 = all(fc in (6, 7) for fc in fc_vals)
            county_spread = sample["COUNTY_ID"].nunique()
            lines.append(f"- FC values: {fc_vals}")
            lines.append(f"- All FC 6-7: {'YES' if all_fc67 else 'NO — INVESTIGATE'}")
            lines.append(f"- Distinct counties in sample: {county_spread}")
        lines.append("")

    lines += ["## Sanity check 2: Actual station overlap with synthetic flags", ""]

    for year in HPMS_YEARS:
        violations = actual_violations.get(year, [])
        if violations:
            lines.append(f"### Year {year}: {len(violations)} VIOLATION(s)")
            for v in violations[:5]:
                lines.append(f"  - {v['unique_id']}: TC={v['tc_number']}, actual={v['aadt_actual']}, hpms={v['hpms_value']}")
        else:
            lines.append(f"### Year {year}: PASS (0 violations)")
        lines.append("")

    lines += ["## Hard outlier flags (2022, 2023)", ""]
    lines.append("| Year | FC bin | Value | Repeat count | Flagged rows |")
    lines.append("|------|--------|------:|-------------:|-------------:|")

    for year in (2022, 2023):
        stats = hard_outlier_stats.get(year, {})
        if stats.get("flagged_values"):
            for fv in stats["flagged_values"]:
                lines.append(f"| {year} | {fv['fc_bin']} | {fv['value']} | {fv['repeat_count']} | {fv['flagged_rows']} |")
        else:
            lines.append(f"| {year} | — | — | 0 | 0 |")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    print(f"Loading segments from {DB_PATH}...")
    df = _load_segments(conn)
    print(f"  {len(df):,} rows loaded")

    flag_rates: dict[int, dict] = {}
    spot_checks: dict[tuple[int, int], pd.DataFrame] = {}
    actual_violations: dict[int, list[dict]] = {}

    for year in HPMS_YEARS:
        print(f"\nClassifying synthetic defaults for {year}...")
        col = f"AADT_{year}_HPMS"
        syn_col = f"{col}_SYNTHETIC"

        df = classify_synthetic(df, year)

        non_null = df[col].notna()
        rates: dict[str, dict] = {}
        for fc_label, fc_vals in [("1-5", [1.0, 2.0, 3.0, 4.0, 5.0]), ("6", [6.0]), ("7", [7.0])]:
            mask = non_null & df["FUNCTIONAL_CLASS"].isin(fc_vals)
            total = mask.sum()
            synth = (df.loc[mask, syn_col] == 1).sum() if total > 0 else 0
            emp = total - synth
            pct = (synth / total * 100) if total > 0 else 0.0
            rates[fc_label] = {"total": int(total), "synthetic": int(synth), "empirical": int(emp), "pct": pct}
        flag_rates[year] = rates

        written = _write_column(conn, df, syn_col)
        print(f"  {syn_col}: {written:,} non-null values written")

        top_synthetic = (
            df.loc[df[syn_col] == 1]
            .groupby(col)
            .size()
            .sort_values(ascending=False)
        )
        if not top_synthetic.empty:
            top_val = int(top_synthetic.index[0])
            spot_checks[(year, top_val)] = _spot_check_synthetic(df, year, top_val)

        actual_violations[year] = _sanity_check_actuals(conn, df, year)
        if actual_violations[year]:
            print(f"  WARNING: {len(actual_violations[year])} Actual-station violations!")
        else:
            print(f"  Actual-station overlap check: PASS")

    hard_outlier_stats: dict[int, dict] = {}
    for year in (2022, 2023):
        print(f"\nClassifying hard outliers for {year}...")
        col = f"AADT_{year}_HPMS"
        out_col = f"{col}_HARD_OUTLIER"

        df = classify_hard_outliers(df, year)

        written = _write_column(conn, df, out_col)
        print(f"  {out_col}: {written:,} non-null values written")

        flagged_mask = df[out_col] == 1
        if flagged_mask.any():
            flagged_details = []
            fc_bin = df.loc[flagged_mask, "FUNCTIONAL_CLASS"].apply(
                lambda x: f"{int(x)}-{int(x)+1}" if pd.notna(x) else "?"
            )
            for (fb, val), cnt in df.loc[flagged_mask].groupby([fc_bin, col]).size().items():
                flagged_details.append({
                    "fc_bin": fb, "value": int(val),
                    "repeat_count": int(cnt), "flagged_rows": int(cnt),
                })
            hard_outlier_stats[year] = {"flagged_values": flagged_details}
        else:
            hard_outlier_stats[year] = {"flagged_values": []}

    conn.commit()
    conn.close()

    report = _build_report(df, flag_rates, spot_checks, actual_violations, hard_outlier_stats)
    report_path = DOCS_PATH / "aadt_v2_synthetic_classifier.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nReport written to {report_path}")
    print("Done.")


if __name__ == "__main__":
    main()
