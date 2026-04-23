"""Apply station_uid_resolver to historic_stations and write results to DB.

Creates the ``station_uid_resolver`` table with columns:
  year, tc_number, station_uid, resolver_method, resolver_delta_m, resolver_confidence

Writes QC report to 02-Data-Staging/docs/aadt_v2_station_identity_resolver.md.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from station_uid_resolver import resolve_stations, ANCHOR_YEAR

DB_PATH = SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
DOCS_PATH = SCRIPTS.parents[1] / "docs"


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))

    print("Loading historic_stations...")
    stations = pd.read_sql(
        "SELECT year, tc_number, latitude, longitude FROM historic_stations",
        conn,
    )
    print(f"  {len(stations):,} rows loaded across {stations['year'].nunique()} years")

    print("\nResolving station identities...")
    resolved = resolve_stations(stations)

    print("\nWriting station_uid_resolver table...")
    resolver_cols = ["year", "tc_number", "station_uid", "resolver_method",
                     "resolver_delta_m", "resolver_confidence"]
    resolver_df = resolved[resolver_cols].copy()
    resolver_df.to_sql("station_uid_resolver", conn, if_exists="replace", index=False)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_resolver_year_tc
        ON station_uid_resolver (year, tc_number)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_resolver_uid
        ON station_uid_resolver (station_uid)
    """)
    conn.commit()

    total = len(resolver_df)
    print(f"  {total:,} rows written")

    print("\nGenerating QC report...")
    report = _build_report(resolver_df)
    report_path = DOCS_PATH / "aadt_v2_station_identity_resolver.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"Report written to {report_path}")

    conn.close()
    print("Done.")


def _build_report(df: pd.DataFrame) -> str:
    lines = [
        "# AADT v2 Station Identity Resolver Report",
        "",
        "## Summary",
        "",
        f"Total rows resolved: {len(df):,}",
        f"Unique station_uids: {df['station_uid'].nunique():,}",
        "",
        "## Resolution by year and method",
        "",
        "| Year | Total | Anchor | TC match | Spatial | Chained | Unresolved | Resolve % |",
        "|------|------:|-------:|---------:|--------:|--------:|-----------:|----------:|",
    ]

    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        total = len(ydf)
        anchor = (ydf["resolver_method"] == "anchor").sum()
        tc = (ydf["resolver_method"] == "tc_number").sum()
        spatial = (ydf["resolver_method"] == "spatial").sum()
        chained = (ydf["resolver_method"] == "chained_via_2023").sum()
        unresolved = (ydf["resolver_method"] == "unresolved").sum()
        resolved_pct = (total - unresolved) / total * 100 if total > 0 else 0
        lines.append(
            f"| {year} | {total:,} | {anchor:,} | {tc:,} | {spatial:,} | {chained:,} | {unresolved:,} | {resolved_pct:.1f}% |"
        )

    lines += ["", "## Confidence distribution", ""]
    lines.append("| Year | High | Medium | Low |")
    lines.append("|------|-----:|-------:|----:|")
    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        high = (ydf["resolver_confidence"] == "high").sum()
        med = (ydf["resolver_confidence"] == "medium").sum()
        low = (ydf["resolver_confidence"] == "low").sum()
        lines.append(f"| {year} | {high:,} | {med:,} | {low:,} |")

    lines += ["", "## Distance distribution (resolved stations)", ""]

    resolved = df[df["resolver_delta_m"].notna() & (df["resolver_method"] != "anchor")]
    if not resolved.empty:
        lines.append(f"- Count: {len(resolved):,}")
        lines.append(f"- Median: {resolved['resolver_delta_m'].median():.1f}m")
        lines.append(f"- P95: {resolved['resolver_delta_m'].quantile(0.95):.1f}m")
        lines.append(f"- Max: {resolved['resolver_delta_m'].max():.1f}m")
    else:
        lines.append("No resolved non-anchor stations.")

    lines += ["", "## Unresolved stations", ""]
    unresolved = df[df["resolver_method"] == "unresolved"]
    if not unresolved.empty:
        lines.append(f"Total unresolved: {len(unresolved):,} across {unresolved['year'].nunique()} years")
        lines.append("")
        for year in sorted(unresolved["year"].unique()):
            uyr = unresolved[unresolved["year"] == year]
            lines.append(f"### Year {year}: {len(uyr):,} unresolved")
            sample = uyr.head(10)
            for _, r in sample.iterrows():
                lines.append(f"  - {r['tc_number']}")
    else:
        lines.append("No unresolved stations.")

    actual_check = df[
        (df["resolver_method"] == "unresolved")
    ]
    lines += [
        "",
        "## Acceptance criteria",
        "",
        f"- Resolved rate (2020-2023): {_resolve_rate_2020_2023(df):.1f}% (target: >=95%)",
    ]

    lines.append("")
    return "\n".join(lines)


def _resolve_rate_2020_2023(df: pd.DataFrame) -> float:
    pre_anchor = df[df["year"] < ANCHOR_YEAR]
    if len(pre_anchor) == 0:
        return 100.0
    resolved = (pre_anchor["resolver_method"] != "unresolved").sum()
    return resolved / len(pre_anchor) * 100


if __name__ == "__main__":
    main()
