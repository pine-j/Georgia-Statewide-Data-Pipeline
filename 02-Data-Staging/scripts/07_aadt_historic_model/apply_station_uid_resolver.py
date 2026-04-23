"""Apply scored station_uid_resolver to historic_stations and write results.

Creates the ``station_uid_resolver`` table with columns:
  year, tc_number, station_uid, resolver_method, resolver_delta_m, resolver_confidence

Uses the scored_resolver (distance + AADT + FC matching) for all non-anchor
years. 2024 is the anchor year — its rows get resolver_method="anchor".

Writes QC report to 02-Data-Staging/docs/aadt_v2_station_identity_resolver.md.
"""

from __future__ import annotations

import math
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from scored_resolver import build_scored_resolver

ANCHOR_YEAR = 2024
RESOLVER_TABLE = "station_uid_resolver"
DB_PATH = SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
DOCS_PATH = SCRIPTS.parents[1] / "docs"


def map_confidence(margin: float) -> str:
    """Map score_margin to confidence tier."""
    if isinstance(margin, float) and math.isnan(margin):
        return "low"
    if margin > 0.10:
        return "high"
    if margin > 0.05:
        return "medium"
    return "low"


def run_resolver(conn: sqlite3.Connection) -> pd.DataFrame:
    """Run the scored resolver against historic_stations and write results."""
    stations = pd.read_sql(
        "SELECT year, tc_number, latitude, longitude, aadt, functional_class "
        "FROM historic_stations",
        conn,
    )

    anchor_df = stations[
        (stations["year"] == ANCHOR_YEAR)
        & stations["latitude"].notna()
        & stations["longitude"].notna()
    ].drop_duplicates(subset=["tc_number"], keep="first").reset_index(drop=True)

    anchor_rows = pd.DataFrame({
        "year": [ANCHOR_YEAR] * len(anchor_df),
        "tc_number": anchor_df["tc_number"].values,
        "station_uid": anchor_df["tc_number"].map(lambda tc: f"GA24_{tc}"),
        "resolver_method": ["anchor"] * len(anchor_df),
        "resolver_delta_m": [0.0] * len(anchor_df),
        "resolver_confidence": ["high"] * len(anchor_df),
    })

    all_results = [anchor_rows]

    non_anchor_years = sorted(
        y for y in stations["year"].unique() if y != ANCHOR_YEAR
    )

    for year in non_anchor_years:
        year_df = stations[
            (stations["year"] == year)
            & stations["latitude"].notna()
            & stations["longitude"].notna()
        ].drop_duplicates(subset=["tc_number"], keep="first").reset_index(drop=True)

        if year_df.empty:
            continue

        resolved = build_scored_resolver(anchor_df, year_df, year)
        resolved["resolver_confidence"] = resolved["score_margin"].map(map_confidence)
        resolved = resolved[["year", "tc_number", "station_uid", "resolver_method",
                             "resolver_delta_m", "resolver_confidence"]]
        all_results.append(resolved)

    result = pd.concat(all_results, ignore_index=True)

    conn.execute(f"DROP TABLE IF EXISTS {RESOLVER_TABLE}")
    result.to_sql(RESOLVER_TABLE, conn, if_exists="replace", index=False)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_resolver_year_tc
        ON {RESOLVER_TABLE} (year, tc_number)
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_resolver_uid
        ON {RESOLVER_TABLE} (station_uid)
    """)
    conn.commit()

    return result


def _build_report(df: pd.DataFrame) -> str:
    lines = [
        "# AADT v2 Station Identity Resolver Report",
        "",
        "## Summary",
        "",
        f"Total rows resolved: {len(df):,}",
        f"Unique station_uids: {df['station_uid'].nunique():,}",
        f"UID format: TC-based (GA24_{{tc_number}})",
        "",
        "## Resolution by year and method",
        "",
        "| Year | Total | Anchor | TC match | Spatial | Scored | TC conflict | Unresolved | Resolve % |",
        "|------|------:|-------:|---------:|--------:|-------:|------------:|-----------:|----------:|",
    ]

    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        total = len(ydf)
        anchor = (ydf["resolver_method"] == "anchor").sum()
        tc = (ydf["resolver_method"] == "tc_number").sum()
        spatial = (ydf["resolver_method"] == "spatial").sum()
        scored = (ydf["resolver_method"] == "scored").sum()
        tc_conflict = (ydf["resolver_method"] == "tc_conflict").sum()
        unresolved = (ydf["resolver_method"] == "unresolved").sum()
        resolved_pct = (total - unresolved) / total * 100 if total > 0 else 0
        lines.append(
            f"| {year} | {total:,} | {anchor:,} | {tc:,} | {spatial:,} | {scored:,} | {tc_conflict:,} | {unresolved:,} | {resolved_pct:.1f}% |"
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

    lines += [
        "",
        "## Acceptance criteria",
        "",
        f"- Resolved rate (non-anchor): {_resolve_rate_non_anchor(df):.1f}% (target: >=95%)",
    ]

    lines.append("")
    return "\n".join(lines)


def _resolve_rate_non_anchor(df: pd.DataFrame) -> float:
    pre_anchor = df[df["year"] != ANCHOR_YEAR]
    if len(pre_anchor) == 0:
        return 100.0
    resolved = (pre_anchor["resolver_method"] != "unresolved").sum()
    return resolved / len(pre_anchor) * 100


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))

    print("Loading historic_stations...")
    total = conn.execute("SELECT COUNT(*) FROM historic_stations").fetchone()[0]
    years = conn.execute("SELECT DISTINCT year FROM historic_stations ORDER BY year").fetchall()
    print(f"  {total:,} rows across {len(years)} years: {[y[0] for y in years]}")

    print("\nResolving station identities (scored resolver)...")
    result = run_resolver(conn)
    print(f"  {len(result):,} rows written to {RESOLVER_TABLE}")

    print("\nGenerating QC report...")
    report = _build_report(result)
    report_path = DOCS_PATH / "aadt_v2_station_identity_resolver.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"Report written to {report_path}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
