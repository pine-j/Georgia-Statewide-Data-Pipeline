"""Generate the AADT 2024 cross-validation QC report.

Compares the two GDOT-published 2024 AADT sources (state 2024 GDB and HPMS
federal submission) using the staged `AADT_2024_OFFICIAL`, `AADT_2024_HPMS`,
`AADT_2024_SOURCE_AGREEMENT`, `AADT_2024_STATS_TYPE`, and
`AADT_2024_CONFIDENCE` columns produced by normalize.py.

Output: `02-Data-Staging/reports/aadt_cross_validation_2024.md`
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = PROJECT_ROOT / "02-Data-Staging" / "databases" / "roadway_inventory.db"
REPORT_PATH = PROJECT_ROOT / "02-Data-Staging" / "reports" / "aadt_cross_validation_2024.md"


def load_segments() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            """
            SELECT
                ROUTE_ID,
                SYSTEM_CODE,
                FUNCTIONAL_CLASS,
                FUNCTIONAL_CLASS_LABEL,
                ROUTE_FAMILY,
                AADT,
                AADT_2024_OFFICIAL,
                AADT_2024_HPMS,
                AADT_2024_SOURCE,
                AADT_2024_SOURCE_AGREEMENT,
                AADT_2024_CONFIDENCE,
                AADT_2024_STATS_TYPE,
                AADT_2024_SAMPLE_STATUS,
                segment_length_mi
            FROM segments
            """,
            conn,
        )


def fmt_pct(num: float, denom: float) -> str:
    if denom == 0:
        return "n/a"
    return f"{(num / denom) * 100.0:.2f}%"


def fmt_int(value) -> str:
    if pd.isna(value):
        return "—"
    return f"{int(value):,}"


def fmt_float(value, digits: int = 2) -> str:
    if pd.isna(value):
        return "—"
    return f"{float(value):,.{digits}f}"


def write_section_agreement(lines: list[str], df: pd.DataFrame) -> None:
    counts = df["AADT_2024_SOURCE_AGREEMENT"].value_counts(dropna=False)
    total = len(df)
    lines.append("## Source agreement buckets\n")
    lines.append("| Bucket | Segments | % of all segments | Miles |")
    lines.append("|---|---:|---:|---:|")
    order = ["state_only", "hpms_only", "both_agree", "both_disagree", "missing"]
    for bucket in order:
        n = int(counts.get(bucket, 0))
        miles = float(df.loc[df["AADT_2024_SOURCE_AGREEMENT"] == bucket, "segment_length_mi"].sum())
        lines.append(f"| `{bucket}` | {n:,} | {fmt_pct(n, total)} | {miles:,.1f} |")
    lines.append("")
    lines.append(f"_Total segments: {total:,}._\n")

    both_agree = int(counts.get("both_agree", 0))
    both_disagree = int(counts.get("both_disagree", 0))
    overlap = both_agree + both_disagree
    if overlap > 0:
        lines.append(
            f"**In-band agreement rate (overlap segments only): "
            f"{both_agree:,} / {overlap:,} = {(both_agree / overlap) * 100.0:.3f}%.**\n"
        )


def write_section_disagreement(lines: list[str], df: pd.DataFrame) -> None:
    disagree = df[df["AADT_2024_SOURCE_AGREEMENT"] == "both_disagree"].copy()
    lines.append("## Disagreement diagnostics\n")
    if disagree.empty:
        lines.append("_No `both_disagree` segments — every overlap is within the agreement band._\n")
        return

    disagree["abs_diff"] = (disagree["AADT_2024_HPMS"] - disagree["AADT_2024_OFFICIAL"]).abs()
    disagree["rel_diff_pct"] = (
        disagree["abs_diff"] / disagree["AADT_2024_OFFICIAL"].where(disagree["AADT_2024_OFFICIAL"].abs() > 0)
    ) * 100.0
    disagree["abs_diff_miles"] = disagree["abs_diff"] * disagree["segment_length_mi"]

    n = len(disagree)
    overlap_total = int((df["AADT_2024_SOURCE_AGREEMENT"].isin(["both_agree", "both_disagree"])).sum())
    lines.append(
        f"- `both_disagree` segments: **{n:,}** "
        f"({fmt_pct(n, overlap_total)} of overlap segments, "
        f"{fmt_pct(n, len(df))} of all segments)."
    )
    lines.append(
        f"- Median relative difference: **{disagree['rel_diff_pct'].median():.1f}%** | "
        f"Mean: **{disagree['rel_diff_pct'].mean():.1f}%**."
    )
    lines.append(
        f"- Median absolute difference: **{disagree['abs_diff'].median():,.0f} veh/day** | "
        f"Mean: **{disagree['abs_diff'].mean():,.0f} veh/day**."
    )
    lines.append("")

    lines.append("### Top 20 largest divergences (by abs_diff × miles)\n")
    lines.append("| ROUTE_ID | SYSTEM_CODE | FC | Family | State AADT | HPMS AADT | abs_diff | rel_diff | miles |")
    lines.append("|---|---:|---:|---|---:|---:|---:|---:|---:|")
    top = disagree.sort_values("abs_diff_miles", ascending=False).head(20)
    for row in top.itertuples(index=False):
        lines.append(
            f"| `{row.ROUTE_ID}` | {row.SYSTEM_CODE} | {fmt_int(row.FUNCTIONAL_CLASS)} | "
            f"{row.ROUTE_FAMILY or '—'} | {fmt_int(row.AADT_2024_OFFICIAL)} | "
            f"{fmt_int(row.AADT_2024_HPMS)} | {fmt_int(row.abs_diff)} | "
            f"{fmt_float(row.rel_diff_pct, 1)}% | {fmt_float(row.segment_length_mi, 2)} |"
        )
    lines.append("")

    lines.append("### Histogram of relative differences\n")
    bins = [0, 25, 50, 100, 250, 500, 1000, float("inf")]
    labels = [
        "0-25%", "25-50%", "50-100%", "100-250%", "250-500%", "500-1000%", "≥1000%",
    ]
    rel = disagree["rel_diff_pct"].dropna()
    if rel.empty:
        lines.append("_No relative differences computable (all state AADT values were zero)._\n")
    else:
        binned = pd.cut(rel, bins=bins, labels=labels, right=False, include_lowest=True)
        hist = binned.value_counts().reindex(labels, fill_value=0)
        lines.append("| Rel diff bucket | Segments |")
        lines.append("|---|---:|")
        for label, count in hist.items():
            lines.append(f"| {label} | {int(count):,} |")
        lines.append("")


def write_section_confidence_overall(lines: list[str], df: pd.DataFrame) -> None:
    lines.append("## Confidence tier — overall\n")
    counts = df["AADT_2024_CONFIDENCE"].value_counts(dropna=False)
    total = len(df)
    lines.append("| Tier | Segments | % | Miles |")
    lines.append("|---|---:|---:|---:|")
    for tier in ["high", "medium", "low", "missing"]:
        n = int(counts.get(tier, 0))
        miles = float(df.loc[df["AADT_2024_CONFIDENCE"] == tier, "segment_length_mi"].sum())
        lines.append(f"| `{tier}` | {n:,} | {fmt_pct(n, total)} | {miles:,.1f} |")
    lines.append("")


def write_section_confidence_by_fc(lines: list[str], df: pd.DataFrame) -> None:
    lines.append("## Confidence tier — by functional class\n")
    lines.append("| FC | FC label | Total | high | medium | low | missing |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|")
    work = df.copy()
    work["FUNCTIONAL_CLASS"] = pd.to_numeric(work["FUNCTIONAL_CLASS"], errors="coerce")
    grouped = work.groupby(["FUNCTIONAL_CLASS", "FUNCTIONAL_CLASS_LABEL"], dropna=False)
    rows = []
    for (fc, label), sub in grouped:
        counts = sub["AADT_2024_CONFIDENCE"].value_counts(dropna=False)
        rows.append({
            "fc": fc,
            "label": label or "—",
            "total": len(sub),
            "high": int(counts.get("high", 0)),
            "medium": int(counts.get("medium", 0)),
            "low": int(counts.get("low", 0)),
            "missing": int(counts.get("missing", 0)),
        })
    rows.sort(key=lambda r: (r["fc"] if pd.notna(r["fc"]) else 99, r["label"]))
    for r in rows:
        fc_disp = "—" if pd.isna(r["fc"]) else f"{int(r['fc'])}"
        lines.append(
            f"| {fc_disp} | {r['label']} | {r['total']:,} | "
            f"{r['high']:,} | {r['medium']:,} | {r['low']:,} | {r['missing']:,} |"
        )
    lines.append("")


def write_section_confidence_by_system(lines: list[str], df: pd.DataFrame) -> None:
    lines.append("## Confidence tier — by SYSTEM_CODE\n")
    lines.append("| SYSTEM_CODE | Total | high | medium | low | missing |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    grouped = df.groupby(df["SYSTEM_CODE"].astype(str), dropna=False)
    for sc, sub in grouped:
        counts = sub["AADT_2024_CONFIDENCE"].value_counts(dropna=False)
        lines.append(
            f"| {sc} | {len(sub):,} | "
            f"{int(counts.get('high', 0)):,} | "
            f"{int(counts.get('medium', 0)):,} | "
            f"{int(counts.get('low', 0)):,} | "
            f"{int(counts.get('missing', 0)):,} |"
        )
    lines.append("")


def write_section_stats_type(lines: list[str], df: pd.DataFrame) -> None:
    lines.append("## Statistics_Type breakdown (state 2024 GDB)\n")
    state_matched = df[df["AADT_2024_OFFICIAL"].notna()]
    counts = state_matched["AADT_2024_STATS_TYPE"].value_counts(dropna=False)
    lines.append(
        f"_Segments with a state 2024 GDB AADT match: **{len(state_matched):,}**. "
        f"`Statistics_Type` only applies to state-GDB records; non-state rows are excluded._\n"
    )
    lines.append("| Statistics_Type | Segments |")
    lines.append("|---|---:|")
    for value, n in counts.items():
        label = value if pd.notna(value) else "_(none)_"
        lines.append(f"| {label} | {int(n):,} |")
    lines.append("")


def write_section_coverage_uplift(lines: list[str], df: pd.DataFrame) -> None:
    lines.append("## Coverage uplift summary\n")
    total = len(df)
    has_state = int(df["AADT_2024_OFFICIAL"].notna().sum())
    has_hpms = int(df["AADT_2024_HPMS"].notna().sum())
    has_either = int((df["AADT_2024_OFFICIAL"].notna() | df["AADT_2024_HPMS"].notna()).sum())
    has_canonical = int(df["AADT"].notna().sum())
    lines.append(f"- Total segments: **{total:,}**.")
    lines.append(f"- Has state 2024 GDB AADT: **{has_state:,}** ({fmt_pct(has_state, total)}).")
    lines.append(f"- Has HPMS 2024 AADT: **{has_hpms:,}** ({fmt_pct(has_hpms, total)}).")
    lines.append(f"- Has at least one GDOT-published source: **{has_either:,}** ({fmt_pct(has_either, total)}).")
    lines.append(f"- Final canonical AADT populated (incl. derived): **{has_canonical:,}** ({fmt_pct(has_canonical, total)}).")
    lines.append("")
    src_counts = df["AADT_2024_SOURCE"].value_counts(dropna=False)
    lines.append("### Canonical AADT source distribution\n")
    lines.append("| AADT_2024_SOURCE | Segments | % |")
    lines.append("|---|---:|---:|")
    for source, n in src_counts.items():
        lines.append(f"| `{source}` | {int(n):,} | {fmt_pct(int(n), total)} |")
    lines.append("")


def main() -> None:
    df = load_segments()
    lines: list[str] = []
    lines.append("# AADT 2024 cross-validation report\n")
    lines.append(
        "Compares the two GDOT-published 2024 AADT sources (state 2024 GDB and "
        "HPMS federal submission) and reports the new confidence tiers derived "
        "in the `aadt-2024-hygiene` pass."
    )
    lines.append("")
    lines.append(
        "_Generated from `02-Data-Staging/databases/roadway_inventory.db` "
        "after running `normalize.py`._\n"
    )

    write_section_coverage_uplift(lines, df)
    write_section_agreement(lines, df)
    write_section_disagreement(lines, df)
    write_section_confidence_overall(lines, df)
    write_section_confidence_by_system(lines, df)
    write_section_confidence_by_fc(lines, df)
    write_section_stats_type(lines, df)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
