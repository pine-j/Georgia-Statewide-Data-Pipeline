"""Regression fixture for evacuation-route flagging.

Runs in seconds against the staged base_network.gpkg and the per-stage
snapshots written by apply_evacuation_enrichment(). Pins representative
true-positives and false-positives that previous filter/mirror iterations
have established.

The per-stage snapshots let a failure be localized to the exact stage
that regressed it (matcher+filter / contraflow / split / direction mirror).

Usage:
    python 02-Data-Staging/tests/test_evac_flagging.py

Exits 0 on pass, 1 on any failure. Prints a per-case table and, for any
failure, a per-stage trace.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GPKG_PATH = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"
FIXTURE_PATH = Path(__file__).resolve().parent / "evac_flagging_fixture.csv"
SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshots"

STAGE_LABELS = [
    "01_after_matcher_and_filter",
    "02_after_contraflow",
    "03_after_split",
    "04_after_direction_mirror",
]

# Per-corridor bounds: (min, max). A corridor sliding outside these bounds
# is a regression signal. Tuned against the post-filter, post-mirror baseline.
CORRIDOR_BOUNDS: dict[str, tuple[int, int]] = {
    "I 75 North": (400, 550),
    "SR 1/US 27": (320, 440),
    "SR 3": (240, 340),
    "SR 520": (180, 280),
    "SR 26": (170, 240),
    "I 75 South": (30, 90),
}


def load_gpkg() -> gpd.GeoDataFrame:
    if not GPKG_PATH.exists():
        print(f"FAIL: GPKG not found at {GPKG_PATH}", file=sys.stderr)
        sys.exit(2)
    return gpd.read_file(GPKG_PATH, layer="roadway_segments")


def load_fixture() -> list[dict]:
    with FIXTURE_PATH.open(newline="") as f:
        return list(csv.DictReader(f))


def load_snapshots() -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    if not SNAPSHOT_DIR.exists():
        return out
    for label in STAGE_LABELS:
        path = SNAPSHOT_DIR / f"{label}.parquet"
        if path.exists():
            out[label] = pd.read_parquet(path)
    return out


def row_match(df: pd.DataFrame, hwy: str, county: str, suffix: str) -> pd.DataFrame:
    mask = (df["HWY_NAME"] == hwy) & (df["COUNTY_NAME"] == county)
    if suffix and "ROUTE_ID" in df.columns:
        mask &= df["ROUTE_ID"].astype("string").str.endswith(suffix, na=False)
    return df[mask]


def check_case(gdf: pd.DataFrame, case: dict) -> tuple[bool, str]:
    hwy = case["hwy_name"]
    county = case["county"]
    suffix = case.get("route_id_suffix", "") or ""
    expected = case["expected_sec_evac"].strip().lower() == "true"

    matches = row_match(gdf, hwy, county, suffix)
    if matches.empty:
        return False, f"no rows found for {hwy}/{county}/{suffix or '*'}"

    any_flagged = bool(matches["SEC_EVAC"].fillna(False).any())
    if expected and not any_flagged:
        return False, f"expected flagged but {len(matches)} rows, 0 SEC_EVAC=True"
    if not expected and any_flagged:
        flagged_count = int(matches["SEC_EVAC"].fillna(False).sum())
        return False, f"expected NOT flagged but {flagged_count}/{len(matches)} SEC_EVAC=True"
    return True, f"{len(matches)} rows, flagged={any_flagged}"


def stage_trace(snapshots: dict[str, pd.DataFrame], case: dict) -> list[tuple[str, str]]:
    """Return (stage, flagged_count/row_count) for each available stage."""
    hwy = case["hwy_name"]
    county = case["county"]
    suffix = case.get("route_id_suffix", "") or ""
    trace = []
    for label in STAGE_LABELS:
        snap = snapshots.get(label)
        if snap is None:
            trace.append((label, "missing"))
            continue
        rows = row_match(snap, hwy, county, suffix)
        if rows.empty:
            trace.append((label, "no-rows"))
            continue
        flagged = int(rows["SEC_EVAC"].fillna(False).sum())
        trace.append((label, f"{flagged}/{len(rows)} flagged"))
    return trace


def check_corridor_bounds(gdf: pd.DataFrame) -> list[tuple[str, bool, str]]:
    counts: dict[str, int] = {}
    named = gdf.loc[
        gdf["SEC_EVAC"].fillna(False) & gdf["SEC_EVAC_ROUTE_NAME"].notna(),
        "SEC_EVAC_ROUTE_NAME",
    ]
    for val in named:
        for name in str(val).split("; "):
            name = name.strip()
            if name:
                counts[name] = counts.get(name, 0) + 1

    results = []
    for corridor, (lo, hi) in CORRIDOR_BOUNDS.items():
        actual = counts.get(corridor, 0)
        ok = lo <= actual <= hi
        results.append((corridor, ok, f"{actual} (bounds [{lo}, {hi}])"))
    return results


def main() -> int:
    gdf = load_gpkg()
    fixture = load_fixture()
    snapshots = load_snapshots()

    print(f"Loaded GPKG: {len(gdf)} rows, SEC_EVAC=True: {int(gdf['SEC_EVAC'].sum())}")
    if snapshots:
        print(f"Loaded {len(snapshots)}/{len(STAGE_LABELS)} stage snapshots")
    else:
        print("WARNING: no per-stage snapshots found — run the pipeline to generate them")
    print(f"Running {len(fixture)} case(s) from {FIXTURE_PATH.name}\n")

    failures = 0
    failed_cases: list[dict] = []
    print(f"{'CATEGORY':<16} {'HWY':<10} {'COUNTY':<14} {'SUF':<4} {'EXP':<5} {'RESULT'}")
    print("-" * 100)
    for case in fixture:
        ok, detail = check_case(gdf, case)
        status = "PASS" if ok else "FAIL"
        print(
            f"{case['category']:<16} {case['hwy_name']:<10} {case['county']:<14} "
            f"{(case.get('route_id_suffix') or '-'):<4} {case['expected_sec_evac']:<5} {status}: {detail}"
        )
        if not ok:
            failures += 1
            failed_cases.append(case)

    # Per-stage trace for any failure — localizes which stage owns the regression.
    if failed_cases and snapshots:
        print()
        print("Per-stage trace for failed cases:")
        for case in failed_cases:
            key = f"{case['hwy_name']}/{case['county']}/{case.get('route_id_suffix') or '*'}"
            print(f"  {key} (expected SEC_EVAC={case['expected_sec_evac']}):")
            for label, detail in stage_trace(snapshots, case):
                print(f"    {label:<32} {detail}")

    print()
    print("Per-corridor count bounds:")
    print(f"{'CORRIDOR':<20} {'RESULT'}")
    print("-" * 80)
    for corridor, ok, detail in check_corridor_bounds(gdf):
        status = "PASS" if ok else "FAIL"
        print(f"{corridor:<20} {status}: {detail}")
        if not ok:
            failures += 1

    print()
    if failures:
        print(f"FAILED: {failures} regression(s) detected")
        return 1
    print("OK: all cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
