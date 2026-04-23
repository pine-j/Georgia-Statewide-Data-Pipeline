"""Phase 1b pipeline: apply GRIP, EPZ, sole-connection, NHFN gap-fill, and SRP derivation.

Usage:
    python run_phase1b.py                # full run (requires HPMS 2024 JSON for NHFN)
    python run_phase1b.py --skip-hpms    # skip NHFN gap-fill if HPMS data unavailable

Reads segments from base_network.gpkg, applies each enrichment in sequence,
writes new columns back to roadway_inventory.db and base_network.gpkg,
and produces summary reports.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from grip_corridors import apply_grip_enrichment
from nuclear_epz import apply_nuclear_epz_enrichment
from sole_county_seat_connections import apply_sole_county_seat_enrichment
from srp_derivation import derive_srp_priority, write_srp_derivation_summary

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
GPKG_PATH = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"
DB_PATH = PROJECT_ROOT / "02-Data-Staging" / "databases" / "roadway_inventory.db"
REPORTS_DIR = PROJECT_ROOT / "02-Data-Staging" / "reports"

PHASE1B_COLUMNS = [
    "NHFN", "STRAHNET_TYPE",
    "IS_GRIP_CORRIDOR", "GRIP_CORRIDOR_NAME",
    "IS_NUCLEAR_EPZ_ROUTE", "NUCLEAR_EPZ_PLANT",
    "IS_SOLE_COUNTY_SEAT_CONNECTION", "SOLE_CONNECTION_COUNTY_SEAT",
    "SRP_DERIVED", "SRP_DERIVED_REASONS",
]


def load_segments() -> gpd.GeoDataFrame:
    LOGGER.info("Loading segments from %s ...", GPKG_PATH)
    gdf = gpd.read_file(GPKG_PATH, layer="roadway_segments", engine="pyogrio")
    LOGGER.info("Loaded %d segments with %d columns", len(gdf), len(gdf.columns))
    return gdf


def apply_hpms_nhfn_gap_fill(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Targeted gap-fill for NHFN and STRAHNET_TYPE from HPMS 2024."""
    from hpms_enrichment import load_hpms_data, _build_hpms_lookup, _find_best_hpms_match

    hpms = load_hpms_data()
    if hpms.empty:
        LOGGER.warning("HPMS data not available — NHFN/STRAHNET_TYPE will remain null")
        if "NHFN" not in gdf.columns:
            gdf["NHFN"] = None
        if "STRAHNET_TYPE" not in gdf.columns:
            gdf["STRAHNET_TYPE"] = None
        return gdf

    lookup = _build_hpms_lookup(hpms)

    if "NHFN" not in gdf.columns:
        gdf["NHFN"] = None
    if "STRAHNET_TYPE" not in gdf.columns:
        gdf["STRAHNET_TYPE"] = None

    nhfn_count = 0
    strahnet_type_count = 0
    matched = 0

    for idx in gdf.index:
        route_id = str(gdf.at[idx, "ROUTE_ID"]) if pd.notna(gdf.at[idx, "ROUTE_ID"]) else ""
        if not route_id or route_id == "nan":
            continue

        match = _find_best_hpms_match(
            route_id,
            gdf.at[idx, "FROM_MILEPOINT"] if "FROM_MILEPOINT" in gdf.columns else None,
            gdf.at[idx, "TO_MILEPOINT"] if "TO_MILEPOINT" in gdf.columns else None,
            lookup,
        )
        if match is None:
            continue
        matched += 1

        if pd.isna(gdf.at[idx, "NHFN"]):
            val = match.get("nhfn")
            if val is not None and not pd.isna(val):
                try:
                    gdf.at[idx, "NHFN"] = int(val)
                    nhfn_count += 1
                except (ValueError, TypeError):
                    pass

        if pd.isna(gdf.at[idx, "STRAHNET_TYPE"]):
            val = match.get("strahnet_type")
            if val is not None and not pd.isna(val):
                try:
                    gdf.at[idx, "STRAHNET_TYPE"] = int(val)
                    strahnet_type_count += 1
                except (ValueError, TypeError):
                    pass

    LOGGER.info(
        "NHFN gap-fill: %d matched, %d NHFN filled, %d STRAHNET_TYPE filled",
        matched, nhfn_count, strahnet_type_count,
    )
    return gdf


def update_database(gdf: gpd.GeoDataFrame) -> None:
    """Write Phase 1b columns back to the SQLite database."""
    LOGGER.info("Updating database at %s ...", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(segments)")
        existing_cols = {row[1] for row in cur.fetchall()}

        for col in PHASE1B_COLUMNS:
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE segments ADD COLUMN [{col}]")
                LOGGER.info("  Added column: %s", col)

        uid_col = "unique_id" if "unique_id" in gdf.columns else None
        if uid_col is None:
            LOGGER.error("No unique_id column — cannot map updates to DB rows")
            return

        update_cols = [c for c in PHASE1B_COLUMNS if c in gdf.columns]
        set_clause = ", ".join(f"[{c}] = ?" for c in update_cols)
        sql = f"UPDATE segments SET {set_clause} WHERE unique_id = ?"

        batch = []
        for idx in gdf.index:
            values = []
            for c in update_cols:
                val = gdf.at[idx, c]
                if pd.isna(val):
                    values.append(None)
                elif isinstance(val, (bool, np.bool_)):
                    values.append(int(val))
                else:
                    values.append(val)
            values.append(gdf.at[idx, uid_col])
            batch.append(tuple(values))

        cur.executemany(sql, batch)
        conn.commit()
        LOGGER.info("Updated %d rows in segments table", len(batch))
    finally:
        conn.close()


def update_gpkg(gdf: gpd.GeoDataFrame) -> None:
    """Overwrite the roadway_segments layer in the GPKG with enriched data."""
    LOGGER.info("Updating GPKG at %s ...", GPKG_PATH)
    gdf.to_file(GPKG_PATH, layer="roadway_segments", driver="GPKG", engine="pyogrio", mode="w")
    LOGGER.info("GPKG roadway_segments layer updated with %d segments", len(gdf))


def write_phase1b_summary(gdf: gpd.GeoDataFrame) -> None:
    """Write a Phase 1b enrichment summary report."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    summary = {
        "total_segments": len(gdf),
        "nhfn_non_null": int(gdf["NHFN"].notna().sum()) if "NHFN" in gdf.columns else 0,
        "strahnet_type_non_null": int(gdf["STRAHNET_TYPE"].notna().sum()) if "STRAHNET_TYPE" in gdf.columns else 0,
        "grip_flagged": int(gdf["IS_GRIP_CORRIDOR"].sum()) if "IS_GRIP_CORRIDOR" in gdf.columns else 0,
        "epz_flagged": int(gdf["IS_NUCLEAR_EPZ_ROUTE"].sum()) if "IS_NUCLEAR_EPZ_ROUTE" in gdf.columns else 0,
        "sole_connection_flagged": int(gdf["IS_SOLE_COUNTY_SEAT_CONNECTION"].sum()) if "IS_SOLE_COUNTY_SEAT_CONNECTION" in gdf.columns else 0,
    }

    if "SRP_DERIVED" in gdf.columns:
        summary["srp_tier_counts"] = gdf["SRP_DERIVED"].value_counts().to_dict()
        summary["srp_null_count"] = int(gdf["SRP_DERIVED"].isna().sum())

    output = REPORTS_DIR / "phase1b_enrichment_summary.json"
    output.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    LOGGER.info("Wrote Phase 1b summary to %s", output)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    skip_hpms = "--skip-hpms" in sys.argv

    t0 = time.time()

    gdf = load_segments()

    LOGGER.info("=" * 60)
    LOGGER.info("Step 1/5: NHFN / STRAHNET_TYPE gap-fill from HPMS 2024")
    LOGGER.info("=" * 60)
    if skip_hpms:
        LOGGER.info("Skipping HPMS gap-fill (--skip-hpms flag)")
        if "NHFN" not in gdf.columns:
            gdf["NHFN"] = None
        if "STRAHNET_TYPE" not in gdf.columns:
            gdf["STRAHNET_TYPE"] = None
    else:
        gdf = apply_hpms_nhfn_gap_fill(gdf)

    LOGGER.info("=" * 60)
    LOGGER.info("Step 2/5: GRIP corridor enrichment")
    LOGGER.info("=" * 60)
    gdf = apply_grip_enrichment(gdf)

    LOGGER.info("=" * 60)
    LOGGER.info("Step 3/5: Nuclear EPZ route enrichment")
    LOGGER.info("=" * 60)
    gdf = apply_nuclear_epz_enrichment(gdf, write_buffers=True)

    LOGGER.info("=" * 60)
    LOGGER.info("Step 4/5: Sole county-seat connection analysis")
    LOGGER.info("=" * 60)
    gdf = apply_sole_county_seat_enrichment(gdf)

    LOGGER.info("=" * 60)
    LOGGER.info("Step 5/5: SRP derivation")
    LOGGER.info("=" * 60)
    gdf = derive_srp_priority(gdf)

    LOGGER.info("=" * 60)
    LOGGER.info("Writing results ...")
    LOGGER.info("=" * 60)

    write_srp_derivation_summary(gdf)
    write_phase1b_summary(gdf)
    update_database(gdf)
    update_gpkg(gdf)

    elapsed = time.time() - t0
    LOGGER.info("Phase 1b pipeline complete in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
