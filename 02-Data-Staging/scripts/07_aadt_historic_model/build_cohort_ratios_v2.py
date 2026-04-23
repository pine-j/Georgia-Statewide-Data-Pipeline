"""Orchestrator: rebuild cohort_ratios from Actual station AADTs (v2).

Reads historic_stations (Actual only) + station_uid_resolver +
segment attributes, builds cohort ratios with 6 versions (full + 5 folds),
writes to cohort_ratios_v2 table.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cohort_ratios import fc_bin_for, urban_rural_for
from cohort_ratios_v2 import V2_VERSIONS, build_all_v2_versions

logger = logging.getLogger(__name__)

DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
TABLE_NAME = "cohort_ratios_v2"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    logger.info("Loading Actual stations...")
    stations = pd.read_sql(
        "SELECT year, tc_number, aadt FROM historic_stations WHERE statistics_type = 'Actual'",
        conn,
    )
    logger.info("  %d Actual station-year rows", len(stations))

    logger.info("Loading resolver...")
    resolver = pd.read_sql("SELECT year, tc_number, station_uid FROM station_uid_resolver", conn)

    logger.info("Loading segment attributes...")
    seg_attrs = pd.read_sql("SELECT unique_id, FUNCTIONAL_CLASS, URBAN_CODE, DISTRICT FROM segments", conn)

    logger.info("Loading k=1 station-segment links for cohort assignment...")
    knn_k1 = pd.read_sql(
        "SELECT year, nearest_tc_number AS tc_number, unique_id FROM segment_station_link_knn WHERE k_rank = 1",
        conn,
    )

    logger.info("Building station cohort assignments...")
    merged = stations.merge(
        resolver[["year", "tc_number", "station_uid"]],
        on=["year", "tc_number"],
        how="left",
    )

    station_to_seg = (
        merged[["year", "tc_number"]]
        .drop_duplicates()
        .merge(knn_k1, on=["year", "tc_number"], how="left")
    )
    station_to_seg = station_to_seg.merge(
        seg_attrs, on="unique_id", how="left"
    )

    attr_map = {}
    for _, row in station_to_seg.iterrows():
        key = (row["year"], row["tc_number"])
        attr_map[key] = {
            "fc_bin": fc_bin_for(row.get("FUNCTIONAL_CLASS")),
            "urban_rural": urban_rural_for(row.get("URBAN_CODE")),
            "district": row.get("DISTRICT"),
        }

    merged["fc_bin"] = merged.apply(lambda r: attr_map.get((r["year"], r["tc_number"]), {}).get("fc_bin"), axis=1)
    merged["urban_rural"] = merged.apply(lambda r: attr_map.get((r["year"], r["tc_number"]), {}).get("urban_rural"), axis=1)
    merged["district"] = merged.apply(lambda r: attr_map.get((r["year"], r["tc_number"]), {}).get("district"), axis=1)

    unlinked = merged["station_uid"].isna().sum()
    if unlinked > 0:
        logger.warning("  %d station rows could not be linked to a station_uid (dropped)", unlinked)
        merged = merged.dropna(subset=["station_uid"])

    no_fc = merged["fc_bin"].isna().sum()
    logger.info("  %d rows with fc_bin, %d without (NULL FC segments)", len(merged) - no_fc, no_fc)

    logger.info("Building all 6 cohort ratio versions...")
    cohort_df = build_all_v2_versions(merged)
    logger.info("  %d cohort ratio rows across %d versions", len(cohort_df), cohort_df["version"].nunique())

    logger.info("Writing to %s...", TABLE_NAME)
    cohort_df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    conn.commit()

    for version in V2_VERSIONS:
        vdf = cohort_df[cohort_df["version"] == version]
        fallback_rate = vdf["cohort_fallback_used"].mean() * 100
        logger.info("  %s: %d cells, %.1f%% fallback rate", version, len(vdf), fallback_rate)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
