"""Build station_training_rows table — the pre-joined training dataset.

One row per (station_uid, year) where statistics_type='Actual' and
year in {2020, 2022, 2023, 2024}. Each row has all features from
plan §Feature construction:

1. Station-on-segment attributes (from nearest segment)
2. k-nearest-station geometry (distances, same_route, same_fc)
3. k-nearest-station attributes (AADTs, k_factor, etc.)
4. Cohort-ratio features
5. Temporal features

2021 is excluded from training (used for inference only).
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

from cohort_ratios import fc_bin_for, urban_rural_for
from cohort_ratios_v2 import assign_station_folds, N_FOLDS

logger = logging.getLogger(__name__)

DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
TRAINING_YEARS = [2020, 2022, 2023, 2024]
KNN_K = 5

SEGMENT_FEATURE_COLS = [
    "FUNCTIONAL_CLASS", "SYSTEM_CODE", "FACILITY_TYPE", "NUM_LANES",
    "LANE_WIDTH", "MEDIAN_TYPE", "SPEED_LIMIT", "URBAN_CODE", "NHS_IND",
    "ROUTE_TYPE_GDOT", "K_FACTOR", "D_FACTOR", "segment_length_mi",
    "DISTRICT", "COUNTY_ID",
]

COVID_PHASE_WEIGHT = {2020: 1.0, 2021: 0.6, 2022: 0.15, 2023: 0.05, 2024: 0.0}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    logger.info("Loading Actual stations for training years...")
    stations = pd.read_sql(
        f"SELECT year, tc_number, aadt, k_factor, d_factor, functional_class AS station_fc, "
        f"station_type, traffic_class, statistics_type "
        f"FROM historic_stations "
        f"WHERE statistics_type = 'Actual' AND year IN ({','.join(str(y) for y in TRAINING_YEARS)})",
        conn,
    )
    logger.info("  %d Actual rows", len(stations))

    logger.info("Loading resolver...")
    resolver = pd.read_sql("SELECT year, tc_number, station_uid FROM station_uid_resolver", conn)
    stations = stations.merge(resolver, on=["year", "tc_number"], how="left")
    stations = stations.dropna(subset=["station_uid"])
    stations = stations.drop_duplicates(subset=["station_uid", "year"], keep="first")
    logger.info("  %d rows with station_uid (deduped)", len(stations))

    logger.info("Assigning station folds...")
    stations["fold"] = assign_station_folds(stations["station_uid"]).values

    logger.info("Loading segment attributes...")
    seg_cols_str = ", ".join(["unique_id"] + SEGMENT_FEATURE_COLS)
    segments = pd.read_sql(f"SELECT {seg_cols_str} FROM segments", conn)

    logger.info("Loading k-NN links...")
    knn = pd.read_sql(
        f"SELECT unique_id, year, k_rank, nearest_tc_number, station_distance_m, "
        f"same_route_flag FROM segment_station_link_knn "
        f"WHERE year IN ({','.join(str(y) for y in TRAINING_YEARS)})",
        conn,
    )

    logger.info("Finding nearest segment for each station-year...")
    knn_k1 = knn[knn["k_rank"] == 1][["year", "nearest_tc_number", "unique_id"]].copy()
    knn_k1 = knn_k1.rename(columns={"nearest_tc_number": "tc_number", "unique_id": "nearest_segment_uid"})
    knn_k1 = knn_k1.drop_duplicates(subset=["year", "tc_number"], keep="first")

    stations = stations.merge(knn_k1, on=["year", "tc_number"], how="left")
    stations = stations.rename(columns={
        "k_factor": "station_k_factor",
        "d_factor": "station_d_factor",
    })
    stations = stations.merge(segments, left_on="nearest_segment_uid", right_on="unique_id", how="left", suffixes=("", "_seg"))
    logger.info("  %d rows after segment join", len(stations))

    logger.info("Building k-NN station features...")
    all_stations_for_knn = pd.read_sql(
        f"SELECT year, tc_number, aadt, k_factor, d_factor, functional_class, "
        f"station_type, traffic_class, statistics_type "
        f"FROM historic_stations WHERE year IN ({','.join(str(y) for y in TRAINING_YEARS)})",
        conn,
    )

    knn_features = _build_knn_features(stations, knn, all_stations_for_knn, knn_k1)
    stations = stations.merge(knn_features, on=["station_uid", "year"], how="left")
    logger.info("  %d rows after k-NN features", len(stations))

    logger.info("Adding cohort ratio features...")
    cohort_ratios = pd.read_sql("SELECT * FROM cohort_ratios_v2 WHERE version = 'full'", conn)
    stations["fc_bin"] = stations["FUNCTIONAL_CLASS"].map(fc_bin_for)
    stations["urban_rural"] = stations["URBAN_CODE"].map(urban_rural_for)

    cohort_join = cohort_ratios[["fc_bin", "urban_rural", "district",
                                "cohort_median_2020_actual", "cohort_ratio_2020_to_2022",
                                "cohort_ratio_2020_to_2024", "cohort_size"]].copy()
    stations = stations.merge(
        cohort_join,
        left_on=["fc_bin", "urban_rural", "DISTRICT"],
        right_on=["fc_bin", "urban_rural", "district"],
        how="left",
    )
    stations = stations.drop(columns=["district"], errors="ignore")

    logger.info("Adding temporal features...")
    stations["years_since_2020"] = stations["year"] - 2020
    stations["covid_phase_weight"] = stations["year"].map(COVID_PHASE_WEIGHT)

    logger.info("Adding target...")
    stations["target"] = np.log(stations["aadt"].clip(lower=1))

    logger.info("Adding sample weight...")
    is_covid_urban = (
        (stations["year"] == 2020)
        & (stations["FUNCTIONAL_CLASS"].isin([1.0, 2.0, 3.0]))
        & (stations["urban_rural"] == "urban")
    )
    stations["sample_weight"] = 1.0
    stations.loc[is_covid_urban, "sample_weight"] = 0.5

    drop_cols = [c for c in stations.columns if c.endswith("_seg")]
    if "unique_id" in stations.columns:
        drop_cols.append("unique_id")
    stations = stations.drop(columns=drop_cols, errors="ignore")

    dupes = stations.columns[stations.columns.duplicated()].unique().tolist()
    if dupes:
        logger.warning("Dropping duplicate columns: %s", dupes)
        stations = stations.loc[:, ~stations.columns.duplicated()]

    logger.info("Writing station_training_rows table (%d columns)...", len(stations.columns))
    stations.to_sql("station_training_rows", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_training_uid ON station_training_rows(station_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_training_fold ON station_training_rows(fold)")
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM station_training_rows").fetchone()[0]
    per_year = conn.execute(
        "SELECT year, COUNT(*) FROM station_training_rows GROUP BY year ORDER BY year"
    ).fetchall()
    logger.info("  %d total rows", total)
    for year, cnt in per_year:
        logger.info("  Year %d: %d rows", year, cnt)

    conn.close()
    logger.info("Done.")


def _build_knn_features(
    training_stations: pd.DataFrame,
    knn: pd.DataFrame,
    all_stations: pd.DataFrame,
    knn_k1: pd.DataFrame,
) -> pd.DataFrame:
    """Build k-NN station features for each training station."""

    training_with_seg = training_stations[["station_uid", "year", "tc_number", "nearest_segment_uid"]].copy()

    seg_knn = knn.merge(
        training_with_seg[["tc_number", "year", "nearest_segment_uid", "station_uid"]].drop_duplicates(),
        left_on=["unique_id", "year"],
        right_on=["nearest_segment_uid", "year"],
        how="inner",
    )

    seg_knn = seg_knn[seg_knn["nearest_tc_number"] != seg_knn["tc_number"]]

    seg_knn = seg_knn.sort_values(["station_uid", "year", "station_distance_m"])
    seg_knn["new_rank"] = seg_knn.groupby(["station_uid", "year"]).cumcount() + 1
    seg_knn = seg_knn[seg_knn["new_rank"] <= KNN_K]

    station_attrs = all_stations.set_index(["year", "tc_number"])

    result_rows = []
    for (uid, year), group in seg_knn.groupby(["station_uid", "year"]):
        row_data = {"station_uid": uid, "year": year}

        actual_group = group.copy()
        for i in range(KNN_K):
            rank = i + 1
            if i < len(actual_group):
                r = actual_group.iloc[i]
                tc = r["nearest_tc_number"]
                row_data[f"knn_distance_{rank}"] = r["station_distance_m"]
                row_data[f"knn_same_route_{rank}"] = r["same_route_flag"]

                key = (year, tc)
                if key in station_attrs.index:
                    sa = station_attrs.loc[key]
                    if isinstance(sa, pd.DataFrame):
                        sa = sa.iloc[0]
                    row_data[f"knn_aadt_{rank}"] = sa.get("aadt")
                    row_data[f"knn_k_factor_{rank}"] = sa.get("k_factor")
                    row_data[f"knn_d_factor_{rank}"] = sa.get("d_factor")
                    row_data[f"knn_fc_{rank}"] = sa.get("functional_class")
                    row_data[f"knn_station_type_{rank}"] = sa.get("station_type")
                    row_data[f"knn_traffic_class_{rank}"] = sa.get("traffic_class")
                    row_data[f"knn_is_actual_{rank}"] = 1 if sa.get("statistics_type") == "Actual" else 0
                else:
                    for col in ["knn_aadt", "knn_k_factor", "knn_d_factor", "knn_fc",
                                "knn_station_type", "knn_traffic_class", "knn_is_actual"]:
                        row_data[f"{col}_{rank}"] = None
            else:
                row_data[f"knn_distance_{rank}"] = None
                row_data[f"knn_same_route_{rank}"] = None
                for col in ["knn_aadt", "knn_k_factor", "knn_d_factor", "knn_fc",
                            "knn_station_type", "knn_traffic_class", "knn_is_actual"]:
                    row_data[f"{col}_{rank}"] = None

        dists = [row_data.get(f"knn_distance_{r+1}") for r in range(KNN_K)]
        valid_dists = [d for d in dists if d is not None]
        row_data["num_neighbors_within_1km"] = sum(1 for d in valid_dists if d <= 1000)
        row_data["num_neighbors_within_5km"] = sum(1 for d in valid_dists if d <= 5000)

        result_rows.append(row_data)

    return pd.DataFrame(result_rows)


if __name__ == "__main__":
    main()
