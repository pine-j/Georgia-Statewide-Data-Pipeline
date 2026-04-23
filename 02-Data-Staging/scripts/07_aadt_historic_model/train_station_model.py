"""Train station AADT model with 5-fold station-uid CV.

HistGradientBoostingRegressor for P50 (squared_error) and P10/P90
(quantile loss). Training target: log(station.aadt_actual).

Saves fold predictions, CV metrics, and production model.
"""

from __future__ import annotations

import logging
import pickle
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_percentage_error

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cohort_ratios_v2 import N_FOLDS

logger = logging.getLogger(__name__)

DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
SCRATCH = _SCRIPTS.parents[2] / "_scratch"

FEATURE_COLS = [
    "FUNCTIONAL_CLASS", "SYSTEM_CODE", "FACILITY_TYPE", "NUM_LANES",
    "LANE_WIDTH", "MEDIAN_TYPE", "SPEED_LIMIT", "URBAN_CODE", "NHS_IND",
    "K_FACTOR", "D_FACTOR", "segment_length_mi", "DISTRICT", "COUNTY_ID",
    "knn_distance_1", "knn_distance_2", "knn_distance_3", "knn_distance_4", "knn_distance_5",
    "knn_same_route_1", "knn_same_route_2", "knn_same_route_3", "knn_same_route_4", "knn_same_route_5",
    "knn_aadt_1", "knn_aadt_2", "knn_aadt_3", "knn_aadt_4", "knn_aadt_5",
    "knn_k_factor_1", "knn_k_factor_2", "knn_k_factor_3", "knn_k_factor_4", "knn_k_factor_5",
    "knn_d_factor_1", "knn_d_factor_2", "knn_d_factor_3", "knn_d_factor_4", "knn_d_factor_5",
    "knn_fc_1", "knn_fc_2", "knn_fc_3", "knn_fc_4", "knn_fc_5",
    "knn_is_actual_1", "knn_is_actual_2", "knn_is_actual_3", "knn_is_actual_4", "knn_is_actual_5",
    "num_neighbors_within_1km", "num_neighbors_within_5km",
    "cohort_median_2020_actual", "cohort_ratio_2020_to_2022", "cohort_ratio_2020_to_2024", "cohort_size",
    "years_since_2020", "covid_phase_weight",
]

MAPE_WARN = 0.25
MAPE_HARD_STOP = 0.35


def _make_model(loss: str = "squared_error", quantile: float | None = None) -> HistGradientBoostingRegressor:
    params = dict(
        max_iter=500,
        max_depth=8,
        learning_rate=0.05,
        min_samples_leaf=20,
        l2_regularization=1.0,
        random_state=42,
    )
    if loss == "quantile" and quantile is not None:
        return HistGradientBoostingRegressor(loss="quantile", quantile=quantile, **params)
    return HistGradientBoostingRegressor(loss=loss, **params)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    SCRATCH.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    logger.info("Loading training rows...")
    df = pd.read_sql("SELECT * FROM station_training_rows", conn)
    logger.info("  %d rows, %d columns", len(df), len(df.columns))

    available = [c for c in FEATURE_COLS if c in df.columns]
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        logger.warning("Missing features (will be skipped): %s", missing)
    features = available
    logger.info("  Using %d features", len(features))

    fold_results = []

    for fold_id in range(N_FOLDS):
        logger.info("\n=== Fold %d ===", fold_id)
        train_mask = df["fold"] != fold_id
        test_mask = df["fold"] == fold_id

        X_train = df.loc[train_mask, features]
        y_train = df.loc[train_mask, "target"]
        w_train = df.loc[train_mask, "sample_weight"]

        X_test = df.loc[test_mask, features]
        y_test = df.loc[test_mask, "target"]

        logger.info("  Train: %d rows, Test: %d rows", len(X_train), len(X_test))

        model_p50 = _make_model("squared_error")
        model_p50.fit(X_train, y_train, sample_weight=w_train)

        pred_log = model_p50.predict(X_test)
        pred = np.exp(pred_log)
        actual = np.exp(y_test.values)

        mape = mean_absolute_percentage_error(actual, pred)
        median_ape = float(np.median(np.abs(actual - pred) / actual))

        logger.info("  P50 MAPE: %.2f%%, Median APE: %.2f%%", mape * 100, median_ape * 100)

        if mape > MAPE_HARD_STOP:
            logger.error("  HARD STOP: Fold %d MAPE %.1f%% > %.0f%% threshold!", fold_id, mape * 100, MAPE_HARD_STOP * 100)
        elif mape > MAPE_WARN:
            logger.warning("  WARNING: Fold %d MAPE %.1f%% > %.0f%% threshold", fold_id, mape * 100, MAPE_WARN * 100)

        fold_pred = df.loc[test_mask, ["station_uid", "year", "aadt", "fold"]].copy()
        fold_pred["pred_aadt"] = pred
        fold_pred["pred_log"] = pred_log
        fold_pred["actual_log"] = y_test.values
        fold_pred["ape"] = np.abs(actual - pred) / actual
        fold_pred.to_parquet(SCRATCH / f"fold_{fold_id}_predictions.parquet", index=False)

        fold_results.append({
            "fold": fold_id,
            "train_rows": int(train_mask.sum()),
            "test_rows": int(test_mask.sum()),
            "mape": float(mape),
            "median_ape": float(median_ape),
            "gate_met": mape <= MAPE_WARN,
            "hard_stop": mape > MAPE_HARD_STOP,
        })

    logger.info("\n=== CV Summary ===")
    mean_mape = np.mean([r["mape"] for r in fold_results])
    mean_median_ape = np.mean([r["median_ape"] for r in fold_results])
    logger.info("Mean MAPE: %.2f%%", mean_mape * 100)
    logger.info("Mean Median APE: %.2f%%", mean_median_ape * 100)

    for r in fold_results:
        status = "HARD STOP" if r["hard_stop"] else ("WARN" if not r["gate_met"] else "PASS")
        logger.info("  Fold %d: MAPE=%.2f%% MedAPE=%.2f%% [%s]", r["fold"], r["mape"] * 100, r["median_ape"] * 100, status)

    any_hard_stop = any(r["hard_stop"] for r in fold_results)
    if any_hard_stop:
        logger.error("DECISION GATE 2: HARD STOP triggered. Do not proceed to production model.")
    elif mean_mape > MAPE_WARN:
        logger.warning("DECISION GATE 1: Mean MAPE %.1f%% > 25%%. Consider feature iteration.", mean_mape * 100)
    else:
        logger.info("All decision gates PASSED.")

    logger.info("\n=== Training production model ===")
    X_all = df[features]
    y_all = df["target"]
    w_all = df["sample_weight"]

    prod_p50 = _make_model("squared_error")
    prod_p50.fit(X_all, y_all, sample_weight=w_all)

    prod_p10 = _make_model("quantile", quantile=0.10)
    prod_p10.fit(X_all, y_all, sample_weight=w_all)

    prod_p90 = _make_model("quantile", quantile=0.90)
    prod_p90.fit(X_all, y_all, sample_weight=w_all)

    models = {
        "p50": prod_p50,
        "p10": prod_p10,
        "p90": prod_p90,
        "features": features,
        "fold_results": fold_results,
        "mean_mape": float(mean_mape),
        "mean_median_ape": float(mean_median_ape),
    }
    model_path = SCRATCH / "production_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(models, f)
    logger.info("Production model saved to %s", model_path)

    cv_df = pd.DataFrame(fold_results)
    cv_df.to_sql("cv_fold_results", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()

    logger.info("Done.")


if __name__ == "__main__":
    main()
