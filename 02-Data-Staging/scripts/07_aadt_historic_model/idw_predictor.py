"""IDW (inverse-distance-weighted) AADT predictor.

Predicts segment-level AADT by spatially interpolating from the k nearest
stations, weighted by 1/distance. Only predicts where the nearest station
is within CUTOFF_M; leaves the rest as NULL.

Confidence tiers:
  high:   nearest station within 500m AND same_route_flag=1
  medium: nearest station within 2000m (but not high)
  none:   nearest station beyond 2000m -> NULL prediction
"""

from __future__ import annotations

import numpy as np
import pandas as pd

CUTOFF_M = 2000
HIGH_CONFIDENCE_M = 500
MIN_DISTANCE_M = 1.0  # avoid division by zero

OUTPUT_COLUMNS = [
    "unique_id",
    "AADT_MODELED",
    "AADT_NEIGHBOR_MIN",
    "AADT_NEIGHBOR_MAX",
    "AADT_CONFIDENCE",
    "AADT_SOURCE",
    "AADT_NEAREST_STATION_DIST_M",
    "AADT_NEAREST_STATION_TC",
    "AADT_N_STATIONS_USED",
]


def predict_idw(
    knn: pd.DataFrame,
    stations: pd.DataFrame,
) -> pd.DataFrame:
    """Predict AADT for each segment using IDW of k-nearest stations.

    Parameters
    ----------
    knn : segment_station_link_knn rows with unique_id, k_rank,
          nearest_tc_number, station_distance_m, same_route_flag.
    stations : station rows with tc_number, aadt.

    Returns
    -------
    One row per unique_id with prediction columns (year-neutral names).
    """
    if knn.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    knn = knn.merge(
        stations[["tc_number", "aadt"]],
        left_on="nearest_tc_number",
        right_on="tc_number",
        how="left",
    )

    results = []
    for uid, group in knn.groupby("unique_id"):
        group = group.sort_values("k_rank")

        within = group[
            (group["station_distance_m"] <= CUTOFF_M)
            & group["aadt"].notna()
        ]

        nearest = group.iloc[0]
        nearest_dist = nearest["station_distance_m"]
        nearest_tc = nearest["nearest_tc_number"]

        if len(within) == 0 or nearest_dist > CUTOFF_M:
            results.append(_null_row(uid, nearest_dist, nearest_tc))
            continue

        dists = np.maximum(within["station_distance_m"].values, MIN_DISTANCE_M)
        aadts = within["aadt"].values.astype(float)
        weights = 1.0 / dists
        weights /= weights.sum()

        modeled = int(round(np.dot(weights, aadts)))
        neighbor_min = int(round(np.min(aadts)))
        neighbor_max = int(round(np.max(aadts)))

        if neighbor_min > modeled:
            neighbor_min = modeled
        if neighbor_max < modeled:
            neighbor_max = modeled

        nearest_same_route = int(nearest["same_route_flag"]) if pd.notna(nearest["same_route_flag"]) else 0
        if nearest_dist <= HIGH_CONFIDENCE_M and nearest_same_route == 1:
            confidence = "high"
        elif nearest_dist <= CUTOFF_M:
            confidence = "medium"
        else:
            confidence = "none"

        results.append({
            "unique_id": uid,
            "AADT_MODELED": modeled,
            "AADT_NEIGHBOR_MIN": neighbor_min,
            "AADT_NEIGHBOR_MAX": neighbor_max,
            "AADT_CONFIDENCE": confidence,
            "AADT_SOURCE": "station_idw_v2",
            "AADT_NEAREST_STATION_DIST_M": round(nearest_dist, 1),
            "AADT_NEAREST_STATION_TC": nearest_tc,
            "AADT_N_STATIONS_USED": len(within),
        })

    return pd.DataFrame(results)


def _null_row(uid: str, nearest_dist: float, nearest_tc: str) -> dict:
    return {
        "unique_id": uid,
        "AADT_MODELED": np.nan,
        "AADT_NEIGHBOR_MIN": np.nan,
        "AADT_NEIGHBOR_MAX": np.nan,
        "AADT_CONFIDENCE": "none",
        "AADT_SOURCE": "station_idw_v2",
        "AADT_NEAREST_STATION_DIST_M": round(nearest_dist, 1),
        "AADT_NEAREST_STATION_TC": nearest_tc,
        "AADT_N_STATIONS_USED": 0,
    }
