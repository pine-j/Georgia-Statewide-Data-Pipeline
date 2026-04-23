"""k-NN segment-station link builder (k=5).

For each segment × year, finds the k=5 nearest stations and returns one row
per (unique_id, year, k_rank). Reuses the projection and cKDTree approach
from the 1-NN segment_station_link module.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from segment_station_link import _project_lon_lat_to_metric, _station_inferred_route

logger = logging.getLogger(__name__)

KNN_K = 5

KNN_LINK_COLUMNS = [
    "unique_id",
    "year",
    "k_rank",
    "nearest_tc_number",
    "station_distance_m",
    "nearest_tc_latitude",
    "nearest_tc_longitude",
    "same_route_flag",
]


def _ensure_metric(stations: pd.DataFrame) -> pd.DataFrame:
    if "x_m" in stations.columns and stations["x_m"].notna().all():
        return stations
    out = stations.copy()
    x, y = _project_lon_lat_to_metric(out["longitude"].values, out["latitude"].values)
    out["x_m"] = x
    out["y_m"] = y
    return out


def knn_per_segment(
    segments: pd.DataFrame,
    stations: pd.DataFrame,
    k: int = KNN_K,
) -> pd.DataFrame:
    """Find k nearest stations for each segment.

    Returns DataFrame with k rows per segment, ordered by k_rank (1=nearest).
    """
    if len(stations) == 0:
        raise ValueError("No stations provided")

    stations = stations.reset_index(drop=True)
    station_xy = np.c_[stations["x_m"].values, stations["y_m"].values]
    tree = cKDTree(station_xy)

    seg_xy = np.c_[segments["mid_x_m"].values, segments["mid_y_m"].values]
    actual_k = min(k, len(stations))
    dist, idx = tree.query(seg_xy, k=actual_k)

    if actual_k == 1:
        dist = dist.reshape(-1, 1)
        idx = idx.reshape(-1, 1)

    n_segs = len(segments)
    rows = []
    seg_uids = segments["unique_id"].values

    for rank in range(k):
        if rank < actual_k:
            matched = stations.iloc[idx[:, rank]].reset_index(drop=True)
            rank_df = pd.DataFrame({
                "unique_id": seg_uids,
                "k_rank": rank + 1,
                "nearest_tc_number": matched["tc_number"].values,
                "station_distance_m": dist[:, rank].astype("float64"),
                "nearest_tc_latitude": matched["latitude"].values,
                "nearest_tc_longitude": matched["longitude"].values,
            })
        else:
            rank_df = pd.DataFrame({
                "unique_id": seg_uids,
                "k_rank": rank + 1,
                "nearest_tc_number": pd.array([None] * n_segs, dtype="object"),
                "station_distance_m": np.full(n_segs, np.nan),
                "nearest_tc_latitude": np.full(n_segs, np.nan),
                "nearest_tc_longitude": np.full(n_segs, np.nan),
            })
        rows.append(rank_df)

    return pd.concat(rows, ignore_index=True)


def build_knn_link_rows(
    segments: pd.DataFrame,
    stations: pd.DataFrame,
    years: list[int],
    k: int = KNN_K,
) -> pd.DataFrame:
    """Build the full k-NN link table across years."""
    stations = _ensure_metric(stations)

    if "mid_x_m" not in segments.columns:
        from segment_station_link import _require_metric_coords
        segments = _require_metric_coords(
            segments, "mid_x_m", "mid_y_m", "mid_longitude", "mid_latitude"
        )

    out_frames = []
    for year in years:
        yr_stations = stations[stations["year"] == year].copy()
        yr_stations = yr_stations.drop_duplicates(subset=["tc_number"], keep="first")
        if len(yr_stations) == 0:
            logger.warning("No stations for year %d", year)
            continue

        logger.info("Building k=%d NN for year %d (%d stations)", k, year, len(yr_stations))
        linked = knn_per_segment(segments, yr_stations, k=k)
        linked["year"] = year

        station_route = _station_inferred_route(yr_stations, segments)
        tc_to_route = dict(zip(station_route["tc_number"], station_route["inferred_route_id"]))
        linked_route = linked["nearest_tc_number"].map(tc_to_route)
        seg_routes = segments.set_index("unique_id")["ROUTE_ID"]
        linked["same_route_flag"] = (
            linked_route.values == linked["unique_id"].map(seg_routes).values
        ).astype("int64")
        linked.loc[linked["nearest_tc_number"].isna(), "same_route_flag"] = 0

        out_frames.append(linked)

    combined = pd.concat(out_frames, ignore_index=True)
    combined = combined.sort_values(["unique_id", "year", "k_rank"]).reset_index(drop=True)
    return combined[KNN_LINK_COLUMNS]
