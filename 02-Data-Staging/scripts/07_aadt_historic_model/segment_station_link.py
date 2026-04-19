"""Compute nearest-station-per-segment-year for the modeling pipeline.

One row per `segments.unique_id` × year, for 5 years (2020-2024). Distance
is computed in a projected metric CRS (UTM 17N, EPSG:32617 — the CRS the
base network is staged in).

Plan reference: `aadt-modeling-scoped-2020-2024.md` §Prerequisite #3.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.spatial import cKDTree

logger = logging.getLogger(__name__)

LINK_COLUMNS = [
    "unique_id",
    "year",
    "nearest_tc_number",
    "station_distance_m",
    "nearest_tc_latitude",
    "nearest_tc_longitude",
    "same_route_flag",
]

# UTM Zone 17N — matches the base_network.gpkg staged CRS.
_PROJ_EPSG = 32617
_TRANSFORMER = Transformer.from_crs("EPSG:4326", f"EPSG:{_PROJ_EPSG}", always_xy=True)


def _project_lon_lat_to_metric(lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    x, y = _TRANSFORMER.transform(lon, lat)
    return np.asarray(x, dtype="float64"), np.asarray(y, dtype="float64")


def _require_metric_coords(df: pd.DataFrame, x_col: str, y_col: str, lon_col: str, lat_col: str) -> pd.DataFrame:
    """Populate x_m/y_m columns if missing, projecting from lat/lon."""

    if x_col in df.columns and y_col in df.columns and df[x_col].notna().all():
        return df
    x, y = _project_lon_lat_to_metric(df[lon_col].to_numpy(), df[lat_col].to_numpy())
    out = df.copy()
    out[x_col] = x
    out[y_col] = y
    return out


def nearest_station_per_segment(segments: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    """Match each segment to its nearest station (by centroid).

    Expects `segments` with columns: unique_id, mid_x_m, mid_y_m, ROUTE_ID.
    Expects `stations` with columns: tc_number, x_m, y_m, latitude, longitude.
    """

    if len(stations) == 0:
        raise ValueError("No stations provided to nearest_station_per_segment")

    stations = stations.reset_index(drop=True)
    station_xy = np.c_[stations["x_m"].to_numpy(), stations["y_m"].to_numpy()]
    tree = cKDTree(station_xy)

    seg_xy = np.c_[segments["mid_x_m"].to_numpy(), segments["mid_y_m"].to_numpy()]
    dist, idx = tree.query(seg_xy, k=1)

    matched = stations.iloc[idx].reset_index(drop=True)
    out = pd.DataFrame(
        {
            "unique_id": segments["unique_id"].to_numpy(),
            "nearest_tc_number": matched["tc_number"].to_numpy(),
            "station_distance_m": dist.astype("float64"),
            "nearest_tc_latitude": matched["latitude"].to_numpy(),
            "nearest_tc_longitude": matched["longitude"].to_numpy(),
        }
    )
    return out


def build_link_rows(
    segments: pd.DataFrame,
    stations: pd.DataFrame,
    years: list[int],
) -> pd.DataFrame:
    """Build the full segment_station_link table across `years`."""

    segments = segments.copy()
    if "mid_x_m" not in segments.columns:
        segments = _require_metric_coords(
            segments, "mid_x_m", "mid_y_m", "mid_longitude", "mid_latitude"
        )

    stations = stations.copy()
    if "x_m" not in stations.columns or stations["x_m"].isna().any():
        stations = _require_metric_coords(
            stations, "x_m", "y_m", "longitude", "latitude"
        )

    out_frames: list[pd.DataFrame] = []
    for year in years:
        yr_stations = stations[stations["year"] == year].copy()
        if len(yr_stations) == 0:
            logger.warning("No stations for year %d", year)
            continue
        linked = nearest_station_per_segment(segments=segments, stations=yr_stations)
        linked["year"] = year
        linked["same_route_flag"] = 0
        out_frames.append(linked)

    combined = pd.concat(out_frames, axis=0, ignore_index=True)
    return combined[LINK_COLUMNS]


__all__ = [
    "LINK_COLUMNS",
    "build_link_rows",
    "nearest_station_per_segment",
]
