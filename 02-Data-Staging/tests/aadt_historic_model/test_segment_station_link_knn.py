"""Red/green tests for the k-NN segment-station link builder.

Plan §Step 3 — extend segment_station_link to k=5 nearest stations per
segment-year pair. Output: segment_station_link_knn table with k_rank 1-5.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from segment_station_link_knn import KNN_K, KNN_LINK_COLUMNS, build_knn_link_rows


def _segments(n: int, x_start: float = 500_000.0, y_start: float = 3_700_000.0) -> pd.DataFrame:
    """Create n synthetic segments with metric coords."""
    return pd.DataFrame({
        "unique_id": [f"seg_{i}" for i in range(n)],
        "ROUTE_ID": [f"R{i % 3}" for i in range(n)],
        "mid_x_m": [x_start + i * 1000 for i in range(n)],
        "mid_y_m": [y_start] * n,
    })


def _stations(n: int, year: int, x_start: float = 500_500.0, y_start: float = 3_700_000.0) -> pd.DataFrame:
    """Create n synthetic stations for a given year with metric coords."""
    return pd.DataFrame({
        "tc_number": [f"TC_{year}_{i:03d}" for i in range(n)],
        "year": [year] * n,
        "latitude": [33.5 + i * 0.01 for i in range(n)],
        "longitude": [-84.5 + i * 0.01 for i in range(n)],
        "x_m": [x_start + i * 800 for i in range(n)],
        "y_m": [y_start + i * 100 for i in range(n)],
    })


def test_knn_k_is_5() -> None:
    assert KNN_K == 5


def test_output_has_k_ranks_1_through_5() -> None:
    segs = _segments(3)
    stas = _stations(10, 2020)
    result = build_knn_link_rows(segs, stas, years=[2020])
    assert set(result["k_rank"].unique()) == {1, 2, 3, 4, 5}


def test_row_count_is_segments_times_years_times_k() -> None:
    segs = _segments(5)
    stas = pd.concat([_stations(10, y) for y in [2020, 2022]], ignore_index=True)
    result = build_knn_link_rows(segs, stas, years=[2020, 2022])
    assert len(result) == 5 * 2 * 5  # 5 segments × 2 years × 5 k


def test_k_rank_1_is_nearest() -> None:
    segs = _segments(1, x_start=500_000)
    stas = _stations(6, 2020, x_start=500_100)
    result = build_knn_link_rows(segs, stas, years=[2020])
    r1 = result[result["k_rank"] == 1].iloc[0]
    r5 = result[result["k_rank"] == 5].iloc[0]
    assert r1["station_distance_m"] <= r5["station_distance_m"]


def test_distances_increase_with_k_rank() -> None:
    segs = _segments(1)
    stas = _stations(10, 2020)
    result = build_knn_link_rows(segs, stas, years=[2020])
    dists = result.sort_values("k_rank")["station_distance_m"].values
    assert all(dists[i] <= dists[i + 1] for i in range(len(dists) - 1))


def test_same_route_flag_correct() -> None:
    segs = pd.DataFrame({
        "unique_id": ["seg_0"],
        "ROUTE_ID": ["R0"],
        "mid_x_m": [500_000.0],
        "mid_y_m": [3_700_000.0],
    })
    stas = pd.DataFrame({
        "tc_number": [f"TC_{i}" for i in range(5)],
        "year": [2020] * 5,
        "latitude": [33.5] * 5,
        "longitude": [-84.5] * 5,
        "x_m": [500_100 + i * 100 for i in range(5)],
        "y_m": [3_700_000.0] * 5,
    })
    result = build_knn_link_rows(segs, stas, years=[2020])
    assert len(result) == 5
    assert set(result.columns) == set(KNN_LINK_COLUMNS)


def test_output_columns_match_schema() -> None:
    segs = _segments(2)
    stas = _stations(6, 2020)
    result = build_knn_link_rows(segs, stas, years=[2020])
    assert list(result.columns) == KNN_LINK_COLUMNS


def test_fewer_than_k_stations_pads() -> None:
    """When fewer than 5 stations available, fill remaining ranks with NaN."""
    segs = _segments(1)
    stas = _stations(3, 2020)
    result = build_knn_link_rows(segs, stas, years=[2020])
    assert len(result) == 5
    assert result[result["k_rank"] <= 3]["station_distance_m"].notna().all()
    assert result[result["k_rank"] > 3]["nearest_tc_number"].isna().all()
