"""Red/green tests for the segment_station_link nearest-station builder.

Covers the 5-segment × 3-station synthetic grid described in the plan
§Development discipline.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from segment_station_link import (
    LINK_COLUMNS,
    build_link_rows,
    nearest_station_per_segment,
)


def test_nearest_station_per_segment_picks_closest() -> None:
    # 3 stations placed at y=0, 100, 200 on a north-south line at x=0.
    stations = pd.DataFrame(
        {
            "tc_number": ["S_BOTTOM", "S_MID", "S_TOP"],
            "latitude": [0.0, 0.0, 0.0],
            "longitude": [0.0, 0.0, 0.0],
            "x_m": [0.0, 0.0, 0.0],
            "y_m": [0.0, 100.0, 200.0],
            "year": [2020, 2020, 2020],
        }
    )
    # 5 segment midpoints at y = 10, 60, 110, 190, 500.
    # 60 > half-way (50) to S_MID, so unique nearest = S_MID.
    segments = pd.DataFrame(
        {
            "unique_id": ["seg_A", "seg_B", "seg_C", "seg_D", "seg_E"],
            "mid_x_m": [0.0, 0.0, 0.0, 0.0, 0.0],
            "mid_y_m": [10.0, 60.0, 110.0, 190.0, 500.0],
            "ROUTE_ID": ["R1"] * 5,
        }
    )

    out = nearest_station_per_segment(segments=segments, stations=stations)

    mapping = dict(zip(out["unique_id"], out["nearest_tc_number"]))
    assert mapping["seg_A"] == "S_BOTTOM"
    assert mapping["seg_B"] == "S_MID"
    assert mapping["seg_C"] == "S_MID"
    assert mapping["seg_D"] == "S_TOP"
    assert mapping["seg_E"] == "S_TOP"

    dist = dict(zip(out["unique_id"], out["station_distance_m"]))
    assert dist["seg_A"] == pytest.approx(10.0, abs=0.1)
    assert dist["seg_E"] == pytest.approx(300.0, abs=0.1)


def test_nearest_station_per_segment_keeps_all_segments() -> None:
    stations = pd.DataFrame(
        {
            "tc_number": ["S"],
            "latitude": [0.0],
            "longitude": [0.0],
            "x_m": [0.0],
            "y_m": [0.0],
            "year": [2020],
        }
    )
    segments = pd.DataFrame(
        {
            "unique_id": [f"seg_{i}" for i in range(10)],
            "mid_x_m": [0.0] * 10,
            "mid_y_m": [float(i * 10000) for i in range(10)],  # up to 90 km away
            "ROUTE_ID": ["R1"] * 10,
        }
    )
    out = nearest_station_per_segment(segments=segments, stations=stations)

    # No tolerance cap per plan — even 10+ mile stations kept.
    assert len(out) == 10
    assert out["nearest_tc_number"].isna().sum() == 0


def test_build_link_rows_emits_full_schema() -> None:
    stations = pd.DataFrame(
        {
            "year": [2020, 2021],
            "tc_number": ["S_A", "S_B"],
            "latitude": [33.0, 33.01],
            "longitude": [-84.0, -84.0],
        }
    )
    segments = pd.DataFrame(
        {
            "unique_id": ["seg_1"],
            "mid_latitude": [33.005],
            "mid_longitude": [-84.0],
            "ROUTE_ID": ["R1"],
        }
    )
    out = build_link_rows(segments=segments, stations=stations, years=[2020, 2021])

    assert list(out.columns) == LINK_COLUMNS
    assert len(out) == 2
    assert set(out["year"].unique()) == {2020, 2021}
    for _, row in out.iterrows():
        assert not pd.isna(row["nearest_tc_number"])
        assert not pd.isna(row["station_distance_m"])
        assert row["station_distance_m"] >= 0


def test_build_link_rows_one_row_per_segment_year() -> None:
    # 2 segments × 3 years = 6 rows expected.
    stations = pd.DataFrame(
        {
            "year": [2020, 2021, 2022, 2020, 2021, 2022],
            "tc_number": ["S_A"] * 3 + ["S_B"] * 3,
            "latitude": [33.0] * 3 + [33.1] * 3,
            "longitude": [-84.0] * 3 + [-84.0] * 3,
        }
    )
    segments = pd.DataFrame(
        {
            "unique_id": ["seg_1", "seg_2"],
            "mid_latitude": [33.05, 33.01],
            "mid_longitude": [-84.0, -84.0],
            "ROUTE_ID": ["R1", "R2"],
        }
    )
    out = build_link_rows(segments=segments, stations=stations, years=[2020, 2021, 2022])

    assert len(out) == 6
    for yr in [2020, 2021, 2022]:
        per_year = out[out["year"] == yr]
        assert len(per_year) == 2
        assert set(per_year["unique_id"]) == {"seg_1", "seg_2"}
