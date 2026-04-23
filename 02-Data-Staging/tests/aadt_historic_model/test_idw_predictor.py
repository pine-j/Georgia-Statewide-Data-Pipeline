"""Red/green tests for the IDW station-based AADT predictor.

Segments get AADT via inverse-distance-weighted interpolation of
k nearest stations, with a confidence tier and a 2000m cutoff.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from idw_predictor import (
    CUTOFF_M,
    HIGH_CONFIDENCE_M,
    OUTPUT_COLUMNS,
    predict_idw,
)


def _knn_row(uid: str, k_rank: int, tc: str, dist: float, same_route: int) -> dict:
    return {
        "unique_id": uid,
        "k_rank": k_rank,
        "nearest_tc_number": tc,
        "station_distance_m": dist,
        "same_route_flag": same_route,
    }


def _station(tc: str, aadt: int) -> dict:
    return {"tc_number": tc, "aadt": aadt}


def test_cutoff_is_2000m() -> None:
    assert CUTOFF_M == 2000


def test_high_confidence_is_500m() -> None:
    assert HIGH_CONFIDENCE_M == 500


def test_single_station_within_cutoff() -> None:
    """One station at 300m -> prediction equals that station's AADT."""
    knn = pd.DataFrame([_knn_row("seg_0", 1, "TC1", 300.0, 1)])
    stations = pd.DataFrame([_station("TC1", 5000)])
    result = predict_idw(knn, stations)
    assert len(result) == 1
    assert result.iloc[0]["AADT_MODELED"] == 5000
    assert result.iloc[0]["AADT_CONFIDENCE"] == "high"


def test_beyond_cutoff_is_null() -> None:
    """Station at 3000m -> no prediction."""
    knn = pd.DataFrame([_knn_row("seg_0", 1, "TC1", 3000.0, 0)])
    stations = pd.DataFrame([_station("TC1", 5000)])
    result = predict_idw(knn, stations)
    assert len(result) == 1
    assert pd.isna(result.iloc[0]["AADT_MODELED"])
    assert result.iloc[0]["AADT_CONFIDENCE"] == "none"


def test_high_confidence_same_route_within_500m() -> None:
    """Station at 200m + same_route -> high confidence."""
    knn = pd.DataFrame([
        _knn_row("seg_0", 1, "TC1", 200.0, 1),
        _knn_row("seg_0", 2, "TC2", 800.0, 0),
    ])
    stations = pd.DataFrame([_station("TC1", 3000), _station("TC2", 6000)])
    result = predict_idw(knn, stations)
    assert result.iloc[0]["AADT_CONFIDENCE"] == "high"


def test_medium_confidence_beyond_500m() -> None:
    """Nearest station at 700m -> medium confidence."""
    knn = pd.DataFrame([_knn_row("seg_0", 1, "TC1", 700.0, 1)])
    stations = pd.DataFrame([_station("TC1", 4000)])
    result = predict_idw(knn, stations)
    assert result.iloc[0]["AADT_CONFIDENCE"] == "medium"


def test_medium_confidence_no_same_route() -> None:
    """Station at 200m but NOT same_route -> medium confidence."""
    knn = pd.DataFrame([_knn_row("seg_0", 1, "TC1", 200.0, 0)])
    stations = pd.DataFrame([_station("TC1", 4000)])
    result = predict_idw(knn, stations)
    assert result.iloc[0]["AADT_CONFIDENCE"] == "medium"


def test_idw_weights_closer_station_more() -> None:
    """Two stations: one near, one far. IDW should weight nearer one more."""
    knn = pd.DataFrame([
        _knn_row("seg_0", 1, "TC1", 100.0, 0),
        _knn_row("seg_0", 2, "TC2", 1000.0, 0),
    ])
    stations = pd.DataFrame([_station("TC1", 1000), _station("TC2", 9000)])
    result = predict_idw(knn, stations)
    pred = result.iloc[0]["AADT_MODELED"]
    assert pred < 3000
    assert pred > 1000


def test_multiple_segments() -> None:
    """Two segments, each with different nearest stations."""
    knn = pd.DataFrame([
        _knn_row("seg_0", 1, "TC1", 100.0, 1),
        _knn_row("seg_1", 1, "TC2", 500.0, 0),
    ])
    stations = pd.DataFrame([_station("TC1", 2000), _station("TC2", 8000)])
    result = predict_idw(knn, stations)
    assert len(result) == 2
    r = result.set_index("unique_id")
    assert r.loc["seg_0", "AADT_MODELED"] == 2000
    assert r.loc["seg_1", "AADT_MODELED"] == 8000


def test_only_within_cutoff_stations_used() -> None:
    """k=1 at 500m, k=2 at 3000m -> only k=1 contributes."""
    knn = pd.DataFrame([
        _knn_row("seg_0", 1, "TC1", 500.0, 0),
        _knn_row("seg_0", 2, "TC2", 3000.0, 0),
    ])
    stations = pd.DataFrame([_station("TC1", 2000), _station("TC2", 99999)])
    result = predict_idw(knn, stations)
    assert result.iloc[0]["AADT_MODELED"] == 2000


def test_output_columns() -> None:
    knn = pd.DataFrame([_knn_row("seg_0", 1, "TC1", 100.0, 1)])
    stations = pd.DataFrame([_station("TC1", 5000)])
    result = predict_idw(knn, stations)
    assert set(OUTPUT_COLUMNS).issubset(set(result.columns))


def test_neighbor_bounds() -> None:
    """NEIGHBOR_MIN <= MODELED <= NEIGHBOR_MAX."""
    knn = pd.DataFrame([
        _knn_row("seg_0", 1, "TC1", 100.0, 0),
        _knn_row("seg_0", 2, "TC2", 500.0, 0),
        _knn_row("seg_0", 3, "TC3", 1000.0, 0),
    ])
    stations = pd.DataFrame([
        _station("TC1", 2000), _station("TC2", 5000), _station("TC3", 8000),
    ])
    result = predict_idw(knn, stations)
    r = result.iloc[0]
    assert r["AADT_NEIGHBOR_MIN"] <= r["AADT_MODELED"]
    assert r["AADT_MODELED"] <= r["AADT_NEIGHBOR_MAX"]


def test_result_is_integer() -> None:
    knn = pd.DataFrame([
        _knn_row("seg_0", 1, "TC1", 200.0, 0),
        _knn_row("seg_0", 2, "TC2", 800.0, 0),
    ])
    stations = pd.DataFrame([_station("TC1", 3333), _station("TC2", 7777)])
    result = predict_idw(knn, stations)
    val = result.iloc[0]["AADT_MODELED"]
    assert val == int(val)


def test_empty_input_returns_empty_with_schema() -> None:
    """Empty knn DataFrame -> empty result with correct columns."""
    knn = pd.DataFrame(columns=["unique_id", "k_rank", "nearest_tc_number",
                                "station_distance_m", "same_route_flag"])
    stations = pd.DataFrame([_station("TC1", 5000)])
    result = predict_idw(knn, stations)
    assert len(result) == 0
    assert list(result.columns) == OUTPUT_COLUMNS
