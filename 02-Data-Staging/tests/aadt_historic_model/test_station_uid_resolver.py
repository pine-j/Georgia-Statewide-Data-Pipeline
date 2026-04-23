"""Red/green tests for the station identity resolver.

Plan §Station identity resolver — resolves TC_NUMBER instability across
the 2023→2024 boundary by assigning a canonical station_uid to every
(year, tc_number) pair, anchored on 2024 stations.
"""

from __future__ import annotations

import pandas as pd
import pytest

from station_uid_resolver import resolve_stations, ANCHOR_YEAR


def _station(year: int, tc: str, lat: float, lon: float) -> dict:
    return {"year": year, "tc_number": tc, "latitude": lat, "longitude": lon}


def test_anchor_year_is_2024() -> None:
    assert ANCHOR_YEAR == 2024


def test_same_tc_within_500m_primary_match() -> None:
    """2023 station with same TC as a 2024 station, <500m apart -> primary match."""
    stations = pd.DataFrame([
        _station(2024, "TC-001", 33.5000, -84.5000),
        _station(2023, "TC-001", 33.5001, -84.5000),  # ~11m away
    ])
    result = resolve_stations(stations)
    r23 = result[(result["year"] == 2023) & (result["tc_number"] == "TC-001")]
    r24 = result[(result["year"] == 2024) & (result["tc_number"] == "TC-001")]
    assert len(r23) == 1
    assert len(r24) == 1
    assert r23.iloc[0]["station_uid"] == r24.iloc[0]["station_uid"]
    assert r23.iloc[0]["resolver_method"] == "tc_number"


def test_same_tc_far_away_spatial_fallback() -> None:
    """2023 station same TC as 2024 but 10km away, another 2024 station is 100m away."""
    stations = pd.DataFrame([
        _station(2024, "TC-001", 33.5000, -84.5000),
        _station(2024, "TC-002", 33.6000, -84.5000),  # ~11km north
        _station(2023, "TC-001", 33.6001, -84.5000),   # 100m from TC-002, 11km from TC-001
    ])
    result = resolve_stations(stations)
    r23 = result[(result["year"] == 2023) & (result["tc_number"] == "TC-001")]
    r24_002 = result[(result["year"] == 2024) & (result["tc_number"] == "TC-002")]
    assert r23.iloc[0]["station_uid"] == r24_002.iloc[0]["station_uid"]
    assert r23.iloc[0]["resolver_method"] == "spatial"


def test_no_2024_within_500m_unresolved() -> None:
    """2023 station with no 2024 station within 500m -> unresolved, gets fresh uid."""
    stations = pd.DataFrame([
        _station(2024, "TC-100", 34.0000, -84.0000),
        _station(2023, "TC-200", 33.0000, -84.0000),  # ~111km away
    ])
    result = resolve_stations(stations)
    r23 = result[(result["year"] == 2023) & (result["tc_number"] == "TC-200")]
    r24 = result[(result["year"] == 2024) & (result["tc_number"] == "TC-100")]
    assert r23.iloc[0]["station_uid"] != r24.iloc[0]["station_uid"]
    assert r23.iloc[0]["resolver_method"] == "unresolved"


def test_2020_chains_through_2023() -> None:
    """2020 station with same TC as 2023, which resolves to 2024 -> 2020 inherits uid."""
    stations = pd.DataFrame([
        _station(2024, "TC-001", 33.5000, -84.5000),
        _station(2023, "TC-001", 33.5001, -84.5000),
        _station(2020, "TC-001", 33.5001, -84.5000),
    ])
    result = resolve_stations(stations)
    uid_24 = result[(result["year"] == 2024) & (result["tc_number"] == "TC-001")].iloc[0]["station_uid"]
    uid_23 = result[(result["year"] == 2023) & (result["tc_number"] == "TC-001")].iloc[0]["station_uid"]
    uid_20 = result[(result["year"] == 2020) & (result["tc_number"] == "TC-001")].iloc[0]["station_uid"]
    assert uid_24 == uid_23 == uid_20


def test_2024_duplicates_deduplicated() -> None:
    """Multiple 2024 rows with same TC -> single station_uid."""
    stations = pd.DataFrame([
        _station(2024, "TC-001", 33.5000, -84.5000),
        _station(2024, "TC-001", 33.5000, -84.5000),
        _station(2024, "TC-001", 33.5000, -84.5000),
    ])
    result = resolve_stations(stations)
    r24 = result[(result["year"] == 2024) & (result["tc_number"] == "TC-001")]
    assert len(r24) == 3
    assert r24["station_uid"].nunique() == 1


def test_output_schema() -> None:
    """Resolver output has required columns."""
    stations = pd.DataFrame([
        _station(2024, "TC-001", 33.5, -84.5),
        _station(2023, "TC-001", 33.5, -84.5),
    ])
    result = resolve_stations(stations)
    required = {"year", "tc_number", "station_uid", "resolver_method",
                "resolver_delta_m", "resolver_confidence"}
    assert required.issubset(set(result.columns))


def test_confidence_levels() -> None:
    """Primary match -> high confidence. Spatial -> medium. Unresolved -> low."""
    stations = pd.DataFrame([
        _station(2024, "TC-A", 33.5000, -84.5000),
        _station(2024, "TC-B", 34.0000, -84.0000),
        _station(2023, "TC-A", 33.5001, -84.5000),   # primary
        _station(2023, "TC-X", 34.0001, -84.0000),    # spatial -> TC-B
        _station(2023, "TC-Y", 32.0000, -84.0000),    # unresolved
    ])
    result = resolve_stations(stations)
    r23 = result[result["year"] == 2023].set_index("tc_number")
    assert r23.loc["TC-A", "resolver_confidence"] == "high"
    assert r23.loc["TC-X", "resolver_confidence"] == "medium"
    assert r23.loc["TC-Y", "resolver_confidence"] == "low"
