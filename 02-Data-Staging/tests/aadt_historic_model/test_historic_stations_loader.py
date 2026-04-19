"""Red/green tests for the historic_stations xlsx loader.

Covers the two schema variants described in
`historic_traffic_inventory.md §3c` — 2020/2021 use a single `Lat/Long` text
column, 2022/2023 use numeric `Latitude` and `Longitude`. Both flow into the
same schema. The 2024 rows come from the GDB in Option A and are tested
separately.
"""

from __future__ import annotations

import pandas as pd
import pytest

from historic_stations_loader import (
    HISTORIC_STATIONS_COLUMNS,
    SCHEMA_2020_2021_TEXT_LATLONG,
    SCHEMA_2022_2023_NUMERIC_LATLONG,
    load_station_xlsx,
    parse_functional_class,
    parse_lat_long,
)


def _minimal_2020_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Station ID": ["001-0101", "002-0010", "003-0055"],
            "Functional Class": [
                "3U : Urban Principal Arterial - Other",
                "7R : Rural Local",
                "1U : Urban Principal Arterial - Interstate",
            ],
            "Lat/Long": [
                "31.715570, -82.381860",
                "32.100000, -83.500000",
                " 33.0, -84.0 ",
            ],
            "Year": [2020, 2020, 2020],
            "AADT": [3840, 1500, 120000],
            "Statistics type": ["Estimated", "Actual", "Actual"],
            "Single-Unit Truck AADT": [286, 40, 5000],
            "Combo-Unit Truck AADT": [336, 10, 8000],
            "% Peak SU Trucks": [0.07, 0.05, 0.09],
            "% Peak CU Trucks": [0.087, 0.03, 0.15],
            "K-Factor": [0.0851, 0.09, 0.085],
            "D-Factor": [0.52, 0.55, 0.51],
            "Future AADT": [8480, 2000, 180000],
            "Station Type": ["Short Term", "Short Term", "CCS"],
        }
    )


def _minimal_2022_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Station ID": ["001-0101", "002-0010"],
            "Functional Class": ["3U : Urban Principal Arterial - Other", "7R : Rural Local"],
            "Latitude": [31.715570, 32.100000],
            "Longitude": [-82.381860, -83.500000],
            "Year": [2022, 2022],
            "AADT": [5000, 1600],
            "Statistics type": ["Actual", "Estimated"],
            "Single-Unit Truck AADT": [300, 40],
            "Combo-Unit Truck AADT": [400, 10],
            "% Peak SU Trucks": [0.07, 0.05],
            "% Peak CU Trucks": [0.09, 0.03],
            "K-Factor": [0.085, 0.09],
            "D-Factor": [0.52, 0.55],
            "Future AADT": [9000, 2100],
            "Station Type": ["Short Term", "Short Term"],
        }
    )


def test_parse_lat_long_text_accepts_comma_separated() -> None:
    lat, lon = parse_lat_long("31.715570, -82.381860")
    assert lat == pytest.approx(31.715570)
    assert lon == pytest.approx(-82.381860)


def test_parse_lat_long_text_strips_whitespace() -> None:
    lat, lon = parse_lat_long(" 33.0, -84.0 ")
    assert lat == pytest.approx(33.0)
    assert lon == pytest.approx(-84.0)


def test_parse_lat_long_rejects_garbage() -> None:
    lat, lon = parse_lat_long("not a coord")
    assert pd.isna(lat)
    assert pd.isna(lon)


def test_parse_functional_class_extracts_integer() -> None:
    assert parse_functional_class("3U : Urban Principal Arterial - Other") == 3
    assert parse_functional_class("7R : Rural Local") == 7
    assert parse_functional_class("1U : Urban Principal Arterial - Interstate") == 1


def test_parse_functional_class_handles_missing() -> None:
    assert parse_functional_class(None) is None
    assert parse_functional_class("") is None
    assert parse_functional_class("unknown prefix string") is None


def test_load_station_xlsx_2020_schema_variant(tmp_path) -> None:
    raw = _minimal_2020_frame()
    xlsx = tmp_path / "2020.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx,
        year=2020,
        schema_variant=SCHEMA_2020_2021_TEXT_LATLONG,
        source_tag="xlsx:2020_annualized_statistics.xlsx",
    )

    assert list(out.columns) == HISTORIC_STATIONS_COLUMNS
    assert len(out) == 3
    assert out["year"].unique().tolist() == [2020]
    assert out.loc[out["tc_number"] == "001-0101", "latitude"].iloc[0] == pytest.approx(31.715570)
    assert out.loc[out["tc_number"] == "001-0101", "longitude"].iloc[0] == pytest.approx(-82.381860)
    assert out.loc[out["tc_number"] == "001-0101", "functional_class"].iloc[0] == 3
    assert out["statistics_type"].tolist() == ["Estimated", "Actual", "Actual"]
    assert (out["source"] == "xlsx:2020_annualized_statistics.xlsx").all()
    assert out["traffic_class"].isna().all()


def test_load_station_xlsx_2022_schema_variant(tmp_path) -> None:
    raw = _minimal_2022_frame()
    xlsx = tmp_path / "2022.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx,
        year=2022,
        schema_variant=SCHEMA_2022_2023_NUMERIC_LATLONG,
        source_tag="xlsx:2022 annualized_statistics.xlsx",
    )

    assert list(out.columns) == HISTORIC_STATIONS_COLUMNS
    assert len(out) == 2
    assert out["year"].unique().tolist() == [2022]
    assert out.loc[out["tc_number"] == "001-0101", "latitude"].iloc[0] == pytest.approx(31.715570)
    assert out.loc[out["tc_number"] == "001-0101", "longitude"].iloc[0] == pytest.approx(-82.381860)
    assert out["source"].iloc[0] == "xlsx:2022 annualized_statistics.xlsx"


def test_load_station_xlsx_statistics_type_distribution_preserved(tmp_path) -> None:
    raw = _minimal_2020_frame()
    xlsx = tmp_path / "2020.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx,
        year=2020,
        schema_variant=SCHEMA_2020_2021_TEXT_LATLONG,
        source_tag="xlsx:test.xlsx",
    )

    counts = out["statistics_type"].value_counts().to_dict()
    assert counts == {"Actual": 2, "Estimated": 1}


def test_load_station_xlsx_rejects_wrong_variant(tmp_path) -> None:
    raw = _minimal_2020_frame()
    xlsx = tmp_path / "2020.xlsx"
    raw.to_excel(xlsx, index=False)

    # 2020 file read as if it were 2022-schema should fail loudly — no
    # Latitude/Longitude numeric columns exist.
    with pytest.raises(KeyError):
        load_station_xlsx(
            xlsx_path=xlsx,
            year=2020,
            schema_variant=SCHEMA_2022_2023_NUMERIC_LATLONG,
            source_tag="xlsx:mismatch.xlsx",
        )
