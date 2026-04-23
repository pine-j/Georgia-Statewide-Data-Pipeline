"""Red/green tests for the historic_stations xlsx loader.

Covers all schema variants described in `historic_traffic_inventory.md §3c`:
- 2015: `Coordinate` text + lowercase `aadt` (no FC, no stats type)
- 2016: CSV with `TC_NUMBER`, separate `Lat`/`Long`
- 2017/2019: same as 2020/2021 (Lat/Long text, Statistics type words)
- 2018: separate `Lat`/`Long` columns + `Unnamed: 4` artifact
- 2020/2021: `Lat/Long` text column
- 2022/2023: numeric `Latitude`/`Longitude`

The 2024 rows come from the GDB and are tested separately.
"""

from __future__ import annotations

import pandas as pd
import pytest

from historic_stations_loader import (
    HISTORIC_STATIONS_COLUMNS,
    SCHEMA_2015_COORDINATE,
    SCHEMA_2016_CSV,
    SCHEMA_2017_2019_TEXT_LATLONG,
    SCHEMA_2018_SEPARATE_LATLONG,
    SCHEMA_2020_2021_TEXT_LATLONG,
    SCHEMA_2022_2023_NUMERIC_LATLONG,
    _null_out_bad_coords,
    load_station_csv,
    load_station_xlsx,
    normalize_tc_number,
    parse_functional_class,
    parse_lat_long,
    recode_stats,
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


# ---- 2015 Coordinate schema ----


def _minimal_2015_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Station ID": ["001-0101", "002-0010", "003-0055"],
            "Coordinate": [
                "31.715570, -82.381860",
                "32.100000, -83.500000",
                " 33.0, -84.0 ",
            ],
            "Year": [2015, 2015, 2015],
            "aadt": [3840, 1500, 120000],
            "aadtt": [622, 50, 13000],
            "k30": [0.0851, 0.09, 0.085],
            "d30": [0.52, 0.55, 0.51],
        }
    )


def test_load_2015_coordinate_schema(tmp_path) -> None:
    raw = _minimal_2015_frame()
    xlsx = tmp_path / "2015.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx,
        year=2015,
        schema_variant=SCHEMA_2015_COORDINATE,
        source_tag="xlsx:2015_end_of_year_all_tcs.xlsx",
    )

    assert list(out.columns) == HISTORIC_STATIONS_COLUMNS
    assert len(out) == 3
    assert out["year"].unique().tolist() == [2015]
    assert out.loc[out["tc_number"] == "001-0101", "latitude"].iloc[0] == pytest.approx(31.715570)
    assert out.loc[out["tc_number"] == "001-0101", "longitude"].iloc[0] == pytest.approx(-82.381860)
    assert out.loc[out["tc_number"] == "001-0101", "aadt"].iloc[0] == 3840
    assert out["statistics_type"].isna().all()
    assert out["functional_class"].isna().all()
    assert out["single_unit_aadt"].isna().all()
    assert out["combo_unit_aadt"].isna().all()
    assert out.loc[out["tc_number"] == "001-0101", "k_factor"].iloc[0] == pytest.approx(0.0851)
    assert out.loc[out["tc_number"] == "001-0101", "d_factor"].iloc[0] == pytest.approx(0.52)
    assert out["future_aadt"].isna().all()
    assert (out["source"] == "xlsx:2015_end_of_year_all_tcs.xlsx").all()


# ---- 2016 CSV schema ----


def _minimal_2016_csv_content() -> str:
    return (
        "ROUTE_ID,FROM_MILEP,TO_MILEPOI,COUNTY_COD,TC_NUMBER,AADT,AADT_SINGL,PCT_PEAK_S,"
        "AADT_COMBI,PCT_PEAK_C,K_FACTOR,D_Factor,FUTURE_AAD,Shape_Leng,Shape_Le_1,Lat,Long\n"
        "GA001,0.0,1.0,001,001-0101,3840,286,0.07,336,0.087,0.0851,0.52,8480,1000,1000,31.715570,-82.381860\n"
        "GA001,1.0,2.0,001,001-0101,3840,286,0.07,336,0.087,0.0851,0.52,8480,1000,1000,31.715570,-82.381860\n"
        "GA002,0.0,1.0,002,002-0010,1500,40,0.05,10,0.03,0.09,0.55,2000,500,500,32.100000,-83.500000\n"
    )


def test_load_2016_csv_schema(tmp_path) -> None:
    csv_path = tmp_path / "Traffic_Published_2016.csv"
    csv_path.write_text(_minimal_2016_csv_content())

    out = load_station_csv(
        csv_path=csv_path,
        year=2016,
        source_tag="csv:Traffic_Published_2016.csv",
    )

    assert list(out.columns) == HISTORIC_STATIONS_COLUMNS
    # Two segments with TC 001-0101 should be deduped to 1 station
    assert len(out) == 2
    assert out["year"].unique().tolist() == [2016]
    row1 = out[out["tc_number"] == "001-0101"].iloc[0]
    assert row1["latitude"] == pytest.approx(31.715570)
    assert row1["longitude"] == pytest.approx(-82.381860)
    assert row1["aadt"] == 3840
    assert row1["single_unit_aadt"] == 286
    assert row1["combo_unit_aadt"] == 336
    assert row1["future_aadt"] == 8480
    assert out["statistics_type"].isna().all()
    assert out["functional_class"].isna().all()


# ---- 2017/2019 text Lat/Long schema ----


def _minimal_2017_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Station ID": ["001-0101", "002-0010"],
            "Functional Class": [
                "3U : Urban Principal Arterial - Other",
                "7R : Rural Local",
            ],
            "Lat/Long": [
                "31.715570, -82.381860",
                "32.100000, -83.500000",
            ],
            "Year": [2017, 2017],
            "AADT": [4200, 1550],
            "Statistics type": ["Estimated", "Actual"],
            "Single-Unit Truck AADT": [290, 42],
            "Combo-Unit Truck AADT": [340, 12],
            "% Peak SU Trucks": [0.07, 0.05],
            "% Peak CU Trucks": [0.087, 0.03],
            "K-Factor": [0.085, 0.09],
            "D-Factor": [0.52, 0.55],
            "Future AADT": [8800, 2050],
            "Station Type": ["Short Term", "Short Term"],
        }
    )


def test_load_2017_2019_text_latlong(tmp_path) -> None:
    raw = _minimal_2017_frame()
    xlsx = tmp_path / "2017.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx,
        year=2017,
        schema_variant=SCHEMA_2017_2019_TEXT_LATLONG,
        source_tag="xlsx:2017 Annual Statistics.xlsx",
    )

    assert list(out.columns) == HISTORIC_STATIONS_COLUMNS
    assert len(out) == 2
    assert out["year"].unique().tolist() == [2017]
    assert out.loc[out["tc_number"] == "001-0101", "latitude"].iloc[0] == pytest.approx(31.715570)
    assert out.loc[out["tc_number"] == "001-0101", "aadt"].iloc[0] == 4200
    assert out["statistics_type"].tolist() == ["Estimated", "Actual"]
    assert out.loc[out["tc_number"] == "001-0101", "functional_class"].iloc[0] == 3
    assert out.loc[out["tc_number"] == "001-0101", "future_aadt"].iloc[0] == 8800


# ---- 2018 separate Lat/Long schema ----


def _minimal_2018_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Station ID": ["001-0101", "002-0010"],
            "Functional Class": [
                "3U : Urban Principal Arterial - Other",
                "7R : Rural Local",
            ],
            "Lat": [31.715570, 32.100000],
            "Long": [-82.381860, -83.500000],
            "Unnamed: 4": [None, None],
            "Year": [2018, 2018],
            "AADT": [4500, 1580],
            "Statistics type": ["Estimated", "Actual"],
            "Single-Unit Truck AADT": [295, 44],
            "Combo-Unit Truck AADT": [345, 13],
            "% Peak SU Trucks": [0.07, 0.05],
            "% Peak CU Trucks": [0.087, 0.03],
            "K-Factor": [0.085, 0.09],
            "D-Factor": [0.52, 0.55],
            "Future AADT": [9200, 2100],
            "Station Type": ["Short Term", "Short Term"],
        }
    )


def test_load_2018_separate_latlong(tmp_path) -> None:
    raw = _minimal_2018_frame()
    xlsx = tmp_path / "2018.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx,
        year=2018,
        schema_variant=SCHEMA_2018_SEPARATE_LATLONG,
        source_tag="xlsx:annualized_statistics_2018.xlsx",
    )

    assert list(out.columns) == HISTORIC_STATIONS_COLUMNS
    assert len(out) == 2
    assert out["year"].unique().tolist() == [2018]
    assert out.loc[out["tc_number"] == "001-0101", "latitude"].iloc[0] == pytest.approx(31.715570)
    assert out.loc[out["tc_number"] == "001-0101", "longitude"].iloc[0] == pytest.approx(-82.381860)
    assert out.loc[out["tc_number"] == "001-0101", "aadt"].iloc[0] == 4500
    assert out["statistics_type"].tolist() == ["Estimated", "Actual"]
    assert out.loc[out["tc_number"] == "001-0101", "functional_class"].iloc[0] == 3


# ---- recode_stats and normalize_tc_number ----


def test_recode_stats_ea_codes() -> None:
    assert recode_stats("E") == "Estimated"
    assert recode_stats("A") == "Actual"
    assert recode_stats("Actual_Est") == "Estimated"
    assert recode_stats("Estimated") == "Estimated"
    assert recode_stats("Actual") == "Actual"
    assert recode_stats("Calculated") == "Calculated"
    assert recode_stats(None) is None
    assert recode_stats(float("nan")) is None


def test_normalize_tc_float_suffix() -> None:
    assert normalize_tc_number("12345.0") == "12345"
    assert normalize_tc_number("001-0101") == "001-0101"
    assert normalize_tc_number(12345.0) == "12345"
    assert normalize_tc_number(12345) == "12345"
    assert normalize_tc_number("001-0101.0") == "001-0101"


# ---- bad coordinate filtering ----


def test_null_out_bad_coords_zeros_become_nan() -> None:
    df = pd.DataFrame({
        "latitude": [33.5, 0.0, 0.0004, 31.7],
        "longitude": [-84.5, 0.0, 0.0009, -82.4],
        "aadt": [5000, 3000, 2000, 4000],
    })
    result = _null_out_bad_coords(df)
    assert result.loc[0, "latitude"] == pytest.approx(33.5)
    assert pd.isna(result.loc[1, "latitude"])
    assert pd.isna(result.loc[1, "longitude"])
    assert pd.isna(result.loc[2, "latitude"])
    assert result.loc[3, "latitude"] == pytest.approx(31.7)
    assert result.loc[0, "aadt"] == 5000
    assert result.loc[1, "aadt"] == 3000


def test_2015_zero_coords_nulled(tmp_path) -> None:
    raw = pd.DataFrame({
        "Station ID": ["001-0101", "BAD-0001"],
        "Coordinate": ["31.715570, -82.381860", "0.0, 0.0"],
        "Year": [2015, 2015],
        "aadt": [3840, 1500],
        "aadtt": [622, 50],
        "k30": [0.085, 0.09],
        "d30": [0.52, 0.55],
    })
    xlsx = tmp_path / "2015.xlsx"
    raw.to_excel(xlsx, index=False)

    out = load_station_xlsx(
        xlsx_path=xlsx, year=2015,
        schema_variant=SCHEMA_2015_COORDINATE,
        source_tag="test",
    )
    good = out[out["tc_number"] == "001-0101"].iloc[0]
    bad = out[out["tc_number"] == "BAD-0001"].iloc[0]
    assert good["latitude"] == pytest.approx(31.715570)
    assert pd.isna(bad["latitude"])
    assert pd.isna(bad["longitude"])
    assert bad["aadt"] == 1500


def test_2016_csv_flexible_column_names(tmp_path) -> None:
    """Verify CSV loader handles both AADT_SINGLE_UNIT and AADT_SINGL."""
    content = (
        "ROUTE_ID,FROM_MILEPOINT,TO_MILEPOINT,COUNTY_COD,TC_NUMBER,AADT,"
        "AADT_SINGLE_UNIT,PCT_PEAK_SINGLE,AADT_COMBINATION,PCT_PEAK_COMBINATION,"
        "K_FACTOR,D_Factor,FUTURE_AADT,Lat,Long\n"
        "GA001,0,1,001,001-0101,5350,244,0.38,1090,1.01,8.83,55.95,6170,30.703,-84.388\n"
    )
    csv_path = tmp_path / "test_2016.csv"
    csv_path.write_text(content)

    out = load_station_csv(csv_path=csv_path, year=2016, source_tag="test")
    assert len(out) == 1
    assert out.iloc[0]["single_unit_aadt"] == 244
    assert out.iloc[0]["combo_unit_aadt"] == 1090
    assert out.iloc[0]["future_aadt"] == 6170


def test_2016_csv_missing_required_alias_raises(tmp_path) -> None:
    content = (
        "ROUTE_ID,FROM_MILEPOINT,TO_MILEPOINT,COUNTY_COD,TC_NUMBER,AADT,"
        "PCT_PEAK_SINGLE,AADT_COMBINATION,PCT_PEAK_COMBINATION,"
        "K_FACTOR,D_Factor,FUTURE_AADT,Lat,Long\n"
        "GA001,0,1,001,001-0101,5350,0.38,1090,1.01,8.83,55.95,6170,30.703,-84.388\n"
    )
    csv_path = tmp_path / "test_2016_missing_alias.csv"
    csv_path.write_text(content)

    with pytest.raises(KeyError, match="single_unit_aadt"):
        load_station_csv(csv_path=csv_path, year=2016, source_tag="test")
