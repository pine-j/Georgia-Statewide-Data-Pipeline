"""Load annualized_statistics xlsx/csv rows into the `historic_stations` schema.

Handles schema variants described in
`02-Data-Staging/docs/historic_traffic_inventory.md §3c`:

- 2015: `Coordinate` text column, lowercase `aadt`, no FC/stats type
- 2016: CSV with `TC_NUMBER`, separate `Lat`/`Long`, segment-level dedup
- 2017/2019: `Lat/Long` text column, `Statistics type` words (same as 2020/2021)
- 2018: separate `Lat`/`Long` columns + `Unnamed: 4` artifact
- 2020/2021: `Lat/Long` text column like `"31.715570, -82.381860"`
- 2022/2023: numeric `Latitude` and `Longitude` columns

2024 station rows come from `TRAFFIC_Data_2024.gdb` in a separate orchestrator
code path.
"""

from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd

SCHEMA_2015_COORDINATE = "2015_coordinate"
SCHEMA_2016_CSV = "2016_csv"
SCHEMA_2017_2019_TEXT_LATLONG = "2017_2019_text_latlong"
SCHEMA_2018_SEPARATE_LATLONG = "2018_separate_latlong"
SCHEMA_2020_2021_TEXT_LATLONG = "2020_2021_text_latlong"
SCHEMA_2022_2023_NUMERIC_LATLONG = "2022_2023_numeric_latlong"

HISTORIC_STATIONS_COLUMNS = [
    "year",
    "tc_number",
    "latitude",
    "longitude",
    "aadt",
    "statistics_type",
    "single_unit_aadt",
    "combo_unit_aadt",
    "k_factor",
    "d_factor",
    "functional_class",
    "station_type",
    "traffic_class",
    "future_aadt",
    "source",
]

_FC_PREFIX_RE = re.compile(r"^\s*(\d+)")


def parse_lat_long(value) -> tuple[float, float]:
    """Parse the `Lat/Long` text field.

    Accepts `"<lat>, <lon>"` with optional whitespace. Returns a pair of
    NaNs on any parse failure.
    """

    if value is None or (isinstance(value, float) and math.isnan(value)):
        return (float("nan"), float("nan"))
    if not isinstance(value, str):
        return (float("nan"), float("nan"))
    parts = value.split(",")
    if len(parts) != 2:
        return (float("nan"), float("nan"))
    try:
        return (float(parts[0].strip()), float(parts[1].strip()))
    except ValueError:
        return (float("nan"), float("nan"))


def parse_functional_class(value) -> int | None:
    """Extract the leading integer from an FC string like `"3U : Urban ..."`."""

    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if not isinstance(value, str):
        return None
    match = _FC_PREFIX_RE.match(value)
    if match is None:
        return None
    return int(match.group(1))


def recode_stats(value) -> str | None:
    """Normalize statistics-type values across all schema eras."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    s = str(value).strip()
    if s == "E" or s == "Actual_Est":
        return "Estimated"
    if s == "A":
        return "Actual"
    if s in ("Estimated", "Actual", "Calculated"):
        return s
    return s


def normalize_tc_number(value) -> str:
    """Strip `.0` suffix from TC_NUMBER values read as float."""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if value == int(value):
            return str(int(value))
        return str(value)
    s = str(value)
    if s.endswith(".0"):
        return s[:-2]
    return s


def _coerce_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dtype == object:
        numeric = numeric.astype("float64")
    return numeric.round(0).astype("Int64")


def _coerce_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float64")


def _build_full_schema_frame(
    raw: pd.DataFrame,
    year: int,
    latitude: pd.Series,
    longitude: pd.Series,
    source_tag: str,
) -> pd.DataFrame:
    """Build canonical frame from xlsx variants that carry the full 2017+ column set."""
    n = len(raw)
    return pd.DataFrame(
        {
            "year": [year] * n,
            "tc_number": raw["Station ID"].map(normalize_tc_number).astype("string"),
            "latitude": _coerce_float(latitude),
            "longitude": _coerce_float(longitude),
            "aadt": _coerce_int(raw["AADT"]),
            "statistics_type": raw["Statistics type"].astype("string"),
            "single_unit_aadt": _coerce_int(raw["Single-Unit Truck AADT"]),
            "combo_unit_aadt": _coerce_int(raw["Combo-Unit Truck AADT"]),
            "k_factor": _coerce_float(raw["K-Factor"]),
            "d_factor": _coerce_float(raw["D-Factor"]),
            "functional_class": raw["Functional Class"].map(parse_functional_class).astype("Int64"),
            "station_type": raw["Station Type"].astype("string"),
            "traffic_class": pd.Series([pd.NA] * n, dtype="string"),
            "future_aadt": _coerce_int(raw["Future AADT"]),
            "source": [source_tag] * n,
        }
    )


def load_station_xlsx(
    xlsx_path: Path,
    year: int,
    schema_variant: str,
    source_tag: str,
) -> pd.DataFrame:
    """Read a historical annualized_statistics xlsx and emit canonical rows."""

    raw = pd.read_excel(xlsx_path, sheet_name=0)

    if schema_variant == SCHEMA_2015_COORDINATE:
        latlong = raw["Coordinate"].map(parse_lat_long)
        latitude = latlong.map(lambda pair: pair[0])
        longitude = latlong.map(lambda pair: pair[1])
        n = len(raw)
        out = pd.DataFrame(
            {
                "year": [year] * n,
                "tc_number": raw["Station ID"].map(normalize_tc_number).astype("string"),
                "latitude": _coerce_float(latitude),
                "longitude": _coerce_float(longitude),
                "aadt": _coerce_int(raw["aadt"]),
                "statistics_type": pd.Series([pd.NA] * n, dtype="string"),
                "single_unit_aadt": pd.Series([pd.NA] * n, dtype="Int64"),
                "combo_unit_aadt": pd.Series([pd.NA] * n, dtype="Int64"),
                "k_factor": _coerce_float(raw["k30"]),
                "d_factor": _coerce_float(raw["d30"]),
                "functional_class": pd.Series([pd.NA] * n, dtype="Int64"),
                "station_type": pd.Series([pd.NA] * n, dtype="string"),
                "traffic_class": pd.Series([pd.NA] * n, dtype="string"),
                "future_aadt": pd.Series([pd.NA] * n, dtype="Int64"),
                "source": [source_tag] * n,
            }
        )
        return out[HISTORIC_STATIONS_COLUMNS]

    if schema_variant in (SCHEMA_2017_2019_TEXT_LATLONG, SCHEMA_2020_2021_TEXT_LATLONG):
        latlong = raw["Lat/Long"].map(parse_lat_long)
        latitude = latlong.map(lambda pair: pair[0])
        longitude = latlong.map(lambda pair: pair[1])
    elif schema_variant == SCHEMA_2018_SEPARATE_LATLONG:
        latitude = raw["Lat"]
        longitude = raw["Long"]
    elif schema_variant == SCHEMA_2022_2023_NUMERIC_LATLONG:
        latitude = raw["Latitude"]
        longitude = raw["Longitude"]
    else:
        raise ValueError(f"Unknown schema_variant: {schema_variant!r}")

    out = _build_full_schema_frame(raw, year, latitude, longitude, source_tag)
    return out[HISTORIC_STATIONS_COLUMNS]


def load_station_csv(
    csv_path: Path,
    year: int,
    source_tag: str,
) -> pd.DataFrame:
    """Read 2016 segment-level CSV and deduplicate to station-level rows."""
    raw = pd.read_csv(csv_path)

    raw["TC_NUMBER"] = raw["TC_NUMBER"].map(normalize_tc_number).astype("string")
    raw = raw.drop_duplicates(subset=["TC_NUMBER"], keep="first")
    raw = raw[raw["TC_NUMBER"].notna() & (raw["TC_NUMBER"] != "")]
    raw = raw.reset_index(drop=True)

    def _pick(candidates: list[str]) -> pd.Series:
        for c in candidates:
            if c in raw.columns:
                return raw[c]
        return pd.Series([pd.NA] * len(raw))

    n = len(raw)
    out = pd.DataFrame(
        {
            "year": [year] * n,
            "tc_number": raw["TC_NUMBER"].values,
            "latitude": _coerce_float(raw["Lat"]),
            "longitude": _coerce_float(raw["Long"]),
            "aadt": _coerce_int(raw["AADT"]),
            "statistics_type": pd.Series([pd.NA] * n, dtype="string"),
            "single_unit_aadt": _coerce_int(_pick(["AADT_SINGLE_UNIT", "AADT_SINGL"])),
            "combo_unit_aadt": _coerce_int(_pick(["AADT_COMBINATION", "AADT_COMBI"])),
            "k_factor": _coerce_float(_pick(["K_FACTOR", "K_Factor"])),
            "d_factor": _coerce_float(_pick(["D_Factor", "D_FACTOR"])),
            "functional_class": pd.Series([pd.NA] * n, dtype="Int64"),
            "station_type": pd.Series([pd.NA] * n, dtype="string"),
            "traffic_class": pd.Series([pd.NA] * n, dtype="string"),
            "future_aadt": _coerce_int(_pick(["FUTURE_AADT", "FUTURE_AAD"])),
            "source": [source_tag] * n,
        }
    )
    return out[HISTORIC_STATIONS_COLUMNS]


__all__ = [
    "HISTORIC_STATIONS_COLUMNS",
    "SCHEMA_2015_COORDINATE",
    "SCHEMA_2016_CSV",
    "SCHEMA_2017_2019_TEXT_LATLONG",
    "SCHEMA_2018_SEPARATE_LATLONG",
    "SCHEMA_2020_2021_TEXT_LATLONG",
    "SCHEMA_2022_2023_NUMERIC_LATLONG",
    "load_station_csv",
    "load_station_xlsx",
    "normalize_tc_number",
    "parse_functional_class",
    "parse_lat_long",
    "recode_stats",
]
