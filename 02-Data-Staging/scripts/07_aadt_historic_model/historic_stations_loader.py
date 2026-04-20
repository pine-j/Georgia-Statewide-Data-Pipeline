"""Load annualized_statistics xlsx rows into the `historic_stations` schema.

Handles the two schema variants described in
`02-Data-Staging/docs/historic_traffic_inventory.md §3c`:

- 2020/2021: `Lat/Long` text column like `"31.715570, -82.381860"`.
- 2022/2023: numeric `Latitude` and `Longitude` columns.

All other station attributes are consistent across 2020-2023 (same column
names, same dtypes after light coercion). 2024 station rows come from
`TRAFFIC_Data_2024.gdb` in a separate orchestrator-only code path.
"""

from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd

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


def _coerce_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _coerce_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float64")


def load_station_xlsx(
    xlsx_path: Path,
    year: int,
    schema_variant: str,
    source_tag: str,
) -> pd.DataFrame:
    """Read a historical annualized_statistics xlsx and emit canonical rows."""

    raw = pd.read_excel(xlsx_path, sheet_name=0)

    if schema_variant == SCHEMA_2020_2021_TEXT_LATLONG:
        latlong = raw["Lat/Long"].map(parse_lat_long)
        latitude = latlong.map(lambda pair: pair[0])
        longitude = latlong.map(lambda pair: pair[1])
    elif schema_variant == SCHEMA_2022_2023_NUMERIC_LATLONG:
        latitude = raw["Latitude"]
        longitude = raw["Longitude"]
    else:
        raise ValueError(f"Unknown schema_variant: {schema_variant!r}")

    out = pd.DataFrame(
        {
            "year": [year] * len(raw),
            "tc_number": raw["Station ID"].astype("string"),
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
            "traffic_class": pd.Series([pd.NA] * len(raw), dtype="string"),
            "future_aadt": _coerce_int(raw["Future AADT"]),
            "source": [source_tag] * len(raw),
        }
    )
    return out[HISTORIC_STATIONS_COLUMNS]


__all__ = [
    "HISTORIC_STATIONS_COLUMNS",
    "SCHEMA_2020_2021_TEXT_LATLONG",
    "SCHEMA_2022_2023_NUMERIC_LATLONG",
    "load_station_xlsx",
    "parse_functional_class",
    "parse_lat_long",
]
