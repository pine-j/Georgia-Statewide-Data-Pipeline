"""Normalize Georgia roadway inventory data onto official GDOT route geometry.

This workflow uses the official `GA_2024_Routes` geometry as the base network,
then attaches current and historic GDOT traffic fields by route ID and
milepoint intervals.

Historical route-segment files contribute actual AADT series only. Any legacy
`Future_AADT` / `FUTURE_AAD` values in older files are ignored. The only
canonical future projection kept in the normalized network is `FUTURE_AADT`
from the current 2024 GDOT traffic record.

Data sources:
1. `Road_Inventory_2024.gdb` layer `GA_2024_Routes`
   Official full roadway geometry.
2. `TRAFFIC_Data_2024.gdb` layer `TRAFFIC_DataYear2024`
   Current traffic segmentation with AADT, truck counts, VMT, and factors.
3. `Traffic_Historical.zip`
   Historic segment traffic tables for 2010-2019.

Output:
- `02-Data-Staging/cleaned/roadway_inventory_cleaned.csv`
- `02-Data-Staging/spatial/base_network.gpkg` layers
  `roadway_segments`, `county_boundaries`, `district_boundaries`
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import tempfile
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import force_2d, line_merge
from shapely.geometry import LineString
from shapely.ops import substring

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "GA_RDWY_INV"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
CLEANED_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned"

TARGET_CRS = "EPSG:32617"

GDOT_BOUNDARIES_SERVICE = "https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Boundaries/MapServer"
COUNTY_BOUNDARIES_URL = (
    f"{GDOT_BOUNDARIES_SERVICE}/1/query?where=1%3D1&outFields=*&f=geojson"
)
DISTRICT_BOUNDARIES_URL = (
    f"{GDOT_BOUNDARIES_SERVICE}/3/query?where=1%3D1&outFields=*&f=geojson"
)

DISTRICT_NAME_LOOKUP = {
    1: "District 1 - Gainesville",
    2: "District 2 - Tennille",
    3: "District 3 - Thomaston",
    4: "District 4 - Tifton",
    5: "District 5 - Jesup",
    6: "District 6 - Cartersville",
    7: "District 7 - Chamblee",
}


def load_json_mapping(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


COUNTY_NAME_LOOKUP = {
    str(code).zfill(3): name
    for code, name in load_json_mapping(CONFIG_DIR / "county_codes.json").items()
}
DISTRICT_SHORT_NAME_LOOKUP = {
    int(code): name
    for code, name in load_json_mapping(CONFIG_DIR / "district_codes.json").items()
}
ROADWAY_DOMAIN_LABELS = load_json_mapping(CONFIG_DIR / "roadway_domain_labels.json")

TRAFFIC_GDB_NAME = "TRAFFIC_Data_2024.gdb"
ROAD_INV_GDB_NAME = "Road_Inventory_2024.gdb"
TRAFFIC_HISTORICAL_ZIP = "Traffic_Historical.zip"

ROUTE_LAYER = "GA_2024_Routes"
CURRENT_TRAFFIC_LAYER = "TRAFFIC_DataYear2024"
HISTORICAL_YEARS = list(range(2010, 2021))
HISTORICAL_UNAVAILABLE_YEARS = {
    2021: "GDOT release is station-based Excel only; no route-segment network geometry found.",
    2022: "GDOT release is station-based Excel only; no route-segment network geometry found.",
    2023: "GDOT release is station-based Excel only; no route-segment network geometry found.",
}

MILEPOINT_PRECISION = 4
MILEPOINT_TOLERANCE = 1e-4

ROUTE_MERGE_KEYS = ["ROUTE_ID", "BeginPoint", "EndPoint"]

ROUTE_ATTRIBUTE_LAYERS = {
    "COUNTY_ID": ("COUNTY_IDVn", "COUNTY_ID"),
    "F_SYSTEM": ("F_SYSTEMVn", "F_SYSTEM"),
    "NHS": ("NHSVn", "NHS"),
    "FACILITY_TYPE": ("FACILITY_TYPEVn", "FACILITY_TYPE"),
    "THROUGH_LANES": ("THROUGH_LANESVn", "THROUGH_LANES"),
    "MEDIAN_TYPE": ("MEDIAN_TYPEVn", "MEDIAN_TYPE"),
    "SHOULDER_TYPE": ("SHOULDER_TYPEVn", "SHOULDER_TYPE"),
    "SURFACE_TYPE": ("SURFACE_TYPEVn", "SURFACE_TYPE"),
    "URBAN_ID": ("URBAN_IDVn", "URBAN_ID"),
}

CURRENT_TRAFFIC_FIELDS = {
    "COUNTY_ID": "CURRENT_COUNTY_ID",
    "GDOT_District": "CURRENT_GDOT_DISTRICT",
    "AADTRound": "AADT_2024",
    "Single_Unit_AADT": "SINGLE_UNIT_AADT_2024",
    "Combo_Unit_AADT": "COMBO_UNIT_AADT_2024",
    "Future_AADT": "FUTURE_AADT_2024",
    "K_Factor": "K_FACTOR",
    "D_Factor": "D_FACTOR",
    "VMT": "VMT_2024",
    "TruckVMT": "TRUCK_VMT_2024",
    "Traffic_Class": "TRAFFIC_CLASS_2024",
    "TC_NUMBER": "TC_NUMBER",
}

HISTORICAL_COLUMN_CANDIDATES = {
    "ROUTE_ID": ["ROUTE_ID", "Route_ID", "RCLINK"],
    "FROM_MILEPOINT": ["FROM_MILEPOINT", "Begin_Poin", "BEG_MP", "FROM_MILEP"],
    "TO_MILEPOINT": ["TO_MILEPOINT", "End_Point", "END_MP", "TO_MILEPOI"],
    "COUNTY_ID": ["COUNTY_ID", "COUNTY_COD", "COUNTY_CODE", "COUNTY_COD", "COUNTY"],
    "AADT": ["AADT_VN", "AADT"],
    "COMBO_UNIT_AADT": ["AADT_COMBI", "AADT_Combi"],
    "SINGLE_UNIT_AADT": ["AADT_SINGL", "AADT_Singl"],
    "K_FACTOR": ["K_FACTOR", "K_FACTOR_V"],
    "D_FACTOR": ["D_FACTOR", "D_Factor", "DIR_FACTOR", "Dir_Factor"],
    "PCT_PEAK_C": ["PCT_PEAK_C"],
    "PCT_PEAK_S": ["PCT_PEAK_S"],
    "URBAN_CODE": ["URBAN_CODE"],
    "F_SYSTEM": ["F_SYSTEM", "F_SYSTEM_V"],
    "FACILITY_TYPE": ["FACILITY_TYPE", "FACILITY_T"],
    "NHS": ["NHS", "NHS_VN"],
}


def find_path(raw_dir: Path, pattern: str) -> Path:
    matches = list(raw_dir.rglob(pattern))
    if not matches:
        raise FileNotFoundError(f"Could not find {pattern} under {raw_dir}")
    return matches[0]


def round_milepoint(value: float | int | str | None) -> float:
    if pd.isna(value):
        return np.nan
    rounded = round(float(value), MILEPOINT_PRECISION)
    if abs(rounded) < MILEPOINT_TOLERANCE:
        return 0.0
    return rounded


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        col.strip().replace(" ", "_") if isinstance(col, str) else col
        for col in df.columns
    ]
    return df


def decode_lookup_value(value, lookup: dict, zero_pad: int | None = None) -> str | None:
    if pd.isna(value):
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text in lookup:
            return lookup[text]
        upper_text = text.upper()
        if upper_text in lookup:
            return lookup[upper_text]
        try:
            numeric_text = int(float(text))
        except ValueError:
            return None
        if zero_pad is not None:
            padded = f"{numeric_text:0{zero_pad}d}"
            if padded in lookup:
                return lookup[padded]
        return lookup.get(numeric_text) or lookup.get(str(numeric_text))

    try:
        numeric_value = int(float(value))
    except (TypeError, ValueError):
        return None

    if zero_pad is not None:
        padded = f"{numeric_value:0{zero_pad}d}"
        if padded in lookup:
            return lookup[padded]

    return lookup.get(numeric_value) or lookup.get(str(numeric_value))


def get_or_empty_series(df: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name in df.columns:
        return df[column_name]
    return pd.Series([None] * len(df), index=df.index, dtype="object")


def add_decoded_label_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()

    gdf["COUNTY_NAME"] = get_or_empty_series(gdf, "COUNTY_CODE").map(
        lambda value: decode_lookup_value(value, COUNTY_NAME_LOOKUP, zero_pad=3)
    )
    gdf["DISTRICT_NAME"] = get_or_empty_series(gdf, "DISTRICT").map(
        lambda value: decode_lookup_value(value, DISTRICT_SHORT_NAME_LOOKUP)
    )
    gdf["DISTRICT_LABEL"] = get_or_empty_series(gdf, "DISTRICT").map(
        lambda value: decode_lookup_value(value, DISTRICT_NAME_LOOKUP)
    )

    gdf["SYSTEM_CODE_LABEL"] = get_or_empty_series(gdf, "SYSTEM_CODE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["system_code"])
    )
    gdf["FUNCTION_TYPE_LABEL"] = get_or_empty_series(gdf, "FUNCTION_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["function_type"])
    )
    gdf["PARSED_FUNCTION_TYPE_LABEL"] = get_or_empty_series(gdf, "PARSED_FUNCTION_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["function_type"])
    )
    gdf["F_SYSTEM_LABEL"] = get_or_empty_series(gdf, "F_SYSTEM").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["functional_class"])
    )
    gdf["FUNCTIONAL_CLASS_LABEL"] = get_or_empty_series(gdf, "FUNCTIONAL_CLASS").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["functional_class"])
    )
    gdf["FACILITY_TYPE_LABEL"] = get_or_empty_series(gdf, "FACILITY_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["facility_type"])
    )
    gdf["NHS_LABEL"] = get_or_empty_series(gdf, "NHS").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["nhs"])
    )
    gdf["NHS_IND_LABEL"] = get_or_empty_series(gdf, "NHS_IND").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["nhs"])
    )
    gdf["MEDIAN_TYPE_LABEL"] = get_or_empty_series(gdf, "MEDIAN_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["median_type"])
    )
    gdf["SHOULDER_TYPE_LABEL"] = get_or_empty_series(gdf, "SHOULDER_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["shoulder_type"])
    )
    gdf["SURFACE_TYPE_LABEL"] = get_or_empty_series(gdf, "SURFACE_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["surface_type"])
    )
    gdf["URBAN_CODE_LABEL"] = get_or_empty_series(gdf, "URBAN_CODE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["urban_code"], zero_pad=5)
    )

    gdf["DIRECTION_LABEL"] = get_or_empty_series(gdf, "DIRECTION").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["route_direction"])
    )
    gdf["PARSED_DIRECTION_LABEL"] = get_or_empty_series(gdf, "PARSED_DIRECTION").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["route_direction"])
    )
    gdf["ROUTE_DIRECTION_LABEL"] = get_or_empty_series(gdf, "ROUTE_DIRECTION").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["route_direction"])
    )
    gdf["PARSED_SYSTEM_CODE_LABEL"] = get_or_empty_series(gdf, "PARSED_SYSTEM_CODE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["system_code"])
    )
    gdf["ROUTE_TYPE_LABEL"] = get_or_empty_series(gdf, "ROUTE_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["system_code"])
    )

    return gdf


def parse_route_id(df: pd.DataFrame) -> pd.DataFrame:
    if "ROUTE_ID" not in df.columns:
        return df

    route = df["ROUTE_ID"].fillna("").astype(str)
    df["PARSED_FUNCTION_TYPE"] = route.str[0:1]
    df["PARSED_COUNTY_CODE"] = route.str[1:4]
    df["PARSED_SYSTEM_CODE"] = route.str[4:5]
    df["PARSED_ROUTE_NUMBER"] = route.str[5:11]
    df["PARSED_SUFFIX"] = route.str[11:13]
    df["PARSED_DIRECTION"] = route.str[13:]
    return df


def build_unique_id(df: pd.DataFrame) -> pd.DataFrame:
    df["unique_id"] = (
        df["ROUTE_ID"].astype(str)
        + "_"
        + df["FROM_MILEPOINT"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "NA")
        + "_"
        + df["TO_MILEPOINT"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "NA")
    )
    return df


def load_route_geometry(gdb_path: Path) -> gpd.GeoDataFrame:
    logger.info("Loading route geometry from %s (%s)", gdb_path.name, ROUTE_LAYER)
    gdf = gpd.read_file(gdb_path, layer=ROUTE_LAYER, engine="pyogrio")
    gdf = clean_column_names(gdf)
    gdf["ROUTE_ID"] = gdf["ROUTE_ID"].astype(str)
    gdf["BeginPoint"] = pd.to_numeric(gdf["BeginPoint"], errors="coerce").apply(round_milepoint)
    gdf["EndPoint"] = pd.to_numeric(gdf["EndPoint"], errors="coerce").apply(round_milepoint)
    gdf["FROM_MILEPOINT"] = pd.to_numeric(gdf["FROM_MILEPOINT"], errors="coerce").apply(round_milepoint)
    gdf["TO_MILEPOINT"] = pd.to_numeric(gdf["TO_MILEPOINT"], errors="coerce").apply(round_milepoint)
    logger.info("Loaded %d route features", len(gdf))
    return gdf


def load_route_attribute_layer(
    gdb_path: Path,
    layer_name: str,
    source_value_col: str,
    output_col: str,
) -> pd.DataFrame:
    df = gpd.read_file(gdb_path, layer=layer_name, engine="pyogrio", ignore_geometry=True)
    df = clean_column_names(df)
    df = df.rename(columns={"RouteId": "ROUTE_ID"})
    keep = ["ROUTE_ID", "BeginPoint", "EndPoint", source_value_col]
    df = df[keep].copy()
    df["ROUTE_ID"] = df["ROUTE_ID"].astype(str)
    df["BeginPoint"] = pd.to_numeric(df["BeginPoint"], errors="coerce").apply(round_milepoint)
    df["EndPoint"] = pd.to_numeric(df["EndPoint"], errors="coerce").apply(round_milepoint)
    df[output_col] = pd.to_numeric(df[source_value_col], errors="coerce")
    df = df.drop(columns=[source_value_col])
    return df


def enrich_routes_with_static_attributes(routes: gpd.GeoDataFrame, gdb_path: Path) -> gpd.GeoDataFrame:
    enriched = routes.copy()
    for layer_name, (source_value_col, output_col) in ROUTE_ATTRIBUTE_LAYERS.items():
        logger.info("Joining route attribute layer %s", layer_name)
        layer_df = load_route_attribute_layer(gdb_path, layer_name, source_value_col, output_col)
        before_non_null = enriched.get(output_col, pd.Series(dtype=float)).notna().sum() if output_col in enriched.columns else 0
        enriched = enriched.merge(layer_df, on=ROUTE_MERGE_KEYS, how="left")
        after_non_null = enriched[output_col].notna().sum()
        logger.info(
            "  %s coverage after exact merge: %d / %d",
            output_col,
            after_non_null,
            len(enriched),
        )
        if before_non_null and after_non_null < before_non_null:
            logger.warning("  Non-null count dropped for %s after merge", output_col)
    return enriched


def load_current_traffic(traffic_gdb_path: Path) -> pd.DataFrame:
    logger.info("Loading current traffic from %s (%s)", traffic_gdb_path.name, CURRENT_TRAFFIC_LAYER)
    df = gpd.read_file(
        traffic_gdb_path,
        layer=CURRENT_TRAFFIC_LAYER,
        engine="pyogrio",
        ignore_geometry=True,
    )
    df = clean_column_names(df)
    df["ROUTE_ID"] = df["ROUTE_ID"].astype(str)
    df["FROM_MILEPOINT"] = pd.to_numeric(df["FROM_MILEPOINT"], errors="coerce").apply(round_milepoint)
    df["TO_MILEPOINT"] = pd.to_numeric(df["TO_MILEPOINT"], errors="coerce").apply(round_milepoint)

    keep = ["ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT", *CURRENT_TRAFFIC_FIELDS.keys()]
    df = df[keep].copy()
    df = df.rename(columns=CURRENT_TRAFFIC_FIELDS)

    numeric_cols = [
        "CURRENT_COUNTY_ID",
        "CURRENT_GDOT_DISTRICT",
        "AADT_2024",
        "SINGLE_UNIT_AADT_2024",
        "COMBO_UNIT_AADT_2024",
        "FUTURE_AADT_2024",
        "K_FACTOR",
        "D_FACTOR",
        "VMT_2024",
        "TRUCK_VMT_2024",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["TO_MILEPOINT"] > df["FROM_MILEPOINT"]].copy()
    logger.info(
        "Loaded %d current traffic segments across %d route IDs",
        len(df),
        df["ROUTE_ID"].nunique(),
    )
    return df.sort_values(["ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT"]).reset_index(drop=True)


def build_route_id_lookup(routes: gpd.GeoDataFrame) -> tuple[set[str], dict[str, list[str]]]:
    route_ids = sorted(routes["ROUTE_ID"].astype(str).unique())
    route_id_set = set(route_ids)
    route_base_lookup: dict[str, list[str]] = {}
    for route_id in route_ids:
        route_base_lookup.setdefault(route_id[:-3], []).append(route_id)
    return route_id_set, route_base_lookup


def normalize_current_style_route_base(route_id: str) -> str | None:
    raw = str(route_id).strip()
    if not raw:
        return None

    core = raw[:-3] if len(raw) > 3 and raw[-3:].isalpha() else raw
    if len(core) < 8:
        return None

    prefix = core[:5]
    stem = core[5:]
    if len(stem) < 3:
        return None

    suffix = stem[-2:]
    route_part = stem[:-2]
    if not route_part or len(route_part) > 6:
        return None
    if not suffix.isalnum():
        return None
    if not route_part.isalnum():
        return None

    return f"{prefix}{route_part.zfill(6)}{suffix}"


def legacy_historic_route_base_candidates(route_id: str) -> list[str]:
    raw = str(route_id).strip()
    if len(raw) != 10:
        return []

    county = raw[:3]
    system_code = raw[3]
    route_number = raw[4:8]
    suffix = raw[8:10]

    candidates = [f"1{county}{system_code}00{route_number}{suffix}"]
    if system_code == "1":
        candidates.append(f"1000{system_code}00{route_number}{suffix}")

    deduped: list[str] = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def resolve_historical_route_candidates(
    raw_route_id: str,
    route_id_set: set[str],
    route_base_lookup: dict[str, list[str]],
) -> list[str]:
    raw = str(raw_route_id).strip()
    if not raw or raw.lower() == "nan":
        return []
    if raw in route_id_set:
        return [raw]

    bases: list[str] = []

    current_style_base = normalize_current_style_route_base(raw)
    if current_style_base is not None:
        bases.append(current_style_base)
        if current_style_base[1:4] != "000" and current_style_base[4:5] == "1":
            bases.append(f"{current_style_base[0]}000{current_style_base[4:]}")

    bases.extend(legacy_historic_route_base_candidates(raw))

    resolved: list[str] = []
    seen_bases: set[str] = set()
    for base in bases:
        if base in seen_bases:
            continue
        seen_bases.add(base)
        resolved.extend(route_base_lookup.get(base, []))

    deduped: list[str] = []
    for route_id in resolved:
        if route_id not in deduped:
            deduped.append(route_id)
    return deduped


def read_historical_source_from_zip(traffic_zip: Path, year: int, temp_dir: Path) -> pd.DataFrame:
    with ZipFile(traffic_zip) as zf:
        target_shp = next(
            (name for name in zf.namelist() if name.endswith(f"Traffic_Data_GA_{year}.shp")),
            None,
        )
        if target_shp is not None:
            stem = target_shp[:-4]
            for suffix in [".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"]:
                candidate = f"{stem}{suffix}"
                if candidate in zf.namelist():
                    zf.extract(candidate, temp_dir)
            return gpd.read_file(temp_dir / target_shp, engine="pyogrio", ignore_geometry=True)

        if year == 2020:
            folder_prefix = "2010_thr_2023_Published_Traffic/2020_Published_Traffic/"
            members = [name for name in zf.namelist() if name.startswith(folder_prefix)]
            if not members:
                raise FileNotFoundError(f"No route-segment geodatabase found for {year} in {traffic_zip.name}")

            for member in members:
                zf.extract(member, temp_dir)

            extracted_dir = temp_dir / "2010_thr_2023_Published_Traffic" / "2020_Published_Traffic"
            temp_gdb = temp_dir / "Traffic_Data_GA_2020.gdb"
            if temp_gdb.exists():
                shutil.rmtree(temp_gdb)
            shutil.copytree(extracted_dir, temp_gdb)
            return gpd.read_file(temp_gdb, layer="Traffic_Data_2020", engine="pyogrio", ignore_geometry=True)

    raise FileNotFoundError(f"No route-segment traffic source found for {year} in {traffic_zip.name}")


def first_matching_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def normalize_historical_columns(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = clean_column_names(df)

    rename_map: dict[str, str] = {}
    for target_col, candidates in HISTORICAL_COLUMN_CANDIDATES.items():
        match = first_matching_column(df, candidates)
        if match is not None:
            rename_map[match] = target_col

    df = df.rename(columns=rename_map)

    required = {"ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT", "AADT"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Historic {year} is missing required columns: {sorted(missing)}")

    keep = [
        "ROUTE_ID",
        "FROM_MILEPOINT",
        "TO_MILEPOINT",
        "COUNTY_ID",
        "AADT",
        "COMBO_UNIT_AADT",
        "SINGLE_UNIT_AADT",
        "K_FACTOR",
        "D_FACTOR",
        "PCT_PEAK_C",
        "PCT_PEAK_S",
        "URBAN_CODE",
        "F_SYSTEM",
        "FACILITY_TYPE",
        "NHS",
    ]
    keep = [col for col in keep if col in df.columns]
    df = df[keep].copy()

    df["ROUTE_ID"] = df["ROUTE_ID"].astype(str).str.strip()
    df["FROM_MILEPOINT"] = pd.to_numeric(df["FROM_MILEPOINT"], errors="coerce").apply(round_milepoint)
    df["TO_MILEPOINT"] = pd.to_numeric(df["TO_MILEPOINT"], errors="coerce").apply(round_milepoint)

    for col in [c for c in keep if c not in {"ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT"}]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["TO_MILEPOINT"] > df["FROM_MILEPOINT"]].copy()

    rename_yearly = {
        "AADT": f"AADT_{year}",
        "COMBO_UNIT_AADT": f"COMBO_UNIT_AADT_{year}",
        "SINGLE_UNIT_AADT": f"SINGLE_UNIT_AADT_{year}",
        "K_FACTOR": f"K_FACTOR_{year}",
        "D_FACTOR": f"D_FACTOR_{year}",
    }
    df = df.rename(columns={k: v for k, v in rename_yearly.items() if k in df.columns})

    combo_col = f"COMBO_UNIT_AADT_{year}"
    single_col = f"SINGLE_UNIT_AADT_{year}"
    if combo_col in df.columns or single_col in df.columns:
        combo = df[combo_col] if combo_col in df.columns else pd.Series(0, index=df.index, dtype=float)
        single = df[single_col] if single_col in df.columns else pd.Series(0, index=df.index, dtype=float)
        df[f"TRUCK_AADT_{year}"] = combo.fillna(0) + single.fillna(0)
        df[f"TRUCK_PCT_{year}"] = np.where(
            df[f"AADT_{year}"] > 0,
            (df[f"TRUCK_AADT_{year}"] / df[f"AADT_{year}"]) * 100.0,
            np.nan,
        )

    return df


def expand_historical_route_ids(
    df: pd.DataFrame,
    year: int,
    route_id_set: set[str],
    route_base_lookup: dict[str, list[str]],
) -> pd.DataFrame:
    route_map = {
        route_id: resolve_historical_route_candidates(route_id, route_id_set, route_base_lookup)
        for route_id in df["ROUTE_ID"].dropna().astype(str).unique()
    }

    expanded = df.copy()
    expanded["SOURCE_ROUTE_ID"] = expanded["ROUTE_ID"].astype(str)
    expanded["MATCHED_ROUTE_IDS"] = expanded["SOURCE_ROUTE_ID"].map(route_map)

    unmatched_rows = int((~expanded["MATCHED_ROUTE_IDS"].map(bool)).sum())
    multi_match_rows = int((expanded["MATCHED_ROUTE_IDS"].map(len) > 1).sum())
    expanded = expanded[expanded["MATCHED_ROUTE_IDS"].map(bool)].copy()
    expanded = expanded.explode("MATCHED_ROUTE_IDS").drop(columns=["ROUTE_ID"])
    expanded = expanded.rename(columns={"MATCHED_ROUTE_IDS": "ROUTE_ID"})

    matched_unique_ids = sum(bool(candidates) for candidates in route_map.values())
    logger.info(
        "Historic %s route matching: %d / %d source route IDs matched, %d unmatched rows, %d multi-match rows, %d expanded rows",
        year,
        matched_unique_ids,
        len(route_map),
        unmatched_rows,
        multi_match_rows,
        len(expanded),
    )
    return expanded.reset_index(drop=True)


def load_historical_year(
    traffic_zip: Path,
    year: int,
    temp_dir: Path,
    route_id_set: set[str],
    route_base_lookup: dict[str, list[str]],
) -> pd.DataFrame:
    df = read_historical_source_from_zip(traffic_zip, year, temp_dir)
    df = normalize_historical_columns(df, year)
    df = expand_historical_route_ids(df, year, route_id_set, route_base_lookup)
    logger.info("Loaded historic %d traffic segments for %s after route matching", len(df), year)
    return df.sort_values(["ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT"]).reset_index(drop=True)


def load_historical_traffic(
    traffic_zip: Path,
    route_id_set: set[str],
    route_base_lookup: dict[str, list[str]],
) -> dict[int, pd.DataFrame]:
    historical: dict[int, pd.DataFrame] = {}
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        for year in HISTORICAL_YEARS:
            try:
                historical[year] = load_historical_year(
                    traffic_zip,
                    year,
                    temp_dir,
                    route_id_set,
                    route_base_lookup,
                )
            except FileNotFoundError as exc:
                logger.warning("Skipping historic year %s: %s", year, exc)
            except KeyError as exc:
                logger.warning("Skipping historic year %s due to schema issue: %s", year, exc)
    return historical


def build_interval_lookup(df: pd.DataFrame) -> dict[str, list[dict]]:
    lookup: dict[str, list[dict]] = {}
    for route_id, group in df.groupby("ROUTE_ID", sort=False):
        lookup[route_id] = group.to_dict("records")
    return lookup


def build_county_to_district_lookup(current_traffic: pd.DataFrame) -> dict[int, int]:
    counts = (
        current_traffic.dropna(subset=["CURRENT_COUNTY_ID", "CURRENT_GDOT_DISTRICT"])
        .groupby(["CURRENT_COUNTY_ID", "CURRENT_GDOT_DISTRICT"])
        .size()
        .reset_index(name="segment_count")
        .sort_values(["CURRENT_COUNTY_ID", "segment_count"], ascending=[True, False])
    )
    dominant = counts.groupby("CURRENT_COUNTY_ID", as_index=False).first()
    return {
        int(row.CURRENT_COUNTY_ID): int(row.CURRENT_GDOT_DISTRICT)
        for row in dominant.itertuples(index=False)
    }


def prepare_route_attributes(routes: gpd.GeoDataFrame, current_traffic: pd.DataFrame) -> gpd.GeoDataFrame:
    routes = parse_route_id(routes)
    county_to_district = build_county_to_district_lookup(current_traffic)

    if "COUNTY_ID" in routes.columns:
        routes["COUNTY_ID"] = pd.to_numeric(routes["COUNTY_ID"], errors="coerce")
    else:
        routes["COUNTY_ID"] = np.nan

    parsed_county = pd.to_numeric(routes["PARSED_COUNTY_CODE"], errors="coerce")
    routes["COUNTY_ID"] = routes["COUNTY_ID"].fillna(parsed_county.where(parsed_county > 0))

    routes["COUNTY_CODE"] = routes["COUNTY_ID"].map(
        lambda value: f"{int(value):03d}" if pd.notna(value) else None
    )
    routes["GDOT_District"] = routes["COUNTY_ID"].map(
        lambda value: county_to_district.get(int(value)) if pd.notna(value) else np.nan
    )
    routes["DISTRICT"] = routes["GDOT_District"]

    routes["FUNCTIONAL_CLASS"] = routes.get("F_SYSTEM")
    routes["NUM_LANES"] = routes.get("THROUGH_LANES")
    routes["URBAN_CODE"] = routes.get("URBAN_ID")
    routes["NHS_IND"] = routes.get("NHS")
    routes["ROUTE_TYPE"] = routes["PARSED_SYSTEM_CODE"]
    routes["ROUTE_NUMBER"] = routes["PARSED_ROUTE_NUMBER"]
    routes["ROUTE_SUFFIX"] = routes["PARSED_SUFFIX"]
    routes["ROUTE_DIRECTION"] = routes["PARSED_DIRECTION"]
    return routes


def clamp_interval(
    start: float,
    end: float,
    route_start: float,
    route_end: float,
) -> tuple[float, float] | None:
    clipped_start = max(route_start, round_milepoint(start))
    clipped_end = min(route_end, round_milepoint(end))
    if clipped_end - clipped_start <= MILEPOINT_TOLERANCE:
        return None
    return clipped_start, clipped_end


def get_breakpoints(
    route_start: float,
    route_end: float,
    current_records: list[dict],
    historical_records: dict[int, list[dict]],
) -> list[float]:
    points = {round_milepoint(route_start), round_milepoint(route_end)}
    for record in current_records:
        interval = clamp_interval(record["FROM_MILEPOINT"], record["TO_MILEPOINT"], route_start, route_end)
        if interval:
            points.update(interval)
    for records in historical_records.values():
        for record in records:
            interval = clamp_interval(record["FROM_MILEPOINT"], record["TO_MILEPOINT"], route_start, route_end)
            if interval:
                points.update(interval)
    return sorted(points)


def find_covering_record(records: list[dict], start: float, end: float) -> dict | None:
    for record in records:
        if (
            record["FROM_MILEPOINT"] <= start + MILEPOINT_TOLERANCE
            and record["TO_MILEPOINT"] >= end - MILEPOINT_TOLERANCE
        ):
            return record
    return None


def prepare_route_geometry(geometry) -> LineString | None:
    if geometry is None or geometry.is_empty:
        return None
    flattened = force_2d(geometry)
    merged = line_merge(flattened)
    if merged.geom_type == "LineString":
        return merged
    return None


def slice_route_geometry(
    geometry: LineString,
    route_start: float,
    route_end: float,
    segment_start: float,
    segment_end: float,
):
    route_span = route_end - route_start
    if route_span <= MILEPOINT_TOLERANCE:
        return geometry

    start_ratio = (segment_start - route_start) / route_span
    end_ratio = (segment_end - route_start) / route_span

    start_distance = max(0.0, min(geometry.length, geometry.length * start_ratio))
    end_distance = max(0.0, min(geometry.length, geometry.length * end_ratio))

    if end_distance - start_distance <= 0:
        return None

    piece = substring(geometry, start_distance, end_distance)
    if piece.is_empty or piece.length == 0:
        return None
    return piece


def compute_truck_pct(aadt: float | None, truck_aadt: float | None) -> float | None:
    if pd.isna(aadt) or pd.isna(truck_aadt) or float(aadt) <= 0:
        return np.nan
    return (float(truck_aadt) / float(aadt)) * 100.0


def build_segment_row(
    route_row: pd.Series,
    current_record: dict | None,
    historical_records_by_year: dict[int, list[dict]],
    segment_start: float,
    segment_end: float,
    geometry,
) -> dict:
    row = route_row.drop(labels=["geometry"]).to_dict()
    row["FROM_MILEPOINT"] = segment_start
    row["TO_MILEPOINT"] = segment_end
    row["BeginPoint"] = segment_start
    row["EndPoint"] = segment_end
    row["geometry"] = geometry

    row["AADT"] = np.nan
    row["AADT_YEAR"] = np.nan
    row["TRUCK_AADT"] = np.nan
    row["TRUCK_PCT"] = np.nan
    row["FUTURE_AADT"] = np.nan
    row["VMT"] = np.nan
    row["TruckVMT"] = np.nan
    row["current_aadt_covered"] = False

    if current_record is not None:
        row["AADT"] = current_record.get("AADT_2024")
        row["AADT_YEAR"] = 2024 if pd.notna(current_record.get("AADT_2024")) else np.nan
        row["AADT_2024"] = current_record.get("AADT_2024")
        row["SINGLE_UNIT_AADT_2024"] = current_record.get("SINGLE_UNIT_AADT_2024")
        row["COMBO_UNIT_AADT_2024"] = current_record.get("COMBO_UNIT_AADT_2024")
        single = current_record.get("SINGLE_UNIT_AADT_2024")
        combo = current_record.get("COMBO_UNIT_AADT_2024")
        if pd.notna(single) or pd.notna(combo):
            row["TRUCK_AADT"] = (0 if pd.isna(single) else single) + (0 if pd.isna(combo) else combo)
        row["TRUCK_PCT"] = compute_truck_pct(row["AADT"], row["TRUCK_AADT"])
        row["FUTURE_AADT"] = current_record.get("FUTURE_AADT_2024")
        row["VMT"] = current_record.get("VMT_2024")
        row["TruckVMT"] = current_record.get("TRUCK_VMT_2024")
        row["K_FACTOR"] = current_record.get("K_FACTOR")
        row["D_FACTOR"] = current_record.get("D_FACTOR")
        row["TC_NUMBER"] = current_record.get("TC_NUMBER")
        row["Traffic_Class"] = current_record.get("TRAFFIC_CLASS_2024")
        if pd.isna(row.get("COUNTY_ID")) and pd.notna(current_record.get("CURRENT_COUNTY_ID")):
            row["COUNTY_ID"] = current_record.get("CURRENT_COUNTY_ID")
        if pd.isna(row.get("GDOT_District")) and pd.notna(current_record.get("CURRENT_GDOT_DISTRICT")):
            row["GDOT_District"] = current_record.get("CURRENT_GDOT_DISTRICT")
            row["DISTRICT"] = current_record.get("CURRENT_GDOT_DISTRICT")
        row["current_aadt_covered"] = pd.notna(row["AADT"])
    else:
        row["AADT_2024"] = np.nan
        row["SINGLE_UNIT_AADT_2024"] = np.nan
        row["COMBO_UNIT_AADT_2024"] = np.nan

    historical_years_available = 0
    for year, records in historical_records_by_year.items():
        record = find_covering_record(records, segment_start, segment_end)
        value_col = f"AADT_{year}"
        truck_col = f"TRUCK_AADT_{year}"
        truck_pct_col = f"TRUCK_PCT_{year}"
        row[value_col] = record.get(value_col) if record else np.nan
        if truck_col in (record or {}):
            row[truck_col] = record.get(truck_col)
        elif truck_col not in row:
            row[truck_col] = np.nan
        if truck_pct_col in (record or {}):
            row[truck_pct_col] = record.get(truck_pct_col)
        elif truck_pct_col not in row:
            row[truck_pct_col] = np.nan
        if record and pd.notna(row[value_col]):
            historical_years_available += 1

    row["historical_aadt_years_available"] = historical_years_available
    row["COUNTY_CODE"] = (
        f"{int(row['COUNTY_ID']):03d}" if pd.notna(row.get("COUNTY_ID")) else row.get("COUNTY_CODE")
    )
    return row


def segment_routes(
    routes: gpd.GeoDataFrame,
    current_lookup: dict[str, list[dict]],
    historical_lookup: dict[int, dict[str, list[dict]]],
) -> gpd.GeoDataFrame:
    output_rows: list[dict] = []
    split_failures = 0

    for index, route_row in routes.iterrows():
        if index % 10000 == 0 and index:
            logger.info("Processed %d / %d routes", index, len(routes))

        route_id = route_row["ROUTE_ID"]
        route_start = round_milepoint(route_row["FROM_MILEPOINT"])
        route_end = round_milepoint(route_row["TO_MILEPOINT"])

        current_records = current_lookup.get(route_id, [])
        historical_records = {
            year: lookup.get(route_id, [])
            for year, lookup in historical_lookup.items()
            if route_id in lookup
        }

        if route_end - route_start <= MILEPOINT_TOLERANCE:
            row = build_segment_row(route_row, None, historical_records, route_start, route_end, force_2d(route_row.geometry))
            output_rows.append(row)
            continue

        if not current_records and not historical_records:
            row = build_segment_row(route_row, None, historical_records, route_start, route_end, force_2d(route_row.geometry))
            output_rows.append(row)
            continue

        merged_geometry = prepare_route_geometry(route_row.geometry)
        if merged_geometry is None:
            split_failures += 1
            row = build_segment_row(route_row, None, historical_records, route_start, route_end, force_2d(route_row.geometry))
            output_rows.append(row)
            continue

        breakpoints = get_breakpoints(route_start, route_end, current_records, historical_records)
        for segment_start, segment_end in zip(breakpoints, breakpoints[1:]):
            if segment_end - segment_start <= MILEPOINT_TOLERANCE:
                continue
            piece = slice_route_geometry(
                merged_geometry,
                route_start,
                route_end,
                segment_start,
                segment_end,
            )
            if piece is None:
                continue
            current_record = find_covering_record(current_records, segment_start, segment_end)
            row = build_segment_row(route_row, current_record, historical_records, segment_start, segment_end, piece)
            output_rows.append(row)

    logger.info("Route segmentation complete with %d split failures", split_failures)
    return gpd.GeoDataFrame(output_rows, geometry="geometry", crs=routes.crs)


def compute_segment_length(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.geometry is not None and not gdf.geometry.is_empty.all():
        gdf["segment_length_m"] = gdf.geometry.length
        gdf["segment_length_mi"] = gdf["segment_length_m"] / 1609.344
    else:
        gdf["segment_length_m"] = np.nan
        gdf["segment_length_mi"] = np.nan
    return gdf


def write_match_summary(gdf: gpd.GeoDataFrame) -> None:
    summary = {
        "segment_count": int(len(gdf)),
        "unique_route_ids": int(gdf["ROUTE_ID"].nunique()),
        "current_aadt_segments": int(gdf["AADT"].notna().sum()),
        "current_aadt_miles": float(gdf.loc[gdf["AADT"].notna(), "segment_length_mi"].sum()),
        "historical_year_coverage": {
            str(year): {
                "segments": int(gdf[f"AADT_{year}"].notna().sum()),
                "miles": float(gdf.loc[gdf[f"AADT_{year}"].notna(), "segment_length_mi"].sum()),
            }
            for year in HISTORICAL_YEARS
            if f"AADT_{year}" in gdf.columns
        },
        "historical_unavailable_years": HISTORICAL_UNAVAILABLE_YEARS,
    }
    output_path = CONFIG_DIR / "traffic_match_summary.json"
    output_path.write_text(json.dumps(summary, indent=2))
    logger.info("Wrote traffic match summary to %s", output_path)


def fetch_official_district_boundaries() -> gpd.GeoDataFrame:
    """Load GDOT district polygons from the GDOT_Boundaries service."""
    logger.info("Loading official district boundaries from GDOT service")
    gdf = gpd.read_file(DISTRICT_BOUNDARIES_URL, engine="pyogrio")
    gdf = clean_column_names(gdf)

    keep = [
        "OBJECTID",
        "GDOT_DISTRICT",
        "DISTRICT_NAME",
        "STATUS",
        "EFFECTIVE_DATE",
        "GLOBALID",
        "geometry",
    ]
    available = [col for col in keep if col in gdf.columns]
    gdf = gdf[available].copy()
    if "GDOT_DISTRICT" in gdf.columns:
        gdf["GDOT_DISTRICT"] = pd.to_numeric(gdf["GDOT_DISTRICT"], errors="coerce").astype("Int64")
        gdf["DISTRICT_LABEL"] = gdf["GDOT_DISTRICT"].map(DISTRICT_NAME_LOOKUP)
        gdf["DISTRICT_NAME"] = gdf["DISTRICT_LABEL"].fillna(gdf.get("DISTRICT_NAME"))
    gdf = gdf.to_crs(TARGET_CRS)
    logger.info("Loaded %d district boundary features", len(gdf))
    return gdf


def fetch_official_county_boundaries(district_boundaries: gpd.GeoDataFrame | None = None) -> gpd.GeoDataFrame:
    """Load GDOT-hosted county polygons from the GDOT_Boundaries service."""
    logger.info("Loading official county boundaries from GDOT service")
    gdf = gpd.read_file(COUNTY_BOUNDARIES_URL, engine="pyogrio")
    gdf = clean_column_names(gdf)

    keep = [
        "OBJECTID",
        "COUNTYFP",
        "NAME",
        "GDOT_DISTRICT",
        "CONGRESSIONAL_DISTRICT",
        "SENATE_DISTRICT",
        "HOUSE_DISTRICT",
        "geometry",
    ]
    available = [col for col in keep if col in gdf.columns]
    gdf = gdf[available].copy()
    if "GDOT_DISTRICT" in gdf.columns:
        gdf["GDOT_DISTRICT"] = pd.to_numeric(gdf["GDOT_DISTRICT"], errors="coerce").astype("Int64")
        gdf["DISTRICT_LABEL"] = gdf["GDOT_DISTRICT"].map(DISTRICT_NAME_LOOKUP)
        gdf["DISTRICT_NAME"] = gdf["DISTRICT_LABEL"]
    if "COUNTYFP" in gdf.columns:
        gdf["COUNTYFP"] = gdf["COUNTYFP"].astype(str).str.zfill(3)
    if "NAME" in gdf.columns:
        gdf["NAME"] = gdf["NAME"].astype(str).str.strip()
    if district_boundaries is not None and "DISTRICT_NAME" not in gdf.columns:
        district_name_map = (
            district_boundaries[["GDOT_DISTRICT", "DISTRICT_NAME"]]
            .drop_duplicates(subset=["GDOT_DISTRICT"])
            .set_index("GDOT_DISTRICT")["DISTRICT_NAME"]
            .to_dict()
        )
        gdf["DISTRICT_NAME"] = gdf["GDOT_DISTRICT"].map(district_name_map)
    gdf = gdf.to_crs(TARGET_CRS)
    logger.info("Loaded %d county boundary features", len(gdf))
    return gdf


def write_supporting_boundary_layers(gpkg_path: Path) -> None:
    """Append official GDOT boundary layers to the staged GeoPackage."""
    if gpkg_path.exists():
        with sqlite3.connect(gpkg_path) as conn:
            for layer_name in ["county_boundaries", "district_boundaries"]:
                conn.execute(f'DROP TABLE IF EXISTS "{layer_name}"')
                conn.execute("DELETE FROM gpkg_contents WHERE table_name = ?", (layer_name,))
                conn.execute("DELETE FROM gpkg_geometry_columns WHERE table_name = ?", (layer_name,))
                conn.execute("DELETE FROM gpkg_extensions WHERE table_name = ?", (layer_name,))
            conn.commit()

    district_boundaries = fetch_official_district_boundaries()
    county_boundaries = fetch_official_county_boundaries(district_boundaries=district_boundaries)
    county_boundaries.to_file(
        gpkg_path,
        layer="county_boundaries",
        driver="GPKG",
        engine="pyogrio",
        mode="a",
    )
    logger.info("Appended county_boundaries layer to %s", gpkg_path)

    district_boundaries.to_file(
        gpkg_path,
        layer="district_boundaries",
        driver="GPKG",
        engine="pyogrio",
        mode="a",
    )
    logger.info("Appended district_boundaries layer to %s", gpkg_path)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    road_inv_gdb = find_path(RAW_DIR, ROAD_INV_GDB_NAME)
    traffic_gdb = find_path(RAW_DIR, TRAFFIC_GDB_NAME)
    historical_zip = find_path(RAW_DIR, TRAFFIC_HISTORICAL_ZIP)

    routes = load_route_geometry(road_inv_gdb)
    routes = enrich_routes_with_static_attributes(routes, road_inv_gdb)
    route_id_set, route_base_lookup = build_route_id_lookup(routes)

    current_traffic = load_current_traffic(traffic_gdb)
    historical_traffic = load_historical_traffic(historical_zip, route_id_set, route_base_lookup)

    routes = prepare_route_attributes(routes, current_traffic)
    routes = build_unique_id(routes)

    current_lookup = build_interval_lookup(current_traffic)
    historical_lookup = {
        year: build_interval_lookup(df)
        for year, df in historical_traffic.items()
    }

    segmented = segment_routes(routes, current_lookup, historical_lookup)
    segmented = build_unique_id(segmented)

    if segmented.crs is not None:
        segmented = segmented.to_crs(TARGET_CRS)
        logger.info("Reprojected segmented network to %s", TARGET_CRS)
    else:
        logger.warning("No CRS set on segmented network; cannot reproject")

    segmented = compute_segment_length(segmented)
    segmented = add_decoded_label_columns(segmented)

    logger.info("Final segment count: %d", len(segmented))
    logger.info("Current AADT coverage: %d segments", segmented["AADT"].notna().sum())
    for year in HISTORICAL_YEARS:
        col = f"AADT_{year}"
        if col in segmented.columns:
            logger.info("%s coverage: %d segments", col, segmented[col].notna().sum())

    CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = CLEANED_DIR / "roadway_inventory_cleaned.csv"
    segmented.drop(columns=["geometry"], errors="ignore").to_csv(csv_path, index=False)
    logger.info("Wrote cleaned CSV: %s (%d rows)", csv_path, len(segmented))

    segmented["geometry"] = segmented["geometry"].apply(lambda geom: force_2d(geom) if geom is not None else geom)

    SPATIAL_DIR.mkdir(parents=True, exist_ok=True)
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    segmented.to_file(gpkg_path, layer="roadway_segments", driver="GPKG", engine="pyogrio")
    logger.info("Wrote GeoPackage: %s", gpkg_path)
    write_supporting_boundary_layers(gpkg_path)

    write_match_summary(segmented)
    logger.info("Normalization complete.")


if __name__ == "__main__":
    main()
