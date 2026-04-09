"""Normalize Georgia roadway inventory data onto official GDOT route geometry.

This workflow uses the official `GA_2024_Routes` geometry as the base network,
then attaches current GDOT traffic fields by route ID and milepoint intervals.

The only canonical future projection kept in the normalized network is
`FUTURE_AADT` from the current 2024 GDOT traffic record.

Data sources:
1. `Road_Inventory_2024.gdb` layer `GA_2024_Routes`
   Official full roadway geometry.
2. `TRAFFIC_Data_2024.gdb` layer `TRAFFIC_DataYear2024`
   Current traffic segmentation with AADT, truck counts, VMT, and factors.

Output:
- `02-Data-Staging/tables/roadway_inventory_cleaned.csv`
- `02-Data-Staging/spatial/base_network.gpkg` layers
  `roadway_segments`, `county_boundaries`, `district_boundaries`
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import force_2d, line_merge, make_valid
from shapely.geometry import LineString
from shapely.ops import substring

from hpms_enrichment import apply_hpms_enrichment, write_hpms_enrichment_summary
from rnhp_enrichment import apply_rnhp_enrichment, write_enrichment_summary
from route_family import classify_route_families
from route_verification import (
    apply_signed_route_verification,
    write_signed_route_verification_summary,
)
from route_type_gdot import apply_gdot_route_type_classification
from utils import decode_lookup_value

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"
REPORTS_DIR = PROJECT_ROOT / "02-Data-Staging" / "reports"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
TABLES_DIR = PROJECT_ROOT / "02-Data-Staging" / "tables"

TARGET_CRS = "EPSG:32617"  # NOTE: crs_config.json exists, but this script does not currently read it.

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

ROUTE_LAYER = "GA_2024_Routes"
CURRENT_TRAFFIC_LAYER = "TRAFFIC_DataYear2024"

MILEPOINT_PRECISION = 4
MILEPOINT_TOLERANCE = 1e-4
AADT_2024_GAP_FILL_MAX_INTERPOLATION_MILES = 5.0
COUNTY_ALL_MIN_SHARE = 0.01
COUNTY_ALL_DELIMITER = ", "

ROUTE_MERGE_KEYS = ["ROUTE_ID", "BeginPoint", "EndPoint"]

ROUTE_ATTRIBUTE_LAYERS = {
    "COUNTY_ID": ("COUNTY_IDVn", "COUNTY_ID"),
    "F_SYSTEM": ("F_SYSTEMVn", "F_SYSTEM"),
    "NHS": ("NHSVn", "NHS"),
    "FACILITY_TYPE": ("FACILITY_TYPEVn", "FACILITY_TYPE"),
    "THROUGH_LANES": ("THROUGH_LANESVn", "THROUGH_LANES"),
    "LANE_WIDTH": ("LANE_WIDTHVn", "LANE_WIDTH"),
    "MEDIAN_TYPE": ("MEDIAN_TYPEVn", "MEDIAN_TYPE"),
    "MEDIAN_WIDTH": ("MEDIAN_WIDTHVn", "MEDIAN_WIDTH"),
    "SHOULDER_TYPE": ("SHOULDER_TYPEVn", "SHOULDER_TYPE"),
    "SHOULDER_WIDTH_L": ("SHOULDER_WIDTH_LVn", "SHOULDER_WIDTH_L"),
    "SHOULDER_WIDTH_R": ("SHOULDER_WIDTH_RVn", "SHOULDER_WIDTH_R"),
    "OWNERSHIP": ("OWNERSHIPVn", "OWNERSHIP"),
    "STRAHNET_TYPE": ("STRAHNET_TYPEVn", "STRAHNET"),
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


def find_path(raw_dir: Path, pattern: str) -> Path:
    matches = sorted(raw_dir.rglob(pattern), key=lambda path: str(path).casefold())
    if not matches:
        raise FileNotFoundError(f"Could not find {pattern} under {raw_dir}")
    if len(matches) > 1:
        logger.warning(
            "Multiple matches found for %s under %s; using %s. Matches: %s",
            pattern,
            raw_dir,
            matches[0],
            ", ".join(str(match) for match in matches),
        )
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


def get_or_empty_series(df: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name in df.columns:
        return df[column_name]
    return pd.Series([None] * len(df), index=df.index, dtype="object")


def _clean_optional_text(value) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text in {"", "nan", "None"}:
        return None
    return text


def _dedupe_county_names(names: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        clean_name = _clean_optional_text(name)
        if clean_name is None:
            continue
        name_key = clean_name.casefold()
        if name_key in seen:
            continue
        seen.add(name_key)
        ordered.append(clean_name)
    return ordered


def _merge_county_all_value(county_all_value, county_name) -> str | None:
    county_names = _dedupe_county_names(
        (_clean_optional_text(part) for part in str(county_all_value).split(","))
        if _clean_optional_text(county_all_value) is not None
        else []
    )
    clean_county_name = _clean_optional_text(county_name)
    if clean_county_name is not None:
        county_names = [
            clean_county_name,
            *[
                name
                for name in county_names
                if name.casefold() != clean_county_name.casefold()
            ],
        ]

    if not county_names:
        return None
    return COUNTY_ALL_DELIMITER.join(county_names)


def _move_column_after(df: pd.DataFrame, column_name: str, after_column: str) -> pd.DataFrame:
    if column_name not in df.columns or after_column not in df.columns:
        return df

    ordered_columns = [column for column in df.columns if column != column_name]
    insert_at = ordered_columns.index(after_column) + 1
    ordered_columns.insert(insert_at, column_name)
    return df.loc[:, ordered_columns]


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
    gdf["OWNERSHIP_LABEL"] = get_or_empty_series(gdf, "OWNERSHIP").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["ownership"])
    )
    gdf["STRAHNET_LABEL"] = get_or_empty_series(gdf, "STRAHNET").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["strahnet"])
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
    gdf["ROUTE_TYPE_GDOT_LABEL"] = get_or_empty_series(gdf, "ROUTE_TYPE_GDOT").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["route_type_gdot"])
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

    routes = sync_derived_alias_fields(routes)
    routes["ROUTE_TYPE"] = routes["PARSED_SYSTEM_CODE"]
    routes["ROUTE_NUMBER"] = routes["PARSED_ROUTE_NUMBER"]
    routes["ROUTE_SUFFIX"] = routes["PARSED_SUFFIX"]
    routes["ROUTE_DIRECTION"] = routes["PARSED_DIRECTION"]
    route_families = classify_route_families(routes)
    routes = pd.concat([routes, route_families], axis=1)
    return routes


DERIVED_ALIAS_SYNC_FIELDS = (
    ("F_SYSTEM", "FUNCTIONAL_CLASS"),
    ("THROUGH_LANES", "NUM_LANES"),
    ("NHS", "NHS_IND"),
    ("URBAN_ID", "URBAN_CODE"),
)


def sync_derived_alias_fields(df: pd.DataFrame) -> pd.DataFrame:
    synced = df.copy()
    for source_col, target_col in DERIVED_ALIAS_SYNC_FIELDS:
        if source_col not in synced.columns:
            continue
        if target_col not in synced.columns:
            synced[target_col] = pd.NA
        mask = synced[target_col].isna() & synced[source_col].notna()
        synced.loc[mask, target_col] = synced.loc[mask, source_col]
    return synced


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
) -> list[float]:
    points = {round_milepoint(route_start), round_milepoint(route_end)}
    for record in current_records:
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


def _extract_route_line_components(geometry) -> list[LineString]:
    if geometry is None or geometry.is_empty:
        return []

    flattened = force_2d(geometry)
    merged = line_merge(flattened)

    if merged.geom_type == "LineString":
        return [merged]
    if merged.geom_type == "MultiLineString":
        return [component for component in merged.geoms if component.length > 0]
    if flattened.geom_type == "LineString":
        return [flattened]
    if flattened.geom_type == "MultiLineString":
        return [component for component in flattened.geoms if component.length > 0]
    return []


def prepare_route_geometry_components(
    geometry,
    route_start: float,
    route_end: float,
) -> list[dict[str, LineString | float]]:
    components = _extract_route_line_components(geometry)
    if not components:
        return []

    route_span = route_end - route_start
    total_length = sum(component.length for component in components)
    if total_length <= MILEPOINT_TOLERANCE:
        return []

    prepared_components: list[dict[str, LineString | float]] = []
    cumulative_length = 0.0
    for component_index, component in enumerate(components):
        component_start = route_start + (route_span * (cumulative_length / total_length))
        cumulative_length += component.length
        if component_index == len(components) - 1:
            component_end = route_end
        else:
            component_end = route_start + (route_span * (cumulative_length / total_length))

        prepared_components.append(
            {
                "geometry": component,
                "route_start": round_milepoint(component_start),
                "route_end": round_milepoint(component_end),
            }
        )

    return prepared_components


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
    row["AADT_2024"] = np.nan
    row["AADT_2024_OFFICIAL"] = np.nan
    row["AADT_2024_SOURCE"] = "missing"
    row["AADT_2024_CONFIDENCE"] = None
    row["AADT_2024_FILL_METHOD"] = None
    row["AADT_YEAR"] = np.nan
    row["TRUCK_AADT"] = np.nan
    row["TRUCK_PCT"] = np.nan
    row["FUTURE_AADT"] = np.nan
    row["FUTURE_AADT_2044"] = np.nan
    row["FUTURE_AADT_2044_OFFICIAL"] = np.nan
    row["FUTURE_AADT_2044_SOURCE"] = "missing"
    row["FUTURE_AADT_2044_CONFIDENCE"] = None
    row["FUTURE_AADT_2044_FILL_METHOD"] = None
    row["future_aadt_covered"] = False
    row["VMT"] = np.nan
    row["TruckVMT"] = np.nan
    row["current_aadt_official_covered"] = False
    row["current_aadt_covered"] = False

    if current_record is not None:
        official_aadt = current_record.get("AADT_2024")
        row["AADT"] = official_aadt
        row["AADT_2024"] = official_aadt
        row["AADT_2024_OFFICIAL"] = official_aadt
        row["AADT_YEAR"] = 2024 if pd.notna(official_aadt) else np.nan
        if pd.notna(official_aadt):
            row["AADT_2024_SOURCE"] = "official_exact"
            row["AADT_2024_CONFIDENCE"] = "high"
        row["SINGLE_UNIT_AADT_2024"] = current_record.get("SINGLE_UNIT_AADT_2024")
        row["COMBO_UNIT_AADT_2024"] = current_record.get("COMBO_UNIT_AADT_2024")
        single = current_record.get("SINGLE_UNIT_AADT_2024")
        combo = current_record.get("COMBO_UNIT_AADT_2024")
        if pd.notna(single) or pd.notna(combo):
            row["TRUCK_AADT"] = (0 if pd.isna(single) else single) + (0 if pd.isna(combo) else combo)
        row["TRUCK_PCT"] = compute_truck_pct(row["AADT"], row["TRUCK_AADT"])
        row["FUTURE_AADT"] = current_record.get("FUTURE_AADT_2024")
        future_val = current_record.get("FUTURE_AADT_2024")
        if pd.notna(future_val):
            row["FUTURE_AADT_2044"] = future_val
            row["FUTURE_AADT_2044_OFFICIAL"] = future_val
            row["FUTURE_AADT_2044_SOURCE"] = "official_exact"
            row["FUTURE_AADT_2044_CONFIDENCE"] = "high"
            row["future_aadt_covered"] = True
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
        row["current_aadt_official_covered"] = pd.notna(official_aadt)
        row["current_aadt_covered"] = pd.notna(row["AADT_2024"])
    else:
        row["SINGLE_UNIT_AADT_2024"] = np.nan
        row["COMBO_UNIT_AADT_2024"] = np.nan

    row["COUNTY_CODE"] = (
        f"{int(row['COUNTY_ID']):03d}" if pd.notna(row.get("COUNTY_ID")) else row.get("COUNTY_CODE")
    )
    return row


def segment_routes(
    routes: gpd.GeoDataFrame,
    current_lookup: dict[str, list[dict]],
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

        if route_end - route_start <= MILEPOINT_TOLERANCE:
            current_record = find_covering_record(current_records, route_start, route_end)
            row = build_segment_row(route_row, current_record, route_start, route_end, force_2d(route_row.geometry))
            output_rows.append(row)
            continue

        if not current_records:
            row = build_segment_row(route_row, None, route_start, route_end, force_2d(route_row.geometry))
            output_rows.append(row)
            continue

        prepared_components = prepare_route_geometry_components(
            route_row.geometry,
            route_start,
            route_end,
        )
        if not prepared_components:
            split_failures += 1
            current_record = find_covering_record(current_records, route_start, route_end)
            row = build_segment_row(
                route_row,
                current_record,
                route_start,
                route_end,
                force_2d(route_row.geometry),
            )
            output_rows.append(row)
            continue

        breakpoints = get_breakpoints(route_start, route_end, current_records)
        for component in prepared_components:
            component_geometry = component["geometry"]
            component_start = float(component["route_start"])
            component_end = float(component["route_end"])
            component_breakpoints = {
                round_milepoint(point)
                for point in breakpoints
                if component_start - MILEPOINT_TOLERANCE <= point <= component_end + MILEPOINT_TOLERANCE
            }
            component_breakpoints.update(
                {
                    round_milepoint(component_start),
                    round_milepoint(component_end),
                }
            )

            ordered_breakpoints = sorted(component_breakpoints)
            for segment_start, segment_end in zip(ordered_breakpoints, ordered_breakpoints[1:]):
                if segment_end - segment_start <= MILEPOINT_TOLERANCE:
                    continue
                piece = slice_route_geometry(
                    component_geometry,
                    component_start,
                    component_end,
                    segment_start,
                    segment_end,
                )
                if piece is None:
                    split_failures += 1
                    logger.debug(
                        "Route split produced no geometry for %s %.4f-%.4f within component %.4f-%.4f",
                        route_id,
                        segment_start,
                        segment_end,
                        component_start,
                        component_end,
                    )
                    continue
                current_record = find_covering_record(current_records, segment_start, segment_end)
                row = build_segment_row(
                    route_row,
                    current_record,
                    segment_start,
                    segment_end,
                    piece,
                )
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


def apply_direction_mirror_aadt(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fill uncovered DEC-direction segments from matching INC-direction AADT.

    For divided highways, GDOT reports traffic on the INC direction only.
    This mirrors AADT from INC to DEC for the same route where milepoints
    overlap, since both directions carry the same traffic.
    """

    filled = gdf.copy()

    dec_uncovered = filled[
        (filled["ROUTE_ID"].str.endswith("DEC"))
        & (~filled["current_aadt_covered"].fillna(False).astype(bool))
    ]

    if dec_uncovered.empty:
        logger.info("Direction mirror: no uncovered DEC segments found")
        return filled

    inc_covered = filled[
        (filled["ROUTE_ID"].str.endswith("INC"))
        & (filled["current_aadt_covered"].fillna(False).astype(bool))
    ]

    if inc_covered.empty:
        logger.info("Direction mirror: no covered INC segments found")
        return filled

    inc_lookup: dict[str, list[tuple[float, float, int]]] = {}
    for idx, row in inc_covered.iterrows():
        route_base = str(row["ROUTE_ID"])[:-3]
        from_mp = float(row["FROM_MILEPOINT"]) if pd.notna(row["FROM_MILEPOINT"]) else 0.0
        to_mp = float(row["TO_MILEPOINT"]) if pd.notna(row["TO_MILEPOINT"]) else 0.0
        inc_lookup.setdefault(route_base, []).append((from_mp, to_mp, idx))

    mirror_count = 0
    for dec_idx, dec_row in dec_uncovered.iterrows():
        route_base = str(dec_row["ROUTE_ID"])[:-3]
        candidates = inc_lookup.get(route_base, [])
        if not candidates:
            continue

        dec_from = float(dec_row["FROM_MILEPOINT"]) if pd.notna(dec_row["FROM_MILEPOINT"]) else 0.0
        dec_to = float(dec_row["TO_MILEPOINT"]) if pd.notna(dec_row["TO_MILEPOINT"]) else 0.0
        dec_mid = (dec_from + dec_to) / 2.0

        best_idx = None
        best_overlap = -1.0
        for inc_from, inc_to, inc_idx in candidates:
            overlap = min(dec_to, inc_to) - max(dec_from, inc_from)
            if overlap > best_overlap:
                best_overlap = overlap
                best_idx = inc_idx

        if best_idx is None or best_overlap < -MILEPOINT_TOLERANCE:
            continue

        inc_row = filled.loc[best_idx]
        inc_aadt = inc_row.get("AADT_2024")
        if pd.isna(inc_aadt):
            continue

        filled.at[dec_idx, "AADT_2024"] = inc_aadt
        filled.at[dec_idx, "AADT"] = inc_aadt
        filled.at[dec_idx, "AADT_YEAR"] = 2024
        filled.at[dec_idx, "AADT_2024_SOURCE"] = "direction_mirror"
        filled.at[dec_idx, "AADT_2024_CONFIDENCE"] = "high"
        filled.at[dec_idx, "AADT_2024_FILL_METHOD"] = "inc_to_dec_direction_mirror"
        filled.at[dec_idx, "current_aadt_covered"] = True
        mirror_count += 1

    logger.info("Direction mirror: filled %d DEC segments from INC counterparts", mirror_count)
    return filled


def apply_state_system_current_aadt_gap_fill(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Analytically fill short uncovered 2024 AADT gaps on state-system mainline routes.

    This preserves the direct GDOT match in `AADT_2024_OFFICIAL` and only
    populates the canonical `AADT_2024` / `AADT` fields when a short uncovered
    run is bracketed by official values on the same route.
    """

    filled = gdf.copy()
    for column, default_value in {
        "AADT_2024": np.nan,
        "AADT_2024_OFFICIAL": np.nan,
        "AADT_2024_SOURCE": "missing",
        "AADT_2024_CONFIDENCE": None,
        "AADT_2024_FILL_METHOD": None,
        "current_aadt_official_covered": False,
        "current_aadt_covered": False,
    }.items():
        if column not in filled.columns:
            filled[column] = default_value

    filled["AADT_2024"] = pd.to_numeric(filled["AADT_2024"], errors="coerce")
    filled["AADT_2024_OFFICIAL"] = pd.to_numeric(filled["AADT_2024_OFFICIAL"], errors="coerce")
    filled["AADT"] = pd.to_numeric(
        filled.get("AADT", pd.Series(index=filled.index, dtype="float64")),
        errors="coerce",
    )
    filled["FROM_MILEPOINT"] = pd.to_numeric(filled["FROM_MILEPOINT"], errors="coerce")
    filled["TO_MILEPOINT"] = pd.to_numeric(filled["TO_MILEPOINT"], errors="coerce")
    filled["segment_length_mi"] = pd.to_numeric(
        filled.get("segment_length_mi", pd.Series(index=filled.index, dtype="float64")),
        errors="coerce",
    ).fillna(0.0)
    filled["current_aadt_official_covered"] = (
        filled["current_aadt_official_covered"].fillna(False).astype(bool)
    )

    eligible = filled[
        (filled["SYSTEM_CODE"].astype(str) == "1")
    ].copy()
    if eligible.empty:
        return filled

    eligible = eligible.sort_values(
        by=["ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT"],
        na_position="last",
    )

    filled_segments = 0
    filled_runs = 0
    filled_miles = 0.0

    for route_id, route_group in eligible.groupby("ROUTE_ID", sort=False):
        route_group = route_group.reset_index().rename(columns={"index": "source_index"})
        route_group["official_run_id"] = route_group["current_aadt_official_covered"].ne(
            route_group["current_aadt_official_covered"].shift()
        ).cumsum()

        for _, run_group in route_group.groupby("official_run_id", sort=False):
            if bool(run_group["current_aadt_official_covered"].iloc[0]):
                continue

            run_start = int(run_group.index.min())
            run_end = int(run_group.index.max())
            prev_row = route_group.iloc[run_start - 1] if run_start > 0 else None
            next_row = route_group.iloc[run_end + 1] if run_end < len(route_group) - 1 else None
            if prev_row is None or next_row is None:
                continue
            if not (
                bool(prev_row["current_aadt_official_covered"])
                and bool(next_row["current_aadt_official_covered"])
            ):
                continue

            run_miles = float(run_group["segment_length_mi"].sum())
            if run_miles > AADT_2024_GAP_FILL_MAX_INTERPOLATION_MILES:
                continue

            prev_aadt = prev_row.get("AADT_2024_OFFICIAL")
            next_aadt = next_row.get("AADT_2024_OFFICIAL")
            if pd.isna(prev_aadt) or pd.isna(next_aadt):
                continue

            anchor_start = float(prev_row["TO_MILEPOINT"])
            anchor_end = float(next_row["FROM_MILEPOINT"])
            anchor_span = anchor_end - anchor_start

            for run_row in run_group.itertuples(index=False):
                midpoint = (float(run_row.FROM_MILEPOINT) + float(run_row.TO_MILEPOINT)) / 2.0
                if anchor_span > MILEPOINT_TOLERANCE:
                    interpolation_ratio = (midpoint - anchor_start) / anchor_span
                    interpolation_ratio = max(0.0, min(1.0, interpolation_ratio))
                else:
                    interpolation_ratio = 0.5
                interpolated_aadt = float(prev_aadt) + (
                    (float(next_aadt) - float(prev_aadt)) * interpolation_ratio
                )
                interpolated_aadt = int(round(interpolated_aadt))

                segment_index = run_row.source_index
                filled.at[segment_index, "AADT_2024"] = interpolated_aadt
                filled.at[segment_index, "AADT"] = interpolated_aadt
                filled.at[segment_index, "AADT_YEAR"] = 2024
                filled.at[segment_index, "AADT_2024_SOURCE"] = "analytical_gap_fill"
                filled.at[segment_index, "AADT_2024_CONFIDENCE"] = "medium"
                filled.at[segment_index, "AADT_2024_FILL_METHOD"] = "interpolate_between_adjacent_official"
                filled.at[segment_index, "current_aadt_covered"] = True

            filled_runs += 1
            filled_segments += int(len(run_group))
            filled_miles += run_miles

    gap_fill_mask = filled["AADT_2024_SOURCE"] == "analytical_gap_fill"
    filled.loc[gap_fill_mask, "AADT"] = filled.loc[gap_fill_mask, "AADT_2024"]
    filled.loc[gap_fill_mask, "current_aadt_covered"] = True
    filled["current_aadt_official_covered"] = filled["AADT_2024_OFFICIAL"].notna()

    logger.info(
        "Applied analytical 2024 AADT gap fill to %d segments across %d runs (%.2f miles)",
        filled_segments,
        filled_runs,
        filled_miles,
    )
    return filled


NEAREST_NEIGHBOR_MAX_DISTANCE_MI = 20.0


def apply_nearest_neighbor_aadt(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fill remaining AADT gaps using the nearest covered segment on the same route.

    For each uncovered segment, find the closest covered segment (by milepoint
    distance) on the same ROUTE_ID and copy its AADT value. Limited to a
    maximum distance of 20 miles to prevent nonsensical fills.
    """

    filled = gdf.copy()

    uncovered_mask = ~filled["current_aadt_covered"].fillna(False).astype(bool)
    uncovered = filled[uncovered_mask]

    if uncovered.empty:
        logger.info("Nearest-neighbor AADT: no uncovered segments remain")
        return filled

    covered = filled[filled["current_aadt_covered"].fillna(False).astype(bool)]
    if covered.empty:
        return filled

    covered_lookup: dict[str, list[tuple[float, float]]] = {}
    for idx, row in covered.iterrows():
        rid = str(row["ROUTE_ID"])
        mid = (float(row.get("FROM_MILEPOINT") or 0) + float(row.get("TO_MILEPOINT") or 0)) / 2
        aadt = row.get("AADT_2024")
        if pd.notna(aadt):
            covered_lookup.setdefault(rid, []).append((mid, float(aadt), idx))

    fill_count = 0
    for idx, row in uncovered.iterrows():
        rid = str(row["ROUTE_ID"])
        candidates = covered_lookup.get(rid)
        if not candidates:
            continue

        seg_mid = (float(row.get("FROM_MILEPOINT") or 0) + float(row.get("TO_MILEPOINT") or 0)) / 2

        best_dist = float("inf")
        best_aadt = None
        for cand_mid, cand_aadt, _ in candidates:
            dist = abs(seg_mid - cand_mid)
            if dist < best_dist:
                best_dist = dist
                best_aadt = cand_aadt

        if best_aadt is not None and best_dist <= NEAREST_NEIGHBOR_MAX_DISTANCE_MI:
            filled.at[idx, "AADT_2024"] = int(best_aadt)
            filled.at[idx, "AADT"] = int(best_aadt)
            filled.at[idx, "AADT_YEAR"] = 2024
            filled.at[idx, "AADT_2024_SOURCE"] = "nearest_neighbor"
            filled.at[idx, "AADT_2024_CONFIDENCE"] = "low"
            filled.at[idx, "AADT_2024_FILL_METHOD"] = "nearest_covered_segment_same_route"
            filled.at[idx, "current_aadt_covered"] = True
            fill_count += 1

    logger.info("Nearest-neighbor AADT: filled %d segments", fill_count)
    return filled


def apply_future_aadt_fill_chain(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Apply the same multi-source fill chain to FUTURE_AADT_2044.

    Fill order:
    1. GDOT official (already set in build_segment_row)
    2. HPMS future_aadt (via hpms_enrichment, if wired)
    3. Direction mirror (INC→DEC)
    4. Interpolation between adjacent covered segments
    5. Nearest-neighbor on same route (capped at 20 miles)
    """

    filled = gdf.copy()

    for col, default in {
        "FUTURE_AADT_2044": np.nan,
        "FUTURE_AADT_2044_OFFICIAL": np.nan,
        "FUTURE_AADT_2044_SOURCE": "missing",
        "FUTURE_AADT_2044_CONFIDENCE": None,
        "FUTURE_AADT_2044_FILL_METHOD": None,
        "future_aadt_covered": False,
    }.items():
        if col not in filled.columns:
            filled[col] = default

    filled["FUTURE_AADT_2044"] = pd.to_numeric(filled["FUTURE_AADT_2044"], errors="coerce")
    filled["FROM_MILEPOINT"] = pd.to_numeric(filled["FROM_MILEPOINT"], errors="coerce")
    filled["TO_MILEPOINT"] = pd.to_numeric(filled["TO_MILEPOINT"], errors="coerce")

    # --- Direction mirror ---
    dec_uncovered = filled[
        (filled["ROUTE_ID"].str.endswith("DEC"))
        & (~filled["future_aadt_covered"].fillna(False).astype(bool))
    ]
    inc_covered = filled[
        (filled["ROUTE_ID"].str.endswith("INC"))
        & (filled["future_aadt_covered"].fillna(False).astype(bool))
    ]
    inc_lookup: dict[str, list[tuple[float, float, int]]] = {}
    for idx, row in inc_covered.iterrows():
        route_base = str(row["ROUTE_ID"])[:-3]
        from_mp = float(row["FROM_MILEPOINT"]) if pd.notna(row["FROM_MILEPOINT"]) else 0.0
        to_mp = float(row["TO_MILEPOINT"]) if pd.notna(row["TO_MILEPOINT"]) else 0.0
        inc_lookup.setdefault(route_base, []).append((from_mp, to_mp, idx))

    mirror_count = 0
    for dec_idx, dec_row in dec_uncovered.iterrows():
        route_base = str(dec_row["ROUTE_ID"])[:-3]
        candidates = inc_lookup.get(route_base, [])
        if not candidates:
            continue
        dec_from = float(dec_row["FROM_MILEPOINT"]) if pd.notna(dec_row["FROM_MILEPOINT"]) else 0.0
        dec_to = float(dec_row["TO_MILEPOINT"]) if pd.notna(dec_row["TO_MILEPOINT"]) else 0.0
        best_idx, best_overlap = None, -1.0
        for inc_from, inc_to, inc_idx in candidates:
            overlap = min(dec_to, inc_to) - max(dec_from, inc_from)
            if overlap > best_overlap:
                best_overlap = overlap
                best_idx = inc_idx
        if best_idx is None or best_overlap < -MILEPOINT_TOLERANCE:
            continue
        val = filled.loc[best_idx, "FUTURE_AADT_2044"]
        if pd.notna(val):
            filled.at[dec_idx, "FUTURE_AADT_2044"] = val
            filled.at[dec_idx, "FUTURE_AADT_2044_SOURCE"] = "direction_mirror"
            filled.at[dec_idx, "FUTURE_AADT_2044_CONFIDENCE"] = "high"
            filled.at[dec_idx, "FUTURE_AADT_2044_FILL_METHOD"] = "inc_to_dec_direction_mirror"
            filled.at[dec_idx, "future_aadt_covered"] = True
            mirror_count += 1

    logger.info("Future AADT direction mirror: filled %d DEC segments", mirror_count)

    # --- Interpolation (state-system only) ---
    filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()
    eligible = filled[filled["SYSTEM_CODE"].astype(str) == "1"].copy()
    interp_count = 0
    if not eligible.empty:
        eligible = eligible.sort_values(by=["ROUTE_ID", "FROM_MILEPOINT"])
        eligible["source_index"] = eligible.index
        for route_id, route_group in eligible.groupby("ROUTE_ID", sort=False):
            route_group = route_group.reset_index(drop=True)
            route_group["_covered"] = route_group["future_aadt_covered"].fillna(False).astype(bool)
            route_group["_run_id"] = route_group["_covered"].ne(route_group["_covered"].shift()).cumsum()
            for _, run_group in route_group.groupby("_run_id", sort=False):
                if bool(run_group["_covered"].iloc[0]):
                    continue
                run_start = int(run_group.index.min())
                run_end = int(run_group.index.max())
                if run_start == 0 or run_end >= len(route_group) - 1:
                    continue
                prev_row = route_group.iloc[run_start - 1]
                next_row = route_group.iloc[run_end + 1]
                if not (bool(prev_row["_covered"]) and bool(next_row["_covered"])):
                    continue
                run_miles = float(run_group["segment_length_mi"].sum()) if "segment_length_mi" in run_group.columns else 999
                if run_miles > AADT_2024_GAP_FILL_MAX_INTERPOLATION_MILES:
                    continue
                prev_val = prev_row["FUTURE_AADT_2044"]
                next_val = next_row["FUTURE_AADT_2044"]
                if pd.isna(prev_val) or pd.isna(next_val):
                    continue
                anchor_start = float(prev_row["TO_MILEPOINT"])
                anchor_end = float(next_row["FROM_MILEPOINT"])
                anchor_span = anchor_end - anchor_start
                for run_row in run_group.itertuples(index=False):
                    midpoint = (float(run_row.FROM_MILEPOINT) + float(run_row.TO_MILEPOINT)) / 2.0
                    if anchor_span > MILEPOINT_TOLERANCE:
                        ratio = max(0.0, min(1.0, (midpoint - anchor_start) / anchor_span))
                    else:
                        ratio = 0.5
                    interp_val = int(round(float(prev_val) + (float(next_val) - float(prev_val)) * ratio))
                    seg_idx = run_row.source_index
                    filled.at[seg_idx, "FUTURE_AADT_2044"] = interp_val
                    filled.at[seg_idx, "FUTURE_AADT_2044_SOURCE"] = "analytical_gap_fill"
                    filled.at[seg_idx, "FUTURE_AADT_2044_CONFIDENCE"] = "medium"
                    filled.at[seg_idx, "FUTURE_AADT_2044_FILL_METHOD"] = "interpolate_between_adjacent"
                    filled.at[seg_idx, "future_aadt_covered"] = True
                    interp_count += 1

    logger.info("Future AADT interpolation: filled %d segments", interp_count)

    # --- Nearest-neighbor (capped at 20 miles) ---
    filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()
    uncovered = filled[~filled["future_aadt_covered"]]
    covered = filled[filled["future_aadt_covered"]]
    nn_lookup: dict[str, list[tuple[float, float]]] = {}
    for idx, row in covered.iterrows():
        rid = str(row["ROUTE_ID"])
        mid = (float(row.get("FROM_MILEPOINT") or 0) + float(row.get("TO_MILEPOINT") or 0)) / 2
        val = row["FUTURE_AADT_2044"]
        if pd.notna(val):
            nn_lookup.setdefault(rid, []).append((mid, float(val)))
    nn_count = 0
    for idx, row in uncovered.iterrows():
        rid = str(row["ROUTE_ID"])
        candidates = nn_lookup.get(rid)
        if not candidates:
            continue
        seg_mid = (float(row.get("FROM_MILEPOINT") or 0) + float(row.get("TO_MILEPOINT") or 0)) / 2
        best_dist, best_val = float("inf"), None
        for cand_mid, cand_val in candidates:
            dist = abs(seg_mid - cand_mid)
            if dist < best_dist:
                best_dist = dist
                best_val = cand_val
        if best_val is not None and best_dist <= NEAREST_NEIGHBOR_MAX_DISTANCE_MI:
            filled.at[idx, "FUTURE_AADT_2044"] = int(best_val)
            filled.at[idx, "FUTURE_AADT_2044_SOURCE"] = "nearest_neighbor"
            filled.at[idx, "FUTURE_AADT_2044_CONFIDENCE"] = "low"
            filled.at[idx, "FUTURE_AADT_2044_FILL_METHOD"] = "nearest_covered_segment_same_route"
            filled.at[idx, "future_aadt_covered"] = True
            nn_count += 1

    logger.info("Future AADT nearest-neighbor: filled %d segments", nn_count)

    # Sync FUTURE_AADT alias
    filled["FUTURE_AADT"] = filled["FUTURE_AADT_2044"]
    filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()

    return filled


def write_match_summary(gdf: gpd.GeoDataFrame) -> None:
    summary = {
        "segment_count": int(len(gdf)),
        "unique_route_ids": int(gdf["ROUTE_ID"].nunique()),
        "current_aadt_official_segments": int(gdf["AADT_2024_OFFICIAL"].notna().sum())
        if "AADT_2024_OFFICIAL" in gdf.columns
        else int(gdf["AADT"].notna().sum()),
        "current_aadt_segments": int(gdf["AADT_2024"].notna().sum())
        if "AADT_2024" in gdf.columns
        else int(gdf["AADT"].notna().sum()),
        "current_aadt_miles": float(
            gdf.loc[
                gdf["AADT_2024"].notna() if "AADT_2024" in gdf.columns else gdf["AADT"].notna(),
                "segment_length_mi",
            ].sum()
        ),
        "future_aadt_2044_segments": int(gdf["FUTURE_AADT_2044"].notna().sum())
        if "FUTURE_AADT_2044" in gdf.columns
        else 0,
        "future_aadt_2044_miles": float(
            gdf.loc[gdf["FUTURE_AADT_2044"].notna(), "segment_length_mi"].sum()
        )
        if "FUTURE_AADT_2044" in gdf.columns
        else 0.0,
    }
    output_path = REPORTS_DIR / "traffic_match_summary.json"
    output_path.write_text(json.dumps(summary, indent=2))
    logger.info("Wrote traffic match summary to %s", output_path)


def _group_current_aadt_coverage(
    df: pd.DataFrame,
    group_cols: list[str],
    limit: int | None = None,
) -> list[dict]:
    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            segment_count=("covered_segment", "size"),
            covered_segments=("covered_segment", "sum"),
            uncovered_segments=("uncovered_segment", "sum"),
            segment_miles=("segment_length_mi", "sum"),
            covered_miles=("covered_miles", "sum"),
            uncovered_miles=("uncovered_miles", "sum"),
        )
        .reset_index()
    )
    grouped["covered_pct_segments"] = np.where(
        grouped["segment_count"] > 0,
        (grouped["covered_segments"] / grouped["segment_count"]) * 100.0,
        np.nan,
    )
    grouped["covered_pct_miles"] = np.where(
        grouped["segment_miles"] > 0,
        (grouped["covered_miles"] / grouped["segment_miles"]) * 100.0,
        np.nan,
    )
    grouped = grouped.sort_values(
        by=["uncovered_segments", "uncovered_miles", "segment_count"],
        ascending=[False, False, False],
    )
    if limit is not None:
        grouped = grouped.head(limit)
    grouped = grouped.replace({np.nan: None})
    return grouped.to_dict("records")


def build_state_system_gap_fill_candidates(audit: pd.DataFrame) -> pd.DataFrame:
    """Summarize conservative gap-fill candidates for uncovered state-system runs.

    The intent is analytical, not authoritative value imputation. Each row
    represents a contiguous uncovered run on a `SYSTEM_CODE = 1` route, along
    with the adjacent covered context that could support a future same-route
    gap-fill rule.
    """

    required_columns = {
        "ROUTE_ID",
        "SYSTEM_CODE",
        "FROM_MILEPOINT",
        "TO_MILEPOINT",
        "current_aadt_official_covered",
    }
    if not required_columns.issubset(audit.columns):
        return pd.DataFrame()

    state_subset = audit[audit["SYSTEM_CODE"].astype(str) == "1"].copy()
    if state_subset.empty:
        return pd.DataFrame()

    state_subset["FROM_MILEPOINT"] = pd.to_numeric(
        state_subset["FROM_MILEPOINT"],
        errors="coerce",
    )
    state_subset["TO_MILEPOINT"] = pd.to_numeric(
        state_subset["TO_MILEPOINT"],
        errors="coerce",
    )
    state_subset["AADT_2024_OFFICIAL"] = pd.to_numeric(
        state_subset.get("AADT_2024_OFFICIAL", pd.Series(index=state_subset.index, dtype="float64")),
        errors="coerce",
    )
    state_subset["current_aadt_official_covered"] = (
        state_subset["current_aadt_official_covered"].fillna(False).astype(bool)
    )
    state_subset["segment_length_mi"] = pd.to_numeric(
        state_subset.get("segment_length_mi", pd.Series(index=state_subset.index, dtype="float64")),
        errors="coerce",
    ).fillna(0.0)
    state_subset = state_subset.sort_values(
        by=["ROUTE_ID", "FROM_MILEPOINT", "TO_MILEPOINT"],
        na_position="last",
    )

    candidate_rows: list[dict] = []
    for route_id, route_group in state_subset.groupby("ROUTE_ID", sort=False):
        route_group = route_group.reset_index(drop=True)
        route_group["run_id"] = route_group["current_aadt_official_covered"].ne(
            route_group["current_aadt_official_covered"].shift()
        ).cumsum()
        route_covered_segments = int(route_group["current_aadt_official_covered"].sum())

        for _, run_group in route_group.groupby("run_id", sort=False):
            if bool(run_group["current_aadt_official_covered"].iloc[0]):
                continue

            run_start = int(run_group.index.min())
            run_end = int(run_group.index.max())
            prev_row = route_group.iloc[run_start - 1] if run_start > 0 else None
            next_row = route_group.iloc[run_end + 1] if run_end < len(route_group) - 1 else None

            prev_covered = prev_row is not None and bool(prev_row["current_aadt_official_covered"])
            next_covered = next_row is not None and bool(next_row["current_aadt_official_covered"])

            if prev_covered and next_covered:
                candidate_strategy = "interpolate_between_adjacent_covered"
                candidate_priority = "high"
            elif prev_covered or next_covered:
                candidate_strategy = "single_side_extension"
                candidate_priority = "medium"
            elif route_covered_segments > 0:
                candidate_strategy = "route_has_nonadjacent_covered_only"
                candidate_priority = "low"
            else:
                candidate_strategy = "no_current_aadt_on_route"
                candidate_priority = "low"

            first_row = run_group.iloc[0]
            last_row = run_group.iloc[-1]
            candidate_rows.append(
                {
                    "ROUTE_ID": route_id,
                    "COUNTY_CODE": first_row.get("COUNTY_CODE"),
                    "COUNTY_NAME": first_row.get("COUNTY_NAME"),
                    "DISTRICT": first_row.get("DISTRICT"),
                    "ROUTE_FAMILY": first_row.get("ROUTE_FAMILY"),
                    "ROUTE_FAMILY_DETAIL": first_row.get("ROUTE_FAMILY_DETAIL"),
                    "PARSED_FUNCTION_TYPE": first_row.get("PARSED_FUNCTION_TYPE"),
                    "PARSED_FUNCTION_TYPE_LABEL": first_row.get("PARSED_FUNCTION_TYPE_LABEL"),
                    "uncovered_segments_in_run": int(len(run_group)),
                    "uncovered_miles_in_run": float(run_group["segment_length_mi"].sum()),
                    "gap_from_milepoint": first_row.get("FROM_MILEPOINT"),
                    "gap_to_milepoint": last_row.get("TO_MILEPOINT"),
                    "route_covered_segments": route_covered_segments,
                    "route_uncovered_segments": int((~route_group["current_aadt_official_covered"]).sum()),
                    "previous_segment_covered": prev_covered,
                    "previous_segment_to_milepoint": prev_row.get("TO_MILEPOINT") if prev_row is not None else None,
                    "previous_segment_aadt": prev_row.get("AADT_2024_OFFICIAL") if prev_row is not None else None,
                    "next_segment_covered": next_covered,
                    "next_segment_from_milepoint": next_row.get("FROM_MILEPOINT") if next_row is not None else None,
                    "next_segment_aadt": next_row.get("AADT_2024_OFFICIAL") if next_row is not None else None,
                    "candidate_strategy": candidate_strategy,
                    "candidate_priority": candidate_priority,
                }
            )

    candidates = pd.DataFrame(candidate_rows)
    if candidates.empty:
        return candidates

    priority_order = {"high": 0, "medium": 1, "low": 2}
    candidates["_priority_order"] = candidates["candidate_priority"].map(priority_order).fillna(9)
    candidates = candidates.sort_values(
        by=["_priority_order", "uncovered_segments_in_run", "uncovered_miles_in_run", "ROUTE_ID", "gap_from_milepoint"],
        ascending=[True, False, False, True, True],
        na_position="last",
    ).drop(columns=["_priority_order"])
    return candidates.replace({np.nan: None})


def write_current_aadt_coverage_audit(df: pd.DataFrame) -> None:
    """Write current-year AADT coverage audit artifacts.

    Outputs:
    - reports/current_aadt_coverage_audit_summary.json
    - .tmp/roadway_inventory/current_aadt_audit/current_aadt_uncovered_segments.csv
    - .tmp/roadway_inventory/current_aadt_audit/current_aadt_uncovered_route_summary.csv
    - .tmp/roadway_inventory/current_aadt_audit/current_aadt_state_system_gap_fill_candidates.csv
    """

    audit = df.copy()
    audit["AADT_2024"] = pd.to_numeric(
        audit.get("AADT_2024", audit.get("AADT", pd.Series(index=audit.index, dtype="float64"))),
        errors="coerce",
    )
    audit["AADT_2024_OFFICIAL"] = pd.to_numeric(
        audit.get("AADT_2024_OFFICIAL", pd.Series(index=audit.index, dtype="float64")),
        errors="coerce",
    )
    audit["current_aadt_official_covered"] = audit.get(
        "current_aadt_official_covered",
        audit["AADT_2024_OFFICIAL"].notna(),
    )
    audit["current_aadt_official_covered"] = (
        audit["current_aadt_official_covered"].fillna(False).astype(bool)
    )
    audit["current_aadt_covered"] = audit.get(
        "current_aadt_covered",
        audit["AADT_2024"].notna(),
    )
    audit["current_aadt_covered"] = audit["current_aadt_covered"].fillna(False).astype(bool)
    audit["AADT_2024_SOURCE"] = audit.get(
        "AADT_2024_SOURCE",
        np.where(audit["current_aadt_official_covered"], "official_exact", "missing"),
    )
    audit["segment_length_mi"] = pd.to_numeric(
        audit.get("segment_length_mi", pd.Series(index=audit.index, dtype="float64")),
        errors="coerce",
    ).fillna(0.0)
    audit["covered_segment"] = audit["current_aadt_covered"].astype(int)
    audit["uncovered_segment"] = (~audit["current_aadt_covered"]).astype(int)
    audit["covered_miles"] = np.where(
        audit["current_aadt_covered"],
        audit["segment_length_mi"],
        0.0,
    )
    audit["uncovered_miles"] = np.where(
        ~audit["current_aadt_covered"],
        audit["segment_length_mi"],
        0.0,
    )

    segment_count = int(len(audit))
    covered_segments = int(audit["covered_segment"].sum())
    uncovered_segments = int(audit["uncovered_segment"].sum())
    official_covered_segments = int(audit["current_aadt_official_covered"].sum())
    analytical_gap_fill_segments = int((audit["AADT_2024_SOURCE"] == "analytical_gap_fill").sum())
    total_miles = float(audit["segment_length_mi"].sum())
    covered_miles = float(audit["covered_miles"].sum())
    uncovered_miles = float(audit["uncovered_miles"].sum())
    official_covered_miles = float(
        audit.loc[audit["current_aadt_official_covered"], "segment_length_mi"].sum()
    )
    analytical_gap_fill_miles = float(
        audit.loc[audit["AADT_2024_SOURCE"] == "analytical_gap_fill", "segment_length_mi"].sum()
    )

    state_subset = audit[audit["SYSTEM_CODE"].astype(str) == "1"].copy()
    state_uncovered = int(state_subset["uncovered_segment"].sum())

    summary = {
        "segment_count": segment_count,
        "current_aadt_segments": covered_segments,
        "current_aadt_pct_segments": round((covered_segments / segment_count) * 100.0, 4)
        if segment_count
        else None,
        "current_aadt_official_segments": official_covered_segments,
        "current_aadt_official_pct_segments": round((official_covered_segments / segment_count) * 100.0, 4)
        if segment_count
        else None,
        "current_aadt_analytical_gap_fill_segments": analytical_gap_fill_segments,
        "current_aadt_uncovered_segments": uncovered_segments,
        "segment_miles": total_miles,
        "current_aadt_miles": covered_miles,
        "current_aadt_pct_miles": round((covered_miles / total_miles) * 100.0, 4)
        if total_miles
        else None,
        "current_aadt_official_miles": official_covered_miles,
        "current_aadt_analytical_gap_fill_miles": analytical_gap_fill_miles,
        "current_aadt_uncovered_miles": uncovered_miles,
        "state_system_segments": int(len(state_subset)),
        "state_system_uncovered_segments": state_uncovered,
        "state_system_covered_pct_segments": round(
            ((len(state_subset) - state_uncovered) / len(state_subset)) * 100.0,
            4,
        )
        if len(state_subset)
        else None,
        "by_system_code": _group_current_aadt_coverage(audit, ["SYSTEM_CODE"]),
        "by_route_family": _group_current_aadt_coverage(audit, ["ROUTE_FAMILY"]),
        "by_district": _group_current_aadt_coverage(audit, ["DISTRICT"]),
        "by_aadt_2024_source": _group_current_aadt_coverage(audit, ["AADT_2024_SOURCE"]),
        "by_function_type": _group_current_aadt_coverage(
            audit,
            ["PARSED_FUNCTION_TYPE", "PARSED_FUNCTION_TYPE_LABEL"],
        ),
        "top_counties_by_uncovered_segments": _group_current_aadt_coverage(
            audit,
            ["COUNTY_CODE", "COUNTY_NAME", "DISTRICT"],
            limit=25,
        ),
        "top_state_system_routes_by_uncovered_segments": _group_current_aadt_coverage(
            state_subset,
            ["ROUTE_ID", "COUNTY_CODE", "COUNTY_NAME", "DISTRICT", "ROUTE_FAMILY"],
            limit=50,
        ),
    }

    uncovered_segment_columns = [
        "unique_id",
        "ROUTE_ID",
        "COUNTY_CODE",
        "COUNTY_NAME",
        "DISTRICT",
        "SYSTEM_CODE",
        "SYSTEM_CODE_LABEL",
        "ROUTE_FAMILY",
        "ROUTE_FAMILY_DETAIL",
        "PARSED_FUNCTION_TYPE",
        "PARSED_FUNCTION_TYPE_LABEL",
        "FUNCTIONAL_CLASS",
        "FUNCTIONAL_CLASS_LABEL",
        "FROM_MILEPOINT",
        "TO_MILEPOINT",
        "segment_length_mi",
        "current_aadt_covered",
        "current_aadt_official_covered",
        "AADT_2024_SOURCE",
        "AADT_2024_CONFIDENCE",
    ]
    uncovered_segment_columns = [
        column for column in uncovered_segment_columns if column in audit.columns
    ]
    uncovered_segments_df = audit.loc[
        ~audit["current_aadt_covered"],
        uncovered_segment_columns,
    ].sort_values(
        by=["SYSTEM_CODE", "ROUTE_FAMILY", "DISTRICT", "COUNTY_CODE", "ROUTE_ID", "FROM_MILEPOINT"],
        na_position="last",
    )

    route_summary = pd.DataFrame(
        _group_current_aadt_coverage(
            audit,
            ["ROUTE_ID", "COUNTY_CODE", "COUNTY_NAME", "DISTRICT", "SYSTEM_CODE", "ROUTE_FAMILY"],
        )
    )
    if not route_summary.empty:
        route_summary = route_summary[route_summary["uncovered_segments"] > 0].copy()

    gap_fill_candidates = build_state_system_gap_fill_candidates(audit)
    gap_fill_summary = {
        "candidate_runs": int(len(gap_fill_candidates)),
        "high_priority_candidate_runs": int(
            (gap_fill_candidates.get("candidate_priority") == "high").sum()
        )
        if not gap_fill_candidates.empty
        else 0,
        "medium_priority_candidate_runs": int(
            (gap_fill_candidates.get("candidate_priority") == "medium").sum()
        )
        if not gap_fill_candidates.empty
        else 0,
        "low_priority_candidate_runs": int(
            (gap_fill_candidates.get("candidate_priority") == "low").sum()
        )
        if not gap_fill_candidates.empty
        else 0,
        "candidate_run_miles": float(
            pd.to_numeric(
                gap_fill_candidates.get(
                    "uncovered_miles_in_run",
                    pd.Series(dtype="float64"),
                ),
                errors="coerce",
            ).fillna(0.0).sum()
        )
        if not gap_fill_candidates.empty
        else 0.0,
        "top_candidate_strategies": (
            gap_fill_candidates["candidate_strategy"].value_counts(dropna=False).head(10).to_dict()
            if not gap_fill_candidates.empty and "candidate_strategy" in gap_fill_candidates.columns
            else {}
        ),
    }
    summary["state_system_gap_fill_candidates"] = gap_fill_summary

    summary_path = REPORTS_DIR / "current_aadt_coverage_audit_summary.json"
    audit_tmp_dir = PROJECT_ROOT / ".tmp" / "roadway_inventory" / "current_aadt_audit"
    audit_tmp_dir.mkdir(parents=True, exist_ok=True)
    uncovered_segments_path = audit_tmp_dir / "current_aadt_uncovered_segments.csv"
    uncovered_route_summary_path = audit_tmp_dir / "current_aadt_uncovered_route_summary.csv"
    gap_fill_candidates_path = audit_tmp_dir / "current_aadt_state_system_gap_fill_candidates.csv"

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    uncovered_segments_df.to_csv(uncovered_segments_path, index=False)
    route_summary.to_csv(uncovered_route_summary_path, index=False)
    gap_fill_candidates.to_csv(gap_fill_candidates_path, index=False)

    logger.info("Wrote current AADT coverage audit summary to %s", summary_path)
    logger.info("Wrote uncovered current AADT segment audit to %s", uncovered_segments_path)
    logger.info("Wrote uncovered current AADT route audit to %s", uncovered_route_summary_path)
    logger.info("Wrote state-system AADT gap-fill candidate audit to %s", gap_fill_candidates_path)


def load_county_boundaries_for_attribute_backfill(
    gpkg_path: Path,
) -> gpd.GeoDataFrame | None:
    """Load county boundaries for spatial attribute backfill.

    Prefer existing staged county boundaries so the ETL can still backfill
    county and district values when live boundary refresh is unavailable.
    """

    existing_counties, existing_districts = load_existing_boundary_layers(gpkg_path)
    if existing_counties is not None and not existing_counties.empty:
        logger.info("Using existing staged county boundaries for attribute backfill")
        return existing_counties

    try:
        district_boundaries = fetch_official_district_boundaries()
        county_boundaries = fetch_official_county_boundaries(
            district_boundaries=district_boundaries
        )
        logger.info("Using live GDOT county boundaries for attribute backfill")
        return county_boundaries
    except Exception as exc:
        logger.warning("County boundary backfill source unavailable: %s", exc)
        return None


def _prepare_county_boundaries_for_spatial_use(
    county_boundaries: gpd.GeoDataFrame,
    target_crs,
) -> gpd.GeoDataFrame:
    county_cols = [
        column
        for column in ["COUNTYFP", "NAME", "GDOT_DISTRICT", "geometry"]
        if column in county_boundaries.columns
    ]
    if "geometry" not in county_cols:
        raise ValueError("County boundaries are missing geometry")

    counties = county_boundaries[county_cols].copy()
    if counties.crs is not None and target_crs is not None and counties.crs != target_crs:
        counties = counties.to_crs(target_crs)

    counties["geometry"] = counties.geometry.map(
        lambda geometry: (
            make_valid(geometry)
            if geometry is not None and not geometry.is_empty and not geometry.is_valid
            else geometry
        )
    )
    counties = counties[counties.geometry.notna() & ~counties.geometry.is_empty].copy()

    if "COUNTYFP" in counties.columns:
        counties["COUNTYFP"] = counties["COUNTYFP"].astype(str).str.zfill(3)
    if "NAME" in counties.columns:
        counties["NAME"] = counties["NAME"].astype(str).str.strip()
    if "GDOT_DISTRICT" in counties.columns:
        counties["GDOT_DISTRICT"] = pd.to_numeric(
            counties["GDOT_DISTRICT"],
            errors="coerce",
        ).astype("Int64")

    return counties


def _normalized_county_code_series(series: pd.Series) -> pd.Series:
    return series.map(
        lambda value: (
            f"{int(float(value)):03d}"
            if pd.notna(value) and str(value).strip() not in {"", "nan", "None"}
            else None
        )
    )


def _overlay_segment_county_lengths(
    segments: gpd.GeoDataFrame,
    counties: gpd.GeoDataFrame,
) -> pd.DataFrame:
    if segments.empty:
        return pd.DataFrame(
            columns=[
                "segment_index",
                "COUNTYFP",
                "NAME",
                "GDOT_DISTRICT",
                "intersection_length_m",
            ]
        )

    overlay = gpd.overlay(
        segments,
        counties,
        how="intersection",
        keep_geom_type=False,
    )
    if overlay.empty:
        return pd.DataFrame(
            columns=[
                "segment_index",
                "COUNTYFP",
                "NAME",
                "GDOT_DISTRICT",
                "intersection_length_m",
            ]
        )

    overlay["intersection_length_m"] = overlay.geometry.length
    overlay = overlay[overlay["intersection_length_m"] > MILEPOINT_TOLERANCE].copy()
    if overlay.empty:
        return pd.DataFrame(
            columns=[
                "segment_index",
                "COUNTYFP",
                "NAME",
                "GDOT_DISTRICT",
                "intersection_length_m",
            ]
        )

    group_columns = [
        column
        for column in ["segment_index", "COUNTYFP", "NAME", "GDOT_DISTRICT"]
        if column in overlay.columns
    ]
    return (
        overlay.groupby(group_columns, dropna=False)["intersection_length_m"]
        .sum()
        .reset_index()
    )


def _assign_majority_county_district(
    filled: gpd.GeoDataFrame,
    counties: gpd.GeoDataFrame,
    statewide_mask: pd.Series,
) -> tuple[gpd.GeoDataFrame, dict[str, int]]:
    statewide_segments = filled.loc[statewide_mask, ["geometry"]].copy()
    if statewide_segments.empty:
        return filled, {"segments": 0, "county_fills": 0, "district_fills": 0}

    statewide_segments = gpd.GeoDataFrame(
        statewide_segments,
        geometry="geometry",
        crs=filled.crs,
    )
    statewide_segments["segment_index"] = statewide_segments.index
    statewide_segments = statewide_segments[
        statewide_segments.geometry.notna() & ~statewide_segments.geometry.is_empty
    ].copy()
    if statewide_segments.empty:
        return filled, {"segments": 0, "county_fills": 0, "district_fills": 0}

    all_county_lengths = _overlay_segment_county_lengths(statewide_segments, counties)
    if all_county_lengths.empty:
        return filled, {"segments": 0, "county_fills": 0, "district_fills": 0}

    county_lengths = (
        all_county_lengths.sort_values(
            by=["segment_index", "intersection_length_m", "COUNTYFP"],
            ascending=[True, False, True],
            na_position="last",
        )
        .drop_duplicates(subset=["segment_index"])
    )
    district_lengths = (
        all_county_lengths.groupby(["segment_index", "GDOT_DISTRICT"], dropna=False)["intersection_length_m"]
        .sum()
        .reset_index()
        .sort_values(
            by=["segment_index", "intersection_length_m", "GDOT_DISTRICT"],
            ascending=[True, False, True],
            na_position="last",
        )
        .drop_duplicates(subset=["segment_index"])
    )

    majority_assignments = county_lengths.merge(
        district_lengths[["segment_index", "GDOT_DISTRICT"]],
        on="segment_index",
        how="outer",
        suffixes=("", "_district"),
    )

    county_fill_count = 0
    district_fill_count = 0
    for assignment in majority_assignments.itertuples(index=False):
        segment_index = int(assignment.segment_index)
        countyfp = getattr(assignment, "COUNTYFP", None)
        county_name = getattr(assignment, "NAME", None)
        district_value = getattr(assignment, "GDOT_DISTRICT", None)

        if pd.notna(countyfp):
            county_id_value = int(str(countyfp))
            if pd.isna(filled.at[segment_index, "COUNTY_ID"]) or int(filled.at[segment_index, "COUNTY_ID"]) == 0:
                filled.at[segment_index, "COUNTY_ID"] = county_id_value
                county_fill_count += 1
            existing_county_code = _normalized_county_code_series(
                pd.Series([filled.at[segment_index, "COUNTY_CODE"]])
            ).iloc[0]
            if existing_county_code in {None, "000"}:
                filled.at[segment_index, "COUNTY_CODE"] = str(countyfp).zfill(3)
                county_fill_count += 1
            if "COUNTY_NAME" in filled.columns and (
                pd.isna(filled.at[segment_index, "COUNTY_NAME"])
                or str(filled.at[segment_index, "COUNTY_NAME"]).strip() in {"", "nan", "None"}
            ):
                filled.at[segment_index, "COUNTY_NAME"] = county_name

        if pd.notna(district_value):
            district_int = int(district_value)
            if pd.isna(filled.at[segment_index, "GDOT_District"]):
                filled.at[segment_index, "GDOT_District"] = district_int
                district_fill_count += 1
            if pd.isna(filled.at[segment_index, "DISTRICT"]):
                filled.at[segment_index, "DISTRICT"] = district_int
                district_fill_count += 1

    return filled, {
        "segments": int(len(majority_assignments)),
        "county_fills": county_fill_count,
        "district_fills": district_fill_count,
    }


def backfill_county_district_from_geometry(
    gdf: gpd.GeoDataFrame,
    county_boundaries: gpd.GeoDataFrame | None,
) -> gpd.GeoDataFrame:
    """Spatially backfill missing county and district fields.

    Statewide GDOT route IDs with county code `000` are assigned using the
    county and district that cover the longest portion of the segment. Other
    missing records fall back to a representative-point join.
    """

    if county_boundaries is None or county_boundaries.empty:
        logger.warning("Skipping county/district backfill because no county boundaries are available")
        return gdf

    filled = gdf.copy()
    missing_mask = (
        filled.get("COUNTY_ID", pd.Series(index=filled.index, dtype="float64")).isna()
        | filled.get("COUNTY_CODE", pd.Series(index=filled.index, dtype="object")).isna()
        | filled.get("DISTRICT", pd.Series(index=filled.index, dtype="float64")).isna()
        | filled.get("GDOT_District", pd.Series(index=filled.index, dtype="float64")).isna()
    )
    missing_count = int(missing_mask.sum())
    if missing_count == 0:
        return filled

    try:
        counties = _prepare_county_boundaries_for_spatial_use(county_boundaries, filled.crs)
    except ValueError as exc:
        logger.warning("Skipping county/district backfill because %s", exc)
        return filled

    county_codes = _normalized_county_code_series(
        filled.get("COUNTY_CODE", pd.Series(index=filled.index, dtype="object"))
    )
    parsed_county_codes = (
        filled.get("PARSED_COUNTY_CODE", pd.Series(index=filled.index, dtype="object"))
        .astype(str)
        .str.zfill(3)
        .where(lambda series: series.ne("nan"), None)
    )
    statewide_mask = missing_mask & (
        county_codes.eq("000") | parsed_county_codes.eq("000")
    )
    filled, majority_stats = _assign_majority_county_district(
        filled,
        counties,
        statewide_mask,
    )

    remaining_missing_mask = (
        filled.get("COUNTY_ID", pd.Series(index=filled.index, dtype="float64")).isna()
        | filled.get("COUNTY_CODE", pd.Series(index=filled.index, dtype="object")).isna()
        | filled.get("DISTRICT", pd.Series(index=filled.index, dtype="float64")).isna()
        | filled.get("GDOT_District", pd.Series(index=filled.index, dtype="float64")).isna()
    )
    remaining_missing_mask &= ~statewide_mask

    points = filled.loc[remaining_missing_mask, ["geometry"]].copy()
    points = gpd.GeoDataFrame(points, geometry="geometry", crs=filled.crs)
    points["segment_index"] = points.index
    points["geometry"] = points.geometry.representative_point()

    if points.empty:
        joined = gpd.GeoDataFrame({"segment_index": []}, geometry=[], crs=filled.crs)
    else:
        joined = gpd.sjoin(
            points,
            counties,
            how="left",
            predicate="intersects",
        )
        joined = joined.sort_values(by=["segment_index"]).drop_duplicates(subset=["segment_index"])

    matched_segment_ids = joined.loc[joined.get("COUNTYFP").notna(), "segment_index"] if "COUNTYFP" in joined.columns else pd.Series(dtype="int64")
    unmatched_points = points[~points["segment_index"].isin(matched_segment_ids)].copy()
    nearest_match_count = 0
    if not unmatched_points.empty:
        try:
            nearest_joined = gpd.sjoin_nearest(
                unmatched_points,
                counties,
                how="left",
                distance_col="_county_distance_m",
            )
            nearest_joined = nearest_joined.sort_values(
                by=["segment_index", "_county_distance_m"],
                na_position="last",
            ).drop_duplicates(subset=["segment_index"])
            nearest_match_count = int(nearest_joined["COUNTYFP"].notna().sum()) if "COUNTYFP" in nearest_joined.columns else 0
            joined = pd.concat([joined, nearest_joined], ignore_index=True, sort=False)
            joined["_has_county_match"] = (
                joined.get("COUNTYFP", pd.Series(index=joined.index, dtype="object")).notna()
                | joined.get("GDOT_DISTRICT", pd.Series(index=joined.index, dtype="float64")).notna()
            ).astype(int)
            joined = joined.sort_values(
                by=["segment_index", "_has_county_match", "_county_distance_m"],
                ascending=[True, False, True],
                na_position="last",
            ).drop_duplicates(subset=["segment_index"])
            joined = joined.drop(columns=["_has_county_match"], errors="ignore")
        except Exception as exc:
            logger.warning("Nearest county/district fallback failed: %s", exc)

    backfilled_count = 0
    county_fill_count = 0
    district_fill_count = 0
    for joined_row in joined.itertuples(index=False):
        segment_index = getattr(joined_row, "segment_index")
        countyfp = getattr(joined_row, "COUNTYFP", None)
        county_name = getattr(joined_row, "NAME", None)
        gdot_district = getattr(joined_row, "GDOT_DISTRICT", None)

        if pd.notna(countyfp):
            county_value = int(str(countyfp))
            if pd.isna(filled.at[segment_index, "COUNTY_ID"]):
                filled.at[segment_index, "COUNTY_ID"] = county_value
                county_fill_count += 1
            if pd.isna(filled.at[segment_index, "COUNTY_CODE"]):
                filled.at[segment_index, "COUNTY_CODE"] = str(countyfp).zfill(3)
                county_fill_count += 1
            if "COUNTY_NAME" in filled.columns and pd.isna(filled.at[segment_index, "COUNTY_NAME"]):
                filled.at[segment_index, "COUNTY_NAME"] = county_name

        if pd.notna(gdot_district):
            district_value = int(gdot_district)
            if pd.isna(filled.at[segment_index, "GDOT_District"]):
                filled.at[segment_index, "GDOT_District"] = district_value
                district_fill_count += 1
            if pd.isna(filled.at[segment_index, "DISTRICT"]):
                filled.at[segment_index, "DISTRICT"] = district_value
                district_fill_count += 1

        if pd.notna(countyfp) or pd.notna(gdot_district):
            backfilled_count += 1

    logger.info(
        "Spatial county/district backfill matched %d of %d affected segments; statewide majority matches=%d, county fills=%d, district fills=%d, nearest fallback matches=%d",
        backfilled_count,
        missing_count,
        majority_stats["segments"],
        county_fill_count + majority_stats["county_fills"],
        district_fill_count + majority_stats["district_fills"],
        nearest_match_count,
    )
    return filled


def add_county_all_from_geometry(
    gdf: gpd.GeoDataFrame,
    county_boundaries: gpd.GeoDataFrame | None,
) -> gpd.GeoDataFrame:
    """Populate county_all using county overlap shares plus the staged major county."""

    if county_boundaries is None or county_boundaries.empty:
        logger.warning("Skipping county_all because no county boundaries are available")
        return gdf

    try:
        counties = _prepare_county_boundaries_for_spatial_use(county_boundaries, gdf.crs)
    except ValueError as exc:
        logger.warning("Skipping county_all because %s", exc)
        return gdf

    updated = gdf.copy()
    segments = updated.loc[:, ["geometry", "segment_length_m"]].copy()
    segments = gpd.GeoDataFrame(segments, geometry="geometry", crs=updated.crs)
    segments["segment_index"] = segments.index
    segments["segment_length_m"] = pd.to_numeric(segments["segment_length_m"], errors="coerce")
    segments = segments[
        segments.geometry.notna()
        & ~segments.geometry.is_empty
        & segments["segment_length_m"].gt(MILEPOINT_TOLERANCE)
    ].copy()

    county_all = pd.Series(pd.NA, index=updated.index, dtype="object")
    county_lengths = _overlay_segment_county_lengths(segments, counties)
    if not county_lengths.empty:
        county_lengths = county_lengths.merge(
            segments[["segment_index", "segment_length_m"]],
            on="segment_index",
            how="left",
        )
        county_lengths["NAME"] = county_lengths["NAME"].map(_clean_optional_text)
        county_lengths = county_lengths[county_lengths["NAME"].notna()].copy()
        county_lengths["county_share"] = (
            county_lengths["intersection_length_m"] / county_lengths["segment_length_m"]
        )
        county_lengths = county_lengths[
            county_lengths["county_share"] >= COUNTY_ALL_MIN_SHARE
        ].copy()
        if not county_lengths.empty:
            county_lengths = county_lengths.sort_values(
                by=["segment_index", "county_share", "COUNTYFP"],
                ascending=[True, False, True],
                na_position="last",
            )
            county_all_values = county_lengths.groupby("segment_index")["NAME"].agg(
                lambda values: COUNTY_ALL_DELIMITER.join(_dedupe_county_names(list(values)))
            )
            county_all.loc[county_all_values.index] = county_all_values

    county_name_series = get_or_empty_series(updated, "COUNTY_NAME").map(_clean_optional_text)
    merged_county_all = [
        _merge_county_all_value(county_all_value, county_name)
        for county_all_value, county_name in zip(
            county_all.tolist(),
            county_name_series.tolist(),
        )
    ]
    updated["county_all"] = pd.Series(merged_county_all, index=updated.index, dtype="object")

    non_null_count = int(updated["county_all"].notna().sum())
    multi_county_count = int(
        updated["county_all"].fillna("").str.contains(COUNTY_ALL_DELIMITER, regex=False).sum()
    )
    logger.info(
        "Computed county_all for %d segments; %d multi-county rows retained at %.1f%% overlap threshold",
        non_null_count,
        multi_county_count,
        COUNTY_ALL_MIN_SHARE * 100.0,
    )
    return updated


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
    invalid_count = int((~gdf.geometry.is_valid).sum())
    if invalid_count:
        logger.info("Repairing %d invalid district boundary geometries", invalid_count)
        gdf["geometry"] = gdf.geometry.map(
            lambda geometry: (
                force_2d(make_valid(geometry))
                if geometry is not None and not geometry.is_empty and not geometry.is_valid
                else (force_2d(geometry) if geometry is not None and not geometry.is_empty else geometry)
            )
        )
    logger.info("Loaded %d district boundary features", len(gdf))
    return gdf


def load_existing_boundary_layers(
    gpkg_path: Path,
) -> tuple[gpd.GeoDataFrame | None, gpd.GeoDataFrame | None]:
    """Load existing staged boundary layers for offline fallback."""
    if not gpkg_path.exists():
        return None, None

    county_boundaries = None
    district_boundaries = None
    try:
        county_boundaries = gpd.read_file(
            gpkg_path,
            layer="county_boundaries",
            engine="pyogrio",
        )
    except Exception as exc:
        logger.warning("Could not load existing county boundaries from %s: %s", gpkg_path, exc)

    try:
        district_boundaries = gpd.read_file(
            gpkg_path,
            layer="district_boundaries",
            engine="pyogrio",
        )
    except Exception as exc:
        logger.warning("Could not load existing district boundaries from %s: %s", gpkg_path, exc)

    return county_boundaries, district_boundaries


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
    invalid_count = int((~gdf.geometry.is_valid).sum())
    if invalid_count:
        logger.info("Repairing %d invalid county boundary geometries", invalid_count)
        gdf["geometry"] = gdf.geometry.map(
            lambda geometry: (
                force_2d(make_valid(geometry))
                if geometry is not None and not geometry.is_empty and not geometry.is_valid
                else (force_2d(geometry) if geometry is not None and not geometry.is_empty else geometry)
            )
        )
    logger.info("Loaded %d county boundary features", len(gdf))
    return gdf


def assert_decoded_county_lookup_matches_boundaries(
    gdf: gpd.GeoDataFrame,
    county_boundaries: gpd.GeoDataFrame,
) -> None:
    boundary_lookup = {
        str(row.COUNTYFP).zfill(3): str(row.NAME).strip()
        for row in county_boundaries.itertuples(index=False)
        if pd.notna(getattr(row, "COUNTYFP", None)) and pd.notna(getattr(row, "NAME", None))
    }
    if len(COUNTY_NAME_LOOKUP) != 159:
        raise AssertionError(
            f"county_codes.json must contain 159 Georgia counties; found {len(COUNTY_NAME_LOOKUP)}"
        )
    if len(boundary_lookup) != 159:
        raise AssertionError(
            f"county_boundaries must contain 159 Georgia counties; found {len(boundary_lookup)}"
        )

    mismatches = sorted(
        (
            county_code,
            COUNTY_NAME_LOOKUP.get(county_code),
            boundary_lookup.get(county_code),
        )
        for county_code in sorted(COUNTY_NAME_LOOKUP)
        if COUNTY_NAME_LOOKUP.get(county_code) != boundary_lookup.get(county_code)
    )
    if mismatches:
        sample = ", ".join(
            f"{county_code}:{lookup_name}->{boundary_name}"
            for county_code, lookup_name, boundary_name in mismatches[:10]
        )
        raise AssertionError(
            "county_codes.json does not match the staged county boundary layer: "
            f"{sample}"
        )

    if {"COUNTY_CODE", "COUNTY_NAME"}.issubset(gdf.columns):
        decoded = gdf[["COUNTY_CODE", "COUNTY_NAME"]].copy()
        decoded["COUNTY_CODE"] = _normalized_county_code_series(decoded["COUNTY_CODE"])
        decoded["COUNTY_NAME"] = decoded["COUNTY_NAME"].where(
            decoded["COUNTY_NAME"].notna(),
            None,
        )
        decoded["COUNTY_NAME"] = decoded["COUNTY_NAME"].map(
            lambda value: value.strip() if isinstance(value, str) else value
        )
        decoded = decoded[
            decoded["COUNTY_CODE"].notna()
            & decoded["COUNTY_NAME"].notna()
            & decoded["COUNTY_CODE"].ne("000")
        ].drop_duplicates()

        decoded_mismatches = decoded[
            decoded["COUNTY_CODE"].map(boundary_lookup).ne(decoded["COUNTY_NAME"])
        ]
        if not decoded_mismatches.empty:
            sample = ", ".join(
                f"{row.COUNTY_CODE}:{row.COUNTY_NAME}->{boundary_lookup.get(row.COUNTY_CODE)}"
                for row in decoded_mismatches.head(10).itertuples(index=False)
            )
            raise AssertionError(
                "Decoded county labels do not match the staged county boundary layer: "
                f"{sample}"
            )


def write_supporting_boundary_layers(
    gpkg_path: Path,
    fallback_county_boundaries: gpd.GeoDataFrame | None = None,
    fallback_district_boundaries: gpd.GeoDataFrame | None = None,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Append official GDOT boundary layers to the staged GeoPackage."""
    try:
        district_boundaries = fetch_official_district_boundaries()
        county_boundaries = fetch_official_county_boundaries(district_boundaries=district_boundaries)
    except Exception as exc:
        if fallback_county_boundaries is None or fallback_district_boundaries is None:
            raise
        logger.warning(
            "Official boundary refresh unavailable; reusing existing staged boundaries: %s",
            exc,
        )
        county_boundaries = fallback_county_boundaries
        district_boundaries = fallback_district_boundaries

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
    return county_boundaries, district_boundaries


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    road_inv_gdb = find_path(RAW_DIR, ROAD_INV_GDB_NAME)
    traffic_gdb = find_path(RAW_DIR, TRAFFIC_GDB_NAME)

    routes = load_route_geometry(road_inv_gdb)
    routes = enrich_routes_with_static_attributes(routes, road_inv_gdb)

    current_traffic = load_current_traffic(traffic_gdb)

    routes = prepare_route_attributes(routes, current_traffic)
    routes = build_unique_id(routes)

    current_lookup = build_interval_lookup(current_traffic)
    segmented = segment_routes(routes, current_lookup)
    segmented = build_unique_id(segmented)

    if segmented.crs is not None:
        segmented = segmented.to_crs(TARGET_CRS)
        logger.info("Reprojected segmented network to %s", TARGET_CRS)
    else:
        logger.warning("No CRS set on segmented network; cannot reproject")

    segmented = compute_segment_length(segmented)
    segmented = apply_rnhp_enrichment(segmented)
    existing_gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    county_boundaries_for_backfill = load_county_boundaries_for_attribute_backfill(existing_gpkg_path)
    segmented = backfill_county_district_from_geometry(
        segmented,
        county_boundaries_for_backfill,
    )
    segmented = apply_hpms_enrichment(segmented)
    # Signed-route verification precedence:
    # 1. HPMS enrichment runs first — broad coverage, gap-fills AADT and
    #    attributes, sets initial signed-route classification from federal
    #    routesigning codes.
    # 2. GPAS verification runs second — GDOT's own live reference layers have
    #    final authority for signed-route family. GPAS can upgrade or confirm
    #    but never downgrade a higher-priority family.
    segmented = apply_signed_route_verification(segmented)
    segmented = sync_derived_alias_fields(segmented)
    route_type_fields = apply_gdot_route_type_classification(segmented)
    segmented = pd.concat([segmented, route_type_fields], axis=1)
    segmented = apply_direction_mirror_aadt(segmented)
    segmented = apply_state_system_current_aadt_gap_fill(segmented)
    segmented = apply_nearest_neighbor_aadt(segmented)
    segmented = apply_future_aadt_fill_chain(segmented)
    segmented = add_decoded_label_columns(segmented)
    segmented = add_county_all_from_geometry(segmented, county_boundaries_for_backfill)
    segmented = _move_column_after(segmented, "county_all", "COUNTY_NAME")

    logger.info("Final segment count: %d", len(segmented))
    logger.info("Current AADT official coverage: %d segments", segmented["AADT_2024_OFFICIAL"].notna().sum())
    logger.info("Current AADT final coverage: %d segments", segmented["AADT_2024"].notna().sum())
    if "FUTURE_AADT_2044" in segmented.columns:
        logger.info("Future AADT 2044 coverage: %d segments", segmented["FUTURE_AADT_2044"].notna().sum())

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = TABLES_DIR / "roadway_inventory_cleaned.csv"
    segmented.drop(columns=["geometry"], errors="ignore").to_csv(csv_path, index=False)
    logger.info("Wrote staged roadway table CSV: %s (%d rows)", csv_path, len(segmented))

    segmented["geometry"] = segmented["geometry"].apply(lambda geom: force_2d(geom) if geom is not None else geom)

    SPATIAL_DIR.mkdir(parents=True, exist_ok=True)
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    fallback_county_boundaries, fallback_district_boundaries = load_existing_boundary_layers(gpkg_path)
    if gpkg_path.exists():
        gpkg_path.unlink()
    segmented.to_file(gpkg_path, layer="roadway_segments", driver="GPKG", engine="pyogrio")
    logger.info("Wrote GeoPackage: %s", gpkg_path)
    staged_county_boundaries, _ = write_supporting_boundary_layers(
        gpkg_path,
        fallback_county_boundaries=fallback_county_boundaries,
        fallback_district_boundaries=fallback_district_boundaries,
    )
    assert_decoded_county_lookup_matches_boundaries(segmented, staged_county_boundaries)

    write_match_summary(segmented)
    write_current_aadt_coverage_audit(segmented)
    write_enrichment_summary(segmented)
    write_hpms_enrichment_summary(segmented)
    write_signed_route_verification_summary(segmented)
    logger.info("Normalization complete.")


if __name__ == "__main__":
    main()
