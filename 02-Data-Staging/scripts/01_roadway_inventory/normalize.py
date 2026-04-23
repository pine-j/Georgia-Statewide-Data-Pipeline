"""Normalize Georgia roadway inventory data onto official GDOT route geometry.

This workflow uses the official `GA_2024_Routes` geometry as the base network,
then attaches current GDOT traffic fields by route ID and milepoint intervals.

The only canonical future projection kept in the normalized network is
`FUTURE_AADT` from the current 2024 GDOT traffic record.

AADT 2024 is sourced from two parallel GDOT publications:
1. The state-system 2024 traffic GDB (`TRAFFIC_Data_2024.gdb`) — captured
   verbatim into `AADT_2024_OFFICIAL`.
2. GDOT's federal HPMS 2024 submission — captured into `AADT_2024_HPMS`.

Both columns are populated wherever the source has a value, regardless of
which one ultimately wins the canonical `AADT` / `AADT_2024` field. The two
are cross-validated into `AADT_2024_SOURCE_AGREEMENT` so downstream consumers
can distinguish state-only, hpms-only, agreeing, and disagreeing segments.
The remaining `direction_mirror`, `analytical_gap_fill`, and
`nearest_neighbor` fills are pipeline-derived (not GDOT-published) and are
treated as low-confidence for the `AADT_2024_CONFIDENCE` tier.

Data sources:
1. `Road_Inventory_2024.gdb` layer `GA_2024_Routes`
   Official full roadway geometry.
2. `TRAFFIC_Data_2024.gdb` layer `TRAFFIC_DataYear2024`
   Current traffic segmentation with AADT, truck counts, VMT, factors,
   and per-record measurement metadata (`Statistics_Type`, `SampleStatus`).

Output:
- `02-Data-Staging/tables/roadway_inventory_cleaned.csv`
- `02-Data-Staging/spatial/base_network.gpkg` layers
  `roadway_segments`, `county_boundaries`, `district_boundaries`
"""

from __future__ import annotations

import io
import json
import logging
import tempfile
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import md5
from pathlib import Path
from urllib.request import urlopen

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import force_2d, line_merge, make_valid
from shapely.geometry import LineString
from shapely.ops import substring, unary_union

from admin_breakpoints import (
    BoundaryCrosser,
    compute_route_crossings,
    resolve_segment_admin_attrs,
)
from evacuation_enrichment import apply_evacuation_enrichment, write_evacuation_summary
from hpms_enrichment import apply_hpms_enrichment, write_hpms_enrichment_summary
from rnhp_enrichment import (
    apply_speed_zone_enrichment,
    apply_off_system_speed_zone_enrichment,
    write_enrichment_summary,
)
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
# Layer 1 = Counties (current), Layer 3 = GDOT Districts (current).
# Layers 2 (Area Offices, 2014 - 31 polygons, short of current 38), 4/5/6
# (legislative, pre-2020 census) are intentionally abandoned. Do not reintroduce.
# Area Office derives from config/area_office_codes.json dissolved over Layer 1.
# Legislative layers come from Census TIGER/Line (see TIGER_* constants below).
COUNTY_BOUNDARIES_URL = (
    f"{GDOT_BOUNDARIES_SERVICE}/1/query?where=1%3D1&outFields=*&f=geojson"
)
DISTRICT_BOUNDARIES_URL = (
    f"{GDOT_BOUNDARIES_SERVICE}/3/query?where=1%3D1&outFields=*&f=geojson"
)
MPO_BOUNDARIES_URL = (
    "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
    "Metropolitan_Planning_Organizations/FeatureServer/30/query"
    "?where=1%3D1&outFields=MPO_ID,MPO_NAME,STATE&f=geojson"
)
REGIONAL_COMMISSION_BOUNDARIES_URL = (
    "https://services2.arcgis.com/Gqyymy5JISeLzyNM/arcgis/rest/services/"
    "RegionalCommissions/FeatureServer/0/query?where=1%3D1&outFields=*&f=geojson"
)
# ARC OpenData: Cities Georgia (polygons, Name field, 542 features).
# Used to derive Fulton's three Area Office sub-polygons per
# area_office_codes.json subcounty_splits.
GEORGIA_CITIES_URL = (
    "https://services1.arcgis.com/Ug5xGQbHsD8zuZzM/arcgis/rest/services/"
    "Georgia_Cities_view/FeatureServer/0/query?where=1%3D1&outFields=*&f=geojson"
)

# Census TIGER/Line year + Georgia FIPS. Bump TIGER_YEAR to adopt a new
# redistricting/decennial vintage in one line.
TIGER_YEAR = 2024
GEORGIA_STATE_FIPS = "13"
STATE_HOUSE_BOUNDARIES_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/SLDL/"
    f"tl_{TIGER_YEAR}_{GEORGIA_STATE_FIPS}_sldl.zip"
)
STATE_SENATE_BOUNDARIES_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/SLDU/"
    f"tl_{TIGER_YEAR}_{GEORGIA_STATE_FIPS}_sldu.zip"
)
CONGRESSIONAL_BOUNDARIES_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/CD/"
    f"tl_{TIGER_YEAR}_{GEORGIA_STATE_FIPS}_cd119.zip"
)

REBUILD_OUTPUTS_DIR = PROJECT_ROOT / ".tmp" / "rebuild_outputs"
CURRENT_AADT_AUDIT_DIR = PROJECT_ROOT / ".tmp" / "roadway_inventory" / "current_aadt_audit"
# Offline-resilient boundary cache. Populated by
# 01-Raw-Data/Boundaries/scripts/download_boundaries.py. When a given
# layer's cache file exists, fetch_and_cache_boundary reads it instead
# of hitting the source URL - pipelines can run in environments with
# no internet (e.g. sealed rebuilds, restricted corp networks) as long
# as the cache directory is pre-populated. Re-run the downloader to
# refresh.
RAW_BOUNDARIES_CACHE_DIR = (
    PROJECT_ROOT / "01-Raw-Data" / "Boundaries" / "cache"
)

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

# Columns dropped from final output — duplicates or source-internal fields
# that are only needed during pipeline processing.
COLUMNS_TO_DROP_FROM_OUTPUT = [
    # Source metadata (not meaningful after segmentation)
    "START_M",
    "END_M",
    "RouteId",
    "StateID",
    "BeginDate",
    "Comments",
    "Shape_Length",
    # Exact duplicates kept under canonical names
    "BeginPoint",           # == FROM_MILEPOINT
    "EndPoint",             # == TO_MILEPOINT
    "GDOT_District",        # == DISTRICT
    "AADT_2024",            # == AADT  (metadata fields AADT_2024_* are kept)
    "FUTURE_AADT",          # == FUTURE_AADT_2044
    # Source columns superseded by synchronized aliases
    "F_SYSTEM",             # == FUNCTIONAL_CLASS
    "THROUGH_LANES",        # == NUM_LANES
    "NHS",                  # == NHS_IND
    "URBAN_ID",             # == URBAN_CODE
    # Parsed intermediate fields (copies of final ROUTE_* columns)
    "PARSED_SYSTEM_CODE",
    "PARSED_ROUTE_NUMBER",
    "PARSED_SUFFIX",
    "PARSED_DIRECTION",
    "PARSED_COUNTY_CODE",
    "PARSED_FUNCTION_TYPE",
    # Legacy source fields superseded by enriched columns
    "COUNTY",
]

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
    # GDOT measurement metadata for the state-system AADT record.
    # Statistics_Type values: Actual / Estimated / Calculated.
    # SampleStatus is a free-text descriptor, mostly None.
    "Statistics_Type": "AADT_2024_STATS_TYPE",
    "SampleStatus": "AADT_2024_SAMPLE_STATUS",
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


def _apply_geometry_wins_label(
    gdf: gpd.GeoDataFrame,
    target_col: str,
    decoded: pd.Series,
    crosser_label: str,
) -> gpd.GeoDataFrame:
    """Merge a lookup-decoded label into `target_col` using geometry-wins
    precedence: if the geometry-stamped value is non-null it stays, else
    fall back to the decoded value. Divergences are logged as warnings
    so a rollout run surfaces any systemic mismatch between the
    polygon-level attribute and the code-lookup.
    """
    existing = get_or_empty_series(gdf, target_col)
    disagree = (
        existing.notna()
        & decoded.notna()
        & (existing.astype(str).str.strip() != decoded.astype(str).str.strip())
    )
    disagree_count = int(disagree.sum())
    if disagree_count:
        logger.warning(
            "%d segments have %s (geometry-stamped from %s polygon) diverging "
            "from code-lookup; geometry wins",
            disagree_count,
            target_col,
            crosser_label,
        )
    gdf[target_col] = existing.where(existing.notna(), decoded)
    return gdf


def add_decoded_label_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()

    # Geometry-authoritative precedence: where build_segment_row already
    # stamped COUNTY_NAME / DISTRICT_NAME from the polygon attributes,
    # that value wins over the code-lookup decoding. Divergences are
    # surfaced as warnings.
    decoded_county = get_or_empty_series(gdf, "COUNTY_CODE").map(
        lambda value: decode_lookup_value(value, COUNTY_NAME_LOOKUP, zero_pad=3)
    )
    gdf = _apply_geometry_wins_label(gdf, "COUNTY_NAME", decoded_county, "county")

    decoded_district = get_or_empty_series(gdf, "DISTRICT").map(
        lambda value: decode_lookup_value(value, DISTRICT_SHORT_NAME_LOOKUP)
    )
    gdf = _apply_geometry_wins_label(gdf, "DISTRICT_NAME", decoded_district, "district")

    gdf["SYSTEM_CODE_LABEL"] = get_or_empty_series(gdf, "SYSTEM_CODE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["system_code"])
    )
    gdf["FUNCTION_TYPE_LABEL"] = get_or_empty_series(gdf, "FUNCTION_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["function_type"])
    )
    gdf["FUNCTIONAL_CLASS_LABEL"] = get_or_empty_series(gdf, "FUNCTIONAL_CLASS").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["functional_class"])
    )
    gdf["FACILITY_TYPE_LABEL"] = get_or_empty_series(gdf, "FACILITY_TYPE").map(
        lambda value: decode_lookup_value(value, ROADWAY_DOMAIN_LABELS["facility_type"])
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


UNIQUE_ID_COLLISION_ADMIN_COLS = (
    "DISTRICT",
    "COUNTY_CODE",
    "AREA_OFFICE_ID",
    "MPO_ID",
    "RC_ID",
)


def apply_unique_id_collision_guard(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee unique_id uniqueness after admin-aware re-segmentation.

    ROUTE_ID|FROM|TO should already be unique because milepoint splits
    include every admin-boundary crossing. In the rare case where two
    segments share the same key (admin-line coincident with a traffic
    break could theoretically survive the milepoint set dedup on a
    multi-component route), append a 6-char md5 of the segment's admin
    attribute tuple so downstream tables stay keyable by unique_id.
    No-op for the 99%+ clean case.
    """
    if "unique_id" not in df.columns:
        return df
    duplicated_mask = df["unique_id"].duplicated(keep=False)
    dup_count = int(duplicated_mask.sum())
    if dup_count == 0:
        return df

    logger.warning(
        "unique_id collision guard: %d segments share unique_id; appending admin-tuple hash",
        dup_count,
    )

    def _admin_hash(row: pd.Series) -> str:
        parts: list[str] = []
        for col in UNIQUE_ID_COLLISION_ADMIN_COLS:
            value = row.get(col) if col in row.index else None
            if value is None or (isinstance(value, float) and pd.isna(value)):
                parts.append("")
            else:
                parts.append(str(value))
        return md5("|".join(parts).encode("utf-8")).hexdigest()[:6]

    updated = df.copy()
    dup_subset = updated.loc[duplicated_mask]
    hashes = dup_subset.apply(_admin_hash, axis=1)
    updated.loc[duplicated_mask, "unique_id"] = (
        dup_subset["unique_id"].astype(str) + "_" + hashes
    )

    still_dup = int(updated["unique_id"].duplicated(keep=False).sum())
    if still_dup:
        logger.error(
            "unique_id collision guard: %d segments STILL duplicate after "
            "admin-tuple hash - true collision (identical admin combos). "
            "Downstream unique_id-keyed joins may produce Cartesian rows.",
            still_dup,
        )
    return updated


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


def mirror_inc_breakpoints_to_dec(
    current_lookup: dict[str, list[dict]],
    route_ids: Iterable[str],
) -> dict[str, list[dict]]:
    """For each DEC route with no traffic records, seed geometry-only
    breakpoint entries from its INC sibling so segment_routes can split it.

    GDOT publishes AADT on the INC direction only for divided highways, which
    leaves DEC routes without breakpoints and causes them to emit as a single
    whole-route feature. The seeded records here carry only
    FROM_MILEPOINT/TO_MILEPOINT -- no traffic attributes -- so downstream AADT
    logic stays INC-only until the dedicated direction mirror runs later.

    Returns a new dict; does not mutate ``current_lookup``.
    """
    mirrored = dict(current_lookup)
    added = 0
    for route_id in route_ids:
        if not isinstance(route_id, str) or not route_id.endswith("DEC"):
            continue
        if mirrored.get(route_id):
            continue
        inc_partner = route_id[:-3] + "INC"
        inc_records = mirrored.get(inc_partner)
        if not inc_records:
            continue
        mirrored[route_id] = [
            {
                "ROUTE_ID": route_id,
                "FROM_MILEPOINT": rec["FROM_MILEPOINT"],
                "TO_MILEPOINT": rec["TO_MILEPOINT"],
            }
            for rec in inc_records
        ]
        added += 1
    logger.info("Mirrored INC->DEC breakpoints for %d DEC routes", added)
    return mirrored


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
    routes["FUNCTION_TYPE"] = routes["PARSED_FUNCTION_TYPE"]
    routes["ROUTE_NUMBER"] = routes["PARSED_ROUTE_NUMBER"]
    routes["ROUTE_SUFFIX"] = routes["PARSED_SUFFIX"]
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
    admin_crossings: Iterable[float] = (),
) -> list[float]:
    points = {round_milepoint(route_start), round_milepoint(route_end)}
    for record in current_records:
        interval = clamp_interval(record["FROM_MILEPOINT"], record["TO_MILEPOINT"], route_start, route_end)
        if interval:
            points.update(interval)
    count_before_admin = len(points)
    admin_rounded = 0
    for mp in admin_crossings:
        rounded = round_milepoint(mp)
        if route_start + MILEPOINT_TOLERANCE <= rounded <= route_end - MILEPOINT_TOLERANCE:
            points.add(rounded)
            admin_rounded += 1
    admin_added = len(points) - count_before_admin
    admin_merged = admin_rounded - admin_added
    if admin_merged > 0:
        logger.debug(
            "admin crossings merged with existing breakpoints (near-coincident): %d",
            admin_merged,
        )
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
    crossers: "Iterable[BoundaryCrosser]" = (),
) -> dict:
    row = route_row.drop(labels=["geometry"]).to_dict()
    row["FROM_MILEPOINT"] = segment_start
    row["TO_MILEPOINT"] = segment_end
    row["geometry"] = geometry

    row["AADT"] = np.nan
    row["AADT_2024"] = np.nan
    row["AADT_2024_OFFICIAL"] = np.nan
    row["AADT_2024_HPMS"] = np.nan
    row["AADT_2024_SOURCE"] = "missing"
    row["AADT_2024_SOURCE_AGREEMENT"] = None
    row["AADT_2024_CONFIDENCE"] = None
    row["AADT_2024_FILL_METHOD"] = None
    row["AADT_2024_STATS_TYPE"] = None
    row["AADT_2024_SAMPLE_STATUS"] = None
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
        row["TRAFFIC_CLASS_2024"] = current_record.get("TRAFFIC_CLASS_2024")
        row["AADT_2024_STATS_TYPE"] = _clean_optional_text(
            current_record.get("AADT_2024_STATS_TYPE")
        )
        row["AADT_2024_SAMPLE_STATUS"] = _clean_optional_text(
            current_record.get("AADT_2024_SAMPLE_STATUS")
        )
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

    # Geometry-authoritative admin stamp: runs last so that the polygon
    # containing the segment midpoint overrides ROUTE_ID-parsed or
    # traffic-record county/district values. Only non-null stamps are
    # applied, preserving upstream values when the segment sits outside
    # every polygon of a given crosser (e.g. rural segments outside any
    # MPO). Step 4 handles the non-splitting overlays (legislative, city).
    if geometry is not None and crossers:
        admin_attrs = resolve_segment_admin_attrs(geometry, crossers)
        for out_col, value in admin_attrs.items():
            if value is None:
                continue
            try:
                is_na = pd.isna(value)
            except (TypeError, ValueError):
                is_na = False
            if is_na:
                continue
            row[out_col] = value
            if out_col == "COUNTY_CODE":
                # Keep COUNTY_ID aligned with the geometry-authoritative FIPS so
                # the Int-valued county lookup in downstream enrichment matches
                # the string COUNTY_CODE. Failure to coerce (unexpected non-
                # numeric value from polygon attrs) falls back to leaving
                # COUNTY_ID alone rather than crashing.
                try:
                    row["COUNTY_ID"] = int(str(value))
                except (TypeError, ValueError):
                    pass
            elif out_col == "DISTRICT":
                try:
                    row["GDOT_District"] = int(value)
                except (TypeError, ValueError):
                    pass
    return row


def segment_routes(
    routes: gpd.GeoDataFrame,
    current_lookup: dict[str, list[dict]],
    crossers: "Iterable[BoundaryCrosser]" = (),
) -> gpd.GeoDataFrame:
    output_rows: list[dict] = []
    split_failures = 0
    crossers_tuple = tuple(crossers)
    # Per-crosser miss counter: how many emitted segments had no polygon
    # match (every stamped output column came back None). MPO is expected
    # to have high miss rates in rural GA; county/district/area_office/rc
    # should be near 0% - high values suggest CRS drift or boundary gaps.
    crosser_miss_counts: dict[str, int] = {c.name: 0 for c in crossers_tuple}

    for index, route_row in routes.iterrows():
        if index % 10000 == 0 and index:
            logger.info("Processed %d / %d routes", index, len(routes))

        route_id = route_row["ROUTE_ID"]
        route_start = round_milepoint(route_row["FROM_MILEPOINT"])
        route_end = round_milepoint(route_row["TO_MILEPOINT"])

        current_records = current_lookup.get(route_id, [])

        if route_end - route_start <= MILEPOINT_TOLERANCE:
            current_record = find_covering_record(current_records, route_start, route_end)
            row = build_segment_row(
                route_row,
                current_record,
                route_start,
                route_end,
                force_2d(route_row.geometry),
                crossers=crossers_tuple,
            )
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
                crossers=crossers_tuple,
            )
            output_rows.append(row)
            continue

        for component in prepared_components:
            component_geometry = component["geometry"]
            component_start = float(component["route_start"])
            component_end = float(component["route_end"])

            admin_crossings = compute_route_crossings(
                component_geometry,
                component_start,
                component_end,
                crossers_tuple,
            ) if crossers_tuple else []

            component_breakpoints = get_breakpoints(
                component_start,
                component_end,
                current_records,
                admin_crossings=admin_crossings,
            )

            for segment_start, segment_end in zip(
                component_breakpoints, component_breakpoints[1:]
            ):
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
                    crossers=crossers_tuple,
                )
                output_rows.append(row)

    # Tally per-crosser misses on emitted segments for observability.
    if crossers_tuple:
        for row in output_rows:
            for crosser in crossers_tuple:
                if all(row.get(out_col) is None for out_col in crosser.attribute_cols.values()):
                    crosser_miss_counts[crosser.name] += 1

    total_emitted = len(output_rows)
    logger.info("Route segmentation complete with %d split failures", split_failures)
    if crossers_tuple and total_emitted:
        for crosser in crossers_tuple:
            miss = crosser_miss_counts[crosser.name]
            pct = (miss / total_emitted) * 100.0
            logger.info(
                "crosser=%s: %d/%d segments outside all polygons (%.2f%%)",
                crosser.name,
                miss,
                total_emitted,
                pct,
            )
            # Rural segments outside any MPO are expected - don't warn.
            if crosser.name != "mpo" and pct > 1.0:
                logger.warning(
                    "crosser=%s miss rate %.2f%% exceeds 1%% threshold - "
                    "investigate CRS drift or polygon coverage gaps",
                    crosser.name,
                    pct,
                )
    return gpd.GeoDataFrame(output_rows, geometry="geometry", crs=routes.crs)


def compute_segment_length(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.geometry is not None and not gdf.geometry.is_empty.all():
        gdf["segment_length_m"] = gdf.geometry.length
        gdf["segment_length_mi"] = gdf["segment_length_m"] / 1609.344
    else:
        gdf["segment_length_m"] = np.nan
        gdf["segment_length_mi"] = np.nan
    return gdf


def derive_alignment_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Derive RAPTOR alignment columns: PCT_SADT, PCT_CADT, HWY_DES."""
    result = gdf.copy()

    # PCT_SADT — single-unit truck % of AADT
    has_single = result["SINGLE_UNIT_AADT_2024"].notna() & result["AADT"].notna() & (result["AADT"] > 0)
    result["PCT_SADT"] = np.where(
        has_single,
        (result["SINGLE_UNIT_AADT_2024"] / result["AADT"] * 100.0).round(1),
        np.nan,
    )

    # PCT_CADT — combination-unit truck % of AADT
    has_combo = result["COMBO_UNIT_AADT_2024"].notna() & result["AADT"].notna() & (result["AADT"] > 0)
    result["PCT_CADT"] = np.where(
        has_combo,
        (result["COMBO_UNIT_AADT_2024"] / result["AADT"] * 100.0).round(1),
        np.nan,
    )

    # HWY_DES — highway design class (e.g. "4D", "2U")
    # .get() with a default keeps these as index-aligned Series even when
    # the underlying column is absent (e.g. HPMS_ACCESS_CONTROL is only
    # populated when HPMS data was loaded; without it, pd.to_numeric(None)
    # would collapse to a scalar NaN and the .isin/.notna calls below
    # would raise AttributeError).
    empty_series = pd.Series(index=result.index, dtype="object")
    lanes = pd.to_numeric(result.get("NUM_LANES", empty_series), errors="coerce")
    median = pd.to_numeric(result.get("MEDIAN_TYPE", empty_series), errors="coerce")
    access = pd.to_numeric(result.get("HPMS_ACCESS_CONTROL", empty_series), errors="coerce")
    family = result.get("ROUTE_FAMILY", empty_series).astype("string").str.strip()

    valid_lanes = lanes.notna() & (lanes > 0)
    divided = (
        ((median.notna()) & (median != 1))
        | (family == "Interstate")
        | ((access.isin([1, 2, 3])) & (lanes >= 4))
    )
    suffix = np.where(divided, "D", "U")
    result["HWY_DES"] = np.where(
        valid_lanes,
        np.floor(lanes).astype("Int64").astype(str) + suffix,
        None,
    )

    # TRK_DHV_PCT — truck design-hour volume percent
    # Primary: HPMS pct_dh_single + pct_dh_combination (direct design-hour data)
    # Fallback: TRUCK_PCT (approximation)
    # Same defensive .get-with-default pattern as the HWY_DES block above -
    # missing HPMS columns would otherwise collapse to scalar NaN.
    hpms_single = pd.to_numeric(result.get("HPMS_PCT_DH_SINGLE", empty_series), errors="coerce")
    hpms_combo = pd.to_numeric(result.get("HPMS_PCT_DH_COMBINATION", empty_series), errors="coerce")
    hpms_dhv = hpms_single.fillna(0) + hpms_combo.fillna(0)
    has_hpms_dhv = hpms_single.notna() | hpms_combo.notna()
    truck_pct = pd.to_numeric(result.get("TRUCK_PCT", empty_series), errors="coerce")
    result["TRK_DHV_PCT"] = np.where(
        has_hpms_dhv,
        hpms_dhv.round(1),
        np.where(truck_pct.notna(), truck_pct.round(1), np.nan),
    )

    pct_sadt_coverage = result["PCT_SADT"].notna().sum()
    pct_cadt_coverage = result["PCT_CADT"].notna().sum()
    trk_dhv_pct_coverage = result["TRK_DHV_PCT"].notna().sum()
    hwy_des_coverage = result["HWY_DES"].notna().sum()
    total = len(result)
    logger.info(
        "Texas alignment columns: PCT_SADT %d/%d (%.1f%%), PCT_CADT %d/%d (%.1f%%), TRK_DHV_PCT %d/%d (%.1f%%), HWY_DES %d/%d (%.1f%%)",
        pct_sadt_coverage, total, pct_sadt_coverage / total * 100 if total else 0,
        pct_cadt_coverage, total, pct_cadt_coverage / total * 100 if total else 0,
        trk_dhv_pct_coverage, total, trk_dhv_pct_coverage / total * 100 if total else 0,
        hwy_des_coverage, total, hwy_des_coverage / total * 100 if total else 0,
    )
    return result


def _overlay_winner_by_length(
    segments: gpd.GeoDataFrame,
    layer: gpd.GeoDataFrame,
    id_col: str,
    name_col: str | None = None,
    layer_label: str = "",
) -> pd.DataFrame | None:
    """Intersect segments with `layer`, pick the winning polygon per
    unique_id by length, tie-break by smaller id.

    Returns a DataFrame keyed by unique_id with the winning id (and
    optional name) plus two shares:
      _share: winner_length / total-intersection-length on that segment
              (used to flag the review cohort of borderline assignments)
      _length: absolute length of the winning intersection

    Callers that need within-segment coverage (the city 50% rule) merge
    in their own segment lengths and divide.
    """
    if layer is None or layer.empty:
        logger.warning("overlay: empty layer for %s; skipping", layer_label or id_col)
        return None
    if id_col not in layer.columns:
        logger.warning(
            "overlay: layer %s missing id column %s; skipping",
            layer_label or id_col,
            id_col,
        )
        return None

    cols_to_keep = [id_col]
    if name_col and name_col in layer.columns:
        cols_to_keep.append(name_col)
    layer_subset = layer[cols_to_keep + ["geometry"]].copy()

    seg_subset = segments[["unique_id", "geometry"]].copy()
    seg_subset = seg_subset[
        seg_subset.geometry.notna() & ~seg_subset.geometry.is_empty
    ].copy()
    if seg_subset.empty:
        return None

    if seg_subset.crs is not None and layer_subset.crs is not None and str(seg_subset.crs) != str(layer_subset.crs):
        layer_subset = layer_subset.to_crs(seg_subset.crs)

    # keep_geom_type=False because we're intersecting LineStrings with
    # Polygons and want the resulting LineString pieces back.
    overlay = gpd.overlay(
        seg_subset,
        layer_subset,
        how="intersection",
        keep_geom_type=False,
    )
    if overlay.empty:
        return None
    overlay["_length"] = overlay.geometry.length
    overlay = overlay[overlay["_length"] > 0].copy()
    if overlay.empty:
        return None

    total_inter = (
        overlay.groupby("unique_id")["_length"]
        .sum()
        .rename("_total_intersected_length")
    )
    overlay = overlay.merge(total_inter, on="unique_id")
    overlay["_share"] = overlay["_length"] / overlay["_total_intersected_length"]

    overlay_sorted = overlay.sort_values(
        by=["unique_id", "_length", id_col],
        ascending=[True, False, True],
    )
    winners = overlay_sorted.drop_duplicates("unique_id", keep="first").copy()
    result_cols = ["unique_id", id_col, "_length", "_share"]
    if name_col and name_col in winners.columns:
        result_cols.append(name_col)
    return winners[result_cols]


def apply_admin_overlay_flags(
    gdf: gpd.GeoDataFrame,
    house_boundaries: gpd.GeoDataFrame,
    senate_boundaries: gpd.GeoDataFrame,
    congressional_boundaries: gpd.GeoDataFrame,
    city_boundaries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Post-pass: stamp non-splitting overlay flags on already-split segments.

    Legislative layers (House, Senate, Congressional) cover all of
    Georgia - every segment gets a non-null district by majority-by-
    length overlay, smaller id wins ties.

    City layer is partial coverage. A segment's city assignment fires
    only when the winning city covers >= 50% of the segment length.
    Segments outside all incorporated areas, or whose largest city
    overlap is under 50%, stay null (unincorporated). No
    "Unincorporated {county}" string substitution.

    Logs a single aggregate review-cohort summary at the end.
    """
    result = gdf.copy()
    for col in (
        "STATE_HOUSE_DISTRICT",
        "STATE_SENATE_DISTRICT",
        "CONGRESSIONAL_DISTRICT",
        "CITY_ID",
    ):
        if col not in result.columns:
            result[col] = pd.NA
    if "CITY_NAME" not in result.columns:
        result["CITY_NAME"] = None

    if "unique_id" not in result.columns:
        raise ValueError("apply_admin_overlay_flags requires a 'unique_id' column")

    total_segments = len(result)
    seg_lengths = pd.DataFrame(
        {
            "unique_id": result["unique_id"],
            "_seg_length": result.geometry.length,
        }
    )
    by_layer_borderline: dict[str, int] = {}
    # Track distinct segments that hit the borderline threshold on ANY layer.
    borderline_segment_ids: set = set()

    # --- Legislative overlays (full-coverage; every segment gets a winner) ---
    legislative = [
        ("state_house", house_boundaries, "STATE_HOUSE_DISTRICT"),
        ("state_senate", senate_boundaries, "STATE_SENATE_DISTRICT"),
        ("congressional", congressional_boundaries, "CONGRESSIONAL_DISTRICT"),
    ]
    for layer_name, layer, target_col in legislative:
        winners = _overlay_winner_by_length(
            segments=result,
            layer=layer,
            id_col=target_col,
            layer_label=layer_name,
        )
        if winners is None or winners.empty:
            logger.warning(
                "apply_admin_overlay_flags: no overlay result for %s; leaving NULL",
                layer_name,
            )
            by_layer_borderline[layer_name] = 0
            continue
        result = result.drop(columns=[target_col], errors="ignore").merge(
            winners[["unique_id", target_col]], on="unique_id", how="left"
        )
        borderline_mask = winners["_share"] <= 0.51
        borderline_count = int(borderline_mask.sum())
        by_layer_borderline[layer_name] = borderline_count
        borderline_segment_ids.update(
            winners.loc[borderline_mask, "unique_id"].tolist()
        )
        logger.info(
            "overlay flag %s: %d segments stamped, %d borderline (winning share <= 51%%)",
            target_col,
            int(winners["unique_id"].nunique()),
            borderline_count,
        )
        # Legislative layers cover all of Georgia - every segment should
        # land inside exactly one district. Non-zero NULL counts signal
        # geometry artifacts upstream (zero-length fragments, invalid
        # rings, CRS mismatch) and need follow-up before the rollout is
        # considered clean.
        null_count = int(result[target_col].isna().sum())
        if null_count > 0:
            logger.warning(
                "apply_admin_overlay_flags: %d/%d segments have NULL %s after "
                "overlay - legislative coverage is statewide, so NULLs indicate "
                "geometry artifacts upstream",
                null_count,
                total_segments,
                target_col,
            )

    # --- City overlay (partial coverage; 50%-of-segment threshold) ---
    if city_boundaries is not None and not city_boundaries.empty:
        if "CITY_ID" not in city_boundaries.columns:
            city_boundaries = _assign_city_id(city_boundaries)
        city_winners = _overlay_winner_by_length(
            segments=result,
            layer=city_boundaries,
            id_col="CITY_ID",
            name_col="Name",
            layer_label="city",
        )
    else:
        city_winners = None

    if city_winners is not None and not city_winners.empty:
        city_winners = city_winners.merge(seg_lengths, on="unique_id", how="left")
        city_winners["_within_share"] = (
            city_winners["_length"] / city_winners["_seg_length"]
        )
        in_city_mask = city_winners["_within_share"] >= 0.5
        keep = city_winners.loc[
            in_city_mask, ["unique_id", "CITY_ID", "Name"]
        ].rename(columns={"Name": "CITY_NAME"})
        result = result.drop(
            columns=["CITY_ID", "CITY_NAME"], errors="ignore"
        ).merge(keep, on="unique_id", how="left")

        # Review cohort for city: use the share vs. the segment (not vs.
        # total intersected). A segment ~ 50/50 between Atlanta and
        # Sandy Springs both share 0.5 of the segment and would both
        # register; flag the winner.
        city_borderline_mask = city_winners["_within_share"] <= 0.51
        city_borderline_count = int(city_borderline_mask.sum())
        by_layer_borderline["city"] = city_borderline_count
        borderline_segment_ids.update(
            city_winners.loc[city_borderline_mask, "unique_id"].tolist()
        )
        logger.info(
            "overlay flag CITY_ID: %d segments stamped (>= 50%% of segment in one city), "
            "%d unincorporated (< 50%% or no city intersection), %d borderline",
            int(in_city_mask.sum()),
            total_segments - int(in_city_mask.sum()),
            city_borderline_count,
        )
    else:
        logger.warning(
            "apply_admin_overlay_flags: no city overlay winners; all segments unincorporated"
        )
        by_layer_borderline["city"] = 0

    # --- Aggregate review-cohort summary (single log) ---
    total_borderline_observations = sum(by_layer_borderline.values())
    max_possible = total_segments * 4 if total_segments else 0
    distinct_borderline_segments = len(borderline_segment_ids)
    logger.info(
        "apply_admin_overlay_flags review cohort: %d distinct segments "
        "(%.2f%% of %d) have a borderline (<= 51%%) winner on at least "
        "one of house/senate/congressional/city; total borderline "
        "observations across layers = %d / %d (%.2f%%)",
        distinct_borderline_segments,
        (distinct_borderline_segments / total_segments * 100.0) if total_segments else 0.0,
        total_segments,
        total_borderline_observations,
        max_possible,
        (total_borderline_observations / max_possible * 100.0) if max_possible else 0.0,
    )
    for layer_name in ("state_house", "state_senate", "congressional", "city"):
        logger.info(
            "  %s: %d borderline winners",
            layer_name,
            by_layer_borderline.get(layer_name, 0),
        )

    return result


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

        if best_idx is None or best_overlap < MILEPOINT_TOLERANCE:
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
    """Apply direction-mirror fill to FUTURE_AADT_2044.

    Fill order:
    1. GDOT official (already set in build_segment_row)
    2. HPMS future_aadt (via hpms_enrichment, if wired)
    3. Direction mirror (INC→DEC)
    4. Official implied growth-rate projection (in apply_future_aadt_official_growth_projection)
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
        if best_idx is None or best_overlap < MILEPOINT_TOLERANCE:
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

    # Sync FUTURE_AADT alias
    filled["FUTURE_AADT"] = filled["FUTURE_AADT_2044"]
    filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()

    return filled


def apply_future_aadt_official_growth_projection(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Project missing FUTURE_AADT_2044 values from official implied growth rates."""

    filled = gdf.copy()

    for col, default in {
        "FUTURE_AADT_2044": np.nan,
        "FUTURE_AADT_2044_SOURCE": "missing",
        "FUTURE_AADT_2044_CONFIDENCE": None,
        "FUTURE_AADT_2044_FILL_METHOD": None,
        "future_aadt_covered": False,
    }.items():
        if col not in filled.columns:
            filled[col] = default

    filled["FUTURE_AADT_2044"] = pd.to_numeric(filled["FUTURE_AADT_2044"], errors="coerce")
    filled["AADT_2024"] = pd.to_numeric(filled["AADT_2024"], errors="coerce")
    filled["FROM_MILEPOINT"] = pd.to_numeric(filled["FROM_MILEPOINT"], errors="coerce")
    filled["TO_MILEPOINT"] = pd.to_numeric(filled["TO_MILEPOINT"], errors="coerce")
    filled["COUNTY_ID"] = pd.to_numeric(
        filled.get("COUNTY_ID", pd.Series(index=filled.index, dtype="float64")),
        errors="coerce",
    )
    filled["DISTRICT"] = pd.to_numeric(
        filled.get("DISTRICT", pd.Series(index=filled.index, dtype="float64")),
        errors="coerce",
    )
    filled["SYSTEM_CODE"] = filled.get(
        "SYSTEM_CODE",
        pd.Series(index=filled.index, dtype="object"),
    ).astype("string")

    missing_future_mask = filled["FUTURE_AADT_2044"].isna() & filled["AADT_2024"].notna()
    if not missing_future_mask.any():
        logger.info("Future AADT official growth projection: no eligible FUTURE_AADT_2044 gaps remain")
        filled["FUTURE_AADT"] = filled["FUTURE_AADT_2044"]
        filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()
        return filled

    projection_horizon_years = 20
    official_source_mask = filled["FUTURE_AADT_2044_SOURCE"].isin(["official_exact", "hpms_2024"])
    official_pairs = filled[
        official_source_mask
        & filled["FUTURE_AADT_2044"].notna()
        & filled["AADT_2024"].notna()
        & (filled["AADT_2024"] > 0)
        & (filled["FUTURE_AADT_2044"] > 0)
    ].copy()
    official_pairs["implied_cagr"] = (
        (official_pairs["FUTURE_AADT_2044"] / official_pairs["AADT_2024"]) ** (1.0 / projection_horizon_years)
        - 1.0
    )
    official_pairs["implied_cagr"] = official_pairs["implied_cagr"].clip(-0.03, 0.05)
    official_pairs["SYSTEM_CODE"] = official_pairs["SYSTEM_CODE"].astype("string")

    if official_pairs.empty:
        logger.warning("Future AADT official growth projection: no official FUTURE_AADT_2044/AADT_2024 pairs available")
        filled["FUTURE_AADT"] = filled["FUTURE_AADT_2044"]
        filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()
        return filled

    official_county_system_rates = (
        official_pairs.dropna(subset=["COUNTY_ID", "SYSTEM_CODE"])
        .groupby(["COUNTY_ID", "SYSTEM_CODE"], dropna=False)["implied_cagr"]
        .median()
    )
    official_district_system_rates = (
        official_pairs.dropna(subset=["DISTRICT", "SYSTEM_CODE"])
        .groupby(["DISTRICT", "SYSTEM_CODE"], dropna=False)["implied_cagr"]
        .median()
    )
    official_system_rates = (
        official_pairs.dropna(subset=["SYSTEM_CODE"])
        .groupby("SYSTEM_CODE", dropna=False)["implied_cagr"]
        .median()
    )
    official_statewide_rate = float(official_pairs["implied_cagr"].median())

    logger.info(
        "Future AADT official implied rates: %d pairs, statewide median=%.4f (%.2f%%)",
        len(official_pairs),
        official_statewide_rate,
        official_statewide_rate * 100,
    )

    fill_counts = {
        "official_implied_county_system": 0,
        "official_implied_district_system": 0,
        "official_implied_system_statewide": 0,
        "official_implied_statewide": 0,
    }
    used_growth_rates: list[float] = []

    for idx in filled.index[missing_future_mask]:
        aadt_2024 = filled.at[idx, "AADT_2024"]
        if pd.isna(aadt_2024) or float(aadt_2024) <= 0:
            continue

        growth_rate = np.nan
        method = None

        county_id = filled.at[idx, "COUNTY_ID"]
        district = filled.at[idx, "DISTRICT"]
        system_code = filled.at[idx, "SYSTEM_CODE"]

        county_key = (county_id, system_code)
        district_key = (district, system_code)
        if pd.notna(county_id) and pd.notna(system_code) and county_key in official_county_system_rates.index:
            growth_rate = float(official_county_system_rates.loc[county_key])
            method = "official_implied_county_system"
        elif pd.notna(district) and pd.notna(system_code) and district_key in official_district_system_rates.index:
            growth_rate = float(official_district_system_rates.loc[district_key])
            method = "official_implied_district_system"
        elif pd.notna(system_code) and system_code in official_system_rates.index:
            growth_rate = float(official_system_rates.loc[system_code])
            method = "official_implied_system_statewide"
        else:
            growth_rate = official_statewide_rate
            method = "official_implied_statewide"


        if pd.isna(growth_rate):
            continue

        projected_aadt = int(round(float(aadt_2024) * ((1.0 + float(growth_rate)) ** projection_horizon_years)))
        filled.loc[idx, "FUTURE_AADT_2044"] = projected_aadt
        filled.loc[idx, "FUTURE_AADT_2044_SOURCE"] = "projection_official_implied"
        filled.loc[idx, "FUTURE_AADT_2044_CONFIDENCE"] = "low"
        filled.loc[idx, "FUTURE_AADT_2044_FILL_METHOD"] = method
        filled.loc[idx, "future_aadt_covered"] = True
        fill_counts[method] += 1
        used_growth_rates.append(float(growth_rate))

    if used_growth_rates:
        logger.info(
            "Future AADT official growth projection: filled %d segments (county+system=%d, district+system=%d, system=%d, statewide=%d); median growth=%.4f, mean growth=%.4f",
            len(used_growth_rates),
            fill_counts["official_implied_county_system"],
            fill_counts["official_implied_district_system"],
            fill_counts["official_implied_system_statewide"],
            fill_counts["official_implied_statewide"],
            float(pd.Series(used_growth_rates).median()),
            float(pd.Series(used_growth_rates).mean()),
        )
    else:
        logger.info("Future AADT official growth projection: no FUTURE_AADT_2044 gaps were filled")

    filled["FUTURE_AADT"] = filled["FUTURE_AADT_2044"]
    filled["future_aadt_covered"] = filled["FUTURE_AADT_2044"].notna()

    return filled


# Cross-validation thresholds between the two GDOT-published AADT sources.
# A pair is considered "agreeing" if the absolute difference is within
# AADT_2024_AGREEMENT_ABS_TOL veh/day OR the relative difference (vs. the
# state value) is within AADT_2024_AGREEMENT_REL_TOL.
AADT_2024_AGREEMENT_REL_TOL = 0.15
AADT_2024_AGREEMENT_ABS_TOL = 200


def compute_aadt_2024_source_agreement(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Derive `AADT_2024_SOURCE_AGREEMENT` from the two GDOT-published sources.

    Operates on the audit columns `AADT_2024_OFFICIAL` (state-system 2024 GDB)
    and `AADT_2024_HPMS` (federal HPMS submission). Both are inputs published
    by GDOT; this function does not change which source ultimately wins the
    canonical `AADT` / `AADT_2024` field.

    Values:
      - state_only      — state has a value, HPMS does not
      - hpms_only       — HPMS has a value, state does not
      - both_agree      — both populated and within rel/abs tolerance
      - both_disagree   — both populated but outside the tolerance band
      - missing         — neither source has a value
    """

    out = gdf.copy()

    if "AADT_2024_OFFICIAL" not in out.columns:
        out["AADT_2024_OFFICIAL"] = np.nan
    if "AADT_2024_HPMS" not in out.columns:
        out["AADT_2024_HPMS"] = np.nan

    state = pd.to_numeric(out["AADT_2024_OFFICIAL"], errors="coerce")
    hpms = pd.to_numeric(out["AADT_2024_HPMS"], errors="coerce")

    has_state = state.notna()
    has_hpms = hpms.notna()
    both = has_state & has_hpms

    diff = (hpms - state).abs()
    # Avoid divide-by-zero on the relative band when state == 0; treat any
    # non-zero hpms in that case as a disagreement (handled by abs band).
    rel_diff = diff / state.where(state.abs() > 0)
    within_band = (diff <= AADT_2024_AGREEMENT_ABS_TOL) | (
        rel_diff.abs() <= AADT_2024_AGREEMENT_REL_TOL
    )

    agreement = pd.Series("missing", index=out.index, dtype="object")
    agreement[has_state & ~has_hpms] = "state_only"
    agreement[has_hpms & ~has_state] = "hpms_only"
    agreement[both & within_band.fillna(False)] = "both_agree"
    agreement[both & ~within_band.fillna(False)] = "both_disagree"

    out["AADT_2024_SOURCE_AGREEMENT"] = agreement

    counts = agreement.value_counts(dropna=False).to_dict()
    logger.info(
        "AADT 2024 source agreement: state_only=%d, hpms_only=%d, both_agree=%d, both_disagree=%d, missing=%d",
        int(counts.get("state_only", 0)),
        int(counts.get("hpms_only", 0)),
        int(counts.get("both_agree", 0)),
        int(counts.get("both_disagree", 0)),
        int(counts.get("missing", 0)),
    )
    return out


# AADT_2024_SOURCE values that originate from a GDOT-published source.
# Everything else (direction_mirror, analytical_gap_fill, nearest_neighbor) is
# pipeline-derived and treated as low confidence under the hygiene rules.
_AADT_2024_DERIVED_SOURCES = {
    "direction_mirror",
    "analytical_gap_fill",
    "nearest_neighbor",
}


def recompute_aadt_2024_confidence(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Recompute `AADT_2024_CONFIDENCE` using the hygiene-pass tier rules.

    Tiers (evaluated top-down):
      - high    — `AADT_2024_STATS_TYPE == 'Actual'` OR
                  `AADT_2024_SOURCE_AGREEMENT == 'both_agree'`
      - medium  — `AADT_2024_STATS_TYPE` in {'Estimated', 'Calculated'} OR
                  single-source GDOT-official (state_only / hpms_only)
      - low     — pipeline-derived source (direction_mirror /
                  analytical_gap_fill / nearest_neighbor) OR
                  `AADT_2024_SOURCE_AGREEMENT == 'both_disagree'`
      - missing — `AADT_2024_SOURCE == 'missing'` (no value)

    This intentionally downgrades `direction_mirror` (previously labeled
    `high`) and `analytical_gap_fill` (previously labeled `medium`) to `low`
    so that the confidence tier reflects whether the value was published by
    GDOT or derived by this pipeline.
    """

    out = gdf.copy()

    for col in ("AADT_2024_SOURCE", "AADT_2024_SOURCE_AGREEMENT", "AADT_2024_STATS_TYPE"):
        if col not in out.columns:
            out[col] = None

    source = out["AADT_2024_SOURCE"].astype("string")
    agreement = out["AADT_2024_SOURCE_AGREEMENT"].astype("string")
    stats_type = out["AADT_2024_STATS_TYPE"].astype("string").str.strip()

    confidence = pd.Series(pd.NA, index=out.index, dtype="object")

    # Precedence (top wins on ties):
    #   1. missing  — no AADT value at all
    #   2. low      — pipeline-derived OR cross-source disagreement
    #                 (a bad-signal override that beats positive stats_type/agreement)
    #   3. high     — Actual state measurement OR both sources agree
    #                 (positive signal; beats medium when both criteria match)
    #   4. medium   — Estimated/Calculated state measurement, OR single-source official

    confidence[source == "missing"] = "missing"

    derived_mask = source.isin(_AADT_2024_DERIVED_SOURCES)
    disagree_mask = agreement == "both_disagree"
    confidence[(derived_mask | disagree_mask) & confidence.isna()] = "low"

    high_mask = (stats_type == "Actual") | (agreement == "both_agree")
    confidence[high_mask & confidence.isna()] = "high"

    medium_mask = stats_type.isin(["Estimated", "Calculated"]) | agreement.isin(
        ["state_only", "hpms_only"]
    )
    confidence[medium_mask & confidence.isna()] = "medium"

    # Every row should have matched one of the four explicit rules above; if
    # any remain null after a future rule change, log and default to medium so
    # the regression is visible instead of silently reviving pre-hygiene
    # AADT_2024_CONFIDENCE values.
    unclassified = confidence.isna().sum()
    if unclassified:
        logger.warning(
            "AADT 2024 confidence: %d rows did not match any tier rule; "
            "defaulting them to 'medium'",
            int(unclassified),
        )
        confidence = confidence.where(confidence.notna(), "medium")

    out["AADT_2024_CONFIDENCE"] = confidence

    counts = confidence.value_counts(dropna=False).to_dict()
    logger.info(
        "AADT 2024 confidence tiers: high=%d, medium=%d, low=%d, missing=%d",
        int(counts.get("high", 0)),
        int(counts.get("medium", 0)),
        int(counts.get("low", 0)),
        int(counts.get("missing", 0)),
    )
    return out


def write_match_summary(gdf: gpd.GeoDataFrame) -> None:
    summary = {
        "segment_count": int(len(gdf)),
        "unique_route_ids": int(gdf["ROUTE_ID"].nunique()),
        "current_aadt_official_segments": int(gdf["AADT_2024_OFFICIAL"].notna().sum())
        if "AADT_2024_OFFICIAL" in gdf.columns
        else int(gdf["AADT"].notna().sum()),
        "current_aadt_segments": int(gdf["AADT"].notna().sum()),
        "current_aadt_miles": float(
            gdf.loc[gdf["AADT"].notna(), "segment_length_mi"].sum()
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
            ["FUNCTION_TYPE", "FUNCTION_TYPE_LABEL"],
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
        "FUNCTION_TYPE",
        "FUNCTION_TYPE_LABEL",
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
    audit_tmp_dir = CURRENT_AADT_AUDIT_DIR
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
    county_boundaries: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame | None:
    """Load county boundaries for spatial attribute backfill.

    When county_boundaries is passed directly (e.g. from the checkpoint
    runner), use it as-is.  Otherwise prefer existing staged boundaries,
    then try a live fetch as a last resort.
    """
    if county_boundaries is not None and not county_boundaries.empty:
        logger.info("Using pre-loaded county boundaries for attribute backfill")
        return county_boundaries

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


def _repair_boundary_geometry(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    invalid_count = int((~gdf.geometry.is_valid).sum())
    if invalid_count:
        logger.info("Repairing %d invalid %s geometries", invalid_count, label)
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.map(
        lambda geometry: (
            force_2d(make_valid(geometry))
            if geometry is not None and not geometry.is_empty and not geometry.is_valid
            else (force_2d(geometry) if geometry is not None and not geometry.is_empty else geometry)
        )
    )
    return gdf


def _read_tiger_zip(url: str) -> gpd.GeoDataFrame:
    """Download a zipped TIGER/Line shapefile and read it as a GeoDataFrame."""
    logger.info("Downloading TIGER zip from %s", url)
    with urlopen(url) as response:
        payload = response.read()
    with tempfile.TemporaryDirectory(prefix="tiger_") as tmpdir:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            archive.extractall(tmpdir)
        shp_matches = sorted(Path(tmpdir).rglob("*.shp"))
        if not shp_matches:
            raise FileNotFoundError(f"No .shp in TIGER archive at {url}")
        return gpd.read_file(shp_matches[0], engine="pyogrio")


def _read_tiger_zip_from_file(path: Path) -> gpd.GeoDataFrame:
    """Read a locally-cached zipped TIGER/Line shapefile.

    Mirrors _read_tiger_zip but skips the download. Used when the
    boundary downloader has pre-staged the zip under
    01-Raw-Data/Boundaries/cache/.
    """
    logger.info("Loading TIGER zip from local cache %s", path)
    with tempfile.TemporaryDirectory(prefix="tiger_") as tmpdir:
        with zipfile.ZipFile(path) as archive:
            archive.extractall(tmpdir)
        shp_matches = sorted(Path(tmpdir).rglob("*.shp"))
        if not shp_matches:
            raise FileNotFoundError(f"No .shp in TIGER archive at {path}")
        return gpd.read_file(shp_matches[0], engine="pyogrio")


def _resolve_boundary_local_cache(cache_filename: str | None) -> Path | None:
    """Return the cached Path for a boundary layer if it exists locally.

    Used as the short-circuit path in fetch_and_cache_boundary so
    normalize.py can run offline whenever the downloader has been run.
    """
    if not cache_filename:
        return None
    candidate = RAW_BOUNDARIES_CACHE_DIR / cache_filename
    return candidate if candidate.exists() else None


@dataclass(frozen=True)
class AreaOfficeCodes:
    """Parsed + validated area_office_codes.json.

    counties: FIPS -> {district, area_id} for whole-county assignments.
    subcounty_splits: FIPS -> list of split descriptors for counties whose
        PDF assignment crosses multiple Area Offices (e.g. Fulton).
    area_labels: (district, area_id) -> display label.
    """

    counties: dict[str, dict]
    subcounty_splits: dict[str, list[dict]]
    area_labels: dict[tuple[int, int], str]


def _load_area_office_codes(codes_path: Path) -> AreaOfficeCodes:
    """Load + validate area_office_codes.json.

    Asserts:
    - every GA county FIPS (from county_codes.json) appears exactly once in
      either `counties` or `subcounty_splits` (no overlaps, no gaps),
    - total distinct (district, area_id) pairs is 38,
    - each district in 1..7, each area_id >= 1,
    - each subcounty_splits entry has at most one 'remainder' derivation,
    - derivation='municipalities' entries list non-empty municipality_names.
    """
    payload = load_json_mapping(codes_path)
    counties_raw = payload.get("counties", {})
    subcounty_splits_raw = payload.get("subcounty_splits", {}) or {}
    overrides = payload.get("area_label_overrides", {}) or {}

    if not isinstance(counties_raw, dict):
        raise ValueError(f"{codes_path}: 'counties' must be a mapping")
    if not isinstance(subcounty_splits_raw, dict):
        raise ValueError(f"{codes_path}: 'subcounty_splits' must be a mapping")

    expected_fips = set(COUNTY_NAME_LOOKUP.keys())
    whole_fips = set(counties_raw.keys())
    split_fips = set(subcounty_splits_raw.keys())
    overlap = whole_fips & split_fips
    if overlap:
        raise AssertionError(
            f"FIPS codes appear in both counties and subcounty_splits: {sorted(overlap)}"
        )
    covered_fips = whole_fips | split_fips
    missing = sorted(expected_fips - covered_fips)
    extra = sorted(covered_fips - expected_fips)
    if missing or extra:
        raise AssertionError(
            f"area_office_codes.json must cover every GA county FIPS exactly "
            f"once; missing={missing}, extra={extra}"
        )

    pairs: set[tuple[int, int]] = set()
    for fips, meta in counties_raw.items():
        district = int(meta["district"])
        area_id = int(meta["area_id"])
        if not (1 <= district <= 7):
            raise AssertionError(f"{fips}: district {district} outside 1..7")
        if area_id < 1:
            raise AssertionError(f"{fips}: area_id {area_id} < 1")
        pairs.add((district, area_id))

    for fips, splits in subcounty_splits_raw.items():
        if not isinstance(splits, list) or not splits:
            raise AssertionError(
                f"{fips}: subcounty_splits entry must be a non-empty list"
            )
        remainders = 0
        for split in splits:
            district = int(split["district"])
            area_id = int(split["area_id"])
            derivation = split.get("derivation")
            if derivation not in {"municipalities", "remainder"}:
                raise AssertionError(
                    f"{fips} (D{district} A{area_id}): derivation must be "
                    f"'municipalities' or 'remainder', got {derivation!r}"
                )
            if derivation == "municipalities":
                names = split.get("municipality_names") or []
                if not names:
                    raise AssertionError(
                        f"{fips} (D{district} A{area_id}): derivation=municipalities "
                        "requires non-empty municipality_names"
                    )
            else:
                remainders += 1
            if not (1 <= district <= 7):
                raise AssertionError(f"{fips}: district {district} outside 1..7")
            if area_id < 1:
                raise AssertionError(f"{fips}: area_id {area_id} < 1")
            pairs.add((district, area_id))
        if remainders > 1:
            raise AssertionError(
                f"{fips}: subcounty_splits must have at most one 'remainder' "
                f"entry; got {remainders}"
            )

    if len(pairs) != 38:
        raise AssertionError(
            f"Expected 38 distinct (district, area_id) pairs, got {len(pairs)}"
        )

    # Default labels: "District {d} Area {a} (comma-separated county list
    # from the PDF)". For (d, a) pairs whose PDF grouping spans a county
    # split (e.g. D7 A2/A3 which include Fulton sub-areas), the static
    # area_label_overrides map provides a curated label.
    pair_county_names: dict[tuple[int, int], list[str]] = {}
    for fips, meta in counties_raw.items():
        key = (int(meta["district"]), int(meta["area_id"]))
        name = COUNTY_NAME_LOOKUP.get(fips, fips)
        pair_county_names.setdefault(key, []).append(name)

    area_labels: dict[tuple[int, int], str] = {}
    for pair in pairs:
        d, a = pair
        override_key = f"{d}-{a}"
        if override_key in overrides:
            area_labels[pair] = str(overrides[override_key]).strip()
            continue
        county_names = sorted(pair_county_names.get(pair, []))
        if county_names:
            area_labels[pair] = f"District {d} Area {a} ({', '.join(county_names)})"
        else:
            area_labels[pair] = f"District {d} Area {a}"

    return AreaOfficeCodes(
        counties=dict(counties_raw),
        subcounty_splits=dict(subcounty_splits_raw),
        area_labels=area_labels,
    )


def _municipality_name_col(munis: gpd.GeoDataFrame) -> str:
    for candidate in ("Name", "NAME", "CITY", "PLACENAME"):
        if candidate in munis.columns:
            return candidate
    raise ValueError(
        f"municipality layer must include Name/NAME/CITY column; "
        f"got {list(munis.columns)}"
    )


def _build_subcounty_split_geoms(
    fips: str,
    parent_geom,
    splits: list[dict],
    munis: gpd.GeoDataFrame,
    name_col: str,
) -> dict[tuple[int, int], object]:
    """Build one shapely geometry per (district, area_id) for a split county.

    'municipalities' splits: parent ∩ union(named cities).
    'remainder' split:       parent - union(all 'municipalities' splits).

    Asserts named municipalities exist, pairwise-disjointness between named
    splits, and total-area coverage of the parent within 0.5%.
    """
    parent_area = parent_geom.area
    if parent_area <= 0:
        raise AssertionError(f"county FIPS {fips}: non-positive parent area")

    named_geoms: dict[tuple[int, int], object] = {}
    remainder_key: tuple[int, int] | None = None
    for split in splits:
        key = (int(split["district"]), int(split["area_id"]))
        derivation = split["derivation"]
        if derivation == "municipalities":
            names = list(split["municipality_names"])
            named = munis[munis[name_col].isin(names)]
            found = set(named[name_col].tolist())
            missing_names = sorted(set(names) - found)
            if missing_names:
                raise AssertionError(
                    f"county FIPS {fips} (D{key[0]} A{key[1]}): municipalities "
                    f"missing from layer: {missing_names}"
                )
            muni_union = unary_union(list(named.geometry))
            clipped = muni_union.intersection(parent_geom)
            if clipped.is_empty:
                raise AssertionError(
                    f"county FIPS {fips} (D{key[0]} A{key[1]}): named "
                    f"municipalities do not intersect parent county"
                )
            named_geoms[key] = clipped
        else:  # remainder
            if remainder_key is not None:
                raise AssertionError(
                    f"county FIPS {fips}: multiple 'remainder' entries"
                )
            remainder_key = key

    if remainder_key is not None:
        if not named_geoms:
            raise AssertionError(
                f"county FIPS {fips}: 'remainder' requires at least one "
                f"'municipalities' entry to subtract"
            )
        subtract = unary_union(list(named_geoms.values()))
        remainder_geom = parent_geom.difference(subtract)
        if remainder_geom.is_empty:
            raise AssertionError(
                f"county FIPS {fips}: remainder polygon is empty"
            )
        named_geoms[remainder_key] = remainder_geom

    # Pairwise disjointness between named (non-remainder) splits.
    named_only = [k for k in named_geoms if k != remainder_key]
    for i, k1 in enumerate(named_only):
        for k2 in named_only[i + 1:]:
            inter = named_geoms[k1].intersection(named_geoms[k2])
            if inter.area > 1.0:  # square meters (target_crs is projected)
                raise AssertionError(
                    f"county FIPS {fips}: splits D{k1[0]}A{k1[1]} and "
                    f"D{k2[0]}A{k2[1]} overlap by {inter.area:.2f} m^2"
                )

    total_area = sum(g.area for g in named_geoms.values())
    coverage_ratio = total_area / parent_area
    if not (0.995 <= coverage_ratio <= 1.005):
        raise AssertionError(
            f"county FIPS {fips}: subcounty splits cover {coverage_ratio:.4%} "
            f"of parent county area (expected 99.5-100.5%)"
        )

    return named_geoms


def _derive_area_office_polygons(
    county_boundaries: gpd.GeoDataFrame,
    codes_path: Path,
    target_crs: str,
    municipalities: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    """Produce the 38 GDOT Area Office polygons.

    Whole-county entries (158 of 159 counties) come straight from
    area_office_codes.json.counties. The remaining county (Fulton) is
    handled via subcounty_splits: per-split geometry is carved out of the
    Cities of Georgia layer and the parent county polygon, then all
    sub-polygons are concatenated with the whole-county rows and dissolved
    by (district, area_id) to yield exactly 38 polygons.
    """
    codes = _load_area_office_codes(codes_path)
    counties = county_boundaries.copy()
    if "COUNTYFP" not in counties.columns:
        raise ValueError("county_boundaries must include COUNTYFP")
    counties["COUNTYFP"] = counties["COUNTYFP"].astype(str).str.zfill(3)
    if counties.crs is not None and str(counties.crs) != target_crs:
        counties = counties.to_crs(target_crs)

    whole_mask = counties["COUNTYFP"].isin(codes.counties.keys())
    split_mask = counties["COUNTYFP"].isin(codes.subcounty_splits.keys())
    whole_counties = counties.loc[whole_mask, ["COUNTYFP", "geometry"]].copy()
    split_counties = counties.loc[split_mask, ["COUNTYFP", "geometry"]].copy()

    rows: list[dict] = []
    for _, row in whole_counties.iterrows():
        fips = row["COUNTYFP"]
        meta = codes.counties[fips]
        rows.append({
            "AREA_OFFICE_DISTRICT": int(meta["district"]),
            "AREA_OFFICE_AREA_ID": int(meta["area_id"]),
            "geometry": row["geometry"],
        })

    if not split_counties.empty:
        if municipalities is None:
            raise ValueError(
                "subcounty_splits require municipality polygons; pass "
                "municipalities=fetch_georgia_cities() or call "
                "fetch_area_office_boundaries() which fetches them"
            )
        munis = municipalities.copy()
        if munis.crs is not None and str(munis.crs) != target_crs:
            munis = munis.to_crs(target_crs)
        name_col = _municipality_name_col(munis)

        for fips in codes.subcounty_splits.keys():
            parent_rows = split_counties[split_counties["COUNTYFP"] == fips]
            if parent_rows.empty:
                raise AssertionError(
                    f"county FIPS {fips} listed in subcounty_splits but "
                    f"not present in county boundaries"
                )
            parent_geom = unary_union(list(parent_rows.geometry))
            split_geoms = _build_subcounty_split_geoms(
                fips=fips,
                parent_geom=parent_geom,
                splits=codes.subcounty_splits[fips],
                munis=munis,
                name_col=name_col,
            )
            for (district, area_id), geom in split_geoms.items():
                rows.append({
                    "AREA_OFFICE_DISTRICT": district,
                    "AREA_OFFICE_AREA_ID": area_id,
                    "geometry": geom,
                })

    combined = gpd.GeoDataFrame(rows, geometry="geometry", crs=target_crs)
    combined = _repair_boundary_geometry(combined, "area_office_boundaries (pre-dissolve)")
    combined = combined[
        combined.geometry.notna() & ~combined.geometry.is_empty
    ].copy()

    dissolved = combined.dissolve(
        by=["AREA_OFFICE_DISTRICT", "AREA_OFFICE_AREA_ID"],
        as_index=False,
    )
    dissolved["AREA_OFFICE_DISTRICT"] = dissolved["AREA_OFFICE_DISTRICT"].astype(int)
    dissolved["AREA_OFFICE_AREA_ID"] = dissolved["AREA_OFFICE_AREA_ID"].astype(int)
    # Compound id unique across districts: d*100 + a (e.g. D7 A2 -> 702).
    # CONSTRAINT: this assumes at most 99 areas per district. GDOT's current
    # max is 7 (D2, D4). If any future reorganization crosses 99 areas in a
    # single district the encoding collides - revisit then.
    dissolved["AREA_OFFICE_ID"] = (
        dissolved["AREA_OFFICE_DISTRICT"] * 100 + dissolved["AREA_OFFICE_AREA_ID"]
    )
    dissolved["AREA_OFFICE_NAME"] = dissolved.apply(
        lambda row: codes.area_labels[
            (int(row["AREA_OFFICE_DISTRICT"]), int(row["AREA_OFFICE_AREA_ID"]))
        ],
        axis=1,
    )
    dissolved = _repair_boundary_geometry(dissolved, "area_office_boundaries (dissolved)")
    dissolved = dissolved[
        dissolved.geometry.notna() & ~dissolved.geometry.is_empty
    ].copy()
    if len(dissolved) != 38:
        raise AssertionError(
            f"area_office_boundaries must have 38 polygons after dissolve; "
            f"got {len(dissolved)}"
        )
    return dissolved


def fetch_and_cache_boundary(
    name: str,
    url: str | None,
    target_crs: str,
    source_format: str,
    keep_columns: list[str] | None = None,
    state_clip: gpd.GeoDataFrame | None = None,
    dissolve_by: list[str] | None = None,
    source_gdf: gpd.GeoDataFrame | None = None,
    codes_json_path: Path | None = None,
    municipalities: gpd.GeoDataFrame | None = None,
    local_cache_filename: str | None = None,
) -> gpd.GeoDataFrame:
    """Fetch a boundary layer, reproject, cache to .fgb, return GeoDataFrame.

    source_format:
        "geojson" - direct URL read (ArcGIS /query?f=geojson, FeatureServer)
        "tiger_zip" - download+extract zipped TIGER/Line shapefile
        "derived_area_office" - join county_boundaries (source_gdf) to
            area_office_codes.json (codes_json_path) and dissolve by
            (district, area_id). url is ignored. `municipalities` is
            required when the JSON declares any subcounty_splits.

    When `local_cache_filename` is provided AND the file exists under
    01-Raw-Data/Boundaries/cache/, read from that local cache instead
    of the live URL - the single offline-resilience hook. Populate the
    cache by running 01-Raw-Data/Boundaries/scripts/download_boundaries.py.

    The mid-run cache at .tmp/rebuild_outputs/ is a separate artifact:
    always written, used to hand the boundary GDF to the GPKG finalize
    step as a layer.
    """
    def _post_process(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf = clean_column_names(gdf)
        if keep_columns:
            available = [c for c in keep_columns if c in gdf.columns]
            if "geometry" not in available:
                available = [*available, "geometry"]
            gdf = gdf[available].copy()
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf = gdf.to_crs(target_crs)
        gdf = _repair_boundary_geometry(gdf, name)
        return gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    if source_format == "derived_area_office":
        if source_gdf is None or codes_json_path is None:
            raise ValueError(
                "derived_area_office requires source_gdf (counties) and codes_json_path"
            )
        gdf = _derive_area_office_polygons(
            source_gdf,
            codes_json_path,
            target_crs,
            municipalities=municipalities,
        )
    elif source_format == "geojson":
        local_path = _resolve_boundary_local_cache(local_cache_filename)
        if local_path is not None:
            logger.info("Loading %s from local cache %s", name, local_path)
            gdf = gpd.read_file(local_path, engine="pyogrio")
        else:
            if not url:
                raise ValueError("geojson source_format requires url (no local cache)")
            logger.info("Loading %s from %s", name, url)
            gdf = gpd.read_file(url, engine="pyogrio")
        gdf = _post_process(gdf)
    elif source_format == "tiger_zip":
        local_path = _resolve_boundary_local_cache(local_cache_filename)
        if local_path is not None:
            gdf = _read_tiger_zip_from_file(local_path)
        else:
            if not url:
                raise ValueError("tiger_zip source_format requires url (no local cache)")
            gdf = _read_tiger_zip(url)
        gdf = _post_process(gdf)
    else:
        raise ValueError(f"Unsupported source_format {source_format!r}")

    if state_clip is not None and not state_clip.empty:
        # keep_geom_type=True restricts the overlay to the left input's
        # predominant geometry type (polygons here), preventing LineString
        # fragments from shared edges - which would otherwise leak into
        # the STRtree as rows that look like polygons but aren't, leaving
        # MPO holes along the GA-TN / GA-AL borders.
        clipped = gpd.overlay(
            gdf,
            state_clip.to_crs(target_crs),
            how="intersection",
            keep_geom_type=True,
        )
        clipped = clipped[
            clipped.geometry.notna() & ~clipped.geometry.is_empty
        ].copy()
        if not clipped.empty:
            clipped = _repair_boundary_geometry(clipped, f"{name} (state-clipped)")
        gdf = clipped

    if dissolve_by:
        dissolved = gdf.dissolve(by=dissolve_by, as_index=False)
        dissolved = _repair_boundary_geometry(dissolved, f"{name} (dissolved)")
        gdf = dissolved

    REBUILD_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = REBUILD_OUTPUTS_DIR / f"{name}.fgb"
    if cache_path.exists():
        cache_path.unlink()
    try:
        gdf.to_file(cache_path, driver="FlatGeobuf", engine="pyogrio")
    except Exception as exc:
        logger.warning("Could not cache %s to %s: %s", name, cache_path, exc)

    logger.info("Loaded %d %s features", len(gdf), name)
    return gdf


CITY_ID_OBJECTID_OFFSET = 1 << 48


def _assign_city_id(cities: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Attach a stable integer CITY_ID column.

    PRIMARY key: 48-bit md5 of the lowercased, whitespace-collapsed
    Name. Deterministic across ARC OpenData layer republishes: when
    the source layer is refreshed next quarter with renumbered
    OBJECTIDs or shuffled row order, every unique-named city's
    CITY_ID is unchanged. New cities get a new hash; existing cities'
    ids never move. URL-shareable filter state (?cities=N) stays
    valid across refreshes. Collision probability for 535 distinct
    48-bit hashes is ~8e-10.

    FALLBACK to OBJECTID fires in exactly two cases:
      (1) Name is null or empty after normalization.
      (2) Name is duplicated within the layer (e.g. two distinct ARC
          features both named "Union City"). BOTH duplicate rows -
          not just the later one - fall back to OBJECTID. Rationale:
          symmetric treatment means neither duplicate row silently
          wins the "canonical" name-hash id; when the duplicate is
          later resolved in the source, the surviving row can reclaim
          the name-hash.
    Fallback ids are OBJECTID + 2^48, placing them in a disjoint id
    space from the name hashes (hash space: 0..2^48-1; fallback
    space: >= 2^48). BIGINT in Postgres holds both.

    Missing OBJECTID column AND a null/duplicate Name is a fatal
    error - the layer is unusable.

    Stability caveat: for the duplicate-name case (vanishingly rare
    in the 535-feature ARC Cities of Georgia layer), the fallback id
    depends on OBJECTID which is NOT stable across ARC republishes.
    A duplicate-name city's id can shift between runs. Pipeline runs
    log a WARNING listing every duplicate name so the maintainer can
    verify the source layer is clean.
    """
    cities = cities.copy()
    if "CITY_ID" in cities.columns and cities["CITY_ID"].notna().all():
        return cities
    if "Name" not in cities.columns:
        raise ValueError("cities layer must include a 'Name' column to derive CITY_ID")

    def _normalize_name(value) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        text = " ".join(str(value).strip().lower().split())
        return text or None

    def _hash_normalized(norm: str) -> int:
        return int(md5(norm.encode("utf-8")).hexdigest()[:12], 16)

    normalized = cities["Name"].map(_normalize_name)
    null_mask = normalized.isna()
    dup_mask = normalized.duplicated(keep=False) & normalized.notna()
    fallback_mask = null_mask | dup_mask

    hashed = normalized.map(lambda n: _hash_normalized(n) if isinstance(n, str) else None)
    hashed = pd.array(hashed, dtype="Int64")

    if fallback_mask.any():
        if "OBJECTID" not in cities.columns:
            raise ValueError(
                f"cities layer has {int(fallback_mask.sum())} rows with null/duplicate "
                "Name and no OBJECTID fallback column"
            )
        obj_numeric = pd.to_numeric(cities["OBJECTID"], errors="coerce")
        if obj_numeric[fallback_mask].isna().any():
            raise ValueError(
                "cities layer has rows with null/duplicate Name and missing OBJECTID"
            )
        # Surface the fallback-driven rows so a maintainer can audit the
        # source layer. Duplicate names are the unusual case worth a log.
        null_count = int(null_mask.sum())
        if null_count:
            logger.warning(
                "_assign_city_id: %d city rows have null/empty Name; falling "
                "back to OBJECTID (+2^48) - these ids are not stable across "
                "ARC layer republishes",
                null_count,
            )
        if dup_mask.any():
            dup_names = sorted(
                {str(n) for n in normalized[dup_mask].dropna().unique()}
            )
            logger.warning(
                "_assign_city_id: %d city rows share a Name with another row "
                "(%d distinct duplicate names: %s); all duplicate rows fall "
                "back to OBJECTID (+2^48) - ids for these rows are not "
                "stable across ARC layer republishes",
                int(dup_mask.sum()),
                len(dup_names),
                ", ".join(dup_names[:10]) + ("..." if len(dup_names) > 10 else ""),
            )
        fallback_ids = (obj_numeric.astype("Int64") + CITY_ID_OBJECTID_OFFSET)
        hashed_series = pd.Series(hashed, index=cities.index)
        hashed_series = hashed_series.where(~fallback_mask, fallback_ids)
        hashed = pd.array(hashed_series, dtype="Int64")

    cities["CITY_ID"] = hashed
    return cities


def _write_admin_code_snapshot(
    path: Path,
    layer_label: str,
    gdf: gpd.GeoDataFrame,
    id_col: str,
    name_col: str,
    source: str,
    source_url: str,
) -> None:
    """Write a sorted id -> name snapshot JSON file for a boundary layer.

    Preserves the top-level _source / _source_url / _notes metadata;
    overwrites only the `codes` section. Warns when a layer is missing
    required columns rather than raising - a missing snapshot is
    non-fatal for the pipeline (the webapp can fall back to querying
    the live staged layer).
    """
    if id_col not in gdf.columns or name_col not in gdf.columns:
        logger.warning(
            "skipping %s code snapshot: layer missing %s/%s; columns=%s",
            layer_label,
            id_col,
            name_col,
            list(gdf.columns),
        )
        return
    pairs = gdf[[id_col, name_col]].dropna().drop_duplicates(subset=[id_col])
    codes: dict[str, str] = {}
    for _, row in pairs.iterrows():
        raw_id = row[id_col]
        raw_name = row[name_col]
        key = str(raw_id).strip()
        value = str(raw_name).strip()
        if not key or not value:
            continue
        codes[key] = value
    codes = dict(sorted(codes.items(), key=lambda kv: kv[0]))

    payload = {
        "_source": source,
        "_source_url": source_url,
        "_notes": (
            f"id -> {layer_label} display name. Regenerated on every "
            "normalize.py run via write_admin_code_snapshots(); manual "
            "edits will be overwritten."
        ),
        "codes": codes,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    logger.info("Wrote %d %s codes to %s", len(codes), layer_label, path)


def write_admin_code_snapshots(
    mpo_boundaries: gpd.GeoDataFrame,
    regional_commission_boundaries: gpd.GeoDataFrame,
) -> None:
    """Persist id -> name snapshots for MPO and Regional Commission to
    the config/ directory. These are used by the webapp backend for
    deterministic filter option ordering and by auditors who need a
    human-readable snapshot of the current layer.

    Intentional asymmetry: no city_codes.json. Cities are a filter-
    only geography (no boundary table in Postgres, no map overlay in
    the webapp) - the city option list is built on demand by the
    backend from DISTINCT (CITY_ID, CITY_NAME) rows on the segment
    table. A future maintainer tempted to "fix" this by adding a
    city snapshot should first reconsider whether cities have become
    a load-bearing geography; if so, update area_office patterns too.
    """
    _write_admin_code_snapshot(
        path=CONFIG_DIR / "mpo_codes.json",
        layer_label="MPO",
        gdf=mpo_boundaries,
        id_col="MPO_ID",
        name_col="MPO_NAME",
        source="FHWA/BTS Metropolitan Planning Organizations FeatureServer Layer 30",
        source_url=(
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "Metropolitan_Planning_Organizations/FeatureServer/30"
        ),
    )
    _write_admin_code_snapshot(
        path=CONFIG_DIR / "regional_commission_codes.json",
        layer_label="Regional Commission",
        gdf=regional_commission_boundaries,
        id_col="RC_ID",
        name_col="RC_NAME",
        source="Georgia DCA Regional Commissions FeatureServer",
        source_url=(
            "https://services2.arcgis.com/Gqyymy5JISeLzyNM/arcgis/rest/services/"
            "RegionalCommissions/FeatureServer/0"
        ),
    )


def fetch_georgia_cities() -> gpd.GeoDataFrame:
    """Load the full ARC OpenData Cities of Georgia polygon layer.

    ~535 incorporated-place polygons. Used by BOTH Step 1a (Fulton
    Area Office sub-polygons, subset in memory by municipality name)
    and Step 4 (CITY_ID / CITY_NAME overlay flag). The cached .fgb is
    a single statewide artifact - both callers reuse the same file.
    The returned GeoDataFrame carries a stable CITY_ID column (see
    _assign_city_id).
    """
    cities = fetch_and_cache_boundary(
        name="georgia_cities",
        url=GEORGIA_CITIES_URL,
        target_crs=TARGET_CRS,
        source_format="geojson",
        keep_columns=["OBJECTID", "Name", "Type"],
        local_cache_filename="cities.fgb",
    )
    return _assign_city_id(cities)


def fetch_area_office_boundaries(
    county_boundaries: gpd.GeoDataFrame,
    municipalities: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    """Derive the 38 GDOT Area Office polygons.

    Reads 02-Data-Staging/config/area_office_codes.json. Whole-county
    entries come from County polygons; any counties listed in
    ``subcounty_splits`` (Fulton) are further carved by
    ``fetch_georgia_cities()``. Dissolves by (district, area_id) to 38
    polygons. Emits layer name 'area_office_boundaries' for the downstream
    GPKG finalize.
    """
    codes_path = CONFIG_DIR / "area_office_codes.json"
    codes = _load_area_office_codes(codes_path)
    if municipalities is None and codes.subcounty_splits:
        municipalities = fetch_georgia_cities()
    return fetch_and_cache_boundary(
        name="area_office_boundaries",
        url=None,
        target_crs=TARGET_CRS,
        source_format="derived_area_office",
        source_gdf=county_boundaries,
        codes_json_path=codes_path,
        municipalities=municipalities,
    )


def fetch_official_district_boundaries() -> gpd.GeoDataFrame:
    """Load GDOT district polygons from the GDOT_Boundaries service."""
    gdf = fetch_and_cache_boundary(
        name="district_boundaries",
        url=DISTRICT_BOUNDARIES_URL,
        target_crs=TARGET_CRS,
        source_format="geojson",
        keep_columns=[
            "OBJECTID",
            "GDOT_DISTRICT",
            "DISTRICT_NAME",
            "STATUS",
            "EFFECTIVE_DATE",
            "GLOBALID",
        ],
        local_cache_filename="districts.fgb",
    )
    if "GDOT_DISTRICT" in gdf.columns:
        gdf["GDOT_DISTRICT"] = pd.to_numeric(gdf["GDOT_DISTRICT"], errors="coerce").astype("Int64")
        mapped_name = gdf["GDOT_DISTRICT"].map(DISTRICT_SHORT_NAME_LOOKUP)
        gdf["DISTRICT_NAME"] = mapped_name.fillna(gdf.get("DISTRICT_NAME"))
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
    gdf = fetch_and_cache_boundary(
        name="county_boundaries",
        url=COUNTY_BOUNDARIES_URL,
        target_crs=TARGET_CRS,
        source_format="geojson",
        keep_columns=[
            "OBJECTID",
            "COUNTYFP",
            "NAME",
            "GDOT_DISTRICT",
            "CONGRESSIONAL_DISTRICT",
            "SENATE_DISTRICT",
            "HOUSE_DISTRICT",
        ],
        local_cache_filename="counties.fgb",
    )
    if "GDOT_DISTRICT" in gdf.columns:
        gdf["GDOT_DISTRICT"] = pd.to_numeric(gdf["GDOT_DISTRICT"], errors="coerce").astype("Int64")
        gdf["DISTRICT_NAME"] = gdf["GDOT_DISTRICT"].map(DISTRICT_SHORT_NAME_LOOKUP)
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
    return gdf


def derive_state_boundary_from_counties(
    county_boundaries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Dissolve county polygons into a single Georgia state outline."""
    dissolved = county_boundaries[["geometry"]].copy().dissolve()
    dissolved = _repair_boundary_geometry(dissolved, "state_boundary")
    dissolved = gpd.GeoDataFrame(
        {"STATE": ["GA"]},
        geometry=dissolved.geometry.values,
        crs=county_boundaries.crs,
    )
    return dissolved


def fetch_official_mpo_boundaries(
    state_boundary: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Load FHWA/BTS MPO polygons and clip to Georgia.

    Retains multi-state MPOs (e.g. Chattanooga, Columbus-Phenix City) whose GA
    portions serve GAMPO members.
    """
    gdf = fetch_and_cache_boundary(
        name="mpo_boundaries",
        url=MPO_BOUNDARIES_URL,
        target_crs=TARGET_CRS,
        source_format="geojson",
        keep_columns=["MPO_ID", "MPO_NAME", "STATE"],
        state_clip=state_boundary,
        local_cache_filename="mpos.fgb",
    )
    if "MPO_ID" in gdf.columns:
        # Coerce through Int64 first: on a fresh fetch from the GeoJSON
        # endpoint pyogrio can infer MPO_ID as float64, and a direct
        # .astype(str) would then emit '13197100.0' instead of '13197100'.
        numeric = pd.to_numeric(gdf["MPO_ID"], errors="coerce").astype("Int64")
        gdf["MPO_ID"] = numeric.astype("string").str.strip()
    if "MPO_NAME" in gdf.columns:
        gdf["MPO_NAME"] = gdf["MPO_NAME"].astype(str).str.strip()
    return gdf


def fetch_official_regional_commission_boundaries() -> gpd.GeoDataFrame:
    """Load Georgia Regional Commission polygons from the DCA FeatureServer."""
    gdf = fetch_and_cache_boundary(
        name="regional_commission_boundaries",
        url=REGIONAL_COMMISSION_BOUNDARIES_URL,
        target_crs=TARGET_CRS,
        source_format="geojson",
        keep_columns=["FID", "tiger_cnty", "RC", "Acres"],
        local_cache_filename="regional_commissions.fgb",
    )
    rename_map: dict[str, str] = {}
    if "RC" in gdf.columns:
        rename_map["RC"] = "RC_NAME"
    if "FID" in gdf.columns:
        rename_map["FID"] = "RC_ID"
    if rename_map:
        gdf = gdf.rename(columns=rename_map)
    if "RC_ID" in gdf.columns:
        gdf["RC_ID"] = pd.to_numeric(gdf["RC_ID"], errors="coerce").astype("Int64")
    if "RC_NAME" in gdf.columns:
        gdf["RC_NAME"] = gdf["RC_NAME"].astype(str).str.strip()
    return gdf


def _load_tiger_legislative_layer(
    name: str,
    url: str,
    district_col_candidates: tuple[str, ...],
    output_id_col: str,
    output_name_col: str | None = None,
    local_cache_filename: str | None = None,
) -> gpd.GeoDataFrame:
    gdf = fetch_and_cache_boundary(
        name=name,
        url=url,
        target_crs=TARGET_CRS,
        source_format="tiger_zip",
        local_cache_filename=local_cache_filename,
    )
    district_col = next(
        (col for col in district_col_candidates if col in gdf.columns), None
    )
    if district_col is None:
        raise ValueError(
            f"{name}: none of {district_col_candidates} present; got {list(gdf.columns)}"
        )
    gdf[output_id_col] = pd.to_numeric(gdf[district_col], errors="coerce").astype("Int64")
    if output_name_col:
        label_candidates = ("NAMELSAD20", "NAMELSAD", "NAME20", "NAME")
        label_col = next((col for col in label_candidates if col in gdf.columns), None)
        if label_col:
            gdf[output_name_col] = gdf[label_col].astype(str).str.strip()
    gdf = gdf[gdf[output_id_col].notna()].copy()
    return gdf


def fetch_state_house_boundaries() -> gpd.GeoDataFrame:
    return _load_tiger_legislative_layer(
        name="state_house_boundaries",
        url=STATE_HOUSE_BOUNDARIES_URL,
        district_col_candidates=("SLDLST", "SLDLST20"),
        output_id_col="STATE_HOUSE_DISTRICT",
        output_name_col="STATE_HOUSE_NAME",
        local_cache_filename="state_house.zip",
    )


def fetch_state_senate_boundaries() -> gpd.GeoDataFrame:
    return _load_tiger_legislative_layer(
        name="state_senate_boundaries",
        url=STATE_SENATE_BOUNDARIES_URL,
        district_col_candidates=("SLDUST", "SLDUST20"),
        output_id_col="STATE_SENATE_DISTRICT",
        output_name_col="STATE_SENATE_NAME",
        local_cache_filename="state_senate.zip",
    )


def fetch_congressional_boundaries() -> gpd.GeoDataFrame:
    return _load_tiger_legislative_layer(
        name="congressional_boundaries",
        url=CONGRESSIONAL_BOUNDARIES_URL,
        district_col_candidates=("CD119FP", "CDFP", "CD118FP"),
        output_id_col="CONGRESSIONAL_DISTRICT",
        output_name_col="CONGRESSIONAL_NAME",
        local_cache_filename="congressional.zip",
    )


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


def _load_boundary_from_rebuild_cache(name: str) -> gpd.GeoDataFrame | None:
    """Read a boundary layer's .fgb cache from .tmp/rebuild_outputs/.

    fetch_and_cache_boundary always writes this mid-run cache, so after
    segmentation every boundary GDF is recoverable from disk even if
    the caller didn't retain the in-memory copy.
    """
    cache_path = REBUILD_OUTPUTS_DIR / f"{name}.fgb"
    if not cache_path.exists():
        return None
    try:
        return gpd.read_file(cache_path, engine="pyogrio")
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not read boundary cache %s: %s", cache_path, exc)
        return None


def _append_boundary_layer(
    gpkg_path: Path,
    layer_name: str,
    gdf: gpd.GeoDataFrame | None,
) -> bool:
    """Append one boundary GDF to the staged GPKG; no-op if gdf is None/empty."""
    if gdf is None or gdf.empty:
        logger.warning(
            "skipping %s: no GeoDataFrame available (neither passed-in nor cached)",
            layer_name,
        )
        return False
    gdf.to_file(
        gpkg_path,
        layer=layer_name,
        driver="GPKG",
        engine="pyogrio",
        mode="a",
    )
    logger.info("Appended %s layer to %s", layer_name, gpkg_path)
    return True


def write_supporting_boundary_layers(
    gpkg_path: Path,
    fallback_county_boundaries: gpd.GeoDataFrame | None = None,
    fallback_district_boundaries: gpd.GeoDataFrame | None = None,
    area_office_boundaries: gpd.GeoDataFrame | None = None,
    mpo_boundaries: gpd.GeoDataFrame | None = None,
    regional_commission_boundaries: gpd.GeoDataFrame | None = None,
    state_house_boundaries: gpd.GeoDataFrame | None = None,
    state_senate_boundaries: gpd.GeoDataFrame | None = None,
    congressional_boundaries: gpd.GeoDataFrame | None = None,
    county_boundaries: gpd.GeoDataFrame | None = None,
    district_boundaries: gpd.GeoDataFrame | None = None,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Append all 8 administrative boundary layers to the staged GeoPackage.

    When county_boundaries / district_boundaries are passed directly
    (e.g. from the checkpoint runner that already fetched them in stage
    02), use them as-is — no refetch.  Otherwise fetch fresh, falling
    back to fallback_* if the refresh fails.
    """
    if county_boundaries is None or district_boundaries is None:
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

    _append_boundary_layer(gpkg_path, "county_boundaries", county_boundaries)
    _append_boundary_layer(gpkg_path, "district_boundaries", district_boundaries)

    # For the 6 new layers, prefer the in-memory GDF the main() run
    # produced; fall back to the mid-run .fgb cache written by
    # fetch_and_cache_boundary. This keeps write_supporting_boundary_layers
    # callable standalone (e.g. for a GPKG finalize-only rerun) without
    # re-fetching every live URL.
    new_layers: tuple[tuple[str, gpd.GeoDataFrame | None], ...] = (
        ("area_office_boundaries", area_office_boundaries),
        ("mpo_boundaries", mpo_boundaries),
        ("regional_commission_boundaries", regional_commission_boundaries),
        ("state_house_boundaries", state_house_boundaries),
        ("state_senate_boundaries", state_senate_boundaries),
        ("congressional_boundaries", congressional_boundaries),
    )
    for layer_name, gdf in new_layers:
        resolved = gdf if gdf is not None and not gdf.empty else _load_boundary_from_rebuild_cache(layer_name)
        _append_boundary_layer(gpkg_path, layer_name, resolved)

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

    # Re-project routes into the projected target CRS BEFORE segmentation so
    # BoundaryCrosser geometry checks operate in the same CRS as the routes.
    if routes.crs is not None and str(routes.crs) != TARGET_CRS:
        routes = routes.to_crs(TARGET_CRS)
        logger.info("Reprojected route geometry to %s", TARGET_CRS)

    # Step 2: fetch the five split-driving admin geographies plus Step 4
    # overlay layers (legislative + statewide cities). Cities is fetched
    # once here and reused by BOTH the Fulton Area Office sub-polygon
    # derivation (subsets by municipality name) AND the Step 4 CITY_ID /
    # CITY_NAME overlay flag - single cached .fgb artifact.
    logger.info("Loading boundary crossers for re-segmentation")
    district_boundaries = fetch_official_district_boundaries()
    county_boundaries = fetch_official_county_boundaries(
        district_boundaries=district_boundaries
    )
    state_boundary = derive_state_boundary_from_counties(county_boundaries)
    mpo_boundaries = fetch_official_mpo_boundaries(state_boundary)
    regional_commission_boundaries = fetch_official_regional_commission_boundaries()
    city_boundaries = fetch_georgia_cities()
    area_office_boundaries = fetch_area_office_boundaries(
        county_boundaries,
        municipalities=city_boundaries,
    )
    # Step 4 overlay flag layers - do not drive splits.
    state_house_boundaries = fetch_state_house_boundaries()
    state_senate_boundaries = fetch_state_senate_boundaries()
    congressional_boundaries = fetch_congressional_boundaries()

    # Step 5: persist id->name snapshots for MPO and Regional Commission
    # so the webapp backend has a deterministic option list without
    # re-querying the live service and to leave an auditable trail of
    # the layer vintage that drove this pipeline run.
    write_admin_code_snapshots(
        mpo_boundaries=mpo_boundaries,
        regional_commission_boundaries=regional_commission_boundaries,
    )

    crossers: list[BoundaryCrosser] = [
        BoundaryCrosser(
            name="county",
            gdf=county_boundaries,
            attribute_cols={"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
        ),
        BoundaryCrosser(
            name="district",
            gdf=district_boundaries,
            attribute_cols={
                "GDOT_DISTRICT": "DISTRICT",
                "DISTRICT_NAME": "DISTRICT_NAME",
            },
        ),
        BoundaryCrosser(
            name="area_office",
            gdf=area_office_boundaries,
            attribute_cols={
                "AREA_OFFICE_ID": "AREA_OFFICE_ID",
                "AREA_OFFICE_NAME": "AREA_OFFICE_NAME",
            },
        ),
        BoundaryCrosser(
            name="mpo",
            gdf=mpo_boundaries,
            attribute_cols={"MPO_ID": "MPO_ID", "MPO_NAME": "MPO_NAME"},
        ),
        BoundaryCrosser(
            name="regional_commission",
            gdf=regional_commission_boundaries,
            attribute_cols={"RC_ID": "RC_ID", "RC_NAME": "RC_NAME"},
        ),
    ]

    current_lookup = build_interval_lookup(current_traffic)
    current_lookup = mirror_inc_breakpoints_to_dec(
        current_lookup,
        routes["ROUTE_ID"].dropna().astype(str).unique().tolist(),
    )
    segmented = segment_routes(routes, current_lookup, crossers=crossers)
    segmented = build_unique_id(segmented)
    segmented = apply_unique_id_collision_guard(segmented)

    if segmented.crs is not None and str(segmented.crs) != TARGET_CRS:
        segmented = segmented.to_crs(TARGET_CRS)
        logger.info("Reprojected segmented network to %s", TARGET_CRS)
    elif segmented.crs is None:
        logger.warning("No CRS set on segmented network; cannot reproject")

    # Step 4: overlay-based flag stamping. Runs on already-split segments
    # and stamps STATE_HOUSE_DISTRICT, STATE_SENATE_DISTRICT,
    # CONGRESSIONAL_DISTRICT, CITY_ID, CITY_NAME. City stamp only fires
    # when the winning city covers >= 50% of the segment length;
    # unincorporated segments keep CITY_ID/CITY_NAME null.
    segmented = apply_admin_overlay_flags(
        segmented,
        house_boundaries=state_house_boundaries,
        senate_boundaries=state_senate_boundaries,
        congressional_boundaries=congressional_boundaries,
        city_boundaries=city_boundaries,
    )

    segmented = compute_segment_length(segmented)
    segmented = apply_speed_zone_enrichment(segmented)
    existing_gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    county_boundaries_for_backfill = load_county_boundaries_for_attribute_backfill(
        existing_gpkg_path,
        county_boundaries=county_boundaries,
    )
    segmented = backfill_county_district_from_geometry(
        segmented,
        county_boundaries_for_backfill,
    )
    segmented = apply_hpms_enrichment(segmented)
    # Cross-validate the two GDOT-published AADT 2024 sources (state GDB vs
    # federal HPMS submission). Both inputs are now captured on every row, so
    # we can derive the agreement bucket before the pipeline-derived gap-fill
    # chain runs and overwrites the canonical AADT field.
    segmented = compute_aadt_2024_source_agreement(segmented)
    segmented = apply_off_system_speed_zone_enrichment(segmented)
    # Signed-route verification precedence:
    # 1. HPMS enrichment runs first — broad coverage, gap-fills AADT and
    #    attributes, sets initial signed-route classification from federal
    #    routesigning codes.
    # 2. GPAS verification runs second — GDOT's own live reference layers have
    #    final authority for signed-route family. GPAS can upgrade or confirm
    #    but never downgrade a higher-priority family.
    segmented = apply_signed_route_verification(segmented)
    # Classify ROUTE_TYPE_GDOT / HWY_NAME before evacuation enrichment so the
    # matcher's RAMP-skip filter has HWY_NAME populated and does not flag ramps.
    route_type_fields = apply_gdot_route_type_classification(segmented)
    segmented = pd.concat([segmented, route_type_fields], axis=1)
    segmented = apply_evacuation_enrichment(segmented)
    # Splitting changed milepoints on SEC_EVAC children — rebuild the
    # deterministic unique_id (keyed on ROUTE_ID|FROM|TO milepoint) and
    # re-run the admin-tuple collision guard so downstream tables stay
    # keyable by unique_id.
    segmented = build_unique_id(segmented)
    segmented = apply_unique_id_collision_guard(segmented)
    segmented = sync_derived_alias_fields(segmented)
    segmented = apply_direction_mirror_aadt(segmented)
    segmented = apply_state_system_current_aadt_gap_fill(segmented)
    segmented = apply_nearest_neighbor_aadt(segmented)
    segmented = apply_future_aadt_fill_chain(segmented)
    segmented = apply_future_aadt_official_growth_projection(segmented)
    # Recompute confidence tier now that AADT_2024_SOURCE is final and the
    # cross-source agreement has been derived.
    segmented = recompute_aadt_2024_confidence(segmented)
    segmented = derive_alignment_columns(segmented)
    segmented = add_decoded_label_columns(segmented)
    segmented = add_county_all_from_geometry(segmented, county_boundaries_for_backfill)
    segmented = _move_column_after(segmented, "county_all", "COUNTY_NAME")

    logger.info("Final segment count: %d", len(segmented))
    logger.info("Current AADT official coverage: %d segments", segmented["AADT_2024_OFFICIAL"].notna().sum())
    logger.info("Current AADT final coverage: %d segments", segmented["AADT"].notna().sum())
    if "FUTURE_AADT_2044" in segmented.columns:
        logger.info("Future AADT 2044 coverage: %d segments", segmented["FUTURE_AADT_2044"].notna().sum())

    # Drop duplicate / source-internal columns before output
    cols_to_drop = [c for c in COLUMNS_TO_DROP_FROM_OUTPUT if c in segmented.columns]
    if cols_to_drop:
        segmented = segmented.drop(columns=cols_to_drop)
        logger.info("Dropped %d duplicate/internal columns from output: %s", len(cols_to_drop), ", ".join(cols_to_drop))

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
        area_office_boundaries=area_office_boundaries,
        mpo_boundaries=mpo_boundaries,
        regional_commission_boundaries=regional_commission_boundaries,
        state_house_boundaries=state_house_boundaries,
        state_senate_boundaries=state_senate_boundaries,
        congressional_boundaries=congressional_boundaries,
        county_boundaries=county_boundaries,
        district_boundaries=district_boundaries,
    )
    assert_decoded_county_lookup_matches_boundaries(segmented, staged_county_boundaries)

    write_match_summary(segmented)
    write_current_aadt_coverage_audit(segmented)
    write_enrichment_summary(segmented)
    write_hpms_enrichment_summary(segmented)
    write_signed_route_verification_summary(segmented)
    write_evacuation_summary(segmented)
    logger.info("Normalization complete.")


if __name__ == "__main__":
    main()
