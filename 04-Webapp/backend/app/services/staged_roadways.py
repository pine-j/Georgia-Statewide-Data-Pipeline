from __future__ import annotations

import json
import math
import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Any

import pyogrio
from pyproj import Transformer
from shapely.geometry import mapping

from app.schemas import (
    AnalyticsSummaryResponse,
    CountyOption,
    FunctionalClassSummary,
    GeorgiaFilterOptionsResponse,
    DistrictOption,
    GeoJsonFeature,
    GeoJsonFeatureCollection,
    HighwayTypeOption,
    RoadwayDetailResponse,
    RoadwayFeature,
    RoadwayFeatureCollection,
    RoadwayFeatureProperties,
    RoadwayManifestResponse,
)
from app.services.roadway_visualizations import (
    THEMATIC_PROPERTY_SQL,
    derive_hwy_des,
    derive_percent_of_aadt,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
STAGED_DB_PATH = REPO_ROOT / "02-Data-Staging" / "databases" / "roadway_inventory.db"
STAGED_GPKG_PATH = REPO_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"
GEORGIA_FILTERS_PATH = Path(__file__).resolve().parent.parent / "data" / "georgia_filters.json"
SOURCE_CRS = "EPSG:32617"
TARGET_CRS = "EPSG:4326"
SUPPORTED_STATE = "ga"
DISTRICT_LABELS = {
    1: "District 1 - Gainesville",
    2: "District 2 - Tennille",
    3: "District 3 - Thomaston",
    4: "District 4 - Tifton",
    5: "District 5 - Jesup",
    6: "District 6 - Cartersville",
    7: "District 7 - Chamblee",
}
HIGHWAY_TYPE_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("IH", "IH", "Interstate"),
    ("US", "US", "U.S. Route"),
    ("SH", "SH", "State Route"),
    ("LOCAL", "Local", "Local/Other"),
)
HIGHWAY_TYPE_ROUTE_FAMILY_BY_ID = {
    highway_type_id: route_family
    for highway_type_id, _, route_family in HIGHWAY_TYPE_OPTIONS
}
HIGHWAY_TYPE_ROUTE_FAMILY_ALIASES = {
    "IH": "Interstate",
    "INTERSTATE": "Interstate",
    "I": "Interstate",
    "US": "U.S. Route",
    "U.S. ROUTE": "U.S. Route",
    "US ROUTE": "U.S. Route",
    "SH": "State Route",
    "SR": "State Route",
    "STATE ROUTE": "State Route",
    "LOCAL": "Local/Other",
    "LOCAL/OTHER": "Local/Other",
    "OTHER": "Local/Other",
}
_STAGED_DATA_CACHE_STAMP: tuple[int | None, int | None] | None = None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True

    try:
        return bool(math.isnan(value))
    except (TypeError, ValueError):
        return False


def _normalize_int(value: Any) -> int | None:
    if _is_missing(value):
        return None

    return int(float(value))


def _normalize_float(value: Any) -> float:
    if _is_missing(value):
        return 0.0

    return float(value)


def _normalize_float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None

    return float(value)


def _normalize_county_code(value: Any) -> str | None:
    if _is_missing(value):
        return None

    return f"{int(float(value)):03d}"


def _normalize_json_value(value: Any) -> Any:
    if hasattr(value, "item") and callable(value.item):
        value = value.item()

    if _is_missing(value):
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value) if value.is_integer() else float(value)

    if isinstance(value, str):
        return value

    return str(value)


def _normalize_text(value: Any) -> str | None:
    if _is_missing(value):
        return None

    text = str(value).strip()
    return text or None


def _format_functional_class(value: Any) -> str:
    if _is_missing(value):
        return "Unknown"

    numeric_value = float(value)
    if numeric_value.is_integer():
        return str(int(numeric_value))

    return str(numeric_value)


def _format_road_name(hwy_name: Any, route_id: Any) -> str:
    if hwy_name and str(hwy_name).strip():
        return str(hwy_name)

    if route_id and str(route_id).strip():
        return str(route_id)

    return "Roadway segment"


def get_district_label(district_id: int | None) -> str:
    if district_id is None:
        return "District"

    return DISTRICT_LABELS.get(district_id, f"District {district_id}")


def list_highway_type_options() -> list[HighwayTypeOption]:
    return [
        HighwayTypeOption(
            id=highway_type_id,
            label=label,
            route_family=route_family,
        )
        for highway_type_id, label, route_family in HIGHWAY_TYPE_OPTIONS
    ]


@lru_cache(maxsize=1)
def _load_county_maps() -> tuple[dict[str, str], dict[str, str]]:
    payload = json.loads(GEORGIA_FILTERS_PATH.read_text(encoding="utf-8"))
    code_to_county = {
        item["county_fips"]: item["county"]
        for item in payload["counties"]
    }

    county_to_code = {
        county_name.casefold(): county_code
        for county_code, county_name in code_to_county.items()
    }
    return code_to_county, county_to_code


@lru_cache(maxsize=1)
def _get_transformer() -> Transformer:
    return Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)


def _selected_county_codes(counties: list[str] | None) -> tuple[str, ...]:
    if not counties:
        return ()

    _, county_to_code = _load_county_maps()
    selected_codes = {
        county_to_code[county.casefold()]
        for county in counties
        if county.casefold() in county_to_code
    }
    return tuple(sorted(selected_codes))


def _selected_county_names(counties: list[str] | None) -> tuple[str, ...]:
    if not counties:
        return ()

    code_to_county, county_to_code = _load_county_maps()
    selected_names = {
        code_to_county[county_to_code[county.casefold()]].strip().lower()
        for county in counties
        if county.casefold() in county_to_code
    }
    return tuple(sorted(selected_names))


def _selected_highway_route_families(highway_types: list[str] | None) -> tuple[str, ...]:
    if not highway_types:
        return ()

    selected_route_families = {
        HIGHWAY_TYPE_ROUTE_FAMILY_ALIASES.get(str(highway_type).strip().upper())
        for highway_type in highway_types
    }
    return tuple(
        sorted(route_family for route_family in selected_route_families if route_family is not None)
    )


def _county_all_match_expression(column_name: str = "county_all") -> str:
    return f"(',' || replace(lower(COALESCE({column_name}, '')), ', ', ',') || ',')"


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_sqlite_filters(
    district: tuple[int, ...] | None,
    county_names: tuple[str, ...],
    highway_route_families: tuple[str, ...],
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if district:
        clauses.append("(" + " OR ".join("DISTRICT = ?" for _ in district) + ")")
        params.extend(district)

    if county_names:
        county_expression = _county_all_match_expression()
        clauses.append(
            "(" + " OR ".join(f"{county_expression} LIKE ?" for _ in county_names) + ")"
        )
        params.extend(f"%,{county_name},%" for county_name in county_names)

    if highway_route_families:
        clauses.append(
            "(" + " OR ".join("ROUTE_FAMILY = ?" for _ in highway_route_families) + ")"
        )
        params.extend(highway_route_families)

    if not clauses:
        return "", params

    return f"WHERE {' AND '.join(clauses)}", params


def _build_gpkg_where(
    district: tuple[int, ...] | None,
    county_names: tuple[str, ...],
    highway_route_families: tuple[str, ...],
) -> str:
    clauses: list[str] = []

    if district:
        clauses.append("(" + " OR ".join(f"DISTRICT = {d}" for d in district) + ")")

    if county_names:
        county_expression = _county_all_match_expression()
        county_patterns = " OR ".join(
            f"{county_expression} LIKE '%,{_escape_sql_literal(county_name)},%'"
            for county_name in county_names
        )
        clauses.append(f"({county_patterns})")

    if highway_route_families:
        route_family_patterns = " OR ".join(
            f"ROUTE_FAMILY = '{_escape_sql_literal(route_family)}'"
            for route_family in highway_route_families
        )
        clauses.append(f"({route_family_patterns})")

    if not clauses:
        return ""

    return f"WHERE {' AND '.join(clauses)}"


def _build_boundary_where(
    layer_name: str,
    district: tuple[int, ...] | None,
    county_codes: tuple[str, ...],
) -> str:
    clauses: list[str] = []

    if district and layer_name in {"county_boundaries", "district_boundaries"}:
        clauses.append("(" + " OR ".join(f"GDOT_DISTRICT = {d}" for d in district) + ")")

    if county_codes and layer_name == "county_boundaries":
        quoted_codes = ", ".join(f"'{county_code}'" for county_code in county_codes)
        clauses.append(f"COUNTYFP IN ({quoted_codes})")

    if not clauses:
        return ""

    return f"WHERE {' AND '.join(clauses)}"


def _project_bounds(bounds: tuple[float, float, float, float]) -> list[float]:
    min_x, min_y, max_x, max_y = bounds
    xs = [min_x, min_x, max_x, max_x]
    ys = [min_y, max_y, min_y, max_y]
    longitudes, latitudes = _get_transformer().transform(xs, ys)

    return [
        float(min(longitudes)),
        float(min(latitudes)),
        float(max(longitudes)),
        float(max(latitudes)),
    ]


def _empty_manifest(state_code: str, chunk_size: int) -> RoadwayManifestResponse:
    return RoadwayManifestResponse(
        state_code=state_code,
        total_segments=0,
        chunk_size=chunk_size,
        chunk_count=0,
        bounds=None,
    )


def _empty_summary(state_code: str) -> AnalyticsSummaryResponse:
    return AnalyticsSummaryResponse(
        state_code=state_code,
        roadway_count=0,
        total_miles=0.0,
        classes=[],
    )


def _get_staged_data_cache_stamp() -> tuple[int | None, int | None]:
    def _mtime_ns(path: Path) -> int | None:
        return path.stat().st_mtime_ns if path.exists() else None

    return (_mtime_ns(STAGED_DB_PATH), _mtime_ns(STAGED_GPKG_PATH))


def _clear_staged_data_caches() -> None:
    _get_segment_count.cache_clear()
    _get_class_summary_rows.cache_clear()
    _get_filtered_bounds.cache_clear()


def _ensure_staged_data_cache_fresh() -> None:
    global _STAGED_DATA_CACHE_STAMP

    current_stamp = _get_staged_data_cache_stamp()
    if _STAGED_DATA_CACHE_STAMP is None:
        _STAGED_DATA_CACHE_STAMP = current_stamp
        return

    if current_stamp != _STAGED_DATA_CACHE_STAMP:
        _clear_staged_data_caches()
        _STAGED_DATA_CACHE_STAMP = current_stamp


def _open_sqlite() -> sqlite3.Connection:
    return sqlite3.connect(STAGED_DB_PATH)


@lru_cache(maxsize=256)
def _get_segment_count(
    district: tuple[int, ...],
    county_names: tuple[str, ...],
    highway_route_families: tuple[str, ...],
) -> int:
    where_clause, params = _build_sqlite_filters(
        district,
        county_names,
        highway_route_families,
    )
    query = f"SELECT COUNT(*) FROM segments {where_clause}"

    with _open_sqlite() as connection:
        cursor = connection.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()

    return int(row[0]) if row else 0


@lru_cache(maxsize=256)
def _get_class_summary_rows(
    district: tuple[int, ...],
    county_names: tuple[str, ...],
    highway_route_families: tuple[str, ...],
) -> tuple[tuple[str, int, float], ...]:
    where_clause, params = _build_sqlite_filters(
        district,
        county_names,
        highway_route_families,
    )
    query = f"""
        SELECT
            FUNCTIONAL_CLASS,
            COUNT(*) AS segment_count,
            COALESCE(SUM(segment_length_mi), 0) AS total_miles
        FROM segments
        {where_clause}
        GROUP BY FUNCTIONAL_CLASS
        ORDER BY FUNCTIONAL_CLASS
    """

    with _open_sqlite() as connection:
        cursor = connection.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()

    return tuple(
        (
            _format_functional_class(functional_class),
            int(segment_count),
            float(total_miles),
        )
        for functional_class, segment_count, total_miles in rows
    )


@lru_cache(maxsize=256)
def _get_filtered_bounds(
    district: tuple[int, ...],
    county_names: tuple[str, ...],
    highway_route_families: tuple[str, ...],
) -> list[float] | None:
    if _get_segment_count(district, county_names, highway_route_families) == 0:
        return None

    if not district and not county_names and not highway_route_families:
        info = pyogrio.read_info(STAGED_GPKG_PATH, layer="roadway_segments")
        total_bounds = info.get("total_bounds")
        if total_bounds is None:
            return None

        return _project_bounds(tuple(float(value) for value in total_bounds))

    where_clause = _build_gpkg_where(district, county_names, highway_route_families)
    _, feature_bounds = pyogrio.read_bounds(
        STAGED_GPKG_PATH,
        layer="roadway_segments",
        where=where_clause.replace("WHERE ", "", 1),
    )

    if feature_bounds.size == 0:
        return None

    source_bounds = (
        float(feature_bounds[0].min()),
        float(feature_bounds[1].min()),
        float(feature_bounds[2].max()),
        float(feature_bounds[3].max()),
    )
    return _project_bounds(source_bounds)


def get_staged_roadway_manifest(
    state_code: str,
    chunk_size: int,
    district: list[int] | None = None,
    counties: list[str] | None = None,
    highway_types: list[str] | None = None,
) -> RoadwayManifestResponse:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return _empty_manifest(state_code, chunk_size)

    county_names = _selected_county_names(counties)
    highway_route_families = _selected_highway_route_families(highway_types)
    district_tuple = tuple(sorted(district)) if district else ()
    total_segments = _get_segment_count(district_tuple, county_names, highway_route_families)
    chunk_count = math.ceil(total_segments / chunk_size) if total_segments else 0

    return RoadwayManifestResponse(
        state_code=state_code,
        total_segments=total_segments,
        chunk_size=chunk_size,
        chunk_count=chunk_count,
        bounds=_get_filtered_bounds(district_tuple, county_names, highway_route_families),
    )


def get_staged_roadway_summary(
    state_code: str,
    district: list[int] | None = None,
    counties: list[str] | None = None,
    highway_types: list[str] | None = None,
) -> AnalyticsSummaryResponse:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return _empty_summary(state_code)

    county_names = _selected_county_names(counties)
    highway_route_families = _selected_highway_route_families(highway_types)
    district_tuple = tuple(sorted(district)) if district else ()
    class_rows = _get_class_summary_rows(
        district_tuple,
        county_names,
        highway_route_families,
    )
    classes = [
        FunctionalClassSummary(
            functional_class=functional_class,
            segment_count=segment_count,
            total_miles=total_miles,
        )
        for functional_class, segment_count, total_miles in class_rows
    ]

    return AnalyticsSummaryResponse(
        state_code=state_code,
        roadway_count=sum(item.segment_count for item in classes),
        total_miles=round(sum(item.total_miles for item in classes), 2),
        classes=classes,
    )


def get_staged_roadway_bounds(
    state_code: str,
    district: list[int] | None = None,
    counties: list[str] | None = None,
    highway_types: list[str] | None = None,
) -> list[float] | None:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return None

    county_names = _selected_county_names(counties)
    highway_route_families = _selected_highway_route_families(highway_types)
    district_tuple = tuple(sorted(district)) if district else ()
    return _get_filtered_bounds(district_tuple, county_names, highway_route_families)


def get_staged_roadway_features(
    state_code: str,
    limit: int,
    offset: int = 0,
    district: list[int] | None = None,
    counties: list[str] | None = None,
    highway_types: list[str] | None = None,
) -> RoadwayFeatureCollection:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return RoadwayFeatureCollection(type="FeatureCollection", features=[])

    county_names = _selected_county_names(counties)
    highway_route_families = _selected_highway_route_families(highway_types)
    district_tuple = tuple(sorted(district)) if district else ()
    where_clause = _build_gpkg_where(district_tuple, county_names, highway_route_families)
    thematic_selects = [
        f"{sql_expression} AS {alias}"
        for alias, sql_expression in THEMATIC_PROPERTY_SQL.items()
        if alias != "aadt"
    ]
    query = " ".join(
        [
            "SELECT",
            "ROUTE_ID AS route_id,",
            "HWY_NAME AS hwy_name,",
            "COUNTY_CODE AS county_code,",
            "DISTRICT AS district_id,",
            "DISTRICT_LABEL AS district_label,",
            "FUNCTIONAL_CLASS AS functional_class,",
            "AADT AS aadt,",
            "segment_length_mi AS length_miles,",
            "unique_id AS unique_id,",
            ", ".join(thematic_selects) + ",",
            "geom",
            "FROM roadway_segments",
            where_clause,
            "ORDER BY ROWID",
            f"LIMIT {limit}",
            f"OFFSET {offset}",
        ]
    ).strip()

    gdf = pyogrio.read_dataframe(STAGED_GPKG_PATH, sql=query)
    if gdf.empty:
        return RoadwayFeatureCollection(type="FeatureCollection", features=[])

    gdf = gdf.to_crs(TARGET_CRS)
    county_code_to_name, _ = _load_county_maps()

    features: list[RoadwayFeature] = []
    for item_index, row in enumerate(gdf.itertuples(index=False), start=offset + 1):
        county_code = _normalize_county_code(getattr(row, "county_code", None))
        county_name = county_code_to_name.get(county_code or "", "Unknown")
        district_id = _normalize_int(getattr(row, "district_id", None)) or 0
        district_label = (
            _normalize_text(getattr(row, "district_label", None))
            or get_district_label(district_id)
        )
        aadt = _normalize_int(getattr(row, "aadt", None))
        length_miles = _normalize_float(getattr(row, "length_miles", None))
        unique_id = str(getattr(row, "unique_id", "")).strip() or f"segment-{item_index}"

        features.append(
            RoadwayFeature(
                type="Feature",
                geometry=mapping(row.geometry),
                properties=RoadwayFeatureProperties(
                    id=item_index,
                    unique_id=unique_id,
                    road_name=_format_road_name(
                        getattr(row, "hwy_name", None),
                        getattr(row, "route_id", None),
                    ),
                    functional_class=_format_functional_class(
                        getattr(row, "functional_class", None)
                    ),
                    aadt=aadt,
                    length_miles=length_miles,
                    district=district_id,
                    district_label=district_label,
                    county=county_name,
                    system_code_label=_normalize_text(
                        getattr(row, "system_code_label", None)
                    ),
                    direction_label=_normalize_text(
                        getattr(row, "direction_label", None)
                    ),
                    num_lanes=_normalize_int(getattr(row, "num_lanes", None)),
                    future_aadt_2044=_normalize_int(
                        getattr(row, "future_aadt_2044", None)
                    ),
                    k_factor=_normalize_int(getattr(row, "k_factor", None)),
                    d_factor=_normalize_int(getattr(row, "d_factor", None)),
                    truck_aadt=_normalize_int(getattr(row, "truck_aadt", None)),
                    pct_sadt=_normalize_float_or_none(getattr(row, "pct_sadt", None)),
                    pct_cadt=_normalize_float_or_none(getattr(row, "pct_cadt", None)),
                    vmt=_normalize_float_or_none(getattr(row, "vmt", None)),
                    nhs_ind_label=_normalize_text(
                        getattr(row, "nhs_ind_label", None)
                    ),
                    median_type_label=_normalize_text(
                        getattr(row, "median_type_label", None)
                    ),
                    hwy_des=_normalize_text(getattr(row, "hwy_des", None)),
                    speed_limit=_normalize_int(getattr(row, "speed_limit", None)),
                    truck_pct=_normalize_float_or_none(getattr(row, "truck_pct", None)),
                    functional_class_viz=_normalize_text(
                        getattr(row, "functional_class_viz", None)
                    ),
                    surface_type_label=_normalize_text(
                        getattr(row, "surface_type_label", None)
                    ),
                    ownership_label=_normalize_text(
                        getattr(row, "ownership_label", None)
                    ),
                    facility_type_label=_normalize_text(
                        getattr(row, "facility_type_label", None)
                    ),
                    sec_evac=_normalize_text(
                        getattr(row, "sec_evac", None)
                    ),
                ),
            )
        )

    return RoadwayFeatureCollection(type="FeatureCollection", features=features)


def get_staged_boundary_features(
    state_code: str,
    boundary_type: str,
    district: list[int] | None = None,
    counties: list[str] | None = None,
) -> GeoJsonFeatureCollection:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return GeoJsonFeatureCollection(type="FeatureCollection", features=[])

    layer_name = {
        "counties": "county_boundaries",
        "districts": "district_boundaries",
    }.get(boundary_type)
    if layer_name is None:
        return GeoJsonFeatureCollection(type="FeatureCollection", features=[])

    county_codes = _selected_county_codes(counties)
    district_tuple = tuple(sorted(district)) if district else ()
    where_clause = _build_boundary_where(layer_name, district_tuple, county_codes)
    query = " ".join(
        [
            "SELECT *",
            f"FROM {layer_name}",
            where_clause,
            "ORDER BY ROWID",
        ]
    ).strip()

    gdf = pyogrio.read_dataframe(STAGED_GPKG_PATH, sql=query)
    if gdf.empty:
        return GeoJsonFeatureCollection(type="FeatureCollection", features=[])

    gdf = gdf.to_crs(TARGET_CRS)
    features: list[GeoJsonFeature] = []
    for row in gdf.itertuples(index=False):
        properties = {
            key: _normalize_json_value(value)
            for key, value in row._asdict().items()
            if key != "geometry"
        }
        features.append(
            GeoJsonFeature(
                type="Feature",
                geometry=mapping(row.geometry),
                properties=properties,
            )
        )

    return GeoJsonFeatureCollection(type="FeatureCollection", features=features)


def get_staged_filter_options() -> GeorgiaFilterOptionsResponse:
    _ensure_staged_data_cache_fresh()
    county_code_to_name, _ = _load_county_maps()

    with _open_sqlite() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNTY_CODE, DISTRICT
            FROM segments
            WHERE COUNTY_CODE IS NOT NULL AND DISTRICT IS NOT NULL
            GROUP BY COUNTY_CODE, DISTRICT
            ORDER BY DISTRICT, COUNTY_CODE
            """
        )
        rows = cursor.fetchall()

    counties = [
        CountyOption(
            county=county_code_to_name.get(_normalize_county_code(county_code) or "", "Unknown"),
            county_fips=_normalize_county_code(county_code) or "",
            district=_normalize_int(district) or 0,
        )
        for county_code, district in rows
    ]

    districts = sorted({county.district for county in counties if county.district > 0})

    return GeorgiaFilterOptionsResponse(
        districts=[
            DistrictOption(id=district_id, label=get_district_label(district_id))
            for district_id in districts
        ],
        counties=sorted(counties, key=lambda item: item.county),
        highway_types=list_highway_type_options(),
    )


def get_staged_roadway_detail(unique_id: str) -> RoadwayDetailResponse | None:
    _ensure_staged_data_cache_fresh()
    county_code_to_name, _ = _load_county_maps()

    with _open_sqlite() as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM segments WHERE unique_id = ?", (unique_id,))
        row = cursor.fetchone()

    if row is None:
        return None

    county_code = _normalize_county_code(row["COUNTY_CODE"])
    county_name = county_code_to_name.get(county_code or "", "Unknown")
    district_id = _normalize_int(row["DISTRICT"]) or 0
    route_id = row["ROUTE_ID"] if "ROUTE_ID" in row.keys() else row["unique_id"]
    hwy_name = row["HWY_NAME"] if "HWY_NAME" in row.keys() else None

    attributes = {
        key: _normalize_json_value(row[key])
        for key in row.keys()
    }
    attributes["PCT_SADT"] = derive_percent_of_aadt(
        row["SINGLE_UNIT_AADT_2024"] if "SINGLE_UNIT_AADT_2024" in row.keys() else None,
        row["AADT"] if "AADT" in row.keys() else None,
    )
    attributes["PCT_CADT"] = derive_percent_of_aadt(
        row["COMBO_UNIT_AADT_2024"] if "COMBO_UNIT_AADT_2024" in row.keys() else None,
        row["AADT"] if "AADT" in row.keys() else None,
    )
    attributes["HWY_DES"] = derive_hwy_des(
        row["NUM_LANES"] if "NUM_LANES" in row.keys() else None,
        row["MEDIAN_TYPE"] if "MEDIAN_TYPE" in row.keys() else None,
        row["ROUTE_FAMILY"] if "ROUTE_FAMILY" in row.keys() else None,
        row["HPMS_ACCESS_CONTROL"] if "HPMS_ACCESS_CONTROL" in row.keys() else None,
    )
    district_label = (
        _normalize_text(row["DISTRICT_LABEL"]) if "DISTRICT_LABEL" in row.keys() else None
    ) or get_district_label(district_id)

    return RoadwayDetailResponse(
        unique_id=unique_id,
        road_name=_format_road_name(hwy_name, route_id),
        district=district_id,
        district_label=district_label,
        county=county_name,
        attributes=attributes,
    )
