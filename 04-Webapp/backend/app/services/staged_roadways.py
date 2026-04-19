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
    AreaOfficeOption,
    CityOption,
    CongressionalOption,
    CountyOption,
    DistrictOption,
    FunctionalClassSummary,
    GeoJsonFeature,
    GeoJsonFeatureCollection,
    GeorgiaFilterOptionsResponse,
    HighwayTypeOption,
    MpoOption,
    RegionalCommissionOption,
    RoadwayDetailResponse,
    RoadwayFeature,
    RoadwayFeatureCollection,
    RoadwayFeatureProperties,
    RoadwayFilters,
    RoadwayManifestResponse,
    StateHouseOption,
    StateSenateOption,
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
DISTRICT_NAMES = {
    1: "Gainesville",
    2: "Tennille",
    3: "Thomaston",
    4: "Tifton",
    5: "Jesup",
    6: "Cartersville",
    7: "Chamblee",
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
_PROPERTY_MIN_MAX_CACHE: dict[tuple[str, str], tuple[float | None, float | None]] = {}

# Dispatch dict mapping each boundary layer to the segment filters that
# should constrain its query. Keys name the layer in the staged GPKG;
# values map a RoadwayFilters attribute to the layer column that holds
# the corresponding id. Adding a new filter to an existing layer or a new
# layer to the dispatch is a one-line change - no hardcoded if/elif.
# City boundaries are intentionally omitted: city is filter-only, never a
# map overlay layer.
BOUNDARY_FILTER_COLUMNS: dict[str, dict[str, str]] = {
    "county_boundaries": {
        "district": "GDOT_DISTRICT",
        "counties": "COUNTYFP",
    },
    "district_boundaries": {
        "district": "GDOT_DISTRICT",
    },
    "area_office_boundaries": {
        "district": "AREA_OFFICE_DISTRICT",
        "area_offices": "AREA_OFFICE_ID",
    },
    "mpo_boundaries": {
        "mpos": "MPO_ID",
    },
    "regional_commission_boundaries": {
        "regional_commissions": "RC_ID",
    },
    "state_house_boundaries": {
        "state_house_districts": "STATE_HOUSE_DISTRICT",
    },
    "state_senate_boundaries": {
        "state_senate_districts": "STATE_SENATE_DISTRICT",
    },
    "congressional_boundaries": {
        "congressional_districts": "CONGRESSIONAL_DISTRICT",
    },
}

# Maps the external boundary_type path segment to the staged GPKG layer
# name. City is intentionally absent - see BOUNDARY_FILTER_COLUMNS.
BOUNDARY_TYPE_TO_LAYER: dict[str, str] = {
    "counties": "county_boundaries",
    "districts": "district_boundaries",
    "area_offices": "area_office_boundaries",
    "mpos": "mpo_boundaries",
    "regional_commissions": "regional_commission_boundaries",
    "state_house": "state_house_boundaries",
    "state_senate": "state_senate_boundaries",
    "congressional": "congressional_boundaries",
}


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


def get_district_name(district_id: int | None) -> str:
    if district_id is None:
        return "District"

    return DISTRICT_NAMES.get(district_id, f"District {district_id}")


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


def resolve_filters_from_request(
    district: list[int] | None = None,
    counties: list[str] | None = None,
    highway_types: list[str] | None = None,
    area_offices: list[int] | None = None,
    mpos: list[str] | None = None,
    regional_commissions: list[int] | None = None,
    state_house_districts: list[int] | None = None,
    state_senate_districts: list[int] | None = None,
    congressional_districts: list[int] | None = None,
    cities: list[int] | None = None,
    include_unincorporated: bool = False,
) -> RoadwayFilters:
    """Build the canonical RoadwayFilters from raw FastAPI query params.

    - `counties` (display names) resolves to a sorted tuple of name
      strings; WHERE builders match against the segment table's
      county_all / COUNTY_NAME columns.
    - `highway_types` resolves to ROUTE_FAMILY values (Interstate, etc).
    - All other lists are cast to sorted tuples of ids.
    """
    return RoadwayFilters(
        district=tuple(sorted(district)) if district else (),
        counties=_selected_county_names(counties),
        highway_route_families=_selected_highway_route_families(highway_types),
        area_offices=tuple(sorted(area_offices)) if area_offices else (),
        mpos=tuple(sorted(str(m).strip() for m in mpos if str(m).strip())) if mpos else (),
        regional_commissions=(
            tuple(sorted(regional_commissions)) if regional_commissions else ()
        ),
        state_house_districts=(
            tuple(sorted(state_house_districts)) if state_house_districts else ()
        ),
        state_senate_districts=(
            tuple(sorted(state_senate_districts)) if state_senate_districts else ()
        ),
        congressional_districts=(
            tuple(sorted(congressional_districts)) if congressional_districts else ()
        ),
        cities=tuple(sorted(cities)) if cities else (),
        include_unincorporated=bool(include_unincorporated),
    )


def _segment_in_list_clause(column: str, values: tuple, quote_values: bool) -> str:
    """Build a segment WHERE fragment like `COL IN ('a','b')` or `COL IN (1,2)`.

    Centralized here so the two query-string builders (sqlite /
    pyogrio) share one idempotent format. Callers own the outer
    grouping parentheses when combining with other clauses.
    """
    if not values:
        return ""
    if quote_values:
        rendered = ", ".join(f"'{_escape_sql_literal(str(v))}'" for v in values)
    else:
        rendered = ", ".join(str(v) for v in values)
    return f"{column} IN ({rendered})"


# (segment_attr, column_name, needs_quoting). Shared between
# _build_sqlite_filters and _build_gpkg_where so a new filter added to
# RoadwayFilters only needs one row here, not two WHERE-builder edits.
_SEGMENT_FILTER_SPECS: tuple[tuple[str, str, bool], ...] = (
    ("district", "DISTRICT", False),
    ("area_offices", "AREA_OFFICE_ID", False),
    ("mpos", "MPO_ID", True),
    ("regional_commissions", "RC_ID", False),
    ("state_house_districts", "STATE_HOUSE_DISTRICT", False),
    ("state_senate_districts", "STATE_SENATE_DISTRICT", False),
    ("congressional_districts", "CONGRESSIONAL_DISTRICT", False),
)


def _build_sqlite_filters(filters: RoadwayFilters) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    for attr, column, needs_quote in _SEGMENT_FILTER_SPECS:
        values = getattr(filters, attr)
        if not values:
            continue
        placeholders = ", ".join("?" for _ in values)
        clauses.append(f"{column} IN ({placeholders})")
        params.extend(values)

    if filters.counties:
        county_expression = _county_all_match_expression()
        clauses.append(
            "(" + " OR ".join(f"{county_expression} LIKE ?" for _ in filters.counties) + ")"
        )
        params.extend(f"%,{county_name},%" for county_name in filters.counties)

    if filters.highway_route_families:
        placeholders = ", ".join("?" for _ in filters.highway_route_families)
        clauses.append(f"ROUTE_FAMILY IN ({placeholders})")
        params.extend(filters.highway_route_families)

    if filters.cities or filters.include_unincorporated:
        city_clauses: list[str] = []
        if filters.cities:
            placeholders = ", ".join("?" for _ in filters.cities)
            city_clauses.append(f"CITY_ID IN ({placeholders})")
            params.extend(filters.cities)
        if filters.include_unincorporated:
            city_clauses.append("CITY_ID IS NULL")
        clauses.append("(" + " OR ".join(city_clauses) + ")")

    if not clauses:
        return "", params

    return f"WHERE {' AND '.join(clauses)}", params


def _build_gpkg_where(filters: RoadwayFilters) -> str:
    clauses: list[str] = []

    for attr, column, needs_quote in _SEGMENT_FILTER_SPECS:
        values = getattr(filters, attr)
        fragment = _segment_in_list_clause(column, values, needs_quote)
        if fragment:
            clauses.append(fragment)

    if filters.counties:
        county_expression = _county_all_match_expression()
        county_patterns = " OR ".join(
            f"{county_expression} LIKE '%,{_escape_sql_literal(county_name)},%'"
            for county_name in filters.counties
        )
        clauses.append(f"({county_patterns})")

    if filters.highway_route_families:
        fragment = _segment_in_list_clause(
            "ROUTE_FAMILY", filters.highway_route_families, quote_values=True
        )
        if fragment:
            clauses.append(fragment)

    if filters.cities or filters.include_unincorporated:
        city_clauses: list[str] = []
        if filters.cities:
            city_clauses.append(
                _segment_in_list_clause("CITY_ID", filters.cities, quote_values=False)
            )
        if filters.include_unincorporated:
            city_clauses.append("CITY_ID IS NULL")
        clauses.append("(" + " OR ".join(city_clauses) + ")")

    if not clauses:
        return ""

    return f"WHERE {' AND '.join(clauses)}"


def _build_boundary_where(
    layer_name: str,
    filters: RoadwayFilters,
    county_codes: tuple[str, ...],
) -> str:
    """Generic boundary WHERE builder driven by BOUNDARY_FILTER_COLUMNS.

    Adding a new filter dimension to an existing boundary layer is a
    one-line change in the dispatch dict. This function stays unchanged.
    """
    spec = BOUNDARY_FILTER_COLUMNS.get(layer_name)
    if not spec:
        return ""

    clauses: list[str] = []
    # Counties uses county FIPS codes (COUNTYFP on the county layer). The
    # filter-level `counties` field holds display NAMES because that is
    # what the router receives; we pre-resolved those to 3-digit FIPS
    # codes in the caller (`county_codes`) specifically for the
    # county_boundaries path.
    if "counties" in spec and county_codes:
        column = spec["counties"]
        quoted = ", ".join(f"'{_escape_sql_literal(code)}'" for code in county_codes)
        clauses.append(f"{column} IN ({quoted})")

    for filter_attr, column in spec.items():
        if filter_attr == "counties":
            continue  # handled above via county_codes
        values = getattr(filters, filter_attr, ())
        if not values:
            continue
        # Quote values for string id layers (currently only MPO_ID).
        needs_quote = column == "MPO_ID"
        clauses.append(_segment_in_list_clause(column, values, quote_values=needs_quote))

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
    _PROPERTY_MIN_MAX_CACHE.clear()


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


def get_property_min_max(
    property_sql_or_column: str,
    state_code: str = "ga",
) -> tuple[float | None, float | None]:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return (None, None)

    cache_key = (property_sql_or_column, state_code)
    cached = _PROPERTY_MIN_MAX_CACHE.get(cache_key)
    if cached is not None:
        return cached

    query = f"""
        SELECT
            MIN(value_expr) AS min_value,
            MAX(value_expr) AS max_value
        FROM (
            SELECT {property_sql_or_column} AS value_expr
            FROM segments
        )
        WHERE value_expr IS NOT NULL
    """

    try:
        with _open_sqlite() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
    except Exception:
        result = (None, None)
        _PROPERTY_MIN_MAX_CACHE[cache_key] = result
        return result

    if not row or row[0] is None or row[1] is None:
        result = (None, None)
    else:
        result = (float(row[0]), float(row[1]))

    _PROPERTY_MIN_MAX_CACHE[cache_key] = result
    return result


@lru_cache(maxsize=256)
def _get_segment_count(filters: RoadwayFilters) -> int:
    where_clause, params = _build_sqlite_filters(filters)
    query = f"SELECT COUNT(*) FROM segments {where_clause}"

    with _open_sqlite() as connection:
        cursor = connection.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()

    return int(row[0]) if row else 0


@lru_cache(maxsize=256)
def _get_class_summary_rows(
    filters: RoadwayFilters,
) -> tuple[tuple[str, int, float], ...]:
    where_clause, params = _build_sqlite_filters(filters)
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
def _get_filtered_bounds(filters: RoadwayFilters) -> list[float] | None:
    if _get_segment_count(filters) == 0:
        return None

    if filters.is_empty():
        info = pyogrio.read_info(STAGED_GPKG_PATH, layer="roadway_segments")
        total_bounds = info.get("total_bounds")
        if total_bounds is None:
            return None

        return _project_bounds(tuple(float(value) for value in total_bounds))

    where_clause = _build_gpkg_where(filters)
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
    filters: RoadwayFilters | None = None,
) -> RoadwayManifestResponse:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return _empty_manifest(state_code, chunk_size)

    filters = filters or RoadwayFilters()
    total_segments = _get_segment_count(filters)
    chunk_count = math.ceil(total_segments / chunk_size) if total_segments else 0

    return RoadwayManifestResponse(
        state_code=state_code,
        total_segments=total_segments,
        chunk_size=chunk_size,
        chunk_count=chunk_count,
        bounds=_get_filtered_bounds(filters),
    )


def get_staged_roadway_summary(
    state_code: str,
    filters: RoadwayFilters | None = None,
) -> AnalyticsSummaryResponse:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return _empty_summary(state_code)

    filters = filters or RoadwayFilters()
    class_rows = _get_class_summary_rows(filters)
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
    filters: RoadwayFilters | None = None,
) -> list[float] | None:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return None

    filters = filters or RoadwayFilters()
    return _get_filtered_bounds(filters)


def get_staged_roadway_features(
    state_code: str,
    limit: int,
    offset: int = 0,
    filters: RoadwayFilters | None = None,
) -> RoadwayFeatureCollection:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return RoadwayFeatureCollection(type="FeatureCollection", features=[])

    filters = filters or RoadwayFilters()
    where_clause = _build_gpkg_where(filters)
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
            "DISTRICT_NAME AS district_name,",
            "FUNCTIONAL_CLASS AS functional_class,",
            "AADT AS aadt,",
            "segment_length_mi AS length_miles,",
            "unique_id AS unique_id,",
            # Step 2 admin geographies (geometry-authoritative).
            "AREA_OFFICE_ID AS area_office_id,",
            "AREA_OFFICE_NAME AS area_office_name,",
            "MPO_ID AS mpo_id,",
            "MPO_NAME AS mpo_name,",
            "RC_ID AS rc_id,",
            "RC_NAME AS rc_name,",
            # Step 4 overlay flags.
            "STATE_HOUSE_DISTRICT AS state_house_district,",
            "STATE_SENATE_DISTRICT AS state_senate_district,",
            "CONGRESSIONAL_DISTRICT AS congressional_district,",
            "CITY_ID AS city_id,",
            "CITY_NAME AS city_name,",
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
        district_name = (
            _normalize_text(getattr(row, "district_name", None))
            or get_district_name(district_id)
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
                    district_name=district_name,
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
                    # Step 2 admin geographies.
                    area_office_id=_normalize_int(
                        getattr(row, "area_office_id", None)
                    ),
                    area_office_name=_normalize_text(
                        getattr(row, "area_office_name", None)
                    ),
                    mpo_id=_normalize_text(getattr(row, "mpo_id", None)),
                    mpo_name=_normalize_text(getattr(row, "mpo_name", None)),
                    rc_id=_normalize_int(getattr(row, "rc_id", None)),
                    rc_name=_normalize_text(getattr(row, "rc_name", None)),
                    # Step 4 overlay flags.
                    state_house_district=_normalize_int(
                        getattr(row, "state_house_district", None)
                    ),
                    state_senate_district=_normalize_int(
                        getattr(row, "state_senate_district", None)
                    ),
                    congressional_district=_normalize_int(
                        getattr(row, "congressional_district", None)
                    ),
                    city_id=_normalize_int(getattr(row, "city_id", None)),
                    city_name=_normalize_text(getattr(row, "city_name", None)),
                ),
            )
        )

    return RoadwayFeatureCollection(type="FeatureCollection", features=features)


def get_staged_boundary_features(
    state_code: str,
    boundary_type: str,
    filters: RoadwayFilters | None = None,
    county_codes_override: tuple[str, ...] | None = None,
) -> GeoJsonFeatureCollection:
    _ensure_staged_data_cache_fresh()
    if state_code != SUPPORTED_STATE:
        return GeoJsonFeatureCollection(type="FeatureCollection", features=[])

    layer_name = BOUNDARY_TYPE_TO_LAYER.get(boundary_type)
    if layer_name is None:
        return GeoJsonFeatureCollection(type="FeatureCollection", features=[])

    filters = filters or RoadwayFilters()
    # Resolve county NAMES (from filters.counties) to 3-digit FIPS codes
    # for boundary queries that filter on COUNTYFP. Callers that already
    # hold FIPS codes (e.g. internal cascade helpers) can short-circuit
    # via county_codes_override.
    if county_codes_override is not None:
        county_codes = county_codes_override
    else:
        county_codes = _selected_county_codes(list(filters.counties))
    where_clause = _build_boundary_where(layer_name, filters, county_codes)
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


def _fetch_distinct_rows(cursor: sqlite3.Cursor, sql: str) -> list[tuple]:
    cursor.execute(sql)
    return list(cursor.fetchall())


def _city_cascade_map(
    cursor: sqlite3.Cursor,
) -> dict[int, tuple[int | None, str | None]]:
    """Build CITY_ID -> (district, county_name) using the plan's
    majority-by-length rule.

    For each city we already know: every segment with CITY_ID=X lies
    entirely within one DISTRICT and one COUNTY_CODE (Step 2 splits at
    those boundaries). So "majority city->district" is a pure segment
    aggregation - no polygon overlay needed here; the heavy lifting was
    done once during normalize.py re-segmentation. We use
    segment_length_mi (not count) to match Step 4's overlay convention.
    """
    county_code_to_name, _ = _load_county_maps()
    city_map: dict[int, tuple[int | None, str | None]] = {}

    # District cascade
    cursor.execute(
        """
        SELECT CITY_ID, DISTRICT, COALESCE(SUM(segment_length_mi), 0) AS miles
        FROM segments
        WHERE CITY_ID IS NOT NULL AND DISTRICT IS NOT NULL
        GROUP BY CITY_ID, DISTRICT
        """
    )
    district_by_city: dict[int, tuple[int, float]] = {}
    for city_id, district_value, miles in cursor.fetchall():
        city_key = int(city_id)
        district_int = _normalize_int(district_value)
        miles_float = float(miles or 0.0)
        best = district_by_city.get(city_key)
        if best is None or miles_float > best[1] or (
            miles_float == best[1] and (district_int or 0) < best[0]
        ):
            if district_int is not None:
                district_by_city[city_key] = (district_int, miles_float)

    # County cascade
    cursor.execute(
        """
        SELECT CITY_ID, COUNTY_CODE, COALESCE(SUM(segment_length_mi), 0) AS miles
        FROM segments
        WHERE CITY_ID IS NOT NULL AND COUNTY_CODE IS NOT NULL
        GROUP BY CITY_ID, COUNTY_CODE
        """
    )
    county_by_city: dict[int, tuple[str, float]] = {}
    for city_id, county_code, miles in cursor.fetchall():
        city_key = int(city_id)
        code_str = _normalize_county_code(county_code)
        miles_float = float(miles or 0.0)
        if code_str is None:
            continue
        best = county_by_city.get(city_key)
        if best is None or miles_float > best[1] or (
            miles_float == best[1] and code_str < best[0]
        ):
            county_by_city[city_key] = (code_str, miles_float)

    for city_key in set(district_by_city) | set(county_by_city):
        district_pair = district_by_city.get(city_key)
        county_pair = county_by_city.get(city_key)
        county_name = None
        if county_pair is not None:
            county_name = county_code_to_name.get(county_pair[0])
        city_map[city_key] = (
            district_pair[0] if district_pair else None,
            county_name,
        )
    return city_map


def get_staged_filter_options() -> GeorgiaFilterOptionsResponse:
    _ensure_staged_data_cache_fresh()
    county_code_to_name, _ = _load_county_maps()

    with _open_sqlite() as connection:
        cursor = connection.cursor()

        # County + District (existing).
        county_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT COUNTY_CODE, DISTRICT
            FROM segments
            WHERE COUNTY_CODE IS NOT NULL AND DISTRICT IS NOT NULL
            GROUP BY COUNTY_CODE, DISTRICT
            ORDER BY DISTRICT, COUNTY_CODE
            """,
        )

        area_office_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT AREA_OFFICE_ID, AREA_OFFICE_NAME
            FROM segments
            WHERE AREA_OFFICE_ID IS NOT NULL
            GROUP BY AREA_OFFICE_ID, AREA_OFFICE_NAME
            ORDER BY AREA_OFFICE_ID
            """,
        )

        mpo_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT MPO_ID, MPO_NAME
            FROM segments
            WHERE MPO_ID IS NOT NULL AND MPO_ID != ''
            GROUP BY MPO_ID, MPO_NAME
            ORDER BY MPO_NAME
            """,
        )

        rc_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT RC_ID, RC_NAME
            FROM segments
            WHERE RC_ID IS NOT NULL
            GROUP BY RC_ID, RC_NAME
            ORDER BY RC_NAME
            """,
        )

        state_house_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT STATE_HOUSE_DISTRICT
            FROM segments
            WHERE STATE_HOUSE_DISTRICT IS NOT NULL
            GROUP BY STATE_HOUSE_DISTRICT
            ORDER BY STATE_HOUSE_DISTRICT
            """,
        )

        state_senate_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT STATE_SENATE_DISTRICT
            FROM segments
            WHERE STATE_SENATE_DISTRICT IS NOT NULL
            GROUP BY STATE_SENATE_DISTRICT
            ORDER BY STATE_SENATE_DISTRICT
            """,
        )

        congressional_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT CONGRESSIONAL_DISTRICT
            FROM segments
            WHERE CONGRESSIONAL_DISTRICT IS NOT NULL
            GROUP BY CONGRESSIONAL_DISTRICT
            ORDER BY CONGRESSIONAL_DISTRICT
            """,
        )

        city_rows = _fetch_distinct_rows(
            cursor,
            """
            SELECT CITY_ID, CITY_NAME
            FROM segments
            WHERE CITY_ID IS NOT NULL AND CITY_NAME IS NOT NULL
            GROUP BY CITY_ID, CITY_NAME
            ORDER BY CITY_NAME
            """,
        )

        city_cascade = _city_cascade_map(cursor) if city_rows else {}

    counties = [
        CountyOption(
            county=county_code_to_name.get(
                _normalize_county_code(county_code) or "", "Unknown"
            ),
            county_fips=_normalize_county_code(county_code) or "",
            district=_normalize_int(district) or 0,
        )
        for county_code, district in county_rows
    ]
    districts = sorted({county.district for county in counties if county.district > 0})

    area_offices = [
        AreaOfficeOption(
            id=int(area_office_id),
            label=_normalize_text(area_office_name) or f"Area Office {area_office_id}",
            parent_district=int(area_office_id) // 100,
        )
        for area_office_id, area_office_name in area_office_rows
        if _normalize_int(area_office_id) is not None
    ]

    mpos = [
        MpoOption(
            id=str(mpo_id).strip(),
            label=_normalize_text(mpo_name) or str(mpo_id).strip(),
        )
        for mpo_id, mpo_name in mpo_rows
        if str(mpo_id).strip()
    ]

    regional_commissions = [
        RegionalCommissionOption(
            id=int(rc_id),
            label=_normalize_text(rc_name) or f"RC {rc_id}",
        )
        for rc_id, rc_name in rc_rows
        if _normalize_int(rc_id) is not None
    ]

    state_house_districts = [
        StateHouseOption(
            id=int(district_id),
            label=f"House District {int(district_id)}",
        )
        for (district_id,) in state_house_rows
        if _normalize_int(district_id) is not None
    ]

    state_senate_districts = [
        StateSenateOption(
            id=int(district_id),
            label=f"Senate District {int(district_id)}",
        )
        for (district_id,) in state_senate_rows
        if _normalize_int(district_id) is not None
    ]

    congressional_districts = [
        CongressionalOption(
            id=int(district_id),
            label=f"Congressional District {int(district_id)}",
        )
        for (district_id,) in congressional_rows
        if _normalize_int(district_id) is not None
    ]

    cities: list[CityOption] = []
    for city_id, city_name in city_rows:
        city_int = _normalize_int(city_id)
        if city_int is None:
            continue
        district_id, county_name = city_cascade.get(city_int, (None, None))
        cities.append(
            CityOption(
                id=city_int,
                label=_normalize_text(city_name) or f"City {city_int}",
                county=county_name,
                district=district_id,
            )
        )

    return GeorgiaFilterOptionsResponse(
        districts=[
            DistrictOption(id=district_id, label=get_district_name(district_id))
            for district_id in districts
        ],
        counties=sorted(counties, key=lambda item: item.county),
        highway_types=list_highway_type_options(),
        area_offices=area_offices,
        mpos=mpos,
        regional_commissions=regional_commissions,
        state_house_districts=state_house_districts,
        state_senate_districts=state_senate_districts,
        congressional_districts=congressional_districts,
        cities=cities,
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
    district_name = (
        _normalize_text(row["DISTRICT_NAME"]) if "DISTRICT_NAME" in row.keys() else None
    ) or get_district_name(district_id)

    return RoadwayDetailResponse(
        unique_id=unique_id,
        road_name=_format_road_name(hwy_name, route_id),
        district=district_id,
        district_name=district_name,
        county=county_name,
        attributes=attributes,
    )
