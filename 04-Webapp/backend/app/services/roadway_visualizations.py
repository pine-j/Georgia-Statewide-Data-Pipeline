from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.schemas import (
    RoadwayLegendItem,
    RoadwayVisualizationCatalogResponse,
    RoadwayVisualizationOption,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
CROSSWALK_PATH = REPO_ROOT / "00-Project-Management" / "Texas_vs_Georgia_Header_Crosswalk.csv"
DEFAULT_VISUALIZATION_ID = "aadt"
NO_DATA_COLOR = "#b9c5ca"


def _numeric_legend(
    items: list[tuple[float | None, float | None, str, str]],
) -> list[RoadwayLegendItem]:
    return [
        RoadwayLegendItem(
            min_value=min_value,
            max_value=max_value,
            label=label,
            color=color,
        )
        for min_value, max_value, label, color in items
    ]


def _categorical_legend(
    items: list[tuple[str, str, str]],
) -> list[RoadwayLegendItem]:
    return [
        RoadwayLegendItem(
            value=value,
            label=label,
            color=color,
        )
        for value, label, color in items
    ]


def get_hwy_des_sql_expression(
    *,
    num_lanes_column: str = "NUM_LANES",
    median_type_column: str = "MEDIAN_TYPE",
    route_family_column: str = "ROUTE_FAMILY",
    access_control_column: str = "HPMS_ACCESS_CONTROL",
) -> str:
    return f"""
        CASE
            WHEN {num_lanes_column} IS NULL OR {num_lanes_column} <= 0 THEN NULL
            WHEN (
                {median_type_column} IS NOT NULL AND {median_type_column} <> 1
            ) OR COALESCE({route_family_column}, '') = 'Interstate'
              OR (
                  {access_control_column} IS NOT NULL
                  AND {access_control_column} IN (1, 2, 3)
                  AND {num_lanes_column} >= 4
              )
            THEN CAST(CAST({num_lanes_column} AS INTEGER) AS TEXT) || 'D'
            ELSE CAST(CAST({num_lanes_column} AS INTEGER) AS TEXT) || 'U'
        END
    """.strip()


def derive_percent_of_aadt(numerator: Any, aadt: Any) -> float | None:
    try:
        numerator_value = float(numerator)
        aadt_value = float(aadt)
    except (TypeError, ValueError):
        return None

    if aadt_value <= 0:
        return None

    return round((numerator_value * 100.0) / aadt_value, 1)


def derive_hwy_des(
    num_lanes: Any,
    median_type: Any,
    route_family: Any,
    hpms_access_control: Any,
) -> str | None:
    try:
        lane_count = int(float(num_lanes))
    except (TypeError, ValueError):
        return None

    if lane_count <= 0:
        return None

    divided = False

    try:
        if median_type is not None and int(float(median_type)) != 1:
            divided = True
    except (TypeError, ValueError):
        pass

    if isinstance(route_family, str) and route_family.strip() == "Interstate":
        divided = True

    try:
        access_control = int(float(hpms_access_control))
    except (TypeError, ValueError):
        access_control = None

    if access_control in {1, 2, 3} and lane_count >= 4:
        divided = True

    suffix = "D" if divided else "U"
    return f"{lane_count}{suffix}"


THEMATIC_PROPERTY_SQL: dict[str, str] = {
    "aadt": "AADT",
    "system_code_label": "SYSTEM_CODE_LABEL",
    "direction_label": "DIRECTION_LABEL",
    "num_lanes": "NUM_LANES",
    "future_aadt_2044": "FUTURE_AADT_2044",
    "k_factor": "K_FACTOR",
    "d_factor": "D_FACTOR",
    "truck_aadt": "TRUCK_AADT",
    "pct_sadt": "ROUND((SINGLE_UNIT_AADT_2024 * 100.0) / NULLIF(AADT, 0), 1)",
    "pct_cadt": "ROUND((COMBO_UNIT_AADT_2024 * 100.0) / NULLIF(AADT, 0), 1)",
    "vmt": "VMT",
    "nhs_ind_label": "NHS_IND_LABEL",
    "median_type_label": "MEDIAN_TYPE_LABEL",
    "hwy_des": get_hwy_des_sql_expression(),
    "speed_limit": "SPEED_LIMIT",
    "truck_pct": "TRUCK_PCT",
    "functional_class_viz": "FUNCTIONAL_CLASS",
    "surface_type_label": "SURFACE_TYPE_LABEL",
    "ownership_label": "OWNERSHIP_LABEL",
    "facility_type_label": "FACILITY_TYPE_LABEL",
}


THEMATIC_FIELD_CONFIGS: dict[str, dict[str, Any]] = {
    "HSYS": {
        "id": "system_code",
        "label": "Roadway System",
        "description": "Separate the state highway network from the broader public-road inventory.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "system_code_label",
        "legend_items": _categorical_legend(
            [
                ("State Highway Route", "State Highway Route", "#0f766e"),
                ("Public Road", "Public Road", "#c57b57"),
            ]
        ),
    },
    "DI": {
        "id": "direction",
        "label": "Milepoint Direction",
        "description": "Show whether segment measures increase or decrease along the route.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "direction_label",
        "legend_items": _categorical_legend(
            [
                ("Increasing", "Increasing", "#2878b5"),
                ("Decreasing", "Decreasing", "#c86033"),
            ]
        ),
    },
    "NUM_LANES": {
        "id": "num_lanes",
        "label": "Number of Lanes",
        "description": "Color segments by the normalized lane count used in the Georgia staging pipeline.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "num_lanes",
        "legend_items": _numeric_legend(
            [
                (1, 1, "1 lane", "#f5efe6"),
                (2, 2, "2 lanes", "#e7d5b0"),
                (3, 3, "3 lanes", "#d9b172"),
                (4, 4, "4 lanes", "#c67d36"),
                (5, 5, "5 lanes", "#9f541d"),
                (6, None, "6+ lanes", "#6c330b"),
            ]
        ),
    },
    "ADT_CUR": {
        "id": "aadt",
        "label": "AADT",
        "description": "Annual Average Daily Traffic volume.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "aadt",
        "unit": "vehicles/day",
        "default": True,
        "legend_items": _numeric_legend(
            [
                (0, 1000, "0 to 1,000", "#edf8fb"),
                (1000, 5000, "1,000 to 5,000", "#ccece6"),
                (5000, 15000, "5,000 to 15,000", "#99d8c9"),
                (15000, 50000, "15,000 to 50,000", "#2ca25f"),
                (50000, None, "50,000+", "#0b5d46"),
            ]
        ),
    },
    "AADT_DESGN": {
        "id": "future_aadt_2044",
        "label": "2044 Future AADT",
        "description": "Use Georgia's staged 2044 future AADT as the closest current preview of Texas design AADT.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "future_aadt_2044",
        "unit": "vehicles/day",
        "legend_items": _numeric_legend(
            [
                (0, 1000, "0 to 1,000", "#f4f7fb"),
                (1000, 5000, "1,000 to 5,000", "#d2dfef"),
                (5000, 15000, "5,000 to 15,000", "#9fbfdf"),
                (15000, 50000, "15,000 to 50,000", "#4b88c6"),
                (50000, None, "50,000+", "#1f5f93"),
            ]
        ),
    },
    "K_FAC": {
        "id": "k_factor",
        "label": "K Factor",
        "description": "Highlight the share of daily traffic expected in the peak design hour.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "k_factor",
        "unit": "percent",
        "legend_items": _numeric_legend(
            [
                (0, 0, "0", "#fff3d8"),
                (1, 9, "1 to 9", "#f6cc82"),
                (10, 14, "10 to 14", "#e49a4e"),
                (15, 19, "15 to 19", "#c86a28"),
                (20, None, "20+", "#8b3f12"),
            ]
        ),
    },
    "D_FAC": {
        "id": "d_factor",
        "label": "D Factor",
        "description": "Show directional split where the staged traffic factor is available.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "d_factor",
        "unit": "percent",
        "legend_items": _numeric_legend(
            [
                (1, 1, "1", "#fff5da"),
                (2, 50, "2 to 50", "#f3d189"),
                (51, 60, "51 to 60", "#dca355"),
                (61, 70, "61 to 70", "#b9702b"),
                (71, None, "71+", "#7d4310"),
            ]
        ),
    },
    "AADT_TRUCKS": {
        "id": "truck_aadt",
        "label": "Truck AADT",
        "description": "Annual Average Daily Traffic volume for trucks.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "truck_aadt",
        "unit": "trucks/day",
        "legend_items": _numeric_legend(
            [
                (0, 100, "0 to 100", "#eef7fb"),
                (100, 500, "100 to 500", "#c9e4f1"),
                (500, 2000, "500 to 2,000", "#8ac4de"),
                (2000, 10000, "2,000 to 10,000", "#3d8fc0"),
                (10000, None, "10,000+", "#12577e"),
            ]
        ),
    },
    "PCT_SADT": {
        "id": "pct_sadt",
        "label": "Single-Unit Truck Share",
        "description": "Derived in the web app from single-unit truck AADT divided by total AADT.",
        "kind": "numeric",
        "implementation_status": "derived",
        "property_name": "pct_sadt",
        "unit": "percent",
        "legend_items": _numeric_legend(
            [
                (0, 2, "0% to 2%", "#fff4dc"),
                (2, 5, "2% to 5%", "#f6cf8b"),
                (5, 10, "5% to 10%", "#e59d54"),
                (10, 20, "10% to 20%", "#c3692a"),
                (20, None, "20%+", "#823d11"),
            ]
        ),
    },
    "PCT_CADT": {
        "id": "pct_cadt",
        "label": "Combination Truck Share",
        "description": "Derived in the web app from combination-unit truck AADT divided by total AADT.",
        "kind": "numeric",
        "implementation_status": "derived",
        "property_name": "pct_cadt",
        "unit": "percent",
        "legend_items": _numeric_legend(
            [
                (0, 2, "0% to 2%", "#fff6df"),
                (2, 5, "2% to 5%", "#f0d39b"),
                (5, 10, "5% to 10%", "#d2aa64"),
                (10, 20, "10% to 20%", "#a87934"),
                (20, None, "20%+", "#6e4f17"),
            ]
        ),
    },
    "DVMT": {
        "id": "vmt",
        "label": "Vehicle Miles Traveled",
        "description": "Preview segment-level VMT as the current Georgia stand-in for Texas DVMT.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "vmt",
        "unit": "vehicle-miles/day",
        "legend_items": _numeric_legend(
            [
                (0, 100, "0 to 100", "#eff7f3"),
                (100, 500, "100 to 500", "#cae7d8"),
                (500, 2000, "500 to 2,000", "#95cfb1"),
                (2000, 10000, "2,000 to 10,000", "#4caa78"),
                (10000, None, "10,000+", "#1c6b45"),
            ]
        ),
    },
    "NHFN": {
        "id": "nhs_ind",
        "label": "NHS Role",
        "description": "Use the staged NHS indicator labels as the closest current freight-network stand-in.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "nhs_ind_label",
        "legend_items": _categorical_legend(
            [
                ("Non Connector NHS", "Non-Connector NHS", "#287271"),
                ("Major Airport", "Major Airport", "#4e9f3d"),
                ("Major Port Facility", "Major Port Facility", "#2176ae"),
                ("Major Amtrak Station", "Major Amtrak Station", "#8d5a97"),
                ("Major Rail/Truck Terminal", "Major Rail/Truck Terminal", "#c1666b"),
                (
                    "Major Public Transportation or Multi-Modal Passenger Terminal",
                    "Major Transit or Multi-Modal Terminal",
                    "#c9842e",
                ),
                ("Major Pipeline Terminal", "Major Pipeline Terminal", "#5b6c5d"),
            ]
        ),
    },
    "MED_TYPE": {
        "id": "median_type",
        "label": "Median Type",
        "description": "Show the staged median classification where GDOT or HPMS data populated it.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "median_type_label",
        "legend_items": _categorical_legend(
            [
                ("No Median", "No Median", "#9d8f7b"),
                ("Unprotected", "Unprotected", "#dd9c4b"),
                ("Curbed", "Curbed", "#c86c5b"),
                ("Positive Barrier (unspecified)", "Positive Barrier", "#5b8fb9"),
                ("Positive Barrier (flexible)", "Flexible Barrier", "#4aa1a9"),
                ("Positive Barrier (rigid)", "Rigid Barrier", "#2f6f94"),
                ("Positive Barrier (cable)", "Cable Barrier", "#658a3d"),
            ]
        ),
    },
    "HWY_DES": {
        "id": "hwy_des",
        "label": "Highway Design",
        "description": "Derived cross-section class based on lane count and divided-versus-undivided heuristics.",
        "kind": "categorical",
        "implementation_status": "derived",
        "property_name": "hwy_des",
        "legend_items": _categorical_legend(
            [
                ("1U", "1-lane undivided", "#f4eee5"),
                ("2U", "2-lane undivided", "#ebd7b1"),
                ("3U", "3-lane undivided", "#dca867"),
                ("4U", "4-lane undivided", "#bf7431"),
                ("5U", "5-lane undivided", "#944e1e"),
                ("6U", "6-lane undivided", "#6b320f"),
                ("1D", "1-lane divided", "#e7f3f7"),
                ("2D", "2-lane divided", "#bddfed"),
                ("3D", "3-lane divided", "#89bddc"),
                ("4D", "4-lane divided", "#4f90c0"),
                ("5D", "5-lane divided", "#1d6c99"),
                ("6D", "6-lane divided", "#0b4d72"),
            ]
        ),
    },
    "SPD_MAX": {
        "id": "speed_limit",
        "label": "Speed Limit",
        "description": "Posted speed limit.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "speed_limit",
        "unit": "mph",
        "legend_items": _numeric_legend(
            [
                (0, 25, "0 to 25", "#f7fcf5"),
                (25, 35, "25 to 35", "#c7e9c0"),
                (35, 45, "35 to 45", "#74c476"),
                (45, 55, "45 to 55", "#31a354"),
                (55, 65, "55 to 65", "#006d2c"),
                (65, None, "65+", "#00441b"),
            ]
        ),
    },
    "T_FLAG": {
        "id": "truck_pct",
        "label": "Truck Percentage",
        "description": "Percentage of total traffic that is trucks.",
        "kind": "numeric",
        "implementation_status": "staged",
        "property_name": "truck_pct",
        "unit": "percent",
        "legend_items": _numeric_legend(
            [
                (0, 5, "0% to 5%", "#f7fbff"),
                (5, 10, "5% to 10%", "#c6dbef"),
                (10, 20, "10% to 20%", "#6baed6"),
                (20, 35, "20% to 35%", "#2171b5"),
                (35, None, "35%+", "#084594"),
            ]
        ),
    },
    "F_SYSTEM": {
        "id": "functional_class_viz",
        "label": "Functional Classification",
        "description": "Federal functional classification of the roadway.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "functional_class_viz",
        "legend_items": _categorical_legend(
            [
                ("1", "Interstate", "#e41a1c"),
                ("2", "Principal Arterial - Other Freeways", "#ff7f00"),
                ("3", "Principal Arterial - Other", "#fdbf6f"),
                ("4", "Minor Arterial", "#33a02c"),
                ("5", "Major Collector", "#1f78b4"),
                ("6", "Minor Collector", "#a6cee3"),
                ("7", "Local", "#b2df8a"),
            ]
        ),
    },
    "SURF_TYP": {
        "id": "surface_type",
        "label": "Surface Type",
        "description": "Road surface material classification.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "surface_type_label",
        "legend_items": _categorical_legend(
            [
                ("Unpaved", "Unpaved", "#d9b38c"),
                ("Gravel", "Gravel", "#c4a882"),
                ("Asphalt", "Asphalt", "#4a4a4a"),
                ("Concrete", "Concrete", "#8c8c8c"),
                ("Composite", "Composite", "#6b8e6b"),
                ("Brick/Block", "Brick/Block", "#c0504d"),
            ]
        ),
    },
    "OWNER": {
        "id": "ownership",
        "label": "Ownership",
        "description": "Jurisdictional ownership of the roadway.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "ownership_label",
        "legend_items": _categorical_legend(
            [
                ("State Highway Agency", "State Highway Agency", "#2171b5"),
                ("County Highway Agency", "County Highway Agency", "#6baed6"),
                ("City or Municipal", "City or Municipal", "#bdd7e7"),
                ("Federal Agency", "Federal Agency", "#08519c"),
                ("Other", "Other", "#969696"),
            ]
        ),
    },
    "FAC_TYPE": {
        "id": "facility_type",
        "label": "Facility Type",
        "description": "Access control and facility classification.",
        "kind": "categorical",
        "implementation_status": "staged",
        "property_name": "facility_type_label",
        "legend_items": _categorical_legend(
            [
                ("One-Way Roadway", "One-Way Roadway", "#e7298a"),
                ("Two-Way Roadway", "Two-Way Roadway", "#66a61e"),
                ("Ramp", "Ramp", "#e6ab02"),
                ("Non Mainlane", "Non Mainlane", "#a6761d"),
                ("Non Inventory Direction", "Non Inventory Direction", "#666666"),
            ]
        ),
    },
}


DETAILS_ONLY_FIELD_CONFIGS: dict[str, dict[str, Any]] = {
    "RDBD_ID": {
        "id": "unique_id",
        "label": "Segment ID",
        "description": "Unique segment identifiers are available in the segment popup for inspection and joins.",
        "implementation_status": "popup_only",
        "property_name": "unique_id",
    },
    "HWY": {
        "id": "hwy_name",
        "label": "Highway Name",
        "description": "Highway names have too many distinct values for a readable statewide legend, so they stay in the popup.",
        "implementation_status": "popup_only",
        "property_name": "road_name",
    },
    "FRM_DFO": {
        "id": "from_milepoint",
        "label": "From Milepoint",
        "description": "Begin milepoints remain available in the segment popup for precise corridor review.",
        "implementation_status": "popup_only",
        "property_name": "FROM_MILEPOINT",
    },
    "TO_DFO": {
        "id": "to_milepoint",
        "label": "To Milepoint",
        "description": "End milepoints remain available in the segment popup for precise corridor review.",
        "implementation_status": "popup_only",
        "property_name": "TO_MILEPOINT",
    },
    "CO": {
        "id": "county",
        "label": "County",
        "description": "County remains available in the segment popup while the county boundary overlay handles statewide geography context.",
        "implementation_status": "popup_only",
        "property_name": "county",
    },
}


def _configured_texas_headers() -> list[str]:
    return list(THEMATIC_FIELD_CONFIGS) + list(DETAILS_ONLY_FIELD_CONFIGS)


def _clean_row(row: dict[str, str]) -> dict[str, str]:
    return {key: (value or "").strip() for key, value in row.items()}


@lru_cache(maxsize=1)
def _load_crosswalk_rows() -> list[dict[str, str]]:
    if not CROSSWALK_PATH.exists():
        return []

    with CROSSWALK_PATH.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [_clean_row(row) for row in reader]


def _build_option(
    row: dict[str, str],
    config: dict[str, Any],
    *,
    map_mode: str,
) -> RoadwayVisualizationOption:
    georgia_header = row.get("Georgia Final Output Header") or None
    notes = row.get("Notes") or None
    status = row.get("Current Final Output Status") or "Unknown"

    return RoadwayVisualizationOption(
        id=config["id"],
        texas_header=row.get("Texas Header", config["id"]),
        georgia_header=georgia_header,
        label=config["label"],
        description=config["description"],
        notes=notes,
        kind=config.get("kind"),
        map_mode=map_mode,  # type: ignore[arg-type]
        implementation_status=config["implementation_status"],  # type: ignore[arg-type]
        property_name=config.get("property_name"),
        status=status,
        unit=config.get("unit"),
        default=bool(config.get("default", False)),
        no_data_color=config.get("no_data_color", NO_DATA_COLOR),
        legend_items=config.get("legend_items", []),
    )


def _build_unavailable_option(row: dict[str, str]) -> RoadwayVisualizationOption:
    georgia_header = row.get("Georgia Final Output Header") or None
    notes = row.get("Notes") or None

    return RoadwayVisualizationOption(
        id=(row.get("Texas Header", "unknown") or "unknown").lower(),
        texas_header=row.get("Texas Header", "Unknown"),
        georgia_header=georgia_header,
        label=row.get("Texas Header", "Unavailable field"),
        description="This crosswalk field does not yet have a usable staged Georgia roadway value for map preview.",
        notes=notes,
        map_mode="unavailable",
        implementation_status="unavailable",
        status=row.get("Current Final Output Status") or "Missing",
        no_data_color=NO_DATA_COLOR,
    )


def get_roadway_visualization_catalog() -> RoadwayVisualizationCatalogResponse:
    crosswalk_rows = _load_crosswalk_rows()
    thematic_options: list[RoadwayVisualizationOption] = []
    details_only_options: list[RoadwayVisualizationOption] = []
    unavailable_options: list[RoadwayVisualizationOption] = []
    seen_headers: set[str] = set()

    for row in crosswalk_rows:
        texas_header = row.get("Texas Header")
        if not texas_header or texas_header == "geometry":
            continue

        seen_headers.add(texas_header)

        if texas_header in THEMATIC_FIELD_CONFIGS:
            thematic_options.append(
                _build_option(
                    row,
                    THEMATIC_FIELD_CONFIGS[texas_header],
                    map_mode="thematic",
                )
            )
            continue

        if texas_header in DETAILS_ONLY_FIELD_CONFIGS:
            details_only_options.append(
                _build_option(
                    row,
                    DETAILS_ONLY_FIELD_CONFIGS[texas_header],
                    map_mode="details_only",
                )
            )
            continue

        unavailable_options.append(_build_unavailable_option(row))

    for texas_header in _configured_texas_headers():
        if texas_header in seen_headers:
            continue

        config = THEMATIC_FIELD_CONFIGS.get(texas_header) or DETAILS_ONLY_FIELD_CONFIGS.get(texas_header)
        if config is None:
            continue

        fallback_row = {
            "Texas Header": texas_header,
            "Georgia Final Output Header": "",
            "Current Final Output Status": "Unknown",
            "Notes": "",
        }
        target = thematic_options if texas_header in THEMATIC_FIELD_CONFIGS else details_only_options
        target.append(
            _build_option(
                fallback_row,
                config,
                map_mode="thematic" if texas_header in THEMATIC_FIELD_CONFIGS else "details_only",
            )
        )

    return RoadwayVisualizationCatalogResponse(
        default_option_id=DEFAULT_VISUALIZATION_ID,
        thematic_options=thematic_options,
        details_only_options=details_only_options,
        unavailable_options=unavailable_options,
    )
