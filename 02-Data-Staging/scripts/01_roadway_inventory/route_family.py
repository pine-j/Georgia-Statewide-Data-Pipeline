"""Georgia-specific route-family classification helpers.

This module implements a conservative signed-route crosswalk for the GDOT
16-character `ROUTE_ID`. The official Georgia source documents are:

- GDOT Road & Traffic Data landing page
- GDOT Understanding Route IDs guide
- GDOT Road Inventory Data Dictionary
- GDOT live LRS metadata

The implementation intentionally separates:

- broad family: Interstate / U.S. Route / State Route / Local/Other
- detail: business, spur, ramp, frontage road, county road, city street, etc.
- confidence: high for Interstate and local/public cases; medium for U.S./State
  separation because Georgia route IDs encode state route numbers
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = (
    PROJECT_ROOT / "02-Data-Staging" / "config" / "georgia_route_family_crosswalk.json"
)

RULES = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
INTERSTATE_ROUTE_NUMBERS = frozenset(int(value) for value in RULES["interstate_route_numbers"])
US_ROUTE_NUMBERS = frozenset(int(value) for value in RULES["us_route_numbers"])
ROUTE_SUFFIX_LABELS = RULES["route_suffix_labels"]

FUNCTION_TYPE_DETAILS = {
    "2": "Ramp",
    "3": "Collector Distributor",
    "4": "Ramp-CD Connector",
    "5": "Frontage Road",
    "6": "Alley",
    "7": "Separate Managed Facility",
    "8": "Y-Connector",
    "9": "Private",
    "R": "Roundabout",
}


def _clean_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().upper()


def extract_base_route_number(route_id: str, function_type: str | None = None) -> int | None:
    """Return the family-level route number used for Interstate / US / State tests.

    GDOT's route guide says:
    - mainline-style route IDs use a six-character route code
    - function types 2-4 use digits 6-8 as reference post and digits 9-11 as
      the underlying route number
    """

    raw_route_id = _clean_text(route_id)
    if len(raw_route_id) < 13:
        return None

    route_code = raw_route_id[5:11]
    if not route_code.isdigit():
        return None

    function = _clean_text(function_type) or raw_route_id[0:1]
    if function in {"2", "3", "4"}:
        return int(route_code[-3:])

    return int(route_code)


def _state_family_detail(family: str, function_type: str, suffix: str) -> str:
    if function_type in FUNCTION_TYPE_DETAILS:
        detail = FUNCTION_TYPE_DETAILS[function_type]
        if function_type == "7" and suffix in ROUTE_SUFFIX_LABELS:
            return f"{family} {ROUTE_SUFFIX_LABELS[suffix]}"
        return f"{family} {detail}"

    suffix_label = ROUTE_SUFFIX_LABELS.get(suffix)
    if suffix_label:
        return f"{family} {suffix_label}"
    return family


def _local_other_detail(function_type: str, system_code: str, suffix: str) -> tuple[str, str]:
    if system_code == "3":
        if function_type in FUNCTION_TYPE_DETAILS and function_type != "9":
            return f"Private Road {FUNCTION_TYPE_DETAILS[function_type]}", f"function_type_{function_type}"
        return "Private Road", "system_code_3_private"

    if system_code == "4":
        if function_type in FUNCTION_TYPE_DETAILS and function_type != "9":
            return f"Federal Route {FUNCTION_TYPE_DETAILS[function_type]}", f"function_type_{function_type}"
        return "Federal Route", "system_code_4_federal"

    if function_type in {"2", "3", "4", "5", "6", "7", "8", "9", "R"}:
        detail = FUNCTION_TYPE_DETAILS.get(function_type, "Other")
        if function_type == "9":
            return "Private Road", "function_type_9"
        return f"Public Road {detail}", f"function_type_{function_type}"

    if system_code == "2":
        if suffix == "00":
            return "County Road", "public_road_suffix_00"
        if len(suffix) == 2 and suffix.isdigit():
            return "City Street", "public_road_numeric_suffix"
        return "Public Road", "public_road_fallback"

    return "Other", "unknown_system_code"


def classify_route_family(
    route_id: str,
    function_type: str | None = None,
    system_code: str | None = None,
) -> dict[str, object]:
    raw_route_id = _clean_text(route_id)
    parsed_function_type = _clean_text(function_type) or raw_route_id[0:1]
    parsed_system_code = _clean_text(system_code) or raw_route_id[4:5]
    suffix = raw_route_id[11:13] if len(raw_route_id) >= 13 else ""
    base_route_number = extract_base_route_number(raw_route_id, parsed_function_type)

    family = None
    detail = None
    confidence = "low"
    source = "route_id_unparsed"

    if parsed_system_code == "1":
        if base_route_number in INTERSTATE_ROUTE_NUMBERS:
            family = "Interstate"
            confidence = "high"
            source = "gdot_appendix_f_interstate"
        elif base_route_number in US_ROUTE_NUMBERS:
            family = "U.S. Route"
            confidence = "medium"
            source = "gdot_appendix_g_us_route"
        else:
            family = "State Route"
            confidence = "medium"
            source = "system_code_1_state_route_fallback"
        detail = _state_family_detail(family, parsed_function_type, suffix)
    elif parsed_system_code in {"2", "3", "4"}:
        family = "Local/Other"
        confidence = "high"
        detail, source = _local_other_detail(parsed_function_type, parsed_system_code, suffix)
    elif raw_route_id:
        family = "Local/Other"
        detail = "Other"

    return {
        "BASE_ROUTE_NUMBER": base_route_number,
        "ROUTE_SUFFIX_LABEL": ROUTE_SUFFIX_LABELS.get(suffix),
        "ROUTE_FAMILY": family,
        "ROUTE_FAMILY_DETAIL": detail,
        "ROUTE_FAMILY_CONFIDENCE": confidence if family else None,
        "ROUTE_FAMILY_SOURCE": source if family else None,
    }


def classify_route_families(df: pd.DataFrame) -> pd.DataFrame:
    records = [
        classify_route_family(route_id, function_type, system_code)
        for route_id, function_type, system_code in zip(
            df.get("ROUTE_ID", pd.Series(index=df.index, dtype="object")),
            df.get("PARSED_FUNCTION_TYPE", pd.Series(index=df.index, dtype="object")),
            df.get("PARSED_SYSTEM_CODE", pd.Series(index=df.index, dtype="object")),
        )
    ]
    return pd.DataFrame.from_records(records, index=df.index)
