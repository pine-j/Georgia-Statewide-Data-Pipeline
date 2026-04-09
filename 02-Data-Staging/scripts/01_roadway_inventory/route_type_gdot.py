"""Georgia GDOT route-type classification and highway-name helpers.

This module derives a single granular `ROUTE_TYPE_GDOT` code for every
segment using only fields already materialized in the staged roadway dataset.

Priority order:
1. FHWA HPMS route signing for signed-family distinction
2. GDOT route function / suffix structure for route subtype
3. GDOT route-family fallback fields where HPMS does not resolve the segment
4. Local-system fallback from public-road suffix patterns
"""

from __future__ import annotations

import re
from collections import Counter

import pandas as pd

from utils import _clean_text

INTERSTATE_ROUTE_NUMBER_TO_SIGNED = {
    401: 75,
    402: 20,
    403: 85,
    404: 16,
    405: 95,
    406: 59,
    407: 285,
    408: 475,
    409: 24,
    411: 185,
    413: 675,
    415: 520,
    417: 575,
    419: 985,
    421: 516,
}

BUSINESS_SUFFIXES = {"BU", "SB"}
GENERIC_SUFFIX_ROUTE_TYPES = {
    "SP": "SP",
    "SE": "SP",
    "CO": "CN",
    "EC": "CN",
    "CW": "CN",
    "LO": "LP",
    "AL": "AL",
    "AS": "AL",
    "BY": "BY",
}
EXPRESS_SUFFIXES = {"XL", "XN", "XS", "XE", "XW"}

ROUTE_NAME_NUMBER_PATTERNS = (
    re.compile(r"(?i)(?:^|\b)I[- ]?(\d{1,4})(?:\b|;)"),
    re.compile(r"(?i)(?:^|\b)INTERSTATE\s+(\d{1,4})(?:\b|;)"),
    re.compile(r"(?i)(?:^|\b)US(?:\s+HIGHWAY)?\s*(\d{1,4})(?:\b|;)"),
    re.compile(r"(?i)(?:^|\b)(\d{1,4})(?:AL|BU|BY|CO|SP|LO|WE|EA|SO|NO|SE|SB|AS|CW|EC)(?:\b|;)"),
    re.compile(r"(?i)(?:^|\b)SR[- ]?(\d{1,4})(?:\b|;)"),
    re.compile(r"(?i)(?:^|\b)STATE\s+ROUTE\s+(\d{1,4})(?:\b|;)"),
)
def _clean_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_route_number_from_name(value) -> int | None:
    text = value if isinstance(value, str) else ""
    if not text:
        return None
    for pattern in ROUTE_NAME_NUMBER_PATTERNS:
        match = pattern.search(text)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                continue
    return None


def _hpms_signed_family(signing_value) -> str | None:
    signing = _clean_int(signing_value)
    if signing in {2, 5}:
        return "Interstate"
    if signing == 3:
        return "U.S. Route"
    if signing == 4:
        return "State Route"
    if signing in {6, 7}:
        return "County Road"
    if signing == 8:
        return "City Street"
    if signing in {9, 10}:
        return "Local/Other"
    return None


def _fallback_signed_family(
    route_family: str,
    signed_route_family_primary: str,
    signed_route_verify_source: str,
    hpms_route_signing,
) -> str | None:
    verify_source = _clean_text(signed_route_verify_source).lower()
    hpms_family = _hpms_signed_family(hpms_route_signing)

    if verify_source.startswith("gdot_"):
        if signed_route_family_primary in {"Interstate", "U.S. Route", "State Route"}:
            return signed_route_family_primary
    elif verify_source in {"hpms_2024", "route_id_crosswalk"}:
        if hpms_family in {"Interstate", "U.S. Route", "State Route"}:
            return hpms_family

    if signed_route_family_primary in {"Interstate", "U.S. Route", "State Route"}:
        return signed_route_family_primary

    if route_family in {"Interstate", "U.S. Route", "State Route"}:
        return route_family

    return None


def _fallback_local_family(system_code, function_type, route_suffix: str, hpms_route_signing) -> str | None:
    system = _clean_int(system_code)
    function = _clean_int(function_type)
    hpms_family = _hpms_signed_family(hpms_route_signing)

    if system == 3 or function == 9:
        return "Private Road"
    if system == 4:
        return "Federal Route"
    if hpms_family == "County Road":
        return "County Road"
    if hpms_family == "City Street":
        return "City Street"
    if system == 2 and function == 1:
        if route_suffix == "00":
            return "County Road"
        if len(route_suffix) == 2 and route_suffix.isdigit() and route_suffix != "00":
            return "City Street"
    return None


def _classify_route_type(
    function_type,
    route_suffix: str,
    signed_family: str | None,
    local_family: str | None,
) -> str:
    function = _clean_int(function_type)

    if function == 2:
        return "RP"
    if function == 3:
        return "CD"
    if function == 4:
        return "RC"
    if function == 5:
        return "FR"
    if function == 6:
        return "ALY"
    if function == 7 or route_suffix in EXPRESS_SUFFIXES:
        return "ML"
    if function == 8:
        return "YC"

    if route_suffix in BUSINESS_SUFFIXES:
        if signed_family == "Interstate":
            return "BI"
        if signed_family == "U.S. Route":
            return "BU"
        return "BS"

    if route_suffix in GENERIC_SUFFIX_ROUTE_TYPES:
        return GENERIC_SUFFIX_ROUTE_TYPES[route_suffix]

    if signed_family == "Interstate":
        return "I"
    if signed_family == "U.S. Route":
        return "US"
    if signed_family == "State Route":
        return "SR"
    if local_family == "County Road":
        return "CR"
    if local_family == "City Street":
        return "CS"
    if local_family == "Private Road":
        return "PR"
    if local_family == "Federal Route":
        return "FED"
    return "OT"


def _display_prefix(route_type: str, signed_family: str | None, local_family: str | None) -> str | None:
    if route_type in {"I", "BI"} or signed_family == "Interstate":
        return "I"
    if route_type in {"US", "BU"} or signed_family == "U.S. Route":
        return "US"
    if route_type in {"SR", "BS"} or signed_family == "State Route":
        return "SR"
    if route_type == "CR" or local_family == "County Road":
        return "CR"
    if route_type == "CS" or local_family == "City Street":
        return "CS"
    return None


def _canonical_lookup_key(route_id: str, display_prefix: str | None) -> tuple[str, str | None]:
    route_base_key = route_id[:-3] if route_id else ""
    return route_base_key, display_prefix


def _pick_canonical_number(candidates: list[int]) -> int | None:
    if not candidates:
        return None
    counts = Counter(candidates)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _derive_hwy_name(
    route_type: str,
    display_prefix: str | None,
    base_route_number,
    route_number_candidate,
    route_id: str,
    canonical_lookup: dict[tuple[str, str | None], int],
) -> str:
    base_number = _clean_int(base_route_number)
    route_number = canonical_lookup.get(_canonical_lookup_key(route_id, display_prefix))
    if route_number is None:
        route_number = _clean_int(route_number_candidate)

    if display_prefix == "I" and route_number is None and base_number is not None:
        route_number = INTERSTATE_ROUTE_NUMBER_TO_SIGNED.get(base_number)
    if display_prefix in {"SR", "CR", "CS"} and route_number is None:
        route_number = base_number
    if display_prefix == "US" and route_number is None:
        route_number = base_number

    if route_type == "RP":
        return "RAMP"
    if route_type == "CD":
        return "COLLECTOR-DISTRIBUTOR"
    if route_type == "RC":
        return "RAMP CONNECTOR"
    if route_type == "FR":
        return "FRONTAGE RD"
    if route_type == "ALY":
        return "ALLEY"
    if route_type == "YC":
        return "Y-CONNECTOR"
    if route_type == "ML":
        if display_prefix and route_number is not None:
            return f"{display_prefix}-{route_number} EXPRESS"
        return "EXPRESS LANE"
    if route_type == "PR":
        return "PRIVATE ROAD"
    if route_type == "FED":
        return "FEDERAL ROUTE"

    if display_prefix and route_number is not None:
        if route_type in {"BI", "BU", "BS"}:
            return f"{display_prefix}-{route_number} BUS"
        if route_type == "SP":
            return f"{display_prefix}-{route_number} SPUR"
        if route_type == "CN":
            return f"{display_prefix}-{route_number} CONN"
        if route_type == "LP":
            return f"{display_prefix}-{route_number} LOOP"
        if route_type == "AL":
            return f"{display_prefix}-{route_number} ALT"
        if route_type == "BY":
            return f"{display_prefix}-{route_number} BYP"
        return f"{display_prefix}-{route_number}"

    return "OTHER ROUTE"


def apply_gdot_route_type_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Return `ROUTE_TYPE_GDOT` and `HWY_NAME` derived from staged fields."""

    working = pd.DataFrame(index=df.index)
    working["ROUTE_ID"] = df.get("ROUTE_ID", pd.Series(index=df.index, dtype="object")).fillna("").astype(str)
    working["BASE_ROUTE_NUMBER"] = df.get("BASE_ROUTE_NUMBER", pd.Series(index=df.index, dtype="object"))
    working["FUNCTION_TYPE"] = df.get("FUNCTION_TYPE", pd.Series(index=df.index, dtype="object"))
    working["SYSTEM_CODE"] = df.get("SYSTEM_CODE", pd.Series(index=df.index, dtype="object"))
    working["ROUTE_SUFFIX"] = df.get(
        "ROUTE_SUFFIX",
        pd.Series(index=df.index, dtype="object"),
    ).map(lambda value: _clean_text(value).upper())
    working["ROUTE_FAMILY"] = df.get("ROUTE_FAMILY", pd.Series(index=df.index, dtype="object")).fillna("").astype(str)
    working["SIGNED_ROUTE_FAMILY_PRIMARY"] = df.get(
        "SIGNED_ROUTE_FAMILY_PRIMARY",
        pd.Series(index=df.index, dtype="object"),
    ).fillna("").astype(str)
    working["SIGNED_ROUTE_VERIFY_SOURCE"] = df.get(
        "SIGNED_ROUTE_VERIFY_SOURCE",
        pd.Series(index=df.index, dtype="object"),
    ).fillna("").astype(str)
    working["HPMS_ROUTE_SIGNING"] = df.get("HPMS_ROUTE_SIGNING", pd.Series(index=df.index, dtype="object"))
    working["HPMS_ROUTE_NUMBER"] = df.get("HPMS_ROUTE_NUMBER", pd.Series(index=df.index, dtype="object"))
    working["HPMS_ROUTE_NAME"] = df.get("HPMS_ROUTE_NAME", pd.Series(index=df.index, dtype="object")).fillna("").astype(str)

    signed_families: list[str | None] = []
    local_families: list[str | None] = []
    route_types: list[str] = []
    display_prefixes: list[str | None] = []
    route_number_candidates: list[int | None] = []

    for row in working.itertuples(index=False):
        signed_family = _fallback_signed_family(
            route_family=row.ROUTE_FAMILY,
            signed_route_family_primary=row.SIGNED_ROUTE_FAMILY_PRIMARY,
            signed_route_verify_source=row.SIGNED_ROUTE_VERIFY_SOURCE,
            hpms_route_signing=row.HPMS_ROUTE_SIGNING,
        )
        local_family = _fallback_local_family(
            system_code=row.SYSTEM_CODE,
            function_type=row.FUNCTION_TYPE,
            route_suffix=row.ROUTE_SUFFIX,
            hpms_route_signing=row.HPMS_ROUTE_SIGNING,
        )
        route_type = _classify_route_type(
            function_type=row.FUNCTION_TYPE,
            route_suffix=row.ROUTE_SUFFIX,
            signed_family=signed_family,
            local_family=local_family,
        )
        display_prefix = _display_prefix(
            route_type=route_type,
            signed_family=signed_family,
            local_family=local_family,
        )
        route_number_candidate = _clean_int(row.HPMS_ROUTE_NUMBER)
        if route_number_candidate is None:
            route_number_candidate = _parse_route_number_from_name(row.HPMS_ROUTE_NAME)

        signed_families.append(signed_family)
        local_families.append(local_family)
        route_types.append(route_type)
        display_prefixes.append(display_prefix)
        route_number_candidates.append(route_number_candidate)

    working["SIGNED_FAMILY"] = signed_families
    working["LOCAL_FAMILY"] = local_families
    working["ROUTE_TYPE_GDOT"] = route_types
    working["DISPLAY_PREFIX"] = display_prefixes
    working["ROUTE_NUMBER_CANDIDATE"] = route_number_candidates

    canonical_lookup: dict[tuple[str, str | None], int] = {}
    grouped = working.groupby(
        [
            working["ROUTE_ID"].str[:-3],
            "DISPLAY_PREFIX",
        ],
        dropna=False,
    )
    for (route_base_key, display_prefix), group in grouped:
        candidates = [
            _clean_int(value)
            for value in group["ROUTE_NUMBER_CANDIDATE"].tolist()
            if _clean_int(value) is not None
        ]
        canonical_number = _pick_canonical_number(candidates)
        if canonical_number is not None:
            canonical_lookup[(route_base_key, display_prefix)] = canonical_number

    hwy_names = [
        _derive_hwy_name(
            route_type=row.ROUTE_TYPE_GDOT,
            display_prefix=row.DISPLAY_PREFIX,
            base_route_number=row.BASE_ROUTE_NUMBER,
            route_number_candidate=row.ROUTE_NUMBER_CANDIDATE,
            route_id=row.ROUTE_ID,
            canonical_lookup=canonical_lookup,
        )
        for row in working.itertuples(index=False)
    ]

    return pd.DataFrame(
        {
            "ROUTE_TYPE_GDOT": working["ROUTE_TYPE_GDOT"],
            "HWY_NAME": hwy_names,
        },
        index=df.index,
    )
