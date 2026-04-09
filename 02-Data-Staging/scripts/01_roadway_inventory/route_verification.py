"""Official signed-route verification helpers for Georgia roadway ETL.

This module scaffolds a signed-route verification pass that upgrades the
baseline `ROUTE_ID` crosswalk using official GDOT split route layers.

Current implementation:
- initializes all proposed verification fields from the baseline route-family
  crosswalk
- loads official GDOT `Interstates`, `US Highway`, and `State Routes`
  reference layers
- matches staged segments to those references using derived 10-character
  `RCLINK` candidates plus milepoint overlap

Future phases can add:
- geometry-overlap fallback for cases where keys are insufficient
- TIGER / OSM corroboration
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import geopandas as gpd
import pandas as pd

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory"
CONFIG_PATH = (
    PROJECT_ROOT
    / "02-Data-Staging"
    / "config"
    / "georgia_signed_route_verification_sources.json"
)

CONFIG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
REFERENCE_CONFIG = CONFIG["references"]
VERIFY_SCORES = CONFIG["default_verify_scores"]
QUERY_BATCH_SIZE = int(CONFIG.get("query_batch_size", 500))
USER_AGENT = "Georgia-Statewide-Data-Pipeline signed-route verification"
MILEPOINT_TOLERANCE = 1e-4

SIGNED_ROUTE_PRIORITY = {
    "Interstate": 0,
    "U.S. Route": 1,
    "State Route": 2,
    "Local/Other": 3,
}
REFERENCE_MATCH_ORDER = ["interstates", "us_highway"]

VERIFICATION_COLUMNS = [
    "SIGNED_INTERSTATE_FLAG",
    "SIGNED_US_ROUTE_FLAG",
    "SIGNED_STATE_ROUTE_FLAG",
    "SIGNED_ROUTE_FAMILY_PRIMARY",
    "SIGNED_ROUTE_FAMILY_ALL",
    "SIGNED_ROUTE_VERIFY_SOURCE",
    "SIGNED_ROUTE_VERIFY_METHOD",
    "SIGNED_ROUTE_VERIFY_CONFIDENCE",
    "SIGNED_ROUTE_VERIFY_SCORE",
    "SIGNED_ROUTE_VERIFY_NOTES",
]


def _clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        col.strip().replace(" ", "_").upper() if isinstance(col, str) else col
        for col in df.columns
    ]
    return df


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _round_milepoint(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    rounded = round(float(value), 4)
    return 0.0 if abs(rounded) < MILEPOINT_TOLERANCE else rounded


def _strip_extra_dims(coords: Any) -> Any:
    """Strip M/Z dimensions beyond XY from GeoJSON coordinates."""
    if not coords:
        return coords
    if isinstance(coords[0], (int, float)):
        return coords[:2]
    return [_strip_extra_dims(c) for c in coords]


def _feature_collection_to_gdf(payload: dict[str, Any]) -> gpd.GeoDataFrame:
    features = payload.get("features", [])
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    for feature in features:
        geom = feature.get("geometry")
        if geom and "coordinates" in geom:
            geom["coordinates"] = _strip_extra_dims(geom["coordinates"])
    return gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")


def _get_json(service_url: str, params: dict[str, Any], timeout: int) -> dict[str, Any]:
    query = urlencode(params, doseq=True)
    full_url = f"{service_url}?{query}"
    if len(full_url) > 2000:
        request = Request(
            service_url,
            data=query.encode("utf-8"),
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
    else:
        request = Request(full_url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload


def _fetch_arcgis_object_ids(service_url: str) -> list[int]:
    query_url = f"{service_url.rstrip('/')}/query"
    payload = _get_json(
        query_url,
        {
            "f": "json",
            "where": "1=1",
            "returnIdsOnly": "true",
        },
        timeout=120,
    )
    object_ids = payload.get("objectIds") or []
    return sorted(int(object_id) for object_id in object_ids)


def _fetch_arcgis_features(
    service_url: str,
    object_ids: list[int],
) -> gpd.GeoDataFrame:
    query_url = f"{service_url.rstrip('/')}/query"
    frames: list[gpd.GeoDataFrame] = []

    for start in range(0, len(object_ids), QUERY_BATCH_SIZE):
        batch = object_ids[start : start + QUERY_BATCH_SIZE]
        payload = _get_json(
            query_url,
            {
                "f": "geojson",
                "where": "1=1",
                "objectIds": ",".join(str(object_id) for object_id in batch),
                "outFields": "*",
                "returnGeometry": "true",
                "returnM": "false",
                "returnZ": "false",
                "outSR": 4326,
            },
            timeout=180,
        )
        gdf = _feature_collection_to_gdf(payload)
        if not gdf.empty:
            frames.append(gdf)

    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    merged = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=frames[0].crs)


def _reference_local_path(reference_key: str) -> Path:
    return PROJECT_ROOT / REFERENCE_CONFIG[reference_key]["local_geojson"]


def fetch_reference_layer(reference_key: str, refresh: bool = False) -> gpd.GeoDataFrame:
    """Load an official reference layer from local snapshot or live GDOT service."""

    reference_spec = REFERENCE_CONFIG[reference_key]
    local_path = _reference_local_path(reference_key)
    if local_path.exists() and not refresh:
        LOGGER.info("Loading signed-route reference snapshot: %s", local_path)
        return gpd.read_file(local_path, engine="pyogrio")

    LOGGER.info("Fetching signed-route reference from GDOT service: %s", reference_key)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    object_ids = _fetch_arcgis_object_ids(reference_spec["service_url"])
    gdf = _fetch_arcgis_features(reference_spec["service_url"], object_ids)
    if gdf.empty:
        LOGGER.warning("Reference layer %s returned no features", reference_key)
        return gdf

    gdf.to_file(local_path, driver="GeoJSON", engine="pyogrio")
    LOGGER.info("Wrote signed-route reference snapshot: %s", local_path)
    return gdf


def _pick_first_available_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    upper_columns = {str(column).upper(): column for column in df.columns}
    for candidate in candidates:
        if candidate.upper() in upper_columns:
            return upper_columns[candidate.upper()]
    return None


def normalize_reference_layer(gdf: gpd.GeoDataFrame, reference_key: str) -> pd.DataFrame:
    """Normalize GDOT signed-route reference schema for interval matching."""

    if gdf.empty:
        return pd.DataFrame(
            columns=[
                "REFERENCE_KEY",
                "REFERENCE_FAMILY",
                "RCLINK",
                "FROM_MILEPOINT",
                "TO_MILEPOINT",
                "PRIMARY_LABEL",
                "SECONDARY_LABEL",
            ]
        )

    normalized = _clean_column_names(gdf.copy())
    spec = REFERENCE_CONFIG[reference_key]

    id_col = _pick_first_available_column(normalized, ["RCLINK", "RC_LINK", "ROUTE_ID"])
    from_col = _pick_first_available_column(
        normalized,
        ["BEGINNING_MILEPOINT", "BEGINNING_MP", "FROM_MILEPOINT"],
    )
    to_col = _pick_first_available_column(
        normalized,
        ["ENDING_MILEPOINT", "ENDING_MP", "TO_MILEPOINT"],
    )

    primary_label_col = None
    secondary_label_col = None
    if reference_key == "interstates":
        primary_label_col = _pick_first_available_column(
            normalized,
            ["INTERSTATE_NAME", "INTERSTATE_NUMBER", "INTERSTATE_NUM", "INTERSTATE"],
        )
        secondary_label_col = _pick_first_available_column(
            normalized,
            ["ROUTE_NUMBER", "STATE_ROUTE_NUMBER"],
        )
    elif reference_key == "us_highway":
        primary_label_col = _pick_first_available_column(
            normalized,
            ["US_ROUTE_NUM", "US_ROUTE_NUMBER_ABBREVIATED", "US_ROUTE_NUMBER"],
        )
        secondary_label_col = _pick_first_available_column(
            normalized,
            ["STATE_ROUTE_NUMBER_ABBREVIATED", "STATE_ROUTE_NUMBER"],
        )
    elif reference_key == "state_routes":
        primary_label_col = _pick_first_available_column(
            normalized,
            ["ROUTE_DESCRIPTION", "ROUTE_NUMBER"],
        )
        secondary_label_col = _pick_first_available_column(
            normalized,
            ["ROUTE_SUFFIX"],
        )

    match_key = spec.get("match_key", "rclink")
    normalized["REFERENCE_KEY"] = reference_key
    normalized["REFERENCE_FAMILY"] = spec["reference_family"]
    if match_key == "route_id_base" and id_col is not None:
        normalized["RCLINK"] = (
            normalized[id_col].astype(str).str.strip().str.upper().str[:13]
        )
    else:
        normalized["RCLINK"] = (
            normalized[id_col].astype(str).str.strip().str.upper().str.zfill(10)
            if id_col is not None
            else ""
        )
    normalized["FROM_MILEPOINT"] = (
        normalized[from_col].map(_round_milepoint) if from_col is not None else None
    )
    normalized["TO_MILEPOINT"] = (
        normalized[to_col].map(_round_milepoint) if to_col is not None else None
    )
    normalized["PRIMARY_LABEL"] = (
        normalized[primary_label_col].astype(str).str.strip()
        if primary_label_col is not None
        else None
    )
    normalized["SECONDARY_LABEL"] = (
        normalized[secondary_label_col].astype(str).str.strip()
        if secondary_label_col is not None
        else None
    )

    keep = [
        "REFERENCE_KEY",
        "REFERENCE_FAMILY",
        "RCLINK",
        "FROM_MILEPOINT",
        "TO_MILEPOINT",
        "PRIMARY_LABEL",
        "SECONDARY_LABEL",
    ]
    return normalized[keep].copy()


def derive_rclink_candidates(
    route_id: str,
    function_type: str | None = None,
    system_code: str | None = None,
) -> list[str]:
    """Derive one or more 10-character RCLINK candidates from GDOT ROUTE_ID."""

    raw_route_id = _clean_text(route_id).upper()
    if len(raw_route_id) < 13:
        return []

    parsed_function = _clean_text(function_type) or raw_route_id[0:1]
    parsed_system = _clean_text(system_code) or raw_route_id[4:5]
    county_code = raw_route_id[1:4]
    route_code = raw_route_id[5:11]
    suffix = raw_route_id[11:13]

    route_digits = "".join(character for character in route_code if character.isdigit())
    if not route_digits:
        return []

    if parsed_function in {"2", "3", "4"}:
        route_number = route_digits[-3:].zfill(4)
    else:
        route_number = route_digits[-4:].zfill(4)

    candidates = [f"{county_code}{parsed_system}{route_number}{suffix}".upper()]
    if parsed_system == "1" and county_code != "000":
        candidates.append(f"000{parsed_system}{route_number}{suffix}".upper())

    deduped: list[str] = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def build_reference_lookup(reference_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    if reference_df.empty:
        return lookup

    filtered = reference_df.copy()
    filtered = filtered[filtered["RCLINK"].astype(str).str.len() > 0]
    filtered = filtered.sort_values(
        by=["RCLINK", "FROM_MILEPOINT", "TO_MILEPOINT"],
        na_position="last",
    )
    for rclink, group in filtered.groupby("RCLINK", sort=False):
        lookup[str(rclink)] = group.to_dict("records")
    return lookup


def _intervals_overlap(
    segment_from: float | None,
    segment_to: float | None,
    ref_from: float | None,
    ref_to: float | None,
) -> bool:
    if None in {segment_from, segment_to, ref_from, ref_to}:
        return True
    return (
        min(float(segment_to), float(ref_to)) - max(float(segment_from), float(ref_from))
    ) > -MILEPOINT_TOLERANCE


def _match_reference_record(
    row: pd.Series,
    reference_lookup: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    candidates: list[str] = []

    route_id = _clean_text(row.get("ROUTE_ID")).upper()
    if len(route_id) >= 13:
        candidates.append(route_id[:13])

    candidates.extend(
        derive_rclink_candidates(
            row.get("ROUTE_ID"),
            row.get("PARSED_FUNCTION_TYPE"),
            row.get("PARSED_SYSTEM_CODE"),
        )
    )

    for candidate in candidates:
        for record in reference_lookup.get(candidate, []):
            if _intervals_overlap(
                _round_milepoint(row.get("FROM_MILEPOINT")),
                _round_milepoint(row.get("TO_MILEPOINT")),
                record.get("FROM_MILEPOINT"),
                record.get("TO_MILEPOINT"),
            ):
                return record
    return None


def initialize_signed_route_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    verified = gdf.copy()
    baseline_family = verified.get("ROUTE_FAMILY", pd.Series(index=verified.index, dtype="object"))
    baseline_confidence = verified.get(
        "ROUTE_FAMILY_CONFIDENCE", pd.Series(index=verified.index, dtype="object")
    )

    verified["SIGNED_INTERSTATE_FLAG"] = baseline_family.eq("Interstate")
    verified["SIGNED_US_ROUTE_FLAG"] = baseline_family.eq("U.S. Route")
    verified["SIGNED_STATE_ROUTE_FLAG"] = baseline_family.eq("State Route")
    verified["SIGNED_ROUTE_FAMILY_PRIMARY"] = baseline_family
    verified["SIGNED_ROUTE_FAMILY_ALL"] = baseline_family.fillna("").map(
        lambda value: json.dumps([value]) if value else json.dumps([])
    )
    verified["SIGNED_ROUTE_VERIFY_SOURCE"] = "route_id_crosswalk"
    verified["SIGNED_ROUTE_VERIFY_METHOD"] = "route_id_crosswalk"
    verified["SIGNED_ROUTE_VERIFY_CONFIDENCE"] = baseline_confidence
    verified["SIGNED_ROUTE_VERIFY_SCORE"] = baseline_confidence.map(VERIFY_SCORES).fillna(
        VERIFY_SCORES["low"]
    )
    verified["SIGNED_ROUTE_VERIFY_NOTES"] = None
    return verified


def _sorted_route_families(families: set[str]) -> list[str]:
    return sorted(families, key=lambda family: SIGNED_ROUTE_PRIORITY.get(family, 99))


def _verification_method_for_record(match_record: dict[str, Any]) -> str:
    if (
        match_record.get("FROM_MILEPOINT") is not None
        and match_record.get("TO_MILEPOINT") is not None
    ):
        return "derived_rclink_interval"
    return "derived_rclink_exact"


def _update_row_with_reference_match(
    row: pd.Series,
    reference_key: str,
    match_record: dict[str, Any],
) -> pd.Series:
    reference_family = REFERENCE_CONFIG[reference_key]["reference_family"]
    families = set(json.loads(row["SIGNED_ROUTE_FAMILY_ALL"]))
    if reference_family:
        families.add(reference_family)

    ordered_families = _sorted_route_families(families)
    new_primary_family = (
        ordered_families[0] if ordered_families else row["SIGNED_ROUTE_FAMILY_PRIMARY"]
    )
    row["SIGNED_ROUTE_FAMILY_ALL"] = json.dumps(ordered_families)
    row["SIGNED_ROUTE_FAMILY_PRIMARY"] = new_primary_family
    source_token = f"gdot_{reference_key}"
    if reference_family == new_primary_family:
        row["SIGNED_ROUTE_VERIFY_SOURCE"] = source_token
        row["SIGNED_ROUTE_VERIFY_METHOD"] = _verification_method_for_record(match_record)
        row["SIGNED_ROUTE_VERIFY_CONFIDENCE"] = "high"
        row["SIGNED_ROUTE_VERIFY_SCORE"] = VERIFY_SCORES["high"]

    note_parts = [f"official_{reference_key}_match"]
    if _clean_text(match_record.get("PRIMARY_LABEL")):
        note_parts.append(f"label={_clean_text(match_record.get('PRIMARY_LABEL'))}")
    if _clean_text(match_record.get("SECONDARY_LABEL")):
        note_parts.append(f"state_label={_clean_text(match_record.get('SECONDARY_LABEL'))}")
    existing_notes = _clean_text(row.get("SIGNED_ROUTE_VERIFY_NOTES"))
    combined_notes = "; ".join(part for part in [existing_notes, *note_parts] if part)
    row["SIGNED_ROUTE_VERIFY_NOTES"] = combined_notes or None

    if reference_family == "Interstate":
        row["SIGNED_INTERSTATE_FLAG"] = True
    if reference_family == "U.S. Route":
        row["SIGNED_US_ROUTE_FLAG"] = True
    if reference_family == "State Route":
        row["SIGNED_STATE_ROUTE_FLAG"] = True

    return row


def apply_signed_route_verification(
    gdf: gpd.GeoDataFrame,
    refresh_references: bool = False,
) -> gpd.GeoDataFrame:
    """Apply official signed-route verification to staged segments.

    The current scaffold officially upgrades Interstate, `U.S. Route`, and
    `State Route` confidence via GDOT ArcWeb references. If any reference
    cannot be loaded, the ETL keeps the baseline route-family fields and
    continues with whichever official references are available.
    """

    verified = initialize_signed_route_fields(gdf)

    reference_lookups: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for reference_key in REFERENCE_MATCH_ORDER:
        try:
            reference_df = normalize_reference_layer(
                fetch_reference_layer(reference_key, refresh=refresh_references),
                reference_key,
            )
        except Exception as exc:
            LOGGER.warning(
                "Signed-route verification reference unavailable for %s: %s",
                reference_key,
                exc,
            )
            continue

        reference_lookup = build_reference_lookup(reference_df)
        if not reference_lookup:
            LOGGER.warning("No interval lookup built for reference: %s", reference_key)
            continue
        reference_lookups[reference_key] = reference_lookup

    if not reference_lookups:
        return verified

    for reference_key in REFERENCE_MATCH_ORDER:
        reference_lookup = reference_lookups.get(reference_key)
        if reference_lookup is None:
            continue

        match_count = 0
        candidate_indices = verified.index[
            verified["PARSED_SYSTEM_CODE"].astype(str) == "1"
        ]
        for index in candidate_indices:
            row = verified.loc[index]
            match_record = _match_reference_record(row, reference_lookup)
            if match_record is None:
                continue
            updated_row = _update_row_with_reference_match(
                row.copy(),
                reference_key,
                match_record,
            )
            for column in VERIFICATION_COLUMNS:
                verified.at[index, column] = updated_row[column]
            match_count += 1

        LOGGER.info(
            "Signed-route verification matches for %s: %d",
            reference_key,
            match_count,
        )

    return verified


def write_signed_route_verification_summary(gdf: pd.DataFrame) -> None:
    summary = {
        "segment_count": int(len(gdf)),
        "signed_interstate_segments": int(pd.Series(gdf["SIGNED_INTERSTATE_FLAG"]).fillna(False).sum()),
        "signed_us_route_segments": int(pd.Series(gdf["SIGNED_US_ROUTE_FLAG"]).fillna(False).sum()),
        "signed_state_route_segments": int(pd.Series(gdf["SIGNED_STATE_ROUTE_FLAG"]).fillna(False).sum()),
        "primary_family_counts": {
            str(key): int(value)
            for key, value in gdf["SIGNED_ROUTE_FAMILY_PRIMARY"].value_counts(dropna=False).to_dict().items()
        },
        "verification_confidence_counts": {
            str(key): int(value)
            for key, value in gdf["SIGNED_ROUTE_VERIFY_CONFIDENCE"].value_counts(dropna=False).to_dict().items()
        },
        "verification_source_counts": {
            str(key): int(value)
            for key, value in gdf["SIGNED_ROUTE_VERIFY_SOURCE"].value_counts(dropna=False).to_dict().items()
        },
        "verification_method_counts": {
            str(key): int(value)
            for key, value in gdf["SIGNED_ROUTE_VERIFY_METHOD"].value_counts(dropna=False).to_dict().items()
        },
    }
    output_path = (
        PROJECT_ROOT / "02-Data-Staging" / "config" / "signed_route_verification_summary.json"
    )
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote signed-route verification summary to %s", output_path)
