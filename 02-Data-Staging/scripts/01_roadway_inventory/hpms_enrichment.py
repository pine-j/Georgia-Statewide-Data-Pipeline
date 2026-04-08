"""Enrich Georgia roadway segments with FHWA HPMS 2024 data.

HPMS uses the same GDOT ROUTE_ID + milepoint system, enabling direct
interval-overlap matching without spatial joins.

Current enrichment:
- AADT gap-fill for segments missing GDOT official/analytical AADT
- Future AADT gap-fill
- Signed-route classification from routesigning (replaces GPAS verification)
- Pavement condition: IRI, PSR, rutting, cracking_percent
- Safety geometry: access_control, terrain_type, speed_limit
- Roadway attribute gap-fill: through_lanes, lane_width, median, shoulder,
  surface_type, f_system, facility_type, nhs, ownership, urban_id

Downloaded data lives at:
  01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_tabular.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import geopandas as gpd

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
HPMS_PATH = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "FHWA_HPMS" / "2024" / "hpms_ga_2024_tabular.json"

MILEPOINT_TOLERANCE = 1e-4

# HPMS routesigning codes -> route family
ROUTE_SIGNING_MAP = {
    1: "Interstate",
    2: "U.S. Route",
    3: "State Route",
    4: "Local/Other",
    5: "Local/Other",
    6: "Local/Other",
    7: "Local/Other",
    8: "Local/Other",
    9: "Local/Other",
    10: "Local/Other",
}

# GDOT attribute columns that HPMS can gap-fill (only fill where GDOT is null)
HPMS_GAP_FILL_FIELDS = {
    # hpms_field: (target_column, cast_type)
    "through_lanes": ("THROUGH_LANES", "int"),
    "lane_width": ("LANE_WIDTH", "float"),
    "median_type": ("MEDIAN_TYPE", "int"),
    "median_width": ("MEDIAN_WIDTH", "float"),
    "shoulder_type": ("SHOULDER_TYPE", "int"),
    "shoulder_width_l": ("SHOULDER_WIDTH_L", "float"),
    "shoulder_width_r": ("SHOULDER_WIDTH_R", "float"),
    "surface_type": ("SURFACE_TYPE", "int"),
    "f_system": ("F_SYSTEM", "int"),
    "facility_type": ("FACILITY_TYPE", "int"),
    "nhs": ("NHS", "int"),
    "ownership": ("OWNERSHIP", "int"),
    "urban_id": ("URBAN_ID", "int"),
    "speed_limit": ("SPEED_LIMIT", "int"),
}


def load_hpms_data() -> pd.DataFrame:
    """Load the HPMS tabular snapshot."""

    if not HPMS_PATH.exists():
        LOGGER.warning("HPMS data not found at %s", HPMS_PATH)
        return pd.DataFrame()

    LOGGER.info("Loading HPMS data from %s", HPMS_PATH)
    with open(HPMS_PATH, encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame([feat["attributes"] for feat in raw["features"]])
    LOGGER.info("Loaded %d HPMS records with %d columns", len(df), len(df.columns))
    return df


def _build_hpms_lookup(hpms: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Build a ROUTE_ID -> list of HPMS records lookup."""

    lookup: dict[str, list[dict[str, Any]]] = {}
    if hpms.empty:
        return lookup

    valid = hpms[hpms["route_id"].notna()].copy()
    valid["route_id"] = valid["route_id"].astype(str).str.strip()
    valid = valid.sort_values(by=["route_id", "begin_point"])

    for route_id, group in valid.groupby("route_id", sort=False):
        lookup[str(route_id)] = group.to_dict("records")

    LOGGER.info("HPMS lookup: %d route keys", len(lookup))
    return lookup


def _find_best_hpms_match(
    route_id: str,
    from_mp: float | None,
    to_mp: float | None,
    lookup: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    """Find the HPMS record with the best milepoint overlap."""

    candidates = lookup.get(route_id, [])
    if not candidates:
        return None

    if from_mp is None or to_mp is None:
        return candidates[0] if candidates else None

    best = None
    best_overlap = -1.0

    for record in candidates:
        ref_from = record.get("begin_point")
        ref_to = record.get("end_point")
        if ref_from is None or ref_to is None:
            continue

        overlap = min(float(to_mp), float(ref_to)) - max(float(from_mp), float(ref_from))
        if overlap > best_overlap:
            best_overlap = overlap
            best = record

    return best if best_overlap > -MILEPOINT_TOLERANCE else None


def _safe_cast(value: Any, cast_type: str) -> Any:
    """Safely cast a value, returning None on failure."""
    if value is None or pd.isna(value):
        return None
    try:
        if cast_type == "int":
            return int(value)
        elif cast_type == "float":
            return float(value)
        return value
    except (ValueError, TypeError):
        return None


def apply_hpms_enrichment(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Enrich roadway segments with HPMS 2024 data.

    Fills AADT gaps, derives signed-route flags, gap-fills roadway attributes,
    and adds pavement/safety attributes where available.
    Never overwrites existing official or analytical AADT values.
    Never overwrites existing GDOT attribute values (gap-fill only).
    """

    hpms = load_hpms_data()
    if hpms.empty:
        return gdf

    enriched = gdf.copy()
    lookup = _build_hpms_lookup(hpms)

    # Initialize HPMS-sourced columns if not present
    for col in ["HPMS_IRI", "HPMS_PSR", "HPMS_RUTTING", "HPMS_CRACKING_PCT",
                "HPMS_ACCESS_CONTROL", "HPMS_TERRAIN_TYPE",
                "HPMS_ROUTE_SIGNING", "HPMS_ROUTE_NUMBER", "HPMS_ROUTE_NAME"]:
        if col not in enriched.columns:
            enriched[col] = None

    # Initialize signed-route verification columns from baseline route-family
    baseline_family = enriched.get("ROUTE_FAMILY", pd.Series(index=enriched.index, dtype="object"))
    baseline_confidence = enriched.get(
        "ROUTE_FAMILY_CONFIDENCE", pd.Series(index=enriched.index, dtype="object")
    )
    enriched["SIGNED_INTERSTATE_FLAG"] = baseline_family.eq("Interstate")
    enriched["SIGNED_US_ROUTE_FLAG"] = baseline_family.eq("U.S. Route")
    enriched["SIGNED_STATE_ROUTE_FLAG"] = baseline_family.eq("State Route")
    enriched["SIGNED_ROUTE_FAMILY_PRIMARY"] = baseline_family
    enriched["SIGNED_ROUTE_FAMILY_ALL"] = baseline_family.fillna("").map(
        lambda v: json.dumps([v]) if v else json.dumps([])
    )
    enriched["SIGNED_ROUTE_VERIFY_SOURCE"] = "route_id_crosswalk"
    enriched["SIGNED_ROUTE_VERIFY_METHOD"] = "route_id_crosswalk"
    enriched["SIGNED_ROUTE_VERIFY_CONFIDENCE"] = baseline_confidence
    enriched["SIGNED_ROUTE_VERIFY_SCORE"] = baseline_confidence.map(
        {"high": 0.95, "medium": 0.6, "low": 0.3}
    ).fillna(0.3)
    enriched["SIGNED_ROUTE_VERIFY_NOTES"] = None

    aadt_fill_count = 0
    pavement_fill_count = 0
    signing_upgrade_count = 0
    gap_fill_counts = {target: 0 for _, (target, _) in HPMS_GAP_FILL_FIELDS.items()}
    total_matched = 0

    for idx in enriched.index:
        row = enriched.loc[idx]
        route_id = str(row.get("ROUTE_ID", ""))
        if not route_id or route_id == "nan":
            continue

        match = _find_best_hpms_match(
            route_id,
            row.get("FROM_MILEPOINT"),
            row.get("TO_MILEPOINT"),
            lookup,
        )
        if match is None:
            continue

        total_matched += 1

        # --- AADT gap-fill: only fill if not already covered ---
        if not bool(row.get("current_aadt_covered", False)):
            hpms_aadt = match.get("aadt")
            if hpms_aadt is not None and not pd.isna(hpms_aadt):
                enriched.at[idx, "AADT_2024"] = int(hpms_aadt)
                enriched.at[idx, "AADT"] = int(hpms_aadt)
                enriched.at[idx, "AADT_YEAR"] = 2024
                enriched.at[idx, "AADT_2024_SOURCE"] = "hpms_2024"
                enriched.at[idx, "AADT_2024_CONFIDENCE"] = "medium"
                enriched.at[idx, "AADT_2024_FILL_METHOD"] = "hpms_route_id_milepoint_match"
                enriched.at[idx, "current_aadt_covered"] = True
                aadt_fill_count += 1

        # --- Future AADT gap-fill ---
        if not bool(row.get("future_aadt_covered", False)):
            hpms_future = match.get("future_aadt")
            if hpms_future is not None and not pd.isna(hpms_future):
                enriched.at[idx, "FUTURE_AADT_2044"] = int(hpms_future)
                enriched.at[idx, "FUTURE_AADT"] = int(hpms_future)
                enriched.at[idx, "FUTURE_AADT_2044_SOURCE"] = "hpms_2024"
                enriched.at[idx, "FUTURE_AADT_2044_CONFIDENCE"] = "medium"
                enriched.at[idx, "FUTURE_AADT_2044_FILL_METHOD"] = "hpms_route_id_milepoint_match"
                enriched.at[idx, "future_aadt_covered"] = True

        # --- Signed-route classification from HPMS routesigning ---
        signing = match.get("routesigning")
        if signing is not None and not pd.isna(signing):
            signing_int = int(signing)
            enriched.at[idx, "HPMS_ROUTE_SIGNING"] = signing_int
            hpms_family = ROUTE_SIGNING_MAP.get(signing_int)
            if hpms_family:
                current_source = enriched.at[idx, "SIGNED_ROUTE_VERIFY_SOURCE"]
                # Upgrade from baseline crosswalk to HPMS
                if current_source == "route_id_crosswalk":
                    enriched.at[idx, "SIGNED_ROUTE_FAMILY_PRIMARY"] = hpms_family
                    enriched.at[idx, "SIGNED_INTERSTATE_FLAG"] = (hpms_family == "Interstate")
                    enriched.at[idx, "SIGNED_US_ROUTE_FLAG"] = (hpms_family == "U.S. Route")
                    enriched.at[idx, "SIGNED_STATE_ROUTE_FLAG"] = (hpms_family == "State Route")
                    # Update the ALL list
                    existing_all = set(json.loads(enriched.at[idx, "SIGNED_ROUTE_FAMILY_ALL"]))
                    existing_all.add(hpms_family)
                    enriched.at[idx, "SIGNED_ROUTE_FAMILY_ALL"] = json.dumps(sorted(existing_all))
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_SOURCE"] = "hpms_2024"
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_METHOD"] = "hpms_routesigning"
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_CONFIDENCE"] = "high"
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_SCORE"] = 0.95
                    signing_upgrade_count += 1

        routenum = match.get("routenumber")
        if routenum is not None and not pd.isna(routenum):
            try:
                enriched.at[idx, "HPMS_ROUTE_NUMBER"] = int(routenum)
            except (ValueError, TypeError):
                enriched.at[idx, "HPMS_ROUTE_NUMBER"] = str(routenum).strip()

        routename = match.get("routename")
        if routename is not None and not pd.isna(routename):
            enriched.at[idx, "HPMS_ROUTE_NAME"] = str(routename).strip()

        # --- Pavement condition ---
        iri = match.get("iri")
        if iri is not None and not pd.isna(iri):
            enriched.at[idx, "HPMS_IRI"] = float(iri)
            pavement_fill_count += 1

        psr = match.get("psr")
        if psr is not None and not pd.isna(psr):
            enriched.at[idx, "HPMS_PSR"] = float(psr)

        rutting = match.get("rutting")
        if rutting is not None and not pd.isna(rutting):
            enriched.at[idx, "HPMS_RUTTING"] = float(rutting)

        cracking = match.get("cracking_percent")
        if cracking is not None and not pd.isna(cracking):
            enriched.at[idx, "HPMS_CRACKING_PCT"] = float(cracking)

        # --- Safety geometry ---
        ac = match.get("access_control")
        if ac is not None and not pd.isna(ac):
            enriched.at[idx, "HPMS_ACCESS_CONTROL"] = int(ac)

        terrain = match.get("terrain_type")
        if terrain is not None and not pd.isna(terrain):
            enriched.at[idx, "HPMS_TERRAIN_TYPE"] = int(terrain)

        # --- Roadway attribute gap-fill (never overwrite existing GDOT values) ---
        for hpms_field, (target_col, cast_type) in HPMS_GAP_FILL_FIELDS.items():
            current_val = row.get(target_col)
            if current_val is not None and not pd.isna(current_val):
                continue  # GDOT already has a value — don't overwrite
            hpms_val = match.get(hpms_field)
            casted = _safe_cast(hpms_val, cast_type)
            if casted is not None:
                enriched.at[idx, target_col] = casted
                gap_fill_counts[target_col] = gap_fill_counts.get(target_col, 0) + 1
                # Set provenance for speed limit gap-fill
                if target_col == "SPEED_LIMIT":
                    enriched.at[idx, "SPEED_LIMIT_SOURCE"] = "hpms_2024"

    # Sync only HPMS-filled rows (avoid clobbering pre-existing AADT values)
    hpms_mask = enriched["AADT_2024_SOURCE"] == "hpms_2024"
    enriched.loc[hpms_mask, "AADT"] = enriched.loc[hpms_mask, "AADT_2024"]
    enriched.loc[hpms_mask, "current_aadt_covered"] = True

    LOGGER.info(
        "HPMS enrichment: %d segments matched, %d AADT gaps filled, %d with pavement data, %d signing upgrades",
        total_matched,
        aadt_fill_count,
        pavement_fill_count,
        signing_upgrade_count,
    )
    for target_col, count in sorted(gap_fill_counts.items()):
        if count > 0:
            LOGGER.info("  HPMS gap-fill %s: %d segments", target_col, count)

    return enriched


def write_hpms_enrichment_summary(gdf: pd.DataFrame) -> None:
    """Write HPMS enrichment coverage summary."""

    gap_fill_coverage = {}
    for hpms_field, (target_col, _) in HPMS_GAP_FILL_FIELDS.items():
        if target_col in gdf.columns:
            gap_fill_coverage[f"hpms_gap_fill_{target_col.lower()}"] = int(gdf[target_col].notna().sum())

    summary = {
        "segment_count": int(len(gdf)),
        "hpms_aadt_filled": int((gdf.get("AADT_2024_SOURCE", pd.Series()) == "hpms_2024").sum()),
        "hpms_signing_upgrades": int((gdf.get("SIGNED_ROUTE_VERIFY_SOURCE", pd.Series()) == "hpms_2024").sum()),
        "hpms_iri_coverage": int(gdf["HPMS_IRI"].notna().sum()) if "HPMS_IRI" in gdf.columns else 0,
        "hpms_psr_coverage": int(gdf["HPMS_PSR"].notna().sum()) if "HPMS_PSR" in gdf.columns else 0,
        "hpms_rutting_coverage": int(gdf["HPMS_RUTTING"].notna().sum()) if "HPMS_RUTTING" in gdf.columns else 0,
        "hpms_cracking_coverage": int(gdf["HPMS_CRACKING_PCT"].notna().sum()) if "HPMS_CRACKING_PCT" in gdf.columns else 0,
        "hpms_access_control_coverage": int(gdf["HPMS_ACCESS_CONTROL"].notna().sum()) if "HPMS_ACCESS_CONTROL" in gdf.columns else 0,
        "hpms_terrain_coverage": int(gdf["HPMS_TERRAIN_TYPE"].notna().sum()) if "HPMS_TERRAIN_TYPE" in gdf.columns else 0,
        "hpms_route_signing_coverage": int(gdf["HPMS_ROUTE_SIGNING"].notna().sum()) if "HPMS_ROUTE_SIGNING" in gdf.columns else 0,
        "signed_route_source_counts": {
            str(k): int(v)
            for k, v in gdf["SIGNED_ROUTE_VERIFY_SOURCE"].value_counts(dropna=False).items()
        } if "SIGNED_ROUTE_VERIFY_SOURCE" in gdf.columns else {},
        "gap_fill_coverage": gap_fill_coverage,
        "final_aadt_coverage": int(gdf["AADT_2024"].notna().sum()) if "AADT_2024" in gdf.columns else 0,
        "final_aadt_source_counts": {
            str(k): int(v)
            for k, v in gdf["AADT_2024_SOURCE"].value_counts(dropna=False).items()
        } if "AADT_2024_SOURCE" in gdf.columns else {},
    }

    output_path = PROJECT_ROOT / "02-Data-Staging" / "config" / "hpms_enrichment_summary.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote HPMS enrichment summary to %s", output_path)
