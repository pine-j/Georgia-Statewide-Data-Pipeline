"""Enrich Georgia roadway segments with GDOT's FHWA HPMS 2024 submission.

The HPMS 2024 dataset is GDOT's federal HPMS submission — a parallel
GDOT-published view of the same roadway network, not a third-party fallback.
For AADT in particular, HPMS is the canonical source for state-system
segments that are not present in `TRAFFIC_Data_2024.gdb` (the state-system
GDB), and a cross-validation source for segments that appear in both.

Because HPMS uses the same GDOT ROUTE_ID + milepoint system as the state
GDB, this module joins to the segmented network by interval overlap without
any spatial join.

Current enrichment:
- AADT_2024_HPMS — always populated when HPMS has a value, regardless of
  whether the state GDB also has one. This column exists so downstream code
  can cross-check the two GDOT-published sources without losing the HPMS
  reading on rows where the state GDB already wins the canonical AADT.
- AADT canonical fill for segments not covered by the state GDB. The
  existing tie-breaker order (state `official_exact` > `hpms_2024` > derived
  fills) is preserved — this module only writes the canonical AADT field
  when the state GDB had no value for the segment.
- Future AADT fill where the state GDB lacked a value.
- Initial signed-route classification from routesigning.
- Pavement condition: IRI, PSR, rutting, cracking_percent.
- Safety geometry: access_control, terrain_type, speed_limit.
- Roadway attribute gap-fill (state GDB null only): through_lanes,
  lane_width, median, shoulder, surface_type, f_system, facility_type,
  nhs, ownership, urban_id.

Downloaded data lives at:
  01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_tabular.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import geopandas as gpd

from route_family import (
    SIGNED_ROUTE_FAMILIES,
    SIGNED_ROUTE_PRIORITY,
    parse_signed_route_family_list,
    signed_route_family_slots,
    sort_signed_route_families,
)

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
HPMS_PATH = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "FHWA_HPMS" / "2024" / "hpms_ga_2024_tabular.json"

MILEPOINT_TOLERANCE = 1e-4

# HPMS RouteSigning codes are defined in the FHWA HPMS Field Manual.
# Only codes that explicitly distinguish Interstate / U.S. / State families
# should upgrade the signed-family fields. Code 1 is "Not Signed" and should
# not overwrite a baseline GDOT family.
ROUTE_SIGNING_MAP = {
    2: "Interstate",
    3: "U.S. Route",
    4: "State Route",
    5: "Interstate",
}


def ordered_signed_route_families_for_hpms(
    hpms_family: str,
    current_primary: str | None,
    existing_families: set[str],
) -> list[str]:
    families = {family for family in existing_families if family in SIGNED_ROUTE_FAMILIES}
    if current_primary in SIGNED_ROUTE_FAMILIES:
        families.add(current_primary)
    families.add(hpms_family)

    current_rank = SIGNED_ROUTE_PRIORITY.get(current_primary)
    hpms_rank = SIGNED_ROUTE_PRIORITY.get(hpms_family)
    remaining = [family for family in sort_signed_route_families(families) if family != hpms_family]

    if current_rank is not None and hpms_rank is not None and current_rank < hpms_rank:
        return [current_primary] + [family for family in remaining if family != current_primary] + [hpms_family]

    return [hpms_family] + remaining

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
    "nhfn": ("NHFN", "int"),
    "strahnet_type": ("STRAHNET_TYPE", "int"),
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

    return best if best_overlap > MILEPOINT_TOLERANCE else None


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
    """Enrich roadway segments with GDOT's HPMS 2024 submission.

    Captures the HPMS AADT for every matched segment into `AADT_2024_HPMS`
    (the audit/cross-validation column) regardless of which source wins the
    canonical `AADT_2024`. Fills the canonical AADT only for segments where
    the state-system GDB had no value (preserving the existing tie-breaker:
    state `official_exact` > `hpms_2024` > pipeline-derived fills). Also
    derives signed-route flags, gap-fills roadway attributes, and adds
    pavement/safety attributes where available.
    Never overwrites existing GDOT state values (gap-fill only).
    """

    hpms = load_hpms_data()
    if hpms.empty:
        return gdf

    enriched = gdf.copy()
    lookup = _build_hpms_lookup(hpms)

    # Initialize HPMS-sourced columns if not present
    for col in ["HPMS_IRI", "HPMS_PSR", "HPMS_RUTTING", "HPMS_CRACKING_PCT",
                "HPMS_PCT_DH_SINGLE", "HPMS_PCT_DH_COMBINATION",
                "HPMS_ACCESS_CONTROL", "HPMS_TERRAIN_TYPE",
                "HPMS_ROUTE_SIGNING", "HPMS_ROUTE_NUMBER", "HPMS_ROUTE_NAME"]:
        if col not in enriched.columns:
            enriched[col] = None

    # AADT_2024_HPMS is the audit column for the federal HPMS submission and
    # is populated unconditionally whenever HPMS has a value for the matched
    # segment, even when the state GDB already won the canonical AADT.
    if "AADT_2024_HPMS" not in enriched.columns:
        enriched["AADT_2024_HPMS"] = pd.NA

    # Preserve any existing signed-route verification and backfill only missing fields.
    baseline_family = enriched.get("ROUTE_FAMILY", pd.Series(index=enriched.index, dtype="object"))
    baseline_confidence = enriched.get(
        "ROUTE_FAMILY_CONFIDENCE", pd.Series(index=enriched.index, dtype="object")
    )
    signed_route_defaults = {
        "SIGNED_INTERSTATE_FLAG": baseline_family.eq("Interstate"),
        "SIGNED_US_ROUTE_FLAG": baseline_family.eq("U.S. Route"),
        "SIGNED_STATE_ROUTE_FLAG": baseline_family.eq("State Route"),
        "SIGNED_ROUTE_FAMILY_PRIMARY": baseline_family,
        "SECONDARY_SIGNED_ROUTE_FAMILY": None,
        "TERTIARY_SIGNED_ROUTE_FAMILY": None,
        "SIGNED_ROUTE_FAMILY_ALL": baseline_family.fillna("").map(
            lambda v: json.dumps([v]) if v in SIGNED_ROUTE_FAMILIES else json.dumps([])
        ),
        "SIGNED_ROUTE_VERIFY_SOURCE": "route_id_crosswalk",
        "SIGNED_ROUTE_VERIFY_METHOD": "route_id_crosswalk",
        "SIGNED_ROUTE_VERIFY_CONFIDENCE": baseline_confidence,
        "SIGNED_ROUTE_VERIFY_SCORE": baseline_confidence.map(
            {"high": 0.95, "medium": 0.6, "low": 0.3}
        ).fillna(0.3),
        "SIGNED_ROUTE_VERIFY_NOTES": None,
    }
    for column_name, default_value in signed_route_defaults.items():
        if column_name not in enriched.columns:
            enriched[column_name] = default_value

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

        # --- HPMS AADT capture + canonical fill ---
        # Always record the HPMS AADT into AADT_2024_HPMS for cross-validation,
        # whether or not the state GDB already set the canonical value. Then
        # apply the existing tie-breaker: only write the canonical AADT_2024
        # when the state GDB had no value for this segment.
        hpms_aadt = match.get("aadt")
        if hpms_aadt is not None and not pd.isna(hpms_aadt):
            try:
                hpms_aadt_int = int(hpms_aadt)
            except (ValueError, TypeError):
                hpms_aadt_int = None
            if hpms_aadt_int is not None:
                enriched.at[idx, "AADT_2024_HPMS"] = hpms_aadt_int
                if not bool(row.get("current_aadt_covered", False)):
                    enriched.at[idx, "AADT_2024"] = hpms_aadt_int
                    enriched.at[idx, "AADT"] = hpms_aadt_int
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
                current_primary = enriched.at[idx, "SIGNED_ROUTE_FAMILY_PRIMARY"]
                current_method = enriched.at[idx, "SIGNED_ROUTE_VERIFY_METHOD"]
                existing_all = parse_signed_route_family_list(
                    enriched.at[idx, "SIGNED_ROUTE_FAMILY_ALL"]
                )
                ordered_families = ordered_signed_route_families_for_hpms(
                    hpms_family,
                    current_primary,
                    existing_all,
                )
                (
                    primary_family,
                    secondary_family,
                    tertiary_family,
                ) = signed_route_family_slots(ordered_families)
                enriched.at[idx, "SIGNED_ROUTE_FAMILY_PRIMARY"] = primary_family
                enriched.at[idx, "SECONDARY_SIGNED_ROUTE_FAMILY"] = secondary_family
                enriched.at[idx, "TERTIARY_SIGNED_ROUTE_FAMILY"] = tertiary_family
                enriched.at[idx, "SIGNED_ROUTE_FAMILY_ALL"] = json.dumps(ordered_families)
                enriched.at[idx, "SIGNED_INTERSTATE_FLAG"] = "Interstate" in ordered_families
                enriched.at[idx, "SIGNED_US_ROUTE_FLAG"] = "U.S. Route" in ordered_families
                enriched.at[idx, "SIGNED_STATE_ROUTE_FLAG"] = "State Route" in ordered_families

                if primary_family == hpms_family:
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_SOURCE"] = "hpms_2024"
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_METHOD"] = "hpms_routesigning"
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_CONFIDENCE"] = "high"
                    enriched.at[idx, "SIGNED_ROUTE_VERIFY_SCORE"] = 0.95
                    if (
                        current_method != "hpms_routesigning"
                        or current_primary != primary_family
                    ):
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

        pct_dh_single = match.get("pct_dh_single")
        if pct_dh_single is not None and not pd.isna(pct_dh_single):
            enriched.at[idx, "HPMS_PCT_DH_SINGLE"] = float(pct_dh_single)

        pct_dh_combination = match.get("pct_dh_combination")
        if pct_dh_combination is not None and not pd.isna(pct_dh_combination):
            enriched.at[idx, "HPMS_PCT_DH_COMBINATION"] = float(pct_dh_combination)

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


def load_hpms_data_for_year(
    year: int,
    rename_map: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, int]:
    """Load a historic HPMS tabular snapshot, optionally normalizing field names.

    Historic years (2020, 2022, 2023) ship with older FHWA field-naming
    vintages; the download pipeline already applied per-year rename maps at
    fetch time, but this loader accepts an in-memory rename map too so the
    same builder can be pointed at a raw vintage tabular if needed.

    Returns `(dataframe, dropped_duplicates)`. 2020's raw submission duplicates
    each segment 2-3× for section-type sparsity (ownership on one row,
    maintenance_operations on another, etc.) with identical AADT. Dedupe on
    the `(route_id, begin_point, end_point)` tuple keeps the first occurrence
    and returns the dropped-row count so the coverage report can log the
    scope-expansion tax per year. For years where the submission is already
    one-row-per-segment (2022, 2023, 2024), the dedupe is a no-op.
    """

    path = (
        PROJECT_ROOT
        / "01-Raw-Data"
        / "Roadway-Inventory"
        / "FHWA_HPMS"
        / str(year)
        / f"hpms_ga_{year}_tabular.json"
    )
    if not path.exists():
        LOGGER.warning("HPMS %d tabular not found at %s", year, path)
        return pd.DataFrame(), 0

    LOGGER.info("Loading HPMS %d data from %s", year, path)
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame([feat["attributes"] for feat in raw["features"]])
    if rename_map:
        df = df.rename(columns=rename_map)

    before = len(df)
    dedupe_keys = [k for k in ("route_id", "begin_point", "end_point") if k in df.columns]
    if len(dedupe_keys) == 3:
        df = df.drop_duplicates(subset=dedupe_keys, keep="first").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        LOGGER.info(
            "Deduped HPMS %d on (route_id, begin_point, end_point): %d -> %d (%d dropped)",
            year,
            before,
            len(df),
            dropped,
        )
    LOGGER.info("Loaded %d HPMS %d records with %d columns", len(df), year, len(df.columns))
    return df, dropped


def build_historic_hpms_aadt_column(
    gdf: pd.DataFrame,
    year: int,
    rename_map: dict[str, str] | None = None,
) -> tuple[pd.Series, dict]:
    """Return an AADT series for the given historic HPMS year.

    Uses the same ROUTE_ID + milepoint-overlap join that `apply_hpms_enrichment`
    performs for 2024, but produces only the single AADT column — no canonical
    AADT fill, no gap-fill, no signed-route derivation. Caller writes the
    returned series as `AADT_{year}_HPMS` on the segments table.

    Historic HPMS years are single-source (no cross-check to the state GDB) and
    intentionally additive: they never modify `AADT`, `AADT_2024`, or any of
    the 2024-vintage source/agreement/confidence columns.

    Returns `(series, stats)` where `stats` carries the dedupe count so the
    coverage report can log the scope-expansion tax per year.
    """

    hpms, deduped = load_hpms_data_for_year(year, rename_map)
    result = pd.Series([pd.NA] * len(gdf), index=gdf.index, dtype="Int64")
    stats = {"deduped": deduped, "matched": 0, "hpms_rows": len(hpms)}
    if hpms.empty:
        return result, stats

    lookup = _build_hpms_lookup(hpms)
    matched = 0
    for idx in gdf.index:
        row = gdf.loc[idx]
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
        hpms_aadt = match.get("aadt")
        if hpms_aadt is None or pd.isna(hpms_aadt):
            continue
        try:
            result.at[idx] = int(hpms_aadt)
            matched += 1
        except (ValueError, TypeError):
            continue

    stats["matched"] = matched
    LOGGER.info("HPMS %d AADT column: %d segments matched", year, matched)
    return result, stats


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
        "final_aadt_coverage": int(gdf["AADT"].notna().sum()) if "AADT" in gdf.columns else 0,
        "final_aadt_source_counts": {
            str(k): int(v)
            for k, v in gdf["AADT_2024_SOURCE"].value_counts(dropna=False).items()
        } if "AADT_2024_SOURCE" in gdf.columns else {},
    }

    output_path = PROJECT_ROOT / "02-Data-Staging" / "reports" / "hpms_enrichment_summary.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote HPMS enrichment summary to %s", output_path)
