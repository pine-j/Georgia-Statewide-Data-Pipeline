"""Derive State Route Prioritization (SRP) classification.

Applies GDOT's documented SRP criteria in priority order to classify
every roadway segment as Critical, High, Medium, or Low. This derived
SRP is a validation/comparison tool — it will NOT be used as a RAPTOR
scoring input to avoid double-counting with individual Connectivity
metrics. However, the gap-fill fields it requires (NHFN, GRIP,
nuclear EPZ, sole county-seat connections) are standalone boolean
fields available for future RAPTOR use.

Criteria source: GDOT Office of Transportation Data, "State Route
Prioritization Network" (2014-2015, biennial review).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = PROJECT_ROOT / "02-Data-Staging" / "reports"

AADT_THRESHOLD = 3000
US_HIGHWAY_MIN_LANES = 4
SEGMENT_SHORT_MILES = 5.0
SPEED_LOW_MPH = 35


def _get_preferred_aadt(row: pd.Series) -> float | None:
    """Return the best available 2024 AADT value for SRP decisions."""
    for column in ("AADT_2024", "AADT_2024_OFFICIAL", "AADT_2024_HPMS", "AADT"):
        value = row.get(column)
        if pd.notna(value):
            return float(value)
    return None


def _check_critical(row: pd.Series) -> list[str]:
    """Check Critical-tier criteria. Returns list of matching reason strings."""
    reasons = []

    fc = row.get("FUNCTIONAL_CLASS")
    interstate_flag = row.get("SIGNED_INTERSTATE_FLAG")
    if (pd.notna(fc) and int(fc) == 1) or (interstate_flag is True or interstate_flag == 1):
        reasons.append("Interstate")

    nhfn = row.get("NHFN")
    if pd.notna(nhfn) and int(nhfn) != 0:
        reasons.append("National freight corridor (NHFN)")

    strahnet = row.get("STRAHNET")
    if pd.notna(strahnet) and int(strahnet) in (1, 2):
        reasons.append("State freight corridor (STRAHNET)")

    nhs_ind = row.get("NHS_IND")
    if pd.notna(nhs_ind) and int(nhs_ind) in range(2, 10):
        reasons.append("Intermodal connector (NHS)")

    return reasons


def _check_high(row: pd.Series) -> list[str]:
    """Check High-tier criteria."""
    reasons = []

    strahnet = row.get("STRAHNET")
    if pd.notna(strahnet) and int(strahnet) in (1, 2):
        reasons.append("STRAHNET")

    nhs_ind = row.get("NHS_IND")
    aadt = _get_preferred_aadt(row)
    if pd.notna(nhs_ind) and int(nhs_ind) >= 1:
        if aadt is not None and aadt > AADT_THRESHOLD:
            reasons.append("NHS principal arterial (AADT > 3,000)")

    us_flag = row.get("SIGNED_US_ROUTE_FLAG")
    rt_gdot = row.get("ROUTE_TYPE_GDOT")
    if (us_flag is True or us_flag == 1) or (pd.notna(rt_gdot) and str(rt_gdot).strip() == "US"):
        reasons.append("US route")

    grip = row.get("IS_GRIP_CORRIDOR")
    if grip is True or grip == 1:
        reasons.append("GRIP corridor")

    nuclear = row.get("IS_NUCLEAR_EPZ_ROUTE")
    if nuclear is True or nuclear == 1:
        reasons.append("Nuclear plant EPZ route")

    evac = row.get("SEC_EVAC")
    if evac is True or evac == 1:
        reasons.append("GEMA evacuation route")

    sole = row.get("IS_SOLE_COUNTY_SEAT_CONNECTION")
    if sole is True or sole == 1:
        reasons.append("Sole county-seat connection")

    if aadt is not None and aadt > AADT_THRESHOLD:
        if not (pd.notna(nhs_ind) and int(nhs_ind) >= 1):
            reasons.append("AADT > 3,000 (non-NHS)")

    return reasons


def _check_medium(row: pd.Series) -> list[str]:
    """Check Medium-tier criteria."""
    reasons = []

    evac = row.get("SEC_EVAC")
    contraflow = row.get("SEC_EVAC_CONTRAFLOW")
    if (evac is True or evac == 1) or (contraflow is True or contraflow == 1):
        reasons.append("Hurricane evacuation route")

    nhs_ind = row.get("NHS_IND")
    aadt = _get_preferred_aadt(row)
    if pd.notna(nhs_ind) and int(nhs_ind) >= 1:
        if aadt is None or aadt <= AADT_THRESHOLD:
            reasons.append("NHS with AADT <= 3,000")

    us_flag = row.get("SIGNED_US_ROUTE_FLAG")
    rt_gdot = row.get("ROUTE_TYPE_GDOT")
    lanes = row.get("THROUGH_LANES")
    is_us = (us_flag is True or us_flag == 1) or (pd.notna(rt_gdot) and str(rt_gdot).strip() == "US")
    if is_us and pd.notna(lanes) and int(lanes) >= US_HIGHWAY_MIN_LANES:
        reasons.append("US highway with 4+ lanes")

    return reasons


def derive_srp_priority(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Apply SRP criteria in priority order and assign tier + reasons.

    Adds:
      SRP_DERIVED       — Critical / High / Medium / Low
      SRP_DERIVED_REASONS — pipe-delimited list of matching criteria
    """
    result = gdf.copy()
    result["SRP_DERIVED"] = "Low"
    result["SRP_DERIVED_REASONS"] = ""

    critical_count = 0
    high_count = 0
    medium_count = 0
    low_count = 0

    for idx in result.index:
        row = result.loc[idx]

        critical_reasons = _check_critical(row)
        if critical_reasons:
            result.at[idx, "SRP_DERIVED"] = "Critical"
            result.at[idx, "SRP_DERIVED_REASONS"] = " | ".join(critical_reasons)
            critical_count += 1
            continue

        high_reasons = _check_high(row)
        if high_reasons:
            result.at[idx, "SRP_DERIVED"] = "High"
            result.at[idx, "SRP_DERIVED_REASONS"] = " | ".join(high_reasons)
            high_count += 1
            continue

        medium_reasons = _check_medium(row)
        if medium_reasons:
            result.at[idx, "SRP_DERIVED"] = "Medium"
            result.at[idx, "SRP_DERIVED_REASONS"] = " | ".join(medium_reasons)
            medium_count += 1
            continue

        low_reasons = []
        aadt = _get_preferred_aadt(row)
        if aadt is not None and aadt < AADT_THRESHOLD:
            low_reasons.append("AADT < 3,000")
        speed = row.get("SPEED_LIMIT")
        if pd.notna(speed) and float(speed) < SPEED_LOW_MPH:
            low_reasons.append("Speed limit < 35 mph")
        seg_len = row.get("segment_length_mi")
        if pd.notna(seg_len) and float(seg_len) < SEGMENT_SHORT_MILES:
            low_reasons.append("Segment < 5 mi")

        result.at[idx, "SRP_DERIVED_REASONS"] = " | ".join(low_reasons) if low_reasons else "Default (no higher criteria met)"
        low_count += 1

    total = len(result)
    LOGGER.info(
        "SRP derivation complete: %d segments — "
        "Critical=%d (%.1f%%), High=%d (%.1f%%), "
        "Medium=%d (%.1f%%), Low=%d (%.1f%%)",
        total,
        critical_count, 100 * critical_count / total if total else 0,
        high_count, 100 * high_count / total if total else 0,
        medium_count, 100 * medium_count / total if total else 0,
        low_count, 100 * low_count / total if total else 0,
    )

    return result


def write_srp_derivation_summary(gdf: pd.DataFrame) -> None:
    """Write SRP derivation summary report."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    tier_counts = gdf["SRP_DERIVED"].value_counts().to_dict()

    reason_counts: dict[str, int] = {}
    for reasons_str in gdf["SRP_DERIVED_REASONS"].dropna():
        for reason in str(reasons_str).split(" | "):
            reason = reason.strip()
            if reason:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

    tier_by_route_type: dict[str, dict[str, int]] = {}
    if "ROUTE_TYPE_GDOT" in gdf.columns:
        for (rt, tier), count in gdf.groupby(["ROUTE_TYPE_GDOT", "SRP_DERIVED"]).size().items():
            tier_by_route_type.setdefault(str(rt), {})[str(tier)] = int(count)

    summary = {
        "segment_count": int(len(gdf)),
        "tier_counts": {str(k): int(v) for k, v in tier_counts.items()},
        "reason_counts": dict(sorted(reason_counts.items(), key=lambda x: -x[1])),
        "tier_by_route_type": tier_by_route_type,
        "null_srp_count": int(gdf["SRP_DERIVED"].isna().sum()),
    }

    output_path = REPORTS_DIR / "srp_derivation_summary.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote SRP derivation summary to %s", output_path)
