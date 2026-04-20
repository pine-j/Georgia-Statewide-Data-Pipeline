"""Cohort-ratio builder with 5 fold-aware versions.

Plan reference: `aadt-modeling-scoped-2020-2024.md` §Features — fold-aware
cohort-ratio computation.

A cohort is `(FC_BIN, urban/rural, DISTRICT)`. For each cohort:

- `cohort_ratio_2020_to_2022` = median(`AADT_2022_HPMS` / `AADT_2020_HPMS`)
  across HPMS-covered segments in the cohort.
- `cohort_ratio_2020_to_2024` = similarly.
- `cohort_median_2020_hpms` = median 2020 HPMS value per cohort.
- `cohort_size` = member segment count.
- `cohort_fallback_used` = 1 if cohort fell back to the parent
  `(FC_BIN, urban/rural)` aggregate due to <30 members.

The 5 versions all share the same schema — values in the `exclude_{year}`
versions are NULLed on the columns that would otherwise leak the fold's
held-out year.
"""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

COHORT_COLUMNS = [
    "version",
    "fc_bin",
    "urban_rural",
    "district",
    "cohort_size",
    "cohort_fallback_used",
    "cohort_median_2020_hpms",
    "cohort_ratio_2020_to_2022",
    "cohort_ratio_2020_to_2024",
]

COHORT_RATIO_VERSIONS: tuple[str, ...] = (
    "full",
    "exclude_2020",
    "exclude_2022",
    "exclude_2023",
    "exclude_2024",
)

MIN_COHORT_SIZE_FOR_RATIO = 30
RURAL_URBAN_CODE = 99999


def fc_bin_for(fc: int | float | None) -> str | None:
    if fc is None or (isinstance(fc, float) and np.isnan(fc)):
        return None
    fc_int = int(fc)
    if fc_int in (1, 2):
        return "1-2"
    if fc_int in (3, 4):
        return "3-4"
    if fc_int in (5, 6):
        return "5-6"
    if fc_int == 7:
        return "7"
    if fc_int in (8, 9):
        return "8-9"
    return None


def urban_rural_for(code: int | float | None) -> str | None:
    if code is None or (isinstance(code, float) and np.isnan(code)):
        return None
    return "rural" if int(code) == RURAL_URBAN_CODE else "urban"


def assign_cohort(segments: pd.DataFrame) -> pd.DataFrame:
    fc_bin = segments["FUNCTIONAL_CLASS"].map(fc_bin_for)
    urban_rural = segments["URBAN_CODE"].map(urban_rural_for)
    district = segments["DISTRICT"]
    out = pd.DataFrame(
        {
            "fc_bin": fc_bin.to_numpy(),
            "urban_rural": urban_rural.to_numpy(),
            "district": district.to_numpy(),
            "cohort_key": list(zip(fc_bin, urban_rural, district)),
        }
    )
    return out


def _median_ratio(numer: pd.Series, denom: pd.Series) -> float:
    mask = numer.notna() & denom.notna() & (denom > 0)
    if mask.sum() == 0:
        return float("nan")
    return float(np.median(numer[mask] / denom[mask]))


def _cohort_aggregate(group: pd.DataFrame) -> pd.Series:
    ratio_22 = _median_ratio(group["AADT_2022_HPMS"], group["AADT_2020_HPMS"])
    ratio_24 = _median_ratio(group["AADT_2024_HPMS"], group["AADT_2020_HPMS"])
    median_2020 = float(np.median(group["AADT_2020_HPMS"].dropna())) if group["AADT_2020_HPMS"].notna().any() else float("nan")
    return pd.Series(
        {
            "cohort_size": int(len(group)),
            "cohort_median_2020_hpms": median_2020,
            "cohort_ratio_2020_to_2022": ratio_22,
            "cohort_ratio_2020_to_2024": ratio_24,
        }
    )


def _fallback_parent_aggregate(segments: pd.DataFrame) -> pd.DataFrame:
    grouped = segments.groupby(["fc_bin", "urban_rural"], dropna=False)
    parent = grouped.apply(_cohort_aggregate, include_groups=False).reset_index()
    return parent.rename(
        columns={
            "cohort_size": "parent_cohort_size",
            "cohort_median_2020_hpms": "parent_cohort_median_2020_hpms",
            "cohort_ratio_2020_to_2022": "parent_cohort_ratio_2020_to_2022",
            "cohort_ratio_2020_to_2024": "parent_cohort_ratio_2020_to_2024",
        }
    )


def build_cohort_ratios(
    segments: pd.DataFrame,
    version: str,
    min_cohort_size: int = MIN_COHORT_SIZE_FOR_RATIO,
) -> pd.DataFrame:
    """Build the cohort ratios table for one fold-version.

    `segments` must have columns FUNCTIONAL_CLASS, URBAN_CODE, DISTRICT,
    AADT_2020_HPMS, AADT_2022_HPMS, AADT_2023_HPMS, AADT_2024_HPMS.

    Version rules:
    - `full` — both ratios computed from all HPMS data.
    - `exclude_2022` — cohort_ratio_2020_to_2022 := NULL; _to_2024 kept.
    - `exclude_2024` — cohort_ratio_2020_to_2024 := NULL; _to_2022 kept.
    - `exclude_2020` — both ratios AND cohort_median_2020_hpms := NULL.
    - `exclude_2023` — no-op (2023 is not in either production ratio).
    """

    if version not in COHORT_RATIO_VERSIONS:
        raise ValueError(f"Unknown cohort-ratio version: {version!r}")

    enriched = segments.copy()
    assigned = assign_cohort(enriched)
    enriched["fc_bin"] = assigned["fc_bin"]
    enriched["urban_rural"] = assigned["urban_rural"]

    # Primary aggregate on (fc_bin, urban_rural, district).
    primary = (
        enriched.groupby(["fc_bin", "urban_rural", "DISTRICT"], dropna=False)
        .apply(_cohort_aggregate, include_groups=False)
        .reset_index()
        .rename(columns={"DISTRICT": "district"})
    )

    parent = _fallback_parent_aggregate(enriched)

    merged = primary.merge(parent, on=["fc_bin", "urban_rural"], how="left")

    merged["cohort_fallback_used"] = (merged["cohort_size"] < min_cohort_size).astype(int)

    def _apply_fallback(row: pd.Series, primary_col: str, parent_col: str) -> float:
        if row["cohort_fallback_used"]:
            return row[parent_col]
        return row[primary_col]

    for col, parent_col in [
        ("cohort_size", "parent_cohort_size"),
        ("cohort_median_2020_hpms", "parent_cohort_median_2020_hpms"),
        ("cohort_ratio_2020_to_2022", "parent_cohort_ratio_2020_to_2022"),
        ("cohort_ratio_2020_to_2024", "parent_cohort_ratio_2020_to_2024"),
    ]:
        merged[col] = merged.apply(lambda r: _apply_fallback(r, col, parent_col), axis=1)

    # Apply the fold mask.
    if version == "exclude_2022":
        merged["cohort_ratio_2020_to_2022"] = np.nan
    elif version == "exclude_2024":
        merged["cohort_ratio_2020_to_2024"] = np.nan
    elif version == "exclude_2020":
        merged["cohort_ratio_2020_to_2022"] = np.nan
        merged["cohort_ratio_2020_to_2024"] = np.nan
        merged["cohort_median_2020_hpms"] = np.nan
    # full, exclude_2023 → no mask.

    merged.insert(0, "version", version)

    return merged[COHORT_COLUMNS]


def build_all_versions(segments: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(
        [build_cohort_ratios(segments, v) for v in COHORT_RATIO_VERSIONS],
        axis=0,
        ignore_index=True,
    )


__all__ = [
    "COHORT_COLUMNS",
    "COHORT_RATIO_VERSIONS",
    "MIN_COHORT_SIZE_FOR_RATIO",
    "assign_cohort",
    "build_all_versions",
    "build_cohort_ratios",
    "fc_bin_for",
    "urban_rural_for",
]
