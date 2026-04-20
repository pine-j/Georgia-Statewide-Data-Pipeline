"""Red/green tests for the cohort-ratio builder.

Plan §Features — Fold-aware cohort-ratio computation: a cohort table
with 5 versions (`full`, `exclude_{year}` for year ∈ {2020, 2022, 2023,
2024}) so that each fold's feature vector is leak-free. The schema is
invariant across all 5 versions — columns are the same, values are
NULLed in the exclude-versions where the held-out year would leak.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from cohort_ratios import (
    COHORT_COLUMNS,
    COHORT_RATIO_VERSIONS,
    assign_cohort,
    build_cohort_ratios,
    fc_bin_for,
    urban_rural_for,
)


def test_fc_bin_maps_expected_classes() -> None:
    assert fc_bin_for(1) == "1-2"
    assert fc_bin_for(2) == "1-2"
    assert fc_bin_for(3) == "3-4"
    assert fc_bin_for(4) == "3-4"
    assert fc_bin_for(5) == "5-6"
    assert fc_bin_for(6) == "5-6"
    assert fc_bin_for(7) == "7"
    assert fc_bin_for(None) is None


def test_urban_rural_from_urban_code() -> None:
    # URBAN_CODE 99999 = rural per GDOT convention; anything else = urban.
    assert urban_rural_for(99999) == "rural"
    assert urban_rural_for(3817) == "urban"
    assert urban_rural_for(99998) == "urban"  # Small Urban Sections
    assert urban_rural_for(None) is None


def test_assign_cohort_produces_tuple_key() -> None:
    segs = pd.DataFrame(
        {
            "FUNCTIONAL_CLASS": [1, 3, 7, 7],
            "URBAN_CODE": [99999, 3817, 99999, 99999],
            "DISTRICT": [1, 7, 5, 5],
        }
    )
    assigned = assign_cohort(segs)
    assert list(assigned.columns) == ["fc_bin", "urban_rural", "district", "cohort_key"]
    assert assigned["cohort_key"].iloc[0] == ("1-2", "rural", 1)
    assert assigned["cohort_key"].iloc[2] == ("7", "rural", 5)


def test_build_cohort_ratios_full_version_all_ratios_present() -> None:
    # Two cohorts, each with 3 segments, all HPMS years present.
    segs = pd.DataFrame(
        {
            "FUNCTIONAL_CLASS": [1, 1, 1, 7, 7, 7],
            "URBAN_CODE": [99999] * 3 + [3817] * 3,
            "DISTRICT": [1, 1, 1, 5, 5, 5],
            "AADT_2020_HPMS": [1000, 1200, 800, 500, 600, 700],
            "AADT_2022_HPMS": [1100, 1300, 900, 550, 660, 770],
            "AADT_2023_HPMS": [1150, 1350, 920, 580, 680, 780],
            "AADT_2024_HPMS": [1200, 1400, 950, 600, 700, 800],
        }
    )
    out = build_cohort_ratios(segs, version="full")
    assert list(out.columns) == COHORT_COLUMNS
    assert (out["version"] == "full").all()
    # Two cohorts — (1-2, rural, district 1) and (7, urban, district 5).
    assert len(out) == 2
    # Both ratios present (non-NULL).
    assert out["cohort_ratio_2020_to_2022"].notna().all()
    assert out["cohort_ratio_2020_to_2024"].notna().all()
    # cohort_size columns present.
    assert (out["cohort_size"] >= 3).all()


def test_build_cohort_ratios_exclude_2022_nulls_that_column() -> None:
    segs = pd.DataFrame(
        {
            "FUNCTIONAL_CLASS": [1] * 30,
            "URBAN_CODE": [99999] * 30,
            "DISTRICT": [1] * 30,
            "AADT_2020_HPMS": list(range(1000, 1030)),
            "AADT_2022_HPMS": list(range(1100, 1130)),
            "AADT_2023_HPMS": list(range(1150, 1180)),
            "AADT_2024_HPMS": list(range(1200, 1230)),
        }
    )
    out = build_cohort_ratios(segs, version="exclude_2022")
    assert (out["version"] == "exclude_2022").all()
    assert out["cohort_ratio_2020_to_2022"].isna().all()
    # The 2024 ratio is unaffected.
    assert out["cohort_ratio_2020_to_2024"].notna().all()


def test_build_cohort_ratios_exclude_2020_nulls_both_ratios() -> None:
    segs = pd.DataFrame(
        {
            "FUNCTIONAL_CLASS": [1] * 30,
            "URBAN_CODE": [99999] * 30,
            "DISTRICT": [1] * 30,
            "AADT_2020_HPMS": list(range(1000, 1030)),
            "AADT_2022_HPMS": list(range(1100, 1130)),
            "AADT_2023_HPMS": list(range(1150, 1180)),
            "AADT_2024_HPMS": list(range(1200, 1230)),
        }
    )
    out = build_cohort_ratios(segs, version="exclude_2020")
    assert out["cohort_ratio_2020_to_2022"].isna().all()
    assert out["cohort_ratio_2020_to_2024"].isna().all()
    # cohort_median_2020_hpms is also NULLed.
    assert out["cohort_median_2020_hpms"].isna().all()


def test_build_cohort_ratios_exclude_2023_keeps_both_ratios() -> None:
    # Plan: 2023 is never a cohort-feature source, so exclude_2023 mask
    # is a no-op — both ratios remain populated.
    segs = pd.DataFrame(
        {
            "FUNCTIONAL_CLASS": [1] * 30,
            "URBAN_CODE": [99999] * 30,
            "DISTRICT": [1] * 30,
            "AADT_2020_HPMS": list(range(1000, 1030)),
            "AADT_2022_HPMS": list(range(1100, 1130)),
            "AADT_2023_HPMS": list(range(1150, 1180)),
            "AADT_2024_HPMS": list(range(1200, 1230)),
        }
    )
    out = build_cohort_ratios(segs, version="exclude_2023")
    assert (out["cohort_ratio_2020_to_2022"].notna()).all()
    assert (out["cohort_ratio_2020_to_2024"].notna()).all()


def test_cohort_fallback_fires_for_small_cohort() -> None:
    # Cohort has only 5 segments (< 30 threshold). Fallback flag must fire.
    segs = pd.DataFrame(
        {
            "FUNCTIONAL_CLASS": [1, 1, 1, 1, 1],
            "URBAN_CODE": [99999] * 5,
            "DISTRICT": [1] * 5,
            "AADT_2020_HPMS": [1000, 1100, 1200, 1300, 1400],
            "AADT_2022_HPMS": [1050, 1150, 1260, 1370, 1480],
            "AADT_2023_HPMS": [1080, 1180, 1290, 1410, 1520],
            "AADT_2024_HPMS": [1100, 1200, 1320, 1450, 1560],
        }
    )
    out = build_cohort_ratios(segs, version="full")
    assert (out["cohort_fallback_used"] == 1).all()


def test_cohort_ratio_versions_constant_matches_plan() -> None:
    assert COHORT_RATIO_VERSIONS == (
        "full",
        "exclude_2020",
        "exclude_2022",
        "exclude_2023",
        "exclude_2024",
    )
