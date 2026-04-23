"""Red/green tests for the HPMS synthetic-default classifier.

Plan §Synthetic classifier — flags FHWA volumegroup-default fills on FC 6-7
segments. Three predicates must ALL hold for a (FC, year, value) triple:
  1. repeat_count >= 500
  2. distinct_counties >= 50
  3. FUNCTIONAL_CLASS in {6, 7}

Also tests the hard-outlier exclusion for 2022/2023.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from synthetic_classifier import (
    HPMS_YEARS,
    classify_synthetic,
    classify_hard_outliers,
)


def _make_segments(
    n: int,
    fc: float,
    aadt_col: str,
    aadt_val: int,
    n_counties: int = 60,
) -> pd.DataFrame:
    """Helper: build a segments-like DataFrame with n rows sharing one AADT value."""
    return pd.DataFrame({
        "unique_id": [f"seg_{i}" for i in range(n)],
        "FUNCTIONAL_CLASS": [fc] * n,
        "COUNTY_ID": [i % n_counties for i in range(n)],
        aadt_col: [aadt_val] * n,
    })


def test_fc7_high_repeat_high_county_flagged() -> None:
    """Top value in FC 7 with 1000 repeats across 60 counties -> synthetic=1."""
    df = _make_segments(1000, 7.0, "AADT_2020_HPMS", 1662, n_counties=60)
    result = classify_synthetic(df, year=2020)
    assert (result["AADT_2020_HPMS_SYNTHETIC"] == 1).all()


def test_fc7_high_repeat_low_county_still_flagged() -> None:
    """500 repeats in few counties -> still synthetic (FC 6-7 scope handles false positives)."""
    df = _make_segments(500, 7.0, "AADT_2020_HPMS", 1662, n_counties=20)
    result = classify_synthetic(df, year=2020)
    assert (result["AADT_2020_HPMS_SYNTHETIC"] == 1).all()


def test_fc7_low_repeat_not_flagged() -> None:
    """200 repeats across 60 counties -> not synthetic (below repeat threshold)."""
    df = _make_segments(200, 7.0, "AADT_2020_HPMS", 1662, n_counties=60)
    result = classify_synthetic(df, year=2020)
    assert (result["AADT_2020_HPMS_SYNTHETIC"] == 0).all()


def test_fc4_never_flagged() -> None:
    """FC 4 rows are never flagged, even with high repeat+county counts."""
    df = _make_segments(2000, 4.0, "AADT_2020_HPMS", 18749, n_counties=80)
    result = classify_synthetic(df, year=2020)
    assert (result["AADT_2020_HPMS_SYNTHETIC"] == 0).all()


def test_fc1_never_flagged() -> None:
    """FC 1 rows are never flagged."""
    df = _make_segments(1000, 1.0, "AADT_2020_HPMS", 5000, n_counties=100)
    result = classify_synthetic(df, year=2020)
    assert (result["AADT_2020_HPMS_SYNTHETIC"] == 0).all()


def test_null_hpms_produces_null_flag() -> None:
    """NULL HPMS value -> NULL synthetic flag."""
    df = pd.DataFrame({
        "unique_id": ["seg_0", "seg_1"],
        "FUNCTIONAL_CLASS": [7.0, 7.0],
        "COUNTY_ID": [1, 2],
        "AADT_2020_HPMS": [np.nan, np.nan],
    })
    result = classify_synthetic(df, year=2020)
    assert result["AADT_2020_HPMS_SYNTHETIC"].isna().all()


def test_mixed_values_only_dominant_flagged() -> None:
    """Two values in FC 7: one crosses thresholds, the other doesn't."""
    dominant = _make_segments(600, 7.0, "AADT_2020_HPMS", 1662, n_counties=55)
    rare = _make_segments(100, 7.0, "AADT_2020_HPMS", 9999, n_counties=30)
    rare["unique_id"] = [f"rare_{i}" for i in range(len(rare))]
    df = pd.concat([dominant, rare], ignore_index=True)
    result = classify_synthetic(df, year=2020)
    flagged = result.set_index("unique_id")
    assert (flagged.loc[dominant["unique_id"], "AADT_2020_HPMS_SYNTHETIC"] == 1).all()
    assert (flagged.loc[rare["unique_id"], "AADT_2020_HPMS_SYNTHETIC"] == 0).all()


def test_mixed_fc_only_fc67_evaluated() -> None:
    """FC 4 rows with identical values to FC 7 rows: only FC 7 flagged."""
    fc7 = _make_segments(600, 7.0, "AADT_2024_HPMS", 2250, n_counties=60)
    fc4 = _make_segments(600, 4.0, "AADT_2024_HPMS", 2250, n_counties=60)
    fc4["unique_id"] = [f"fc4_{i}" for i in range(len(fc4))]
    df = pd.concat([fc7, fc4], ignore_index=True)
    result = classify_synthetic(df, year=2024)
    flagged = result.set_index("unique_id")
    assert (flagged.loc[fc7["unique_id"], "AADT_2024_HPMS_SYNTHETIC"] == 1).all()
    assert (flagged.loc[fc4["unique_id"], "AADT_2024_HPMS_SYNTHETIC"] == 0).all()


def test_all_four_years_supported() -> None:
    assert HPMS_YEARS == (2020, 2022, 2023, 2024)


# --- Hard outlier tests ---

def test_hard_outlier_2022_high_repeat_flagged() -> None:
    """Value repeating >100 times in an FC bin in 2022 -> hard outlier."""
    df = _make_segments(150, 7.0, "AADT_2022_HPMS", 67, n_counties=10)
    result = classify_hard_outliers(df, year=2022)
    assert (result["AADT_2022_HPMS_HARD_OUTLIER"] == 1).all()


def test_hard_outlier_2022_low_repeat_not_flagged() -> None:
    """Value repeating 50 times in 2022 -> not a hard outlier."""
    df = _make_segments(50, 7.0, "AADT_2022_HPMS", 67, n_counties=10)
    result = classify_hard_outliers(df, year=2022)
    assert (result["AADT_2022_HPMS_HARD_OUTLIER"] == 0).all()


def test_hard_outlier_only_2022_2023() -> None:
    """Hard outlier classification only applies to 2022 and 2023."""
    df = _make_segments(200, 7.0, "AADT_2020_HPMS", 67, n_counties=10)
    with pytest.raises(ValueError, match="2022.*2023"):
        classify_hard_outliers(df, year=2020)


def test_hard_outlier_null_stays_null() -> None:
    df = pd.DataFrame({
        "unique_id": ["seg_0"],
        "FUNCTIONAL_CLASS": [7.0],
        "COUNTY_ID": [1],
        "AADT_2022_HPMS": [np.nan],
    })
    result = classify_hard_outliers(df, year=2022)
    assert result["AADT_2022_HPMS_HARD_OUTLIER"].isna().all()
