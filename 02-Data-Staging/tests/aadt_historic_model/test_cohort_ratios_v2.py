"""Red/green tests for v2 cohort ratios (station Actual-based).

Plan §Step 4 — rebuild cohort_ratios from Actual station AADTs with
station_uid-based folds instead of year-excludes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from cohort_ratios_v2 import (
    N_FOLDS,
    V2_COHORT_COLUMNS,
    V2_VERSIONS,
    assign_station_folds,
    build_v2_cohort_ratios,
    build_all_v2_versions,
)


def _station_table(n_uids: int = 40, years: tuple = (2020, 2022, 2023, 2024)) -> pd.DataFrame:
    """Create synthetic station table with n_uids stations across years."""
    rows = []
    for i in range(n_uids):
        uid = f"GA24_{i:05d}"
        fc_bin = "7" if i % 2 == 0 else "3-4"
        ur = "rural" if i % 3 == 0 else "urban"
        dist = i % 5 + 1
        for year in years:
            rows.append({
                "station_uid": uid,
                "year": year,
                "aadt": 1000 + i * 10 + (year - 2020) * 5,
                "fc_bin": fc_bin,
                "urban_rural": ur,
                "district": dist,
            })
    return pd.DataFrame(rows)


def test_v2_versions_count() -> None:
    assert len(V2_VERSIONS) == 6
    assert V2_VERSIONS[0] == "full"


def test_full_version_has_all_ratios() -> None:
    st = _station_table(40)
    result = build_v2_cohort_ratios(st, version="full")
    assert (result["version"] == "full").all()
    assert result["cohort_ratio_2020_to_2022"].notna().any()
    assert result["cohort_ratio_2020_to_2024"].notna().any()
    assert list(result.columns) == V2_COHORT_COLUMNS


def test_fold_excludes_stations() -> None:
    st = _station_table(50)
    folds = assign_station_folds(st["station_uid"])
    fold_0_uids = set(st.loc[folds == 0, "station_uid"].unique())

    full = build_v2_cohort_ratios(st, version="full")
    fold_0 = build_v2_cohort_ratios(st, version="fold_0", exclude_uids=fold_0_uids)

    full_total = full["cohort_size"].sum()
    fold_total = fold_0["cohort_size"].sum()
    assert fold_total < full_total


def test_assign_folds_deterministic() -> None:
    uids = pd.Series([f"GA24_{i:05d}" for i in range(100)])
    f1 = assign_station_folds(uids)
    f2 = assign_station_folds(uids)
    assert (f1 == f2).all()


def test_assign_folds_balanced() -> None:
    uids = pd.Series([f"GA24_{i:05d}" for i in range(100)])
    folds = assign_station_folds(uids)
    counts = folds.value_counts()
    assert counts.min() == 20
    assert counts.max() == 20


def test_build_all_produces_6_versions() -> None:
    st = _station_table(50)
    result = build_all_v2_versions(st)
    assert result["version"].nunique() == 6
    assert set(result["version"].unique()) == set(V2_VERSIONS)


def test_cohort_fallback_fires_for_small_cohort() -> None:
    st = _station_table(5)
    result = build_v2_cohort_ratios(st, version="full")
    assert (result["cohort_fallback_used"] == 1).all()
