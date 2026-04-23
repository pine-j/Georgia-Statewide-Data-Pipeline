"""HPMS synthetic-default classifier and hard-outlier flagger.

Flags FHWA volumegroup-default fills on FC 6-7 segments. A row is flagged
synthetic iff ALL three predicates hold for its (FC, year, AADT value):
  1. repeat_count >= 500
  2. distinct_counties >= 50
  3. FUNCTIONAL_CLASS in {6, 7}

Hard outlier: any integer value repeating >100 times within an FC bin
in 2022 or 2023 (where synthetic fill is not operative).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

HPMS_YEARS: tuple[int, ...] = (2020, 2022, 2023, 2024)

REPEAT_THRESHOLD = 500
HARD_OUTLIER_REPEAT_THRESHOLD = 100
SYNTHETIC_FC = {6.0, 7.0}


def classify_synthetic(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Add ``AADT_{year}_HPMS_SYNTHETIC`` column (0/1/NULL) to *df*.

    Returns a copy with the new column appended.
    """
    col = f"AADT_{year}_HPMS"
    out_col = f"{col}_SYNTHETIC"
    result = df.copy()

    non_null = result[col].notna()
    is_fc67 = result["FUNCTIONAL_CLASS"].isin(SYNTHETIC_FC)

    result[out_col] = np.nan

    result.loc[non_null & ~is_fc67, out_col] = 0

    fc67_non_null = non_null & is_fc67
    if not fc67_non_null.any():
        return result

    subset = result.loc[fc67_non_null, [col, "FUNCTIONAL_CLASS", "COUNTY_ID"]].copy()
    value_stats = (
        subset
        .groupby(["FUNCTIONAL_CLASS", col])
        .agg(repeat_count=("COUNTY_ID", "size"))
        .reset_index()
    )
    flagged_values = value_stats.loc[
        value_stats["repeat_count"] >= REPEAT_THRESHOLD,
        ["FUNCTIONAL_CLASS", col],
    ]

    if flagged_values.empty:
        result.loc[fc67_non_null, out_col] = 0
        return result

    flag_set = set(zip(flagged_values["FUNCTIONAL_CLASS"], flagged_values[col]))
    is_synthetic = result.apply(
        lambda r: (r["FUNCTIONAL_CLASS"], r[col]) in flag_set if fc67_non_null.get(r.name, False) else False,
        axis=1,
    )
    result.loc[fc67_non_null, out_col] = 0
    result.loc[is_synthetic, out_col] = 1

    return result


def classify_hard_outliers(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Add ``AADT_{year}_HPMS_HARD_OUTLIER`` column (0/1/NULL).

    Only valid for 2022 and 2023.
    """
    if year not in (2022, 2023):
        raise ValueError(f"Hard outlier classification only applies to 2022 and 2023, got {year}")

    col = f"AADT_{year}_HPMS"
    out_col = f"{col}_HARD_OUTLIER"
    result = df.copy()

    non_null = result[col].notna()
    result[out_col] = np.nan

    if not non_null.any():
        return result

    subset = result.loc[non_null, [col, "FUNCTIONAL_CLASS"]].copy()
    fc_bin = subset["FUNCTIONAL_CLASS"].apply(
        lambda x: f"{int(x)}-{int(x)+1}" if x is not None and not (isinstance(x, float) and np.isnan(x)) else None
    )
    subset["fc_bin"] = fc_bin

    value_counts = (
        subset
        .groupby(["fc_bin", col])
        .size()
        .reset_index(name="repeat_count")
    )
    flagged = value_counts.loc[
        value_counts["repeat_count"] > HARD_OUTLIER_REPEAT_THRESHOLD,
        ["fc_bin", col],
    ]

    result.loc[non_null, out_col] = 0

    if flagged.empty:
        return result

    fc_bin_series = result["FUNCTIONAL_CLASS"].apply(
        lambda x: f"{int(x)}-{int(x)+1}" if x is not None and not (isinstance(x, float) and np.isnan(x)) else None
    )
    flag_set = set(zip(flagged["fc_bin"], flagged[col]))
    is_outlier = result.apply(
        lambda r: (fc_bin_series.get(r.name), r[col]) in flag_set if non_null.get(r.name, False) else False,
        axis=1,
    )
    result.loc[is_outlier, out_col] = 1

    return result
