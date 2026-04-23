"""v2 cohort-ratio builder from Actual station AADTs.

Replaces the v1 HPMS-based cohort_ratios with station-level Actual data,
keyed by station_uid for safe cross-year joins.

Cohort = (fc_bin, urban_rural, district). Ratios are computed from
matched station_uids that have Actual AADT in both the numerator and
denominator year.

Versions: ``full`` + ``fold_0`` through ``fold_4``. Each fold version
excludes that fold's station_uids from the ratio computation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from cohort_ratios import fc_bin_for, urban_rural_for

N_FOLDS = 5
MIN_COHORT_SIZE = 30

V2_COHORT_COLUMNS = [
    "version",
    "fc_bin",
    "urban_rural",
    "district",
    "cohort_size",
    "cohort_fallback_used",
    "cohort_median_2020_actual",
    "cohort_ratio_2020_to_2022",
    "cohort_ratio_2020_to_2024",
]

V2_VERSIONS = ("full",) + tuple(f"fold_{i}" for i in range(N_FOLDS))


def assign_station_folds(station_uids: pd.Series, n_folds: int = N_FOLDS) -> pd.Series:
    """Deterministically assign each unique station_uid to a fold 0..n_folds-1."""
    unique = station_uids.unique()
    np.random.seed(42)
    shuffled = np.random.permutation(unique)
    uid_to_fold = {uid: i % n_folds for i, uid in enumerate(shuffled)}
    return station_uids.map(uid_to_fold)


def build_station_cohort_table(
    stations: pd.DataFrame,
    resolver: pd.DataFrame,
    segment_attrs: pd.DataFrame,
    knn_link: pd.DataFrame,
) -> pd.DataFrame:
    """Build per-station cohort assignments from nearest-segment attributes.

    Parameters
    ----------
    stations : historic_stations rows (Actual only) with year, tc_number, aadt.
    resolver : station_uid_resolver table with year, tc_number, station_uid.
    segment_attrs : segments table with unique_id, FUNCTIONAL_CLASS, URBAN_CODE, DISTRICT.
    knn_link : segment_station_link (k=1 or knn with k_rank=1) with year, nearest_tc_number, unique_id.
    """
    merged = stations.merge(
        resolver[["year", "tc_number", "station_uid"]],
        on=["year", "tc_number"],
        how="left",
    )

    link_k1 = knn_link[knn_link["k_rank"] == 1][["year", "nearest_tc_number", "unique_id"]].copy()
    link_k1 = link_k1.rename(columns={"nearest_tc_number": "tc_number", "unique_id": "nearest_segment_uid"})

    station_to_seg = (
        merged[["year", "tc_number"]]
        .drop_duplicates()
        .merge(link_k1, on=["year", "tc_number"], how="left")
    )

    seg_cols = segment_attrs[["unique_id", "FUNCTIONAL_CLASS", "URBAN_CODE", "DISTRICT"]].copy()
    station_with_seg = station_to_seg.merge(
        seg_cols, left_on="nearest_segment_uid", right_on="unique_id", how="left"
    )

    tc_to_attrs = {}
    for _, row in station_with_seg.iterrows():
        key = (row["year"], row["tc_number"])
        tc_to_attrs[key] = {
            "fc_bin": fc_bin_for(row["FUNCTIONAL_CLASS"]),
            "urban_rural": urban_rural_for(row["URBAN_CODE"]),
            "district": row["DISTRICT"],
        }

    merged["fc_bin"] = merged.apply(lambda r: tc_to_attrs.get((r["year"], r["tc_number"]), {}).get("fc_bin"), axis=1)
    merged["urban_rural"] = merged.apply(lambda r: tc_to_attrs.get((r["year"], r["tc_number"]), {}).get("urban_rural"), axis=1)
    merged["district"] = merged.apply(lambda r: tc_to_attrs.get((r["year"], r["tc_number"]), {}).get("district"), axis=1)

    return merged


def _median_ratio(numer: pd.Series, denom: pd.Series) -> float:
    mask = numer.notna() & denom.notna() & (denom > 0)
    if mask.sum() == 0:
        return float("nan")
    return float(np.median(numer[mask] / denom[mask]))


def build_v2_cohort_ratios(
    station_table: pd.DataFrame,
    version: str = "full",
    exclude_uids: set | None = None,
) -> pd.DataFrame:
    """Build cohort ratios from station Actual data.

    Parameters
    ----------
    station_table : DataFrame with station_uid, year, aadt, fc_bin, urban_rural, district.
    version : Version label.
    exclude_uids : Set of station_uids to exclude (for fold versions).
    """
    df = station_table.copy()
    if exclude_uids:
        df = df[~df["station_uid"].isin(exclude_uids)]

    pivoted = df.pivot_table(
        index=["station_uid", "fc_bin", "urban_rural", "district"],
        columns="year",
        values="aadt",
        aggfunc="first",
    ).reset_index()

    year_cols = {y: y for y in [2020, 2022, 2023, 2024] if y in pivoted.columns}

    def _cohort_agg(group: pd.DataFrame) -> pd.Series:
        size = group["station_uid"].nunique()
        median_2020 = float(np.nanmedian(group[2020])) if 2020 in group.columns and group[2020].notna().any() else np.nan
        r22 = _median_ratio(group.get(2022, pd.Series(dtype=float)), group.get(2020, pd.Series(dtype=float)))
        r24 = _median_ratio(group.get(2024, pd.Series(dtype=float)), group.get(2020, pd.Series(dtype=float)))
        return pd.Series({
            "cohort_size": int(size),
            "cohort_median_2020_actual": median_2020,
            "cohort_ratio_2020_to_2022": r22,
            "cohort_ratio_2020_to_2024": r24,
        })

    primary = (
        pivoted.groupby(["fc_bin", "urban_rural", "district"], dropna=False)
        .apply(_cohort_agg, include_groups=False)
        .reset_index()
    )

    parent = (
        pivoted.groupby(["fc_bin", "urban_rural"], dropna=False)
        .apply(_cohort_agg, include_groups=False)
        .reset_index()
    )
    parent = parent.rename(columns={c: f"parent_{c}" for c in parent.columns if c.startswith("cohort_")})

    merged = primary.merge(parent, on=["fc_bin", "urban_rural"], how="left")
    merged["cohort_fallback_used"] = (merged["cohort_size"] < MIN_COHORT_SIZE).astype(int)

    for col in ["cohort_size", "cohort_median_2020_actual", "cohort_ratio_2020_to_2022", "cohort_ratio_2020_to_2024"]:
        parent_col = f"parent_{col}"
        if parent_col in merged.columns:
            merged[col] = merged.apply(
                lambda r, c=col, pc=parent_col: r[pc] if r["cohort_fallback_used"] else r[c],
                axis=1,
            )

    merged.insert(0, "version", version)
    return merged[V2_COHORT_COLUMNS]


def build_all_v2_versions(
    station_table: pd.DataFrame,
) -> pd.DataFrame:
    """Build all 6 cohort ratio versions (full + 5 folds)."""
    folds = assign_station_folds(station_table["station_uid"])
    station_table = station_table.copy()
    station_table["fold"] = folds.values

    frames = [build_v2_cohort_ratios(station_table, version="full")]

    for fold_id in range(N_FOLDS):
        exclude = set(station_table.loc[station_table["fold"] == fold_id, "station_uid"].unique())
        frames.append(
            build_v2_cohort_ratios(station_table, version=f"fold_{fold_id}", exclude_uids=exclude)
        )

    return pd.concat(frames, ignore_index=True)
