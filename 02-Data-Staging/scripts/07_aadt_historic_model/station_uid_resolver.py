"""Station identity resolver for cross-year TC_NUMBER instability.

Assigns a canonical ``station_uid`` to every (year, tc_number) row in
``historic_stations``, anchored on 2024 stations.

Resolution order for non-anchor years:
  1. **Primary (tc_number):** same TC exists in 2024 and coords within 500m.
  2. **Spatial:** nearest 2024 station within 500m (by haversine).
  3. **Unresolved:** no 2024 station within 500m; gets a fresh uid.

2020-2022 chain through 2023's resolver since those years share a
stable TC roster.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

ANCHOR_YEAR = 2024
MATCH_RADIUS_M = 500.0
EARTH_RADIUS_M = 6_371_000.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in meters between two points."""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return float(2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a)))


def _build_balltree(df: pd.DataFrame) -> BallTree:
    coords_rad = np.deg2rad(df[["latitude", "longitude"]].values)
    return BallTree(coords_rad, metric="haversine")


def resolve_stations(stations: pd.DataFrame) -> pd.DataFrame:
    """Resolve all station rows to canonical station_uids.

    Parameters
    ----------
    stations : DataFrame with columns [year, tc_number, latitude, longitude].

    Returns
    -------
    DataFrame with original columns plus [station_uid, resolver_method,
    resolver_delta_m, resolver_confidence].
    """
    result = stations.copy()
    result["station_uid"] = None
    result["resolver_method"] = None
    result["resolver_delta_m"] = np.nan
    result["resolver_confidence"] = None

    anchor = (
        result[(result["year"] == ANCHOR_YEAR) & result["latitude"].notna() & result["longitude"].notna()]
        .drop_duplicates(subset=["tc_number"], keep="first")
    )

    uid_map_24: dict[str, str] = {}
    for i, (_, row) in enumerate(anchor.iterrows()):
        uid = f"GA24_{i:05d}"
        uid_map_24[row["tc_number"]] = uid

    anchor_mask = result["year"] == ANCHOR_YEAR
    result.loc[anchor_mask, "station_uid"] = result.loc[anchor_mask, "tc_number"].map(uid_map_24)
    result.loc[anchor_mask, "resolver_method"] = "anchor"
    result.loc[anchor_mask, "resolver_delta_m"] = 0.0
    result.loc[anchor_mask, "resolver_confidence"] = "high"

    null_anchor = result[(result["year"] == ANCHOR_YEAR) & (result["latitude"].isna() | result["longitude"].isna())]
    if not null_anchor.empty:
        for tc in null_anchor["tc_number"].unique():
            if tc not in uid_map_24:
                uid_map_24[tc] = f"GA24_NULL_{tc}"
        null_mask = (result["year"] == ANCHOR_YEAR) & (result["latitude"].isna() | result["longitude"].isna())
        result.loc[null_mask, "station_uid"] = result.loc[null_mask, "tc_number"].map(uid_map_24)
        result.loc[null_mask, "resolver_method"] = "anchor"
        result.loc[null_mask, "resolver_confidence"] = "low"

    anchor_coords = anchor[["tc_number", "latitude", "longitude"]].copy()
    anchor_tc_list = anchor_coords["tc_number"].tolist()
    anchor_tc_set = set(anchor_tc_list)
    anchor_lats = anchor_coords["latitude"].values
    anchor_lons = anchor_coords["longitude"].values

    if len(anchor_coords) > 0:
        tree = _build_balltree(anchor_coords)
    else:
        _assign_unresolved_years(result, [2023, 2022, 2021, 2020])
        return result

    uid_2023: dict[str, str] = {}
    method_2023: dict[str, str] = {}
    delta_2023: dict[str, float] = {}
    confidence_2023: dict[str, str] = {}

    year_2023 = result[result["year"] == 2023].drop_duplicates(subset=["tc_number"], keep="first")

    for _, row in year_2023.iterrows():
        tc = row["tc_number"]
        lat, lon = row["latitude"], row["longitude"]

        if tc in uid_map_24 and tc in anchor_tc_set:
            idx = anchor_tc_list.index(tc)
            dist = _haversine_m(lat, lon, anchor_lats[idx], anchor_lons[idx])
            if dist <= MATCH_RADIUS_M:
                uid_2023[tc] = uid_map_24[tc]
                method_2023[tc] = "tc_number"
                delta_2023[tc] = dist
                confidence_2023[tc] = "high"
                continue

        query_rad = np.deg2rad([[lat, lon]])
        radius_rad = MATCH_RADIUS_M / EARTH_RADIUS_M
        ind = tree.query_radius(query_rad, r=radius_rad)[0]

        if len(ind) == 0:
            uid_2023[tc] = f"GA23_{tc}"
            method_2023[tc] = "unresolved"
            delta_2023[tc] = np.nan
            confidence_2023[tc] = "low"
        elif len(ind) == 1:
            matched_tc = anchor_tc_list[ind[0]]
            dist = _haversine_m(lat, lon, anchor_lats[ind[0]], anchor_lons[ind[0]])
            uid_2023[tc] = uid_map_24[matched_tc]
            method_2023[tc] = "spatial"
            delta_2023[tc] = dist
            confidence_2023[tc] = "medium"
        else:
            best_idx = None
            best_score = float("inf")
            for i in ind:
                dist = _haversine_m(lat, lon, anchor_lats[i], anchor_lons[i])
                score = dist
                if best_idx is None or score < best_score:
                    best_score = score
                    best_idx = i
            matched_tc = anchor_tc_list[best_idx]
            dist = _haversine_m(lat, lon, anchor_lats[best_idx], anchor_lons[best_idx])
            uid_2023[tc] = uid_map_24[matched_tc]
            method_2023[tc] = "spatial"
            delta_2023[tc] = dist
            confidence_2023[tc] = "medium"

    mask_23 = result["year"] == 2023
    result.loc[mask_23, "station_uid"] = result.loc[mask_23, "tc_number"].map(uid_2023)
    result.loc[mask_23, "resolver_method"] = result.loc[mask_23, "tc_number"].map(method_2023)
    result.loc[mask_23, "resolver_delta_m"] = result.loc[mask_23, "tc_number"].map(delta_2023)
    result.loc[mask_23, "resolver_confidence"] = result.loc[mask_23, "tc_number"].map(confidence_2023)

    for year in [2022, 2021, 2020]:
        mask = result["year"] == year
        year_df = result[mask].drop_duplicates(subset=["tc_number"], keep="first")

        uid_year: dict[str, str] = {}
        method_year: dict[str, str] = {}
        delta_year: dict[str, float] = {}
        conf_year: dict[str, str] = {}

        for _, row in year_df.iterrows():
            tc = row["tc_number"]
            lat, lon = row["latitude"], row["longitude"]

            if tc in uid_2023:
                resolved_uid = uid_2023[tc]
                if resolved_uid.startswith("GA23_"):
                    uid_year[tc] = f"GA{year % 100:02d}_{tc}"
                    method_year[tc] = "unresolved"
                    delta_year[tc] = np.nan
                    conf_year[tc] = "low"
                else:
                    uid_idx = [i for i, a_tc in enumerate(anchor_tc_list) if uid_map_24.get(a_tc) == resolved_uid]
                    if uid_idx:
                        dist = _haversine_m(lat, lon, anchor_lats[uid_idx[0]], anchor_lons[uid_idx[0]])
                        if dist <= MATCH_RADIUS_M:
                            uid_year[tc] = resolved_uid
                            method_year[tc] = "chained_via_2023"
                            delta_year[tc] = dist
                            conf_year[tc] = confidence_2023.get(tc, "medium")
                            continue

                    uid_year[tc] = resolved_uid
                    method_year[tc] = "chained_via_2023"
                    delta_year[tc] = delta_2023.get(tc, np.nan)
                    conf_year[tc] = confidence_2023.get(tc, "medium")
            else:
                query_rad = np.deg2rad([[lat, lon]])
                radius_rad = MATCH_RADIUS_M / EARTH_RADIUS_M
                ind = tree.query_radius(query_rad, r=radius_rad)[0]

                if len(ind) == 0:
                    uid_year[tc] = f"GA{year % 100:02d}_{tc}"
                    method_year[tc] = "unresolved"
                    delta_year[tc] = np.nan
                    conf_year[tc] = "low"
                else:
                    best_idx = ind[np.argmin([
                        _haversine_m(lat, lon, anchor_lats[i], anchor_lons[i])
                        for i in ind
                    ])]
                    matched_tc = anchor_tc_list[best_idx]
                    dist = _haversine_m(lat, lon, anchor_lats[best_idx], anchor_lons[best_idx])
                    uid_year[tc] = uid_map_24[matched_tc]
                    method_year[tc] = "spatial"
                    delta_year[tc] = dist
                    conf_year[tc] = "medium"

        result.loc[mask, "station_uid"] = result.loc[mask, "tc_number"].map(uid_year)
        result.loc[mask, "resolver_method"] = result.loc[mask, "tc_number"].map(method_year)
        result.loc[mask, "resolver_delta_m"] = result.loc[mask, "tc_number"].map(delta_year)
        result.loc[mask, "resolver_confidence"] = result.loc[mask, "tc_number"].map(conf_year)

    return result
