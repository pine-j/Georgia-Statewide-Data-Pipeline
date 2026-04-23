"""Scored station_uid_resolver: resolves historic stations to a 2024 anchor
using TC_NUMBER primary match + multi-feature scored spatial fallback.

Scoring uses three features with proven discriminative power:
- Distance (30%): spatial proximity to candidate
- AADT log-ratio (35%): traffic volume agreement
- Functional class match (35%): road classification agreement

K-factor, D-factor, and truck % showed no discriminative power in testing
and are excluded from the score.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd


WEIGHT_DIST = 0.30
WEIGHT_AADT = 0.35
WEIGHT_FC = 0.35

NEUTRAL_PENALTY = 0.5


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000.0
    lat1r, lat2r = np.radians(lat1), np.radians(lat2)
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def candidate_score(
    dist_m: float,
    aadt_ratio: float | None,
    fc_match: bool | None,
    max_dist_m: float = 500.0,
) -> float:
    """Compute a match score for a single candidate. Lower = better match."""
    s_dist = min(dist_m / max_dist_m, 1.0)

    if aadt_ratio is not None and not (isinstance(aadt_ratio, float) and math.isnan(aadt_ratio)):
        s_aadt = min(abs(math.log(max(aadt_ratio, 0.01))), 2.0) / 2.0
    else:
        s_aadt = NEUTRAL_PENALTY

    if fc_match is not None and not (isinstance(fc_match, float) and math.isnan(fc_match)):
        s_fc = 0.0 if fc_match else 1.0
    else:
        s_fc = NEUTRAL_PENALTY

    return WEIGHT_DIST * s_dist + WEIGHT_AADT * s_aadt + WEIGHT_FC * s_fc


def _safe_ratio(a, b):
    if pd.isna(a) or pd.isna(b):
        return None
    a, b = float(a), float(b)
    if a <= 0 or b <= 0:
        return None
    return a / b


def _safe_fc_match(fc_src, fc_cand):
    if pd.isna(fc_src) or pd.isna(fc_cand):
        return None
    return int(fc_src) == int(fc_cand)


def build_scored_resolver(
    anchor: pd.DataFrame,
    target: pd.DataFrame,
    target_year: int,
    max_dist_m: float = 500.0,
) -> pd.DataFrame:
    """Resolve target-year stations to anchor (2024) UIDs using scored matching.

    Returns DataFrame with columns:
        year, tc_number, station_uid, resolver_method, resolver_delta_m, score_margin
    """
    anchor_tc_set = set(anchor["tc_number"])
    anchor_idx = anchor.set_index("tc_number")
    anchor_lats = anchor["latitude"].values
    anchor_lons = anchor["longitude"].values
    anchor_tcs = anchor["tc_number"].values

    has_aadt = "aadt" in anchor.columns
    has_fc = "functional_class" in anchor.columns

    results = []
    for _, row in target.iterrows():
        tc = row["tc_number"]
        lat, lon = row["latitude"], row["longitude"]

        # Primary: same TC_NUMBER within max_dist_m
        tc_matched = False
        if tc in anchor_tc_set:
            a = anchor_idx.loc[tc]
            if isinstance(a, pd.DataFrame):
                a = a.iloc[0]
            delta = float(haversine_m(lat, lon, a["latitude"], a["longitude"]))
            if delta <= max_dist_m:
                results.append({
                    "year": target_year, "tc_number": tc,
                    "station_uid": f"GA24_{tc}",
                    "resolver_method": "tc_number",
                    "resolver_delta_m": delta,
                    "score_margin": float("nan"),
                })
                continue
            tc_matched = True

        # Spatial: find all candidates within max_dist_m
        dists = haversine_m(lat, lon, anchor_lats, anchor_lons)
        within_mask = dists <= max_dist_m
        n_within = int(within_mask.sum())

        if n_within == 0:
            results.append({
                "year": target_year, "tc_number": tc,
                "station_uid": f"GA{target_year % 100:02d}_{tc}",
                "resolver_method": "tc_conflict" if tc_matched else "unresolved",
                "resolver_delta_m": float(np.min(dists)),
                "score_margin": float("nan"),
            })
            continue

        if n_within == 1:
            idx = int(np.argmin(dists))
            results.append({
                "year": target_year, "tc_number": tc,
                "station_uid": f"GA24_{anchor_tcs[idx]}",
                "resolver_method": "spatial",
                "resolver_delta_m": float(dists[idx]),
                "score_margin": float("nan"),
            })
            continue

        # Ambiguous: score all candidates
        within_idx = np.where(within_mask)[0]
        candidates = []
        for ci in within_idx:
            d = float(dists[ci])
            cand_tc = anchor_tcs[ci]

            aadt_ratio = None
            if has_aadt:
                aadt_ratio = _safe_ratio(anchor.iloc[ci]["aadt"], row.get("aadt"))

            fc_match_val = None
            if has_fc:
                fc_match_val = _safe_fc_match(row.get("functional_class"),
                                               anchor.iloc[ci].get("functional_class"))

            score = candidate_score(d, aadt_ratio, fc_match_val, max_dist_m)
            candidates.append((score, d, cand_tc))

        candidates.sort(key=lambda x: x[0])
        best_score, best_dist, best_tc = candidates[0]
        margin = candidates[1][0] - best_score if len(candidates) > 1 else 1.0

        results.append({
            "year": target_year, "tc_number": tc,
            "station_uid": f"GA24_{best_tc}",
            "resolver_method": "scored",
            "resolver_delta_m": best_dist,
            "score_margin": margin,
        })

    return pd.DataFrame(results)
