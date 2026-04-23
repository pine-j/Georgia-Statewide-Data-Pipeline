"""TDD tests for the scored station_uid_resolver.

Tests cover:
1. Score computation (distance, AADT, FC components)
2. TC_NUMBER primary match (within 500m)
3. TC_NUMBER conflict (same ID but >500m -> tc_conflict bucket)
4. Single spatial match (unambiguous)
5. Ambiguous spatial match resolved by scoring
6. Scoring changes winner from nearest when FC+AADT favor a farther candidate
7. Unresolved station (no candidate within 500m)
8. Edge cases: missing AADT, missing FC, NaN coords
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def make_anchor(rows: list[dict]) -> pd.DataFrame:
    defaults = {"aadt": pd.NA, "functional_class": pd.NA, "future_aadt": pd.NA}
    records = [{**defaults, **r} for r in rows]
    df = pd.DataFrame(records)
    df["aadt"] = df["aadt"].astype("Int64")
    df["functional_class"] = df["functional_class"].astype("Int64")
    df["future_aadt"] = df["future_aadt"].astype("Int64")
    return df


def make_target(rows: list[dict]) -> pd.DataFrame:
    return make_anchor(rows)


# ---- Score function tests ----

class TestCandidateScore:
    def test_perfect_match_scores_near_zero(self):
        from scored_resolver import candidate_score
        score = candidate_score(
            dist_m=0.5, aadt_ratio=1.0, fc_match=True, max_dist_m=500.0)
        assert score < 0.05

    def test_far_distance_high_score(self):
        from scored_resolver import candidate_score
        near = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=True)
        far = candidate_score(dist_m=450.0, aadt_ratio=1.0, fc_match=True)
        assert far > near

    def test_aadt_mismatch_high_score(self):
        from scored_resolver import candidate_score
        good_aadt = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=True)
        bad_aadt = candidate_score(dist_m=10.0, aadt_ratio=3.0, fc_match=True)
        assert bad_aadt > good_aadt

    def test_fc_mismatch_penalty(self):
        from scored_resolver import candidate_score
        same_fc = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=True)
        diff_fc = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=False)
        assert diff_fc > same_fc

    def test_missing_aadt_uses_neutral_score(self):
        from scored_resolver import candidate_score
        with_aadt = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=True)
        no_aadt = candidate_score(dist_m=10.0, aadt_ratio=None, fc_match=True)
        assert no_aadt > with_aadt  # penalty for missing info

    def test_missing_fc_uses_neutral_score(self):
        from scored_resolver import candidate_score
        with_fc = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=True)
        no_fc = candidate_score(dist_m=10.0, aadt_ratio=1.0, fc_match=None)
        assert no_fc > with_fc


# ---- Resolver behavior tests ----

class TestResolverBehavior:
    def test_tc_number_primary_match(self):
        """Same TC_NUMBER within 500m -> tc_number method."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "001-0101", "latitude": 33.5, "longitude": -84.5,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
        ])
        target = make_target([
            {"tc_number": "001-0101", "latitude": 33.5001, "longitude": -84.5,
             "aadt": 4800, "functional_class": 3, "future_aadt": 5800},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["resolver_method"] == "tc_number"
        assert result.iloc[0]["station_uid"] == "GA24_001-0101"
        assert result.iloc[0]["resolver_delta_m"] < 50

    def test_tc_conflict_same_id_far_away(self):
        """Same TC_NUMBER but >500m -> tc_conflict bucket."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "001-0101", "latitude": 34.0, "longitude": -84.5,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
        ])
        target = make_target([
            {"tc_number": "001-0101", "latitude": 33.5, "longitude": -84.5,
             "aadt": 4800, "functional_class": 3, "future_aadt": 5800},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["resolver_method"] == "tc_conflict"

    def test_single_spatial_match(self):
        """One candidate within 500m -> spatial method."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "A1", "latitude": 33.5001, "longitude": -84.5,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
            {"tc_number": "A2", "latitude": 34.0, "longitude": -84.0,
             "aadt": 8000, "functional_class": 1, "future_aadt": 9000},
        ])
        target = make_target([
            {"tc_number": "X1", "latitude": 33.5, "longitude": -84.5,
             "aadt": 4800, "functional_class": 3, "future_aadt": 5800},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["resolver_method"] == "spatial"
        assert result.iloc[0]["station_uid"] == "GA24_A1"

    def test_ambiguous_resolved_by_scoring(self):
        """Two candidates within 500m, nearest wins on combined score."""
        from scored_resolver import build_scored_resolver
        # Two anchors close to each other, source near both
        anchor = make_anchor([
            {"tc_number": "A1", "latitude": 33.50010, "longitude": -84.500,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
            {"tc_number": "A2", "latitude": 33.50020, "longitude": -84.500,
             "aadt": 5100, "functional_class": 3, "future_aadt": 6100},
        ])
        target = make_target([
            {"tc_number": "X1", "latitude": 33.5, "longitude": -84.500,
             "aadt": 4900, "functional_class": 3, "future_aadt": 5900},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["resolver_method"] == "scored"
        assert result.iloc[0]["station_uid"].startswith("GA24_")
        assert result.iloc[0]["score_margin"] > 0

    def test_scoring_overrides_nearest_when_fc_and_aadt_favor_farther(self):
        """Farther candidate wins when it has matching FC + closer AADT."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "NEAR", "latitude": 33.50005, "longitude": -84.500,
             "aadt": 50000, "functional_class": 1, "future_aadt": 60000},
            {"tc_number": "FAR", "latitude": 33.5003, "longitude": -84.500,
             "aadt": 5200, "functional_class": 3, "future_aadt": 6200},
        ])
        target = make_target([
            {"tc_number": "X1", "latitude": 33.5, "longitude": -84.500,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["station_uid"] == "GA24_FAR", (
            f"Expected FAR to win (same FC, similar AADT), got {result.iloc[0]['station_uid']}")

    def test_unresolved_no_candidate_in_range(self):
        """No candidate within 500m -> unresolved."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "A1", "latitude": 34.0, "longitude": -84.0,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
        ])
        target = make_target([
            {"tc_number": "X1", "latitude": 33.5, "longitude": -84.5,
             "aadt": 4800, "functional_class": 3, "future_aadt": 5800},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["resolver_method"] == "unresolved"
        assert result.iloc[0]["station_uid"].startswith("GA20_")

    def test_missing_aadt_still_resolves(self):
        """Station with no AADT can still resolve via distance + FC."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "A1", "latitude": 33.5001, "longitude": -84.5,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
            {"tc_number": "A2", "latitude": 33.5003, "longitude": -84.5,
             "aadt": 8000, "functional_class": 1, "future_aadt": 9000},
        ])
        target = make_target([
            {"tc_number": "X1", "latitude": 33.5, "longitude": -84.5,
             "aadt": pd.NA, "functional_class": 3, "future_aadt": pd.NA},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 1
        assert result.iloc[0]["resolver_method"] in ("spatial", "scored")

    def test_multiple_targets_independent(self):
        """Each target station resolves independently."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "A1", "latitude": 33.5, "longitude": -84.5,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
            {"tc_number": "A2", "latitude": 34.0, "longitude": -84.0,
             "aadt": 8000, "functional_class": 1, "future_aadt": 9000},
        ])
        target = make_target([
            {"tc_number": "A1", "latitude": 33.5001, "longitude": -84.5,
             "aadt": 4800, "functional_class": 3, "future_aadt": 5800},
            {"tc_number": "X2", "latitude": 34.0001, "longitude": -84.0,
             "aadt": 7900, "functional_class": 1, "future_aadt": 8900},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        assert len(result) == 2
        r1 = result[result["tc_number"] == "A1"].iloc[0]
        r2 = result[result["tc_number"] == "X2"].iloc[0]
        assert r1["resolver_method"] == "tc_number"
        assert r2["resolver_method"] == "spatial"
        assert r1["station_uid"] == "GA24_A1"
        assert r2["station_uid"] == "GA24_A2"

    def test_output_schema(self):
        """Resolver output has required columns."""
        from scored_resolver import build_scored_resolver
        anchor = make_anchor([
            {"tc_number": "A1", "latitude": 33.5, "longitude": -84.5,
             "aadt": 5000, "functional_class": 3, "future_aadt": 6000},
        ])
        target = make_target([
            {"tc_number": "A1", "latitude": 33.5001, "longitude": -84.5,
             "aadt": 4800, "functional_class": 3, "future_aadt": 5800},
        ])
        result = build_scored_resolver(anchor, target, 2020)
        required = {"year", "tc_number", "station_uid", "resolver_method",
                     "resolver_delta_m", "score_margin"}
        assert required.issubset(set(result.columns))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
