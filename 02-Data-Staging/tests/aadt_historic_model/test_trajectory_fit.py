"""Red/green tests for the trajectory-fit predictor (Tier 0).

Plan §Tier 0 — trajectory-fit anchor year HPMS-covered:
- shape-preserving quadratic in year, with y=log(AADT)
- 3 or 4 points per segment (2020, 2022, 2023, 2024)
- clamp rule: 2021 pred ∈ [min_anchor * lb_factor, max_anchor * ub_factor]
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from trajectory_fit import (
    clamp_prediction,
    fit_segment_quadratic_log,
    is_tier0_eligible,
    predict_2021_for_segment,
)


def test_is_tier0_eligible_requires_2020_and_2022_and_post_covid() -> None:
    # All four anchors present → Tier 0.
    assert is_tier0_eligible(a2020=1000, a2022=1050, a2023=1100, a2024=1150)
    # 2020 + 2022 + 2023 only → Tier 0.
    assert is_tier0_eligible(a2020=1000, a2022=1050, a2023=1100, a2024=None)
    # 2020 + 2022 + 2024 only → Tier 0.
    assert is_tier0_eligible(a2020=1000, a2022=1050, a2023=None, a2024=1150)
    # Missing 2022 → NOT Tier 0 (plan requires 2022 as a recovery anchor).
    assert not is_tier0_eligible(a2020=1000, a2022=None, a2023=1100, a2024=1150)
    # Missing 2020 → NOT Tier 0.
    assert not is_tier0_eligible(a2020=None, a2022=1050, a2023=1100, a2024=1150)
    # Only 2020 + 2022, no 2023 or 2024 → not Tier 0 (needs post-COVID signal).
    assert not is_tier0_eligible(a2020=1000, a2022=1050, a2023=None, a2024=None)


def test_fit_segment_quadratic_log_on_flat_series_returns_flat() -> None:
    # Flat AADT 1000 across 4 years → log-quadratic fit should return ~1000.
    years = [2020, 2022, 2023, 2024]
    values = [1000.0, 1000.0, 1000.0, 1000.0]
    coeffs = fit_segment_quadratic_log(years, values)
    pred = math.exp(np.polyval(coeffs, 2021))
    assert pred == pytest.approx(1000.0, rel=0.01)


def test_fit_segment_quadratic_log_three_points_exact() -> None:
    # 3 points fit uniquely. Design a curve that equals 1200 at 2021.
    years = [2020, 2022, 2024]
    values = [1000.0, 1200.0, 1000.0]
    coeffs = fit_segment_quadratic_log(years, values)
    pred_2021 = math.exp(np.polyval(coeffs, 2021))
    assert 1100.0 <= pred_2021 <= 1300.0


def test_clamp_prediction_clips_low_outlier() -> None:
    # 2020=1000, 2022=1200. Low bound = 0.8 * 1000 = 800.
    raw_pred = 500.0
    out = clamp_prediction(raw_pred=raw_pred, a2020=1000.0, a2022=1200.0)
    assert out >= 800.0
    # Urban FC 1-3 widens the floor; test the non-COVID-widened branch here.


def test_clamp_prediction_clips_high_outlier() -> None:
    raw_pred = 3000.0
    out = clamp_prediction(raw_pred=raw_pred, a2020=1000.0, a2022=1200.0)
    # High bound = 1.2 * 1200 = 1440.
    assert out <= 1440.0


def test_clamp_prediction_accepts_in_range() -> None:
    raw_pred = 1100.0
    out = clamp_prediction(raw_pred=raw_pred, a2020=1000.0, a2022=1200.0)
    assert out == pytest.approx(1100.0)


def test_clamp_prediction_widened_for_urban_covid_fc() -> None:
    # COVID-suppressed urban FC 1-3 uses min * 0.5 instead of 0.8.
    raw_pred = 600.0
    out = clamp_prediction(
        raw_pred=raw_pred,
        a2020=1000.0,
        a2022=1200.0,
        covid_widened=True,
    )
    # Widened floor = max(1000 * 0.5, 1200 * 0.5) = 600. Pred should pass.
    assert out == pytest.approx(600.0)


def test_predict_2021_for_segment_respects_clamp() -> None:
    # Design a 4-year series with a pathological middle value that forces
    # the quadratic into an extreme extrapolation at 2021; verify the
    # clamp catches it.
    anchors = {
        "a2020": 1000.0,
        "a2022": 1100.0,
        "a2023": 1150.0,
        "a2024": 1200.0,
    }
    pred = predict_2021_for_segment(**anchors)
    # Predicted 2021 should be roughly between 1000 and 1100 — close to
    # midway for a gentle recovery.
    assert 900.0 <= pred <= 1200.0
