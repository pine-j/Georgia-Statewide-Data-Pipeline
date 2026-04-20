"""Tier 0 — trajectory-fit (shape-preserving quadratic in log space).

Plan reference: `aadt-modeling-scoped-2020-2024.md` §Tier 0 — trajectory-fit
anchor year HPMS-covered.

- Fit y = log(AADT) as a quadratic in `year` using the available HPMS
  anchors in {2020, 2022, 2023, 2024}.
- Evaluate at year = 2021.
- Clamp the prediction to [min_anchor × lb_factor, max_anchor × ub_factor]
  where the factors depend on whether this is a COVID-widened urban FC 1-3
  segment.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

ANCHOR_YEARS_ORDER: tuple[int, ...] = (2020, 2022, 2023, 2024)
EVAL_YEAR_2021: int = 2021

# Standard clamp bounds.
CLAMP_LB_FACTOR_STD = 0.8
CLAMP_UB_FACTOR_STD = 1.2
# Widened bounds for COVID-suppressed urban FC 1-3 2020 anchors.
CLAMP_LB_FACTOR_COVID = 0.5
# Never below AADT_2022 × 0.5 regardless of widening.
CLAMP_FLOOR_VS_2022 = 0.5


def _to_finite(values: list[float | int | None]) -> list[float | None]:
    out = []
    for v in values:
        if v is None:
            out.append(None)
        elif isinstance(v, float) and math.isnan(v):
            out.append(None)
        else:
            out.append(float(v))
    return out


def is_tier0_eligible(
    a2020: float | int | None,
    a2022: float | int | None,
    a2023: float | int | None,
    a2024: float | int | None,
) -> bool:
    """Plan criterion: segment has 2020 AND 2022 AND at least one of 2023/2024."""

    a2020_, a2022_, a2023_, a2024_ = _to_finite([a2020, a2022, a2023, a2024])
    if a2020_ is None or a2022_ is None:
        return False
    return (a2023_ is not None) or (a2024_ is not None)


def fit_segment_quadratic_log(years: list[int], values: list[float]) -> np.ndarray:
    """Fit y = log(AADT) as a quadratic in year. Return polyfit coefficients."""

    years_arr = np.asarray(years, dtype="float64")
    log_values = np.log(np.asarray(values, dtype="float64"))
    # 3 points → exact quadratic; 4 points → least squares.
    return np.polyfit(years_arr, log_values, deg=2)


def clamp_prediction(
    raw_pred: float,
    a2020: float,
    a2022: float,
    covid_widened: bool = False,
) -> float:
    lb_factor = CLAMP_LB_FACTOR_COVID if covid_widened else CLAMP_LB_FACTOR_STD
    lower = min(a2020, a2022) * lb_factor
    # Absolute floor: never below a2022 × 0.5.
    lower = max(lower, a2022 * CLAMP_FLOOR_VS_2022)
    upper = max(a2020, a2022) * CLAMP_UB_FACTOR_STD
    if raw_pred < lower:
        return lower
    if raw_pred > upper:
        return upper
    return raw_pred


@dataclass
class TrajectoryFitResult:
    prediction_2021: float
    anchors_used: tuple[int, ...]
    clamp_fired: bool
    source_tag: str


def predict_2021_for_segment(
    a2020: float | int | None,
    a2022: float | int | None,
    a2023: float | int | None,
    a2024: float | int | None,
    covid_widened: bool = False,
) -> float:
    """Return the clamped 2021 prediction for one Tier 0 segment."""

    if not is_tier0_eligible(a2020, a2022, a2023, a2024):
        raise ValueError("Segment is not Tier 0-eligible")

    anchors = {2020: a2020, 2022: a2022, 2023: a2023, 2024: a2024}
    years: list[int] = []
    values: list[float] = []
    for year in ANCHOR_YEARS_ORDER:
        v = anchors[year]
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        years.append(year)
        values.append(float(v))

    coeffs = fit_segment_quadratic_log(years, values)
    raw_pred = math.exp(float(np.polyval(coeffs, EVAL_YEAR_2021)))
    clamped = clamp_prediction(raw_pred, float(a2020), float(a2022), covid_widened=covid_widened)
    return clamped


def predict_year_for_segment(
    anchors: dict[int, float],
    eval_year: int,
    a2020: float,
    a2022: float,
    covid_widened: bool = False,
) -> float:
    """Generalized holdout evaluator — fit from `anchors`, predict eval_year.

    Used by holdout-fold A (hide 2022) and B (hide 2023) during validation.
    """

    if len(anchors) < 3:
        raise ValueError("need at least 3 anchors for a quadratic fit")

    years = sorted(anchors.keys())
    values = [anchors[y] for y in years]
    coeffs = fit_segment_quadratic_log(years, values)
    raw_pred = math.exp(float(np.polyval(coeffs, eval_year)))
    return clamp_prediction(raw_pred, a2020, a2022, covid_widened=covid_widened)


__all__ = [
    "ANCHOR_YEARS_ORDER",
    "CLAMP_LB_FACTOR_COVID",
    "CLAMP_LB_FACTOR_STD",
    "CLAMP_UB_FACTOR_STD",
    "TrajectoryFitResult",
    "clamp_prediction",
    "fit_segment_quadratic_log",
    "is_tier0_eligible",
    "predict_2021_for_segment",
    "predict_year_for_segment",
]
