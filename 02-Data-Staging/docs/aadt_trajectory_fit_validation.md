# AADT trajectory-fit validation (Tier 0)

Holdout validation for the Tier 0 shape-preserving quadratic (log AADT). Each segment in Tier 0 is re-fit using 3 of its 4 HPMS anchors, and the held-out year is predicted; absolute percentage error is computed vs the true HPMS value.

## Gates

- MAPE ≤ 25%  — hardstop
- Median APE ≤ 8%
- Within-10% share ≥ 80%

## Hide-2022 fold

- Segment count: 42,375
- MAPE: 200.36%
- Median APE: 6.02%
- P95 APE: 446.68%
- Within 10%: 63.79%
- Gate met: NO
- Hardstop triggered: YES

| FC bin | Count | MAPE | Median APE |
|---|---|---|---|
| 1-2 | 4,666 | 12.56% | 3.86% |
| 3-4 | 21,016 | 234.35% | 5.08% |
| 5-6 | 16,653 | 209.91% | 8.54% |
| 7 | 40 | 271.36% | 22.86% |

## Hide-2023 fold

- Segment count: 42,375
- MAPE: 13.00%
- Median APE: 4.51%
- P95 APE: 76.19%
- Within 10%: 71.66%
- Gate met: NO
- Hardstop triggered: NO

| FC bin | Count | MAPE | Median APE |
|---|---|---|---|
| 1-2 | 4,666 | 12.29% | 2.90% |
| 3-4 | 21,016 | 10.25% | 3.82% |
| 5-6 | 16,653 | 16.59% | 6.33% |
| 7 | 40 | 40.56% | 17.82% |
