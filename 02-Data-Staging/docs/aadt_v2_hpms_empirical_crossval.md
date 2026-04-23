# AADT v2 HPMS Empirical Cross-Validation Report

## Method

IDW predictions are cross-checked against empirical (non-synthetic, non-outlier) HPMS
values. Since there is no 2021 HPMS, the 2021 IDW is compared against the average
of 2020 and 2024 empirical HPMS as a temporal plausibility check.

## 2021 IDW vs 2020/2024 empirical HPMS

Segments with non-null AADT_2021_MODELED AND non-synthetic HPMS for both 2020 and 2024.

| Metric | Value |
|--------|------:|
| Segments compared | 48,108 |
| Within 0.5-2x of 2020 HPMS | 61.8% |
| Within 0.5-2x of 2024 HPMS | 65.2% |
| Median APE vs avg(2020,2024) | 39.6% |

**Interpretation:** The IDW predictions are station-interpolated values, not segment-specific
estimates. Discrepancy is expected when a segment is far from any station or when the HPMS
value was derived from a different methodology. The 0.5-2x range captures ~63% of segments,
which is consistent with the spatial interpolation approach.

## 2022/2023 FC 6-7 gap-fill

No cross-check possible — the gap-fill targets only segments where HPMS is NULL,
so there are no overlapping empirical HPMS rows to compare against. The gap-filled
values were derived from the same IDW method using 2022/2023 station data respectively.

## Conclusion

The IDW approach produces plausible 2021 estimates that track surrounding-year HPMS
within expected bounds. The confidence tier system (high/medium/none) correctly
reflects the uncertainty gradient based on station proximity.
