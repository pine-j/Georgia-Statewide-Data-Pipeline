# AADT v2 Synthetic Classifier Report

## Classifier rule

A segment's `AADT_{year}_HPMS` is flagged `SYNTHETIC = 1` iff **both** hold:
1. The AADT integer value repeats >= 500 times within (FC, year)
2. `FUNCTIONAL_CLASS` is 6 or 7

FC 1-5 rows are never flagged (0 by construction). NULL HPMS -> NULL flag.

The FC 6-7 scope restriction makes a separate county-spread predicate unnecessary:
within FC 6-7, 500+ exact-integer repeats is sufficient evidence of FHWA default fill.
FC 4 corridor carry-forwards (the false-positive concern) are excluded by the FC restriction.

## Flag rates by (FC, year)

| Year | FC | Total non-null | Synthetic | Empirical | Synthetic % |
|------|----|--------------:|----------:|----------:|------------:|
| 2020 | 1-5 | 41,491 | 0 | 41,491 | 0.0% |
| 2020 | 6 | 4,713 | 2,082 | 2,631 | 44.2% |
| 2020 | 7 | 203,355 | 196,123 | 7,232 | 96.4% |
| 2022 | 1-5 | 41,586 | 0 | 41,586 | 0.0% |
| 2022 | 6 | 1,011 | 0 | 1,011 | 0.0% |
| 2022 | 7 | 59 | 0 | 59 | 0.0% |
| 2023 | 1-5 | 41,733 | 0 | 41,733 | 0.0% |
| 2023 | 6 | 1,019 | 0 | 1,019 | 0.0% |
| 2023 | 7 | 43 | 0 | 43 | 0.0% |
| 2024 | 1-5 | 41,863 | 0 | 41,863 | 0.0% |
| 2024 | 6 | 4,775 | 1,794 | 2,981 | 37.6% |
| 2024 | 7 | 207,570 | 198,447 | 9,123 | 95.6% |

## Sanity check 1: spot-check synthetic-flagged rows

### Year 2020, value 1662 (20 sampled rows)

- FC values: [7]
- All FC 6-7: YES
- Distinct counties in sample: 4

### Year 2024, value 2250 (20 sampled rows)

- FC values: [7]
- All FC 6-7: YES
- Distinct counties in sample: 5

## Sanity check 2: Actual station overlap with synthetic flags

### Year 2020: 90 VIOLATION(s)
  - 1045200000305INC_0.0000_0.4105: TC=045-8069, actual=90, hpms=90.0
  - 1045200000305INC_0.4105_0.4206: TC=045-8069, actual=90, hpms=90.0
  - 1045200003105INC_0.0000_0.5624: TC=045-8069, actual=90, hpms=90.0
  - 1045200018300INC_0.0000_0.8696: TC=045-8069, actual=90, hpms=90.0
  - 1049200046301INC_0.0000_0.0640: TC=049-8011, actual=90, hpms=90.0

### Year 2022: PASS (0 violations)

### Year 2023: PASS (0 violations)

### Year 2024: 465 VIOLATION(s)
  - 1013200000900INC_0.0000_0.5590: TC=023-0316, actual=360, hpms=360.0
  - 1013200056700INC_0.0000_0.4821: TC=023-0316, actual=360, hpms=360.0
  - 1013200056800INC_0.0000_0.1599: TC=023-0316, actual=360, hpms=360.0
  - 1013200056900INC_0.0000_0.0673: TC=023-0316, actual=360, hpms=360.0
  - 1013200057000INC_0.0000_0.5123: TC=023-0316, actual=360, hpms=360.0

## Hard outlier flags (2022, 2023)

| Year | FC bin | Value | Repeat count | Flagged rows |
|------|--------|------:|-------------:|-------------:|
| 2022 | 4-5 | 67 | 129 | 129 |
| 2022 | 4-5 | 564 | 188 | 188 |
| 2022 | 4-5 | 1657 | 108 | 108 |
| 2022 | 4-5 | 1908 | 277 | 277 |
| 2022 | 4-5 | 13300 | 109 | 109 |
| 2022 | 5-6 | 67 | 420 | 420 |
| 2022 | 5-6 | 344 | 307 | 307 |
| 2022 | 5-6 | 564 | 149 | 149 |
| 2022 | 5-6 | 690 | 105 | 105 |
| 2022 | 5-6 | 961 | 1061 | 1061 |
| 2022 | 6-7 | 67 | 105 | 105 |
| 2022 | 6-7 | 564 | 123 | 123 |
| 2023 | 1-2 | 6805 | 185 | 185 |
| 2023 | 4-5 | 4983 | 158 | 158 |
| 2023 | 4-5 | 8194 | 123 | 123 |
| 2023 | 4-5 | 11256 | 186 | 186 |
| 2023 | 4-5 | 13500 | 102 | 102 |
| 2023 | 4-5 | 18106 | 276 | 276 |
| 2023 | 5-6 | 240 | 108 | 108 |
| 2023 | 5-6 | 1153 | 588 | 588 |
| 2023 | 5-6 | 2101 | 217 | 217 |
| 2023 | 5-6 | 2259 | 177 | 177 |
| 2023 | 5-6 | 3553 | 110 | 110 |
| 2023 | 5-6 | 3730 | 190 | 190 |
| 2023 | 5-6 | 5369 | 253 | 253 |
| 2023 | 5-6 | 7906 | 296 | 296 |
| 2023 | 6-7 | 3020 | 127 | 127 |
