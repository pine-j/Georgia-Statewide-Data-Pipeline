# AADT v2 IDW Prediction Report

## Method

Inverse-distance-weighted interpolation of k=5 nearest stations per year.
Cutoff: 2000m. Segments beyond cutoff get NULL prediction.

Confidence tiers:
- **high**: nearest station within 500m AND on the same route
- **medium**: nearest station within 2000m (but not high)
- **none**: nearest station beyond 2000m (prediction is NULL)

## 2021 AADT Coverage

| Metric | Count | % |
|--------|------:|--:|
| Total segments | 263,947 | 100% |
| Predicted | 242,850 | 92.0% |
| NULL (beyond cutoff) | 21,097 | 8.0% |

### Confidence tiers

| Tier | Count | % |
|------|------:|--:|
| high | 28,185 | 10.7% |
| medium | 214,665 | 81.3% |
| none | 21,097 | 8.0% |

### Columns written to segments table

| Column | Type | Description |
|--------|------|-------------|
| AADT_2021_MODELED | INTEGER | IDW-weighted prediction (NULL beyond 2000m) |
| AADT_2021_NEIGHBOR_MIN | INTEGER | Minimum AADT among k nearest stations used |
| AADT_2021_NEIGHBOR_MAX | INTEGER | Maximum AADT among k nearest stations used |
| AADT_2021_CONFIDENCE | TEXT | high / medium / none |
| AADT_2021_SOURCE | TEXT | station_idw_v2 |
| AADT_2021_NEAREST_STATION_DIST_M | REAL | Distance to nearest station |
| AADT_2021_NEAREST_STATION_TC | TEXT | TC_NUMBER of nearest station |
| AADT_2021_N_STATIONS_USED | INTEGER | Stations within 2000m contributing to IDW |

## FC 6-7 Gap-Fill (2022/2023)

FC 6-7 segments where FHWA left HPMS NULL (volumegroup sampling).

| Year | Segments filled | % of NULL FC 6-7 |
|------|---------------:|----------------:|
| 2022 | 192,199 | 91.0% |
| 2023 | 192,204 | 91.0% |

Columns: `AADT_{2022,2023}_LOCAL_FILL` + `_LOCAL_FILL_CONFIDENCE`.

## Synthetic Default Replacement (2020/2024)

FC 6-7 segments where HPMS carries FHWA volumegroup defaults (flagged by
synthetic classifier). IDW provides spatially-differentiated alternatives.

| Year | Segments filled | % of synthetic-flagged |
|------|---------------:|---------------------:|
| 2020 | 180,120 | 90.9% |
| 2024 | 181,706 | 90.7% |

Synthetic HPMS median ~1,100-1,400 vs IDW median ~5,400-5,900 (5.6x gap
confirms the defaults are placeholders, not measurements).

Columns: `AADT_{2020,2024}_LOCAL_FILL` + `_LOCAL_FILL_CONFIDENCE`.

## Model pivot rationale

The original plan called for a HistGradientBoostingRegressor (HGB) station
model. All 5 folds tripped Decision Gate 2 (MAPE 288-353%, Median APE ~57%).
Root cause: predicting absolute AADT from road characteristics is fundamentally
hard due to demand-side variance. The IDW approach sidesteps this by directly
interpolating from nearby station measurements, with a 2000m confidence cutoff
to avoid publishing unreliable predictions.
