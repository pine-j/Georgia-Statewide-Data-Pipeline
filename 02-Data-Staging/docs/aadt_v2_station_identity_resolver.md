# AADT v2 Station Identity Resolver Report

## Summary

Total rows resolved: 128,935
Unique station_uids: 26,294
UID format: TC-based (GA24_{tc_number})

## Resolution by year and method

| Year | Total | Anchor | TC match | Spatial | Scored | TC conflict | Unresolved | Resolve % |
|------|------:|-------:|---------:|--------:|-------:|------------:|-----------:|----------:|
| 2020 | 25,889 | 0 | 19,819 | 2,043 | 3,745 | 14 | 268 | 99.0% |
| 2021 | 25,966 | 0 | 19,880 | 2,056 | 3,753 | 10 | 267 | 99.0% |
| 2022 | 25,668 | 0 | 19,918 | 2,003 | 3,728 | 8 | 11 | 100.0% |
| 2023 | 25,714 | 0 | 19,934 | 2,004 | 3,758 | 7 | 11 | 100.0% |
| 2024 | 25,698 | 25,698 | 0 | 0 | 0 | 0 | 0 | 100.0% |

## Confidence distribution

| Year | High | Medium | Low |
|------|-----:|-------:|----:|
| 2020 | 2,067 | 989 | 22,833 |
| 2021 | 2,092 | 992 | 22,882 |
| 2022 | 2,215 | 930 | 22,523 |
| 2023 | 2,330 | 941 | 22,443 |
| 2024 | 25,696 | 0 | 2 |

## Distance distribution (resolved stations)

- Count: 103,237
- Median: 0.4m
- P95: 8.9m
- Max: 7448.9m

## Unresolved stations

Total unresolved: 557 across 4 years

### Year 2020: 268 unresolved
  - 013-0052
  - 013-0094
  - 013-8015
  - 015-0546
  - 015-0548
  - 015-0558
  - 015-7012
  - 021-8021
  - 025-0201
  - 027-0132
### Year 2021: 267 unresolved
  - 013-0052
  - 013-0094
  - 013-8015
  - 015-0546
  - 015-0548
  - 015-0558
  - 015-7012
  - 021-8021
  - 025-0201
  - 027-0132
### Year 2022: 11 unresolved
  - 059-7107
  - 061-8009
  - 067-r998
  - 067-r999
  - 071-8184
  - 089-8665
  - 151-r999
  - 199-8095
  - 215-r672
  - 215-r673
### Year 2023: 11 unresolved
  - 059-7107
  - 061-8009
  - 067-r998
  - 067-r999
  - 071-8184
  - 089-8665
  - 151-r999
  - 199-8095
  - 215-r672
  - 215-r673

## Acceptance criteria

- Resolved rate (non-anchor): 99.5% (target: >=95%)
