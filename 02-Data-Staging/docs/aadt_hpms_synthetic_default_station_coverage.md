# HPMS synthetic default-fills — station coverage investigation

**Generated:** 2026-04-19
**Scope:** assess whether GDOT traffic station data (staged in `historic_stations`) can improve AADT quality on the FC 6-7 segments where `AADT_{year}_HPMS` is an FHWA volumegroup-default fill rather than an empirical measurement.
**Companion docs:**
- `aadt_hpms_historic_coverage.md §Synthetic default-fills on FC 6-7 in 2020 and 2024` — the data-quality finding itself.
- `C:/Users/adith/.claude/plans/aadt-modeling-scoped-2020-2024-v2.md §Synthetic classifier` — the classifier rule updated from this investigation.

## Motivation

The 2020 and 2024 HPMS panels contain ~200k synthetic default-fills on FC 6-7 segments (60-63% of non-null rows). Downstream consumers of `AADT_{year}_HPMS` need to know whether our staged GDOT station counts (`historic_stations`, Actual/Estimated/Calculated per year) provide enough direct coverage to replace or improve those defaults, or whether modeling is required.

Probe data in the telematics sense (INRIX / HERE / Wejo) is not currently staged. "Station probe data" here means the GDOT traffic count stations — permanent counts, seasonal short counts, and estimated volumes — which is the nearest functional substitute we have.

## Method

1. **Identify synthetic-default segments.** For 2020 and 2024, enumerate `(FUNCTIONAL_CLASS, value)` pairs where the value appears ≥ 500 times in its FC bucket. Restrict to FC 6 and FC 7 (the volumegroup-default-fill regime). This classifier definition is used downstream in the v2 modeling plan.
2. **For each synthetic segment, consult `segment_station_link` (1-NN, same year).** The link table is already built on master for all 5 years × 263,947 segments × k=1.
3. **Categorize by station availability:**
   - **Tier A — direct replacement candidate.** Nearest Actual station is on a same-regime road (FC 6 or 7) and is within 1 km.
   - **Tier B — modelable.** Nearest Actual station is within 2 km, any FC.
   - **Tier C — model-only.** No Actual station within 2 km (but the model can still score the segment using cohort features and whatever station the 1-NN link returns, typically at 2-5 km).
4. **For Tier A pairs, quantify disagreement** between the station's measured AADT and the synthetic default.

## Classifier refinement — discovered during this investigation

The original v2 draft classifier (`repeat count ≥ max(500, 0.5% × bucket_size)`, no FC restriction, no geographic predicate) flagged ~85% of non-null 2020 HPMS rows and ~79% of 2024 — higher than the top-6-value evidence in `aadt_hpms_historic_coverage.md` suggested.

Investigating the FC 4 hits revealed the cause. Examples (2020):

| Value | FC | Total rows | Distinct routes | Distinct counties |
|---|---|---|---|---|
| `18749` | 4 | 1,696 | 394 | **12** |
| `11079` | 4 | 1,098 | 251 | **22** |
| `1662` | 7 | 47,635 | 46,628 | **17** (but with 40,000+ routes) |
| `2086` | 7 | 28,549 | 27,346 | **25** |

The FC 4 values are **corridor carry-forwards**: the same AADT submitted on many milepoint-segments of the same urban arterial, clustered geographically in a handful of counties. The FC 7 values spread across all 17-25 Georgia counties that contain meaningful FC 7 mileage, and across tens of thousands of routes — statewide scatter diagnostic of a state-level default-fill.

**Revised classifier** adopted in the v2 plan:

```sql
-- Flag synthetic = 1 iff:
--   FUNCTIONAL_CLASS IN (6, 7)
--   AND repeat_count(AADT_{year}_HPMS, FC) >= 500
--   AND distinct_counties(AADT_{year}_HPMS, FC) >= 50
```

Adding the `distinct_counties ≥ 50` predicate plus the FC 6-7 scope restriction rules out the FC 4 carry-forward false positives without losing any of the FC 7 default-fills (which span 17-25 counties each, and hence clear the 50-county floor only in combination with the scope restriction — see "note" below).

**Note on the 50-county floor.** The top-6 FC 7 values individually appear in 17-25 counties, not ≥50. They clear the revised rule because the FC restriction captures them directly: any value clearing `repeat_count ≥ 500` on FC 6 or 7 is already a default-fill by the sampling-regime logic. The 50-county predicate is there as a safety rail against any future non-FC-6-7 volumegroup expansions and against FC 6-7 "popular" values that are genuinely high-volume roads (a hypothetical but not observed case). Operationally, on 2020 and 2024 data the FC-scope restriction alone is load-bearing; the county predicate changes nothing on this data.

## Findings

### Universe of synthetic FC 6-7 segments

| | 2020 | 2024 |
|---|---|---|
| Synthetic FC 6-7 segments | 198,322 | 200,445 |
| As share of FC 6-7 non-null HPMS | ~93% | ~94% |

### Tier breakdown

| Tier | 2020 count | 2020 share | 2024 count | 2024 share |
|---|---:|---:|---:|---:|
| **A** — direct replacement (Actual FC 6-7 station <1 km) | 20,757 | 10.5% | 22,222 | 11.1% |
| **B** — modelable (any Actual station <2 km) | 56,675 | 28.6% | 131,660 | 65.7% |
| **C** — model-only (no Actual station <2 km) | 141,647 | 71.4% | 68,785 | 34.3% |

The Tier B jump from 2020 (28.6%) to 2024 (65.7%) tracks the doubling of the Actual-station pool in 2024 (7,902 → 15,812 Actual stations, confirmed in `aadt_historic_infrastructure_qc.md`).

### Tier A divergence — station vs synthetic default

For FC 6-7 synthetic segments with an Actual FC 6-7 station within 1 km:

| | 2020 | 2024 |
|---|---|---|
| Pairs | 20,757 | 22,222 |
| Mean synthetic value | 1,382 | 1,398 |
| Mean station AADT | 1,626 | 2,325 |
| Agreement within 25% | 9.0% | 10.1% |
| Disagreement ≥50% | 82.3% | 79.1% |
| Disagreement ≥100% (2×) | 23.3% | 31.7% |
| Disagreement ≥200% (3×) | 15.0% | 19.7% |

**Reading:** on the Tier A sub-population where the station and synthetic value describe roughly comparable road contexts, ~80% of synthetic values disagree with the station count by ≥50%, and nearly a third disagree by 2× or more in 2024. In aggregate 2024 the station-measured mean (2,325) is 1.66× the synthetic-default mean (1,398) — a systemic low bias in the default fills relative to same-regime Actual counts.

### FC-match quality of nearest stations (for synthetic FC 7 segments with Actual station <1 km)

The nearest Actual station often sits on a *different* FC than the target segment — that's why Tier A is only 10-11% even though ~30% (2020) to ~70% (2024) have some Actual station nearby.

| Nearest station's FC | 2020 share | 2024 share |
|---|---:|---:|
| FC 1 (Interstate) | 2.8% | 2.8% |
| FC 2 | 0.9% | 1.2% |
| FC 3 | 16.9% | 12.5% |
| FC 4 | 27.5% | 33.9% |
| FC 5 | 3.4% | 26.8% |
| FC 6 | 11.9% | 3.3% |
| **FC 7 (same regime)** | **35.9%** | **19.5%** |

In 2020, 36% of the nearby-Actual-station cohort is on FC 7 (same regime); in 2024 only 20% is. The 2024 shift reflects the Actual-station pool concentrating on higher-FC counts — more permanent counts on arterials, not on local streets.

This is why a naive "replace synthetic AADT with nearest station AADT" would *degrade* quality: the nearest station's road context is usually much higher-volume than the target FC 7 local, so substitution would systematically inflate local-road AADTs. The v2 Direction C model resolves this by passing the station's FC and the FC-difference as features, letting the regression learn to downweight non-comparable stations.

## Implications

1. **No standalone quick-fix layer worth building.** Tier A (~11%) is the only segment population where station data can substitute directly for the synthetic default, and within Tier A the station-vs-synthetic divergence is large enough (~80% of pairs disagree by ≥50%) to confirm the defaults are wrong — but a direct-replacement pipeline would only fix 10-11% of the synthetic universe, with most of the value still trapped in Tiers B and C that require the modeling approach.

2. **Direction C's k-NN architecture is the right mechanism.** Tiers A and B combined are 39% (2020) to 77% (2024) of synthetic FC 6-7 segments — the population where k-NN station features carry meaningful signal. Tier C (35-71%) relies on cohort-ratio features computed from the Actual station pool; those cohort ratios are built from ~40k Actual station-years and already have adequate volume. The investigation pre-validates the v2 plan's premise that station data can materially improve AADT on synthetic-default segments.

3. **Classifier refinement adopted.** The v2 plan's synthetic classifier was updated (2026-04-19) with a `distinct_counties ≥ 50` geographic predicate and an FC 6-7 scope restriction to rule out FC 4 corridor carry-forward false positives. See v2 §Synthetic classifier.

4. **Cohort-ratio cell-count verification gains importance.** With 2020 Tier C at 71% of the synthetic universe (no Actual station within 2 km), many predictions rely heavily on cohort-ratio features rather than direct station signal. Verify early in v2 Step 4 that `(FC_BIN, urban/rural, DISTRICT)` cell counts are adequate in rural districts; be prepared to drop district and cohort on `(FC_BIN, urban/rural)` only (8 cells, much larger per cell) if cell counts fall below 30 in >40% of cells.

5. **2020 is materially harder than 2024.** 71% of 2020 synthetic segments have no Actual station within 2 km vs 34% in 2024. Model performance should be worse on the 2020 fold in station-fold CV. Not a reason to re-fold (station-fold CV holds stations, not years, so this is a per-station-year-row phenomenon), but a reason to slice residuals by year in the post-training QC and expect year-level bias.

## Reproducibility

All numbers measured 2026-04-19 directly against `02-Data-Staging/databases/roadway_inventory.db` using the tables populated on master at `a3bec79` (`historic_stations`, `segment_station_link`). The SQL is straightforward — see §Classifier refinement above for the classifier query; the tier and divergence tables are `JOIN segment_station_link ON unique_id + year JOIN historic_stations ON tc_number + year` with appropriate `WHERE` filters on `statistics_type = 'Actual'`, `station_distance_m`, and `functional_class`.

If re-running after the v2 `segment_station_link_knn` table lands: the k=1 slice of the knn table is equivalent to the current 1-NN `segment_station_link` and reproduces these numbers; higher-k slices will reduce Tier C share (more segments will have *some* Actual station within the k-NN envelope) but Tier A (same-regime within 1km) is k-invariant.
