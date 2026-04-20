# AADT historic modeling — infrastructure QC report

Coverage of the Step 3-4 staging outputs for the AADT historic station-to-segment
modeling pipeline. Generated 2026-04-19 as the first deliverable of the
`aadt-historic-model` branch.

This is the staging-infrastructure report. Modeling QC (cross-validation,
trajectory-fit validation, summary) lands as three separate reports when the
model runs complete.

## Data source verification

| Source | Path | Size | Notes |
|---|---|---|---|
| `Traffic_Historical.zip` | [01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip](../../01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip) | 568,610,816 B | Re-downloaded 2026-04-19 from `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/Traffic_Historical.zip` (HTTP 200, Last-Modified 2025-10-03). Not present on machine before this branch. |
| `TRAFFIC_Data_2024.gdb` | [01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_2024_Geodatabase/TRAFFIC_Data_2024.gdb](../../01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_2024_Geodatabase/TRAFFIC_Data_2024.gdb) | — | Pre-existing. 46,029 rows on `TRAFFIC_DataYear2024`, 15,812 Actual. |

## `historic_stations` (Step 3, Fan-out A)

Populated via `stage_historic_stations.py`. Per-year row counts:

| Year | Total | Actual | Estimated | Calculated | Expected (inventory §1) | Match |
|---|---|---|---|---|---|---|
| 2020 | 25,889 | 7,902 | 17,543 | 444 | 25,889 | YES |
| 2021 | 25,966 | 8,491 | 17,031 | 444 | 25,966 | YES |
| 2022 | 25,668 | 8,375 | 16,849 | 444 | 25,668 | YES |
| 2023 | 25,714 | 7,864 | 17,401 | 449 | 25,714 | YES |
| 2024 | 46,029 | 15,812 | 29,354 | 863 | ~46,029 | YES |
| **Total** | **149,266** | | | | | |

Plan's "~93,033 total rows across 5 years" estimate assumed 2024 would be
Actual-only (~15,796 rows). Implementation stages the full 2024 pool
(Actual + Estimated + Calculated = 46,029) to match the xlsx-years
treatment — training-row selection filters to `Actual` at the model-input
stage, not at ingest. Actual-subset count (15,812) matches inventory.

### Assumption #1 verified — 2024 GDB lat/lon are physical counter coords

Spot-checked 5 random TC numbers that appear Actual in both 2020 xlsx and
2024 GDB; all agree within 1.4m:

| TC number | 2020 (lat, lon) | 2024 (lat, lon) | distance |
|---|---|---|---|
| 273-0092 | (31.78700, -84.43295) | (31.78700, -84.43295) | 0.4 m |
| 121-8233 | (34.04595, -84.24585) | (34.04596, -84.24585) | 0.6 m |
| 157-0231 | (34.00580, -83.49738) | (34.00580, -83.49739) | 0.5 m |
| 255-0157 | (33.28851, -84.26261) | (33.28851, -84.26261) | 0.2 m |
| 121-8325 | (33.81644, -84.35418) | (33.81644, -84.35419) | 1.4 m |

All within the 100m tolerance from plan §Open assumptions #1. Assumption
confirmed — GDB coords identify the counter, not a segment centroid.

### Option A deviation from Step 2

Plan Step 2 called for adding `TC_LATITUDE`/`TC_LONGITUDE` passthrough in
`normalize.py` and re-running normalize to land the columns on the
`segments` table. Not executed:

- `create_db.py` uses `to_sql(..., if_exists="replace")`, so re-running
  normalize rewrites the whole `segments` table (drops the Plan B HPMS
  enrichments, evac-enrichment, route-type, admin breakpoints, etc).
- The `feature/evac-splitting` worktree has active uncommitted work on
  shared staged state.
- Per `AGENT.md §Worktree Data Access`, full pipeline runs happen only
  from the main repo and only when no other agent is reading staged data.

Resolution: 2024 station coords are read directly from the GDB into
`historic_stations` in Fan-out A's 2024 pass. Modeling pipeline reads
lat/lon from `historic_stations`, not from `segments`, so the functional
end state is identical. The `normalize.py` edit can land separately as a
post-evac-merge cleanup.

## `segment_station_link` (Step 4)

Populated via `build_segment_station_link.py`. Schema matches plan
§Prerequisite #3. Per-year stats:

| Year | Rows | Median dist (m) | Mean dist (m) | Max dist (m) | same_route count | same_route rate |
|---|---|---|---|---|---|---|
| 2020 | 263,947 | 599.4 | 833.2 | 12,336 | 36,867 | 14.0% |
| 2021 | 263,947 | 597.7 | 830.4 | 12,336 | 36,946 | 14.0% |
| 2022 | 263,947 | 603.3 | 838.0 | 12,336 | 36,760 | 13.9% |
| 2023 | 263,947 | 602.7 | 837.5 | 12,336 | 36,812 | 13.9% |
| 2024 | 263,947 | 602.7 | 837.3 | 12,336 | 36,803 | 13.9% |
| **Total** | **1,319,735** | | | | | |

- Row count = 263,947 × 5 = 1,319,735 (one row per segment × year).
- No segment left unlinked (`nearest_tc_number IS NULL` count = 0).
- Median distance is tight (~600 m) and stable year-over-year (spread
  under 6 m across all 5 years) — the plan's red-flag signal of wildly
  different medians (e.g., 2020 at 2 km vs 2023 at 50 km) would have
  indicated a per-year station-coord bug; no such drift observed.
- Max 12.3 km represents the few remote FC 7 segments that genuinely
  have no close station — plan §Prerequisite #3 explicitly accepts
  these; the distance is a downweighting feature, not a filter.

The `same_route_flag` is computed via a reverse-nearest-neighbor: each
station's ROUTE_ID is inferred as the ROUTE_ID of its closest segment;
segments linked to a station whose inferred ROUTE_ID matches that
segment's ROUTE_ID are flagged 1. On ~14% of segment-years a station sits
on a matching route, consistent with GDOT's count-station footprint
(stations concentrated on higher-class roads).

## Emerging data surprise — FC 7 2022/2023 volumegroup gap

The plan expected FC 8-9 to be the HPMS sampling casualty. Georgia has
no FC 8-9 rows (all Local classifications are FC 7). FC 7 coverage matrix:

| FC | Total | 2020 HPMS | 2022 HPMS | 2023 HPMS | 2024 HPMS |
|---|---|---|---|---|---|
| 1 | 5,100 | 74.2% | 74.6% | 75.3% | 75.4% |
| 2 | 919 | 99.9% | 99.9% | 99.9% | 100.0% |
| 3 | 6,594 | 98.7% | 99.3% | 99.7% | 100.0% |
| 4 | 14,620 | 99.5% | 99.6% | 99.7% | 100.0% |
| 5 | 15,887 | 99.0% | 99.2% | 99.6% | 100.0% |
| 6 | 4,775 | 98.7% | 21.2% | 21.3% | 100.0% |
| **7** | **207,570** | **98.0%** | **0.0%** | **0.0%** | **100.0%** |

Implications for the plan:

1. **Tier 0 eligibility.** Plan §Tier 0 requires `AADT_2020_HPMS` AND
   `AADT_2022_HPMS` AND one of `{2023, 2024}`. Because FC 7 has ~0% 2022
   coverage, **no FC 7 segment qualifies for Tier 0** — all ~203k FC 7
   segments flow into Tier 1 (station-anchored) via the station→segment
   model. This is already accommodated by the three-tier design, but
   rescales the expected tier distribution: Tier 0 becomes an FC 1-5
   Interstate/Arterial/Collector affair.
2. **Tertiary deliverable interpretation.** "FC 8-9 gap-fill for
   2020/2022/2023/2024" in the Georgia context means "FC 7 values for
   2022/2023 (local roads, HPMS dropped them that year)". 2020 and 2024
   FC 7 already have 98%-100% HPMS coverage — no gap-fill needed there.
3. **FC 6 two-year gap.** ~79% of FC 6 rows drop in 2022/2023. Fill
   behaves like FC 7 in the middle two years. Handled by the same local-fill
   mechanism.

These findings are data truth per Plan B's `aadt_hpms_historic_coverage.md`;
the plan structure handles them correctly, but the "FC 8-9 rare cohort"
reasoning in the plan's narrative needs re-reading as "FC 6-7 in the middle
two years only."

## Regression invariants

Verified against staged DB 2026-04-19:

| Invariant | Expected | Actual | Match |
|---|---|---|---|
| `segments` row count | 263,947 | 263,947 | YES |
| `segments.AADT` non-null | 263,802 | 263,802 | YES |
| `segments.AADT_2024_HPMS` populated | 254,208 | 254,208 | YES |
| `segments.AADT_2024_OFFICIAL` populated | 50,450 | 50,450 | YES |
| `segments.AADT_2020_HPMS` populated | 249,604 | 249,604 | YES |

No canonical columns touched. Only additive tables (`historic_stations`,
`segment_station_link`) created.

## Next steps

Steps 5-12 pending: cohort_ratios with 5 fold-aware versions, trajectory-fit
validation, station→segment model training with 4-fold CV, 2021 prediction,
HPMS cross-validation report, FC 6-7 2022/2023 local-fill, and the
independent review.
