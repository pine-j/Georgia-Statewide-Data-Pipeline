# Historic HPMS AADT — Coverage QC Report

**Generated:** 2026-04-19
**Branch:** `aadt-historic-hpms`
**Scope:** 4-year federal HPMS AADT panel (2020, 2022, 2023, 2024). 2021 omitted — see `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2021/NO_DATA.md` for the exhausted-sources log.
**Location note:** this report lives at `02-Data-Staging/docs/aadt_hpms_historic_coverage.md` (tracked). Plan §Step 4 calls for `02-Data-Staging/reports/...` but that path is `.gitignored` repo-wide; the `docs/` path matches the existing convention for committed staging narratives (see `county_district_stale_pairs.md`, `historic_traffic_inventory.md`).

## Re-fetching raw data

The four HPMS tabular JSON files are gitignored and must be re-downloaded if lost (e.g. after a worktree is removed). Use the parameterized downloader:

```bash
python 01-Raw-Data/Roadway-Inventory/scripts/download_hpms.py --year 2020 --service-name HPMS_FULL_GA_2020
python 01-Raw-Data/Roadway-Inventory/scripts/download_hpms.py --year 2022 --service-name HPMS_FULL_GA_2022
python 01-Raw-Data/Roadway-Inventory/scripts/download_hpms.py --year 2023 --service-name HPMS_FULL_GA3_2023
python 01-Raw-Data/Roadway-Inventory/scripts/download_hpms.py --year 2024
```

Then re-run the enrichment:

```bash
python 02-Data-Staging/scripts/01_roadway_inventory/add_historic_hpms_columns.py --year 2020 --year 2022 --year 2023
```

Note: `--service-name` defaults to `HPMS_FULL_GA_<year>` — the 2023 service carries a `3` suffix and must be passed explicitly.

## TL;DR

- **Federal-aid network (FC 1-5): 74-100% coverage** across all four years. Interstate (FC 1) has a ~75% floor in every year including the 2024 Phase 1 baseline — this is a pre-existing segmentation-granularity artefact, not a 2020-specific miss.
- **Local roads (FC 6-9):** 2020 and 2024 cover ~98-100%; **2022 and 2023 cover 0-21%** because FHWA's 2022/2023 Georgia submissions used volumegroup sampling for those classes per standard HPMS practice. The NULLs are intentional, not a pipeline miss — consumers (webapp, modeling, RAPTOR) must not treat them as bugs.
- **FC 6-7 default-fill in 2020 and 2024: ~60% of non-null HPMS values are FHWA-synthesized defaults, not measurements.** Discovered 2026-04-19. See `§Synthetic default-fills on FC 6-7 in 2020 and 2024` below; this materially affects how the panel can be used for modeling or time-series analysis.
- **2024 regression: byte-identical** to the Phase 1 `AADT_2024_HPMS` column (263,947 segments, zero null-alignment mismatches, zero value mismatches).
- **Row count unchanged:** 263,947 before and after enrichment. Canonical `AADT` untouched (263,802 non-null).

## Per-year overall coverage

| Year | Populated | Total | Coverage | HPMS rows loaded | Dedupe drops | Source |
|---|---|---|---|---|---|---|
| 2020 | 249,604 | 263,947 | 94.57% | 734,014 (of 1,584,000) | 849,986 | REST `HPMS_FULL_GA_2020` layer 0 (no where-clause) |
| 2022 | 42,689 | 263,947 | 16.17% | 347,802 | 0 | REST `HPMS_FULL_GA_2022` layer 0 |
| 2023 | 42,796 | 263,947 | 16.21% | 521,567 | 0 | REST `HPMS_FULL_GA3_2023` layer 0 |
| 2024 | 254,208 | 263,947 | 96.31% | 499,372 | 0 | REST `HPMS_FULL_GA_2024` layer 0 (Phase 1 baseline) |

**Dedupe:** the 2020 FHWA submission duplicates each segment 2-3× for section-type sparsity (ownership on one row, maintenance_operations on another, etc.) with identical AADT. `load_hpms_data_for_year` collapses duplicates on the tuple `(route_id, begin_point, end_point)` keeping the first occurrence. 849,986 duplicate rows dropped in 2020; 0 in other years (the submission format changed in 2022 to one row per segment).

## Coverage by FUNCTIONAL_CLASS

The 2022/2023 FC 6-9 columns are NULL **by FHWA submission design**, not because of a pipeline miss. FHWA requires full AADT enumeration only for NHS + FC 1-5. FC 6-9 is submitted with a coded `volumegroup` band instead of a row-level `aadt` value — 60% of 2022 rows and 70% of 2023 rows have `aadt = NULL`. Downstream consumers should treat `AADT_{year}_HPMS IS NULL` on FC 6-9 as "intentionally unreported" for those vintages, not as missing data to fill.

| FC | Class | Segments | **2020** | **2022** | **2023** | **2024** |
|---|---|---|---|---|---|---|
| 1 | Interstate | 5,100 | 74.2% | 74.6% | 75.3% | 75.4% |
| 2 | Principal Art. freeway/expy | 919 | 99.9% | 99.9% | 99.9% | 100.0% |
| 3 | Principal Arterial | 6,594 | 98.7% | 99.3% | 99.7% | 100.0% |
| 4 | Minor Arterial | 14,620 | 99.5% | 99.6% | 99.7% | 100.0% |
| 5 | Major Collector | 15,887 | 99.0% | 99.2% | 99.6% | 100.0% |
| 6 | Minor Collector | 4,775 | 98.7% | 21.2%\* | 21.3%\* | 100.0% |
| 7 | Local | 207,570 | 98.0% | ~0%\* | ~0%\* | 100.0% |

\* = FHWA volumegroup sampling; NULL is the reported value, not a pipeline gap.

### FC 1 Interstate coverage floor — known, pre-existing

All four independent HPMS vintages — 2020, 2022, 2023, and 2024 — land Interstate coverage within a 1.2 percentage-point band (74.2% to 75.4%). Four independent data sources converging on a shared floor is a structural signature, not noise.

The ~25% gap pre-dates this work: the 2024 Phase 1 enrichment (`apply_hpms_enrichment`, merged to master before this branch was forked) produced exactly the same 75.4% FC 1 coverage. It is almost certainly a segmentation-granularity mismatch in `_find_best_hpms_match`: our staged `segments` table splits Interstate rows finer than HPMS reports (for district boundaries, state-system flags, etc.), and the `min(to_mp, ref_to) - max(from_mp, ref_from) > MILEPOINT_TOLERANCE` overlap test leaves 1,200-1,300 child segments without a matching HPMS row in any given year.

This is logged as a follow-up investigation (see `D:/TODO.md` — "Investigate Interstate FC 1 HPMS coverage floor") because widening the tolerance or changing the match strategy would affect all four years together and needs its own regression pass. For Plan B's 4-year panel the 74-75% parity is acceptable and documented.

## Coverage by SYSTEM_CODE

| SYSTEM_CODE | Meaning | Segments | 2020 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|
| 1 | State system | 26,376 | 79.5% | 79.5% | 79.6% | 80.0% |
| 2 | Non-state system | 237,571 | 96.2% | 9.1% | 9.2% | 98.1% |

The 2022/2023 drop on non-state segments is the FC 6-9 volumegroup-sampling effect again (most non-state segments are local roads). On state-system segments, all four years track within 0.5pp of each other at ~80% — a second structural signature of the FC 1 / FC 2 segmentation floor.

## Coverage by URBAN / RURAL

Heuristic: `URBAN_CODE IS NULL OR URBAN_CODE = 99999` = rural; else urban.

| Slice | Segments | 2020 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|
| Rural | 104,520 | 89.4% | 16.0% | 16.0% | 90.7% |
| Urban | 159,427 | 97.9% | 16.3% | 16.4% | 100.0% |

2020 and 2024 both show the expected urban > rural pattern (more federal-aid enumeration in urban areas). 2022/2023 are flat across the urban/rural split because volumegroup sampling dominates for both.

## Year-over-year consistency spot-check

For segments populated in **all four** years (42,375 segments — almost entirely FC 1-5 since FC 6-9 lacks 2022/2023 data), count segments with YoY change > 100% (either direction) between consecutive years:

| Pair | Flagged | Paired total | Flag rate |
|---|---|---|---|
| 2020 → 2022 | 8,322 | 42,427 | 19.61% |
| 2022 → 2023 | 3,581 | 42,627 | 8.40% |
| 2023 → 2024 | 377 | 42,795 | **0.88%** |
| 2020 → 2024 (net) | 20,353 | 249,559 | 8.16% |

**Interpretation:** 2023→2024 at 0.88% establishes the clean baseline — real YoY traffic noise plus occasional join-boundary anomalies. The elevated 2020→2022 and 2022→2023 rates are **COVID suppression and recovery**, not pipeline bugs. Spot-check of sample flagged rows:

| route | fmp | FC | 2020 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|
| 1000100002900INC | 97.20 | 3 | 8509 | 20700 | 9431 | 10400 |
| 1000100002400INC | 1.11 | 5 | 1800 | 5220 | 5210 | 5230 |
| 1000100002400INC | 2.44 | 5 | 1800 | 5220 | 5210 | 5230 |

The first row is a 2022-only spike (likely a localized detour or construction effect); the 1000100002400INC rows show 2020 at ~35% of the post-COVID baseline — classic pandemic suppression, not a bug. Plan §Scope boundaries explicitly calls out that 2020 HPMS values reflect suppressed urban counts as submitted and we do not attempt to reconstruct counterfactual AADT.

**The 2023→2024 rate (0.88%) is the join-quality signal** — it's low enough to rule out a systemic join bug. If join errors were material, this pair would also spike.

## Synthetic default-fills on FC 6-7 in 2020 and 2024

**Added 2026-04-19.** This section documents a data-quality finding that surfaced during execution of the AADT modeling v1 plan (`C:/Users/adith/.claude/plans/aadt-modeling-scoped-2020-2024.md`) and drove the v1 → v2 pivot. The panel itself is unchanged; this is a usage caveat.

### Finding

Between 60% and 63% of non-null `AADT_{year}_HPMS` values in 2020 and 2024 are **mass-repeat integers** — single integer values shared by tens of thousands of segments — almost entirely on FC 7. Direct measurement against `02-Data-Staging/databases/roadway_inventory.db`:

| Year | Non-null HPMS | Top-6 mass-repeat total | Share of non-null |
|---|---|---|---|
| 2020 | 249,604 | 150,566 | **60.3%** |
| 2022 | 42,689 | 3,206 | 7.5% |
| 2023 | 42,796 | 1,831 | 4.3% |
| 2024 | 254,208 | 158,813 | **62.5%** |

2020 top-6 values: `1662` (47,635), `2086` (28,549), `154` (22,627), `296` (22,255), `921` (15,480), `689` (14,020).
2024 top-6 values: `2250` (48,178), `1750` (31,617), `320` (24,959), `140` (24,116), `360` (16,646), `920` (13,297).

### Evidence that these are synthetic defaults, not natural concentration

The decisive signal is the near-total concentration of each mass-repeat value on a single FC:

| Year | Value | Count in FC 7 | Count in FC ≠ 7 | FC 7 share |
|---|---|---|---|---|
| 2020 | 1662 | 47,623 | 12 | 99.97% |
| 2020 | 2086 | 28,534 | 15 | 99.95% |
| 2024 | 2250 | 48,131 | 47 | 99.90% |
| 2024 | 1750 | 31,581 | 36 | 99.89% |

Natural cohort concentration on low-volume FC 7 roads would cluster around several nearby integers, not collide exactly on a single integer at 47k-48k scale. Exact-integer collisions of this magnitude are diagnostic of a **lookup-table fill** — an automated assignment that writes the same integer to every non-sampled segment in a given stratum.

The specific mechanism is FHWA's HPMS volumegroup default assignment: for FC 6-7 segments not individually sampled by the state, FHWA assigns a state-level default AADT integer per volumegroup stratum. 2022 and 2023 leave these segments NULL (~0% FC 7 coverage in those years); 2020 and 2024 fill them with the stratum default. Both are outcomes of the same FHWA volumegroup sampling rule — NULL vs default-fill differs by submission vintage, not by sampling logic.

Alternative explanations ruled out:
- **Rounding convention.** 2024 top-6 values are all multiples of 10; 2020 top-6 are not (`1662`, `2086`, `921`, `689`). Rounding doesn't explain 2020 and wouldn't produce exact-integer collisions regardless.
- **Legitimate cohort concentration.** Addressed above — 48k exact matches on a single integer is not a sampling outcome.
- **Data-dictionary convention (coded magic numbers).** No FHWA HPMS field manual entry documents `1662` / `2086` / `2250` / `1750` as reserved codes; the values differ across years (inconsistent with a code).
- **Rounding to nearest 50.** The 2024 values are not all multiples of 50 (e.g., `1540`, `720`), and 2020 values are not round at all.

### Affected columns

- `AADT_2020_HPMS` — ~150,000 of 249,604 non-null values are default-fills (60%).
- `AADT_2024_HPMS` — ~159,000 of 254,208 non-null values are default-fills (63%).
- `AADT_2022_HPMS`, `AADT_2023_HPMS` — **not affected** by default-fill. FC 6-7 is NULL in these vintages rather than filled. Separately: 657 segments share `AADT_2022_HPMS = 67` (a bad-data cluster, not a default-fill — already documented in `aadt_trajectory_fit_validation.md`).

### What is NOT affected

- **Canonical `AADT` column.** Sourced from the GDOT 2024 traffic GDB, not HPMS. Unaffected.
- **`AADT_2024` / `AADT_2024_OFFICIAL`.** GDOT-sourced. Unaffected.
- **FC 1-5 values in any year.** FHWA requires individual enumeration on NHS + FC 1-5; default-fill is confined to FC 6-7.
- **`AADT_2024_SOURCE_AGREEMENT`.** Already flags HPMS vs state-GDB disagreements; segments where HPMS is a volumegroup default but the state GDB has an empirical count are (by design) flagged as `both_disagree` if the gap is large.

### Consumer guidance

For any downstream analysis that treats `AADT_{year}_HPMS` as empirical per-segment AADT:

1. **Time-series analysis on FC 6-7 segments** — do not compute YoY changes, growth rates, or recovery ratios from the 4-year panel on FC 6-7 segments without first filtering out synthetic values. The 2020→2024 "trend" on a default-filled FC 7 segment reflects FHWA's default change, not a real traffic change.
2. **Modeling with HPMS as training target** — do not train a regression with `target = log(AADT_{year}_HPMS)` on mixed FC because the model will learn FHWA's default rule on ~60% of 2020/2024 FC 7 rows. This is the specific pitfall that tripped v1 of the AADT modeling plan.
3. **Map / webapp visualization** — color ramps on `AADT_{year}_HPMS` for FC 7 in 2020 and 2024 will show large banded regions at the default values. This is not a rendering bug.

For modeling and analytics that need empirical-only HPMS, the AADT modeling v2 plan (`aadt-modeling-scoped-2020-2024-v2.md`) stages a synthetic classifier on `segments`:

- `AADT_{2020,2022,2023,2024}_HPMS_SYNTHETIC` — 0/1/NULL (NULL where HPMS itself is NULL).
- Classifier rule: per `(FUNCTIONAL_CLASS, year)` bucket, flag `synthetic=1` if the value repeats ≥ `max(500, 0.5% × bucket_size)` times in that bucket.
- Plus `AADT_{2022,2023}_HPMS_HARD_OUTLIER` for the 657-row `67` cluster and similar bad-data clusters in 2022/2023.

Analyses that require empirical HPMS only should filter `WHERE AADT_{year}_HPMS_SYNTHETIC = 0 AND COALESCE(AADT_{year}_HPMS_HARD_OUTLIER, 0) = 0`. Empirical-only row counts after filtering: ~37,867 for 2020 (15.2% of non-null) and ~51,703 for 2024 (20.3%). These flag columns are populated by the v2 plan; they do not exist on master until v2 lands.

### Why this wasn't caught earlier

The Phase 1 and Phase B coverage QC steps asked "are the AADT columns populated?" — they were, at 94.6% and 96.3%. Neither pass checked the value distribution. The AADT modeling v1 plan's trajectory-fit validation (Fan-out B, `aadt_trajectory_fit_validation.md`) was the first QC step that took value-distribution assumptions seriously — it tripped a 207% MAPE hardstop on the hide-2022 fold, and digging into the residuals surfaced first the 657-row `67` bad cluster and then, downstream, the FC 7 mass-repeat pattern on 2020 and 2024.

Going forward, HPMS ingestion QC should include a value-distribution check: for each `(FC, year)` bucket, the fraction of rows whose value appears ≥ `max(500, 0.5% × bucket_size)` times in the bucket. A double-digit fraction is a red flag for synthetic fill.

## 2024 regression confirmation

The refactored `build_historic_hpms_aadt_column(gdf, year=2024)` was run against the Phase 1 `AADT_2024_HPMS` column already present on `segments` (from the primary-tree baseline at commit `4efbf23`). Output was written to a scratch `AADT_2024_HPMS_REBUILD` column and diffed:

- **Null-alignment mismatches:** 0 (out of 263,947)
- **Value mismatches** (both non-null, different values): 0
- **REGRESSION OK** — the refactored loader is byte-identical to the Phase 1 behavior.

The scratch column was dropped after verification.

## Scope boundaries reconfirmed

- **Canonical `AADT` untouched:** 263,802 non-null segments before and after enrichment (identical to Phase 1 baseline).
- **`AADT_2024_*` columns untouched:** `AADT_2024_HPMS`, `AADT_2024_SOURCE`, `AADT_2024_SOURCE_AGREEMENT`, `AADT_2024_CONFIDENCE` values are byte-identical to Phase 1.
- **No source / agreement / confidence columns for historic years.** Per plan §Step 3: historic HPMS years are single-source (no state-GDB cross-check to make). The single `AADT_{year}_HPMS` column per year is the entire schema addition.
- **Row count:** 263,947 before and after (verified per-year).

## Downstream implications (informational)

The scoped 2020-2024 modeling plan (`C:/Users/adith/.claude/plans/aadt-modeling-scoped-2020-2024.md`) assumed Tier 0 membership = 2020 HPMS + 2 of {2022, 2023, 2024} HPMS. With 2022/2023 at ~16% overall and FC 6-9 NULL by FHWA design, Tier 0 is in practice a **federal-aid-network-only tier** (FC 1-5). This matches the plan's intent — FC 8-9 flows to Tier 2 cohort modeling — but the Tier-distribution summary should explicitly note that Tier 0 membership is effectively 50k segments, not the full 264k.

No edit to the scoped modeling plan is required here; record this in the Tier-distribution summary when that plan executes.

## Per-year next steps

Out of scope for this report; tracked in `D:/TODO.md`:

- Data-dictionary entries for `AADT_2020_HPMS`, `AADT_2022_HPMS`, `AADT_2023_HPMS` (this commit).
- Phase-1 narrative update to describe the 4-year panel + volumegroup-sampling note (follow-up commit).
- Interstate FC 1 coverage-floor investigation (follow-up ticket).
- Webapp year selector / legend (plan §Step 6, deferred).
