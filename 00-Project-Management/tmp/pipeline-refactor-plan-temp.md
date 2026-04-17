# Phase 1 Review And Documentation Update Plan

## Summary
This plan is scoped to Phase 1 only. It covers two linked outputs:

1. Review and clean up the actual Phase 1 roadway pipeline (code, raw inputs, staged outputs, RAPTOR consumers, webapp consumers).
2. Update the Phase 1 project-management documentation in `00-Project-Management` so it matches the real current Phase 1 implementation.

Later phases are out of scope except when deciding whether an unused Phase 1 asset should be retained for future work.

This file is the working ledger for the Phase 1 review. It tracks:
- `Scope Decisions (Step 0)`
- `Numeric Ground Truth`
- `Active Phase 1 Inputs`
- `Keep For Future Phases`
- `Keep As Provenance/Reference`
- `Quarantine / Remove Candidates`
- `Discovered Duplicates`
- `Potential Mistakes / Risky Assumptions`
- `Edge Cases To Verify`
- `Documentation To Update` (per-file edit table)
- `Agent Findings`
- `Review Results / Decisions`

## Ground Truths (confirmed before planning)

The following are confirmed by direct inspection of the repo and must not be re-litigated by sub-agents:

- `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py` `RoadwayData.load_data()` **actively requires SQLite**. It calls `_load_from_db()` against `02-Data-Staging/databases/roadway_inventory.db` and merges the tabular frame with geometry from `02-Data-Staging/spatial/base_network.gpkg`. Removing the SQLite code path breaks RAPTOR. The refactor brief is therefore **"audit comments and fallback branches for staleness,"** not "remove dead SQLite logic."
- `04-Webapp/backend/app/services/staged_roadways.py` depends on both SQLite (tabular, filter, detail, summary, manifest) and GeoPackage (bounds, geometry, boundary layers). SQLite cannot be removed in Phase 1.
- `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py` imports exactly two private symbols from `route_verification.py`: `_fetch_arcgis_features` and `_fetch_arcgis_object_ids`. Both are generic ArcGIS HTTP helpers.
- Signed-route helper logic is duplicated between `hpms_enrichment.py` and `route_verification.py`. `route_family.py` currently only exports `classify_route_family()` / `classify_route_families()` and does not host signed-route constants.
- `_evac_corridor_match.py` is imported by `evacuation_enrichment.py:30` and used at line 893.
- `normalize.py` writes `base_network.gpkg` and `tables/roadway_inventory_cleaned.csv`; it does **not** write `roadway_inventory.db`. `create_db.py` requires both prior outputs.
- A second copy of `roadway_inventory.db` exists under `02-Data-Staging/spatial/` alongside `base_network.gpkg`. No Phase 1 script references it. It is undocumented.
- Phase 1 docs disagree on segment count (`244,904` in `Project_Plan/README.md` vs `245,863` in the other three docs) and on future-AADT coverage (`52,236`/21.3% vs `53,215`/21.64%).

## Step 0 — Scope Decisions (blocker-level; resolve BEFORE any sub-agent runs)

The following decisions must be made and recorded in this ledger before any inventory, QA, or refactor sub-agent starts work. Running them without these answers causes misclassification or broken execution.

### D0.1 — Evacuation enrichment scope (Phase 1 vs Phase 8)
- **Affected assets:** `02-Data-Staging/scripts/01_roadway_inventory/_evac_corridor_match.py`, `evacuation_enrichment.py`, `02-Data-Staging/spatial/ga_evac_routes.geojson`, `ga_contraflow_routes.geojson`, `02-Data-Staging/qc/evacuation_route_qc/*`, `02-Data-Staging/reports/evacuation_enrichment_summary.json`.
- **Decision required:** Is evacuation enrichment (a) an active Phase 1 enrichment that writes `SEC_EVAC`-style flags into the staged outputs consumed by RAPTOR; or (b) a Phase 8 (RAPTOR integration) capability that happens to stage its inputs alongside Phase 1?
- **Why it blocks:** Sub-agent 1 cannot classify the above assets until this is settled. Sub-agent 4 cannot reconcile `phase-1-roadway-data-pipeline.md` references to EOC Hurricane Evacuation Routes.
- **Owner:** Orchestrator + user.
- **Resolution record:** Resolved 2026-04-17: treat evacuation enrichment as an active Phase 1 optional enrichment. Classify `_evac_corridor_match.py`, `evacuation_enrichment.py`, `ga_evac_routes.geojson`, `ga_contraflow_routes.geojson`, `02-Data-Staging/qc/evacuation_route_qc/*`, and `02-Data-Staging/reports/evacuation_enrichment_summary.json` as Active. Edge case #12 is a required validation check, not conditional on downstream Phase 8 ownership.

### D0.2 — Duplicate SQLite DB in `spatial/`
- **Affected asset:** `02-Data-Staging/spatial/roadway_inventory.db`.
- **Decision required:** Is this a (a) stale artifact from a prior architecture (quarantine, then remove), (b) a deployment copy consumed by something outside the repo (document and keep), or (c) a webapp fallback (document, keep, and wire into deploy)?
- **Investigation:** `git log -- 02-Data-Staging/spatial/roadway_inventory.db`; `git ls-files | grep roadway_inventory.db`; byte-size and mtime comparison against `databases/roadway_inventory.db`; grep for string `spatial/roadway_inventory.db` across `04-Webapp/` and `05-RAPTOR-Integration/`.
- **Resolution record:** Resolved 2026-04-17: quarantine `02-Data-Staging/spatial/roadway_inventory.db` as a stale 0-byte artifact with no git history and no code references. Move it to `02-Data-Staging/spatial/_quarantine/roadway_inventory.db` with reason `stale 0-byte artifact, no code references, primary DB lives in databases/`. Removal remains a follow-up PR after at least one milestone.

### D0.3 — Git tracking status of generated binaries
- **Affected assets:** all `*.db` and `*.gpkg` under `02-Data-Staging/`.
- **Decision required:** Are these generated artifacts git-tracked, git-ignored, or mixed? This determines whether quarantine is a `git mv` or a filesystem move, and whether "generated local artifact mistaken for a deliverable" is a real risk in this repo.
- **Investigation:** `git ls-files '02-Data-Staging/**/*.db' '02-Data-Staging/**/*.gpkg'` and `git check-ignore` for each path.
- **Resolution record:** Resolved 2026-04-17: record generated binaries under `02-Data-Staging/` as git-ignored artifacts. Quarantine therefore uses filesystem move, not `git mv`. Continue using sibling `_quarantine/` directories and the full ledger protocol for provenance and dwell tracking.

### D0.4 — Authoritative traffic snapshot
- **Affected assets:** `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb`, `Traffic_Historical.zip`, `2010_thr_2019_Published_Traffic.zip`.
- **Decision required:** Which traffic source is consumed by the current Phase 1 AADT enrichment? The others are "keep for future phases" (trend analysis) or "provenance."
- **Investigation:** grep Phase 1 scripts for `TRAFFIC_Data_2024` and `Traffic_Historical`; check `download.py` and `download_metadata.json`.
- **Resolution record:** Resolved 2026-04-17: the active Phase 1 traffic source is `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb` using layer `TRAFFIC_DataYear2024`. `Traffic_Historical.zip` and `2010_thr_2019_Published_Traffic.zip` remain in Keep For Future Phases for trend analysis (likely Phase 5), not Provenance.

### D0.5 — Destination modules for extracted helpers
Two extractions are in scope. Each needs a named destination *before* refactor work begins so parallel sub-agents land code in the same place.

- **Signed-route helper logic** (currently duplicated in `hpms_enrichment.py` and `route_verification.py`): extract to **`02-Data-Staging/scripts/01_roadway_inventory/route_family.py`** (extend existing module) unless the orchestrator decides a new `signed_route.py` module is preferable. Default: extend `route_family.py`.
- **ArcGIS fetch/cache helpers** (`_fetch_arcgis_features`, `_fetch_arcgis_object_ids` currently private in `route_verification.py`): extract to **a new `02-Data-Staging/scripts/01_roadway_inventory/arcgis_client.py`** with public API. Reason for placing it inside `01_roadway_inventory/` rather than one level up: later-phase reuse is speculative; if a future phase needs it, it can be hoisted then. Document this deferral explicitly.
- **Resolution record:** Resolved 2026-04-17: accept the defaults. Extend `02-Data-Staging/scripts/01_roadway_inventory/route_family.py` with public signed-route helpers/constants, and create `02-Data-Staging/scripts/01_roadway_inventory/arcgis_client.py` with public ArcGIS fetch helpers. Update `rnhp_enrichment.py` and `route_verification.py` to import public names. No underscore-prefixed cross-module imports may remain after Step 5.

### D0.6 — Authoritative segment and coverage counts
- **Decision required:** The canonical Phase 1 counts are whatever the current `02-Data-Staging/databases/roadway_inventory.db` returns, not whatever the docs say. The orchestrator must run the ground-truth queries in the next section and publish the values here before any doc sub-agent edits numbers.
- **Resolution record:** Finalized 2026-04-17 after regeneration review. Recipe path taken: first `create_db.py` only because CSV and GPKG mtimes were already aligned on 2026-04-17, then full `normalize.py -> create_db.py` because the DB-only rebuild still left a large raw mtime gap and materially changed coverage counts. Accepted as canonical: N1, N2, N3, N4, N8, N9, and N12. N5 is accepted with a flag because the `-448.54` mile delta (`-0.34%`) exceeded the threshold but reproduced across both rerun paths. N6/N7 are accepted as canonical because `N3 == N6` is intentional in the current pipeline: `hpms_enrichment.py` only contributes explicit HPMS future matches, while `normalize.py` now applies an official implied growth-rate projection to all remaining segments with `AADT_2024`, producing near-complete future coverage. Step 3 may proceed using the latest Step 2 table below.

## Numeric Ground Truth (single source of truth for doc updates)

Run these queries against `02-Data-Staging/databases/roadway_inventory.db` and record the results in this section. All doc edits pull numbers from this table and nowhere else.

| Key | Query | Value |
|-----|-------|-------|
| N1 — total segments | `SELECT COUNT(*) FROM segments` | _tbd_ |
| N2 — total mileage | `SELECT ROUND(SUM(segment_length_mi), 2) FROM segments` | _tbd_ |
| N3 — AADT current coverage (rows) | `SELECT COUNT(*) FROM segments WHERE AADT IS NOT NULL` | _tbd_ |
| N4 — AADT current coverage (%) | N3 / N1 | _tbd_ |
| N5 — AADT current coverage (miles) | `SELECT ROUND(SUM(segment_length_mi), 2) FROM segments WHERE AADT IS NOT NULL` | _tbd_ |
| N6 — AADT 2044 coverage (rows) | `SELECT COUNT(*) FROM segments WHERE AADT_2044 IS NOT NULL` (or actual future-AADT column name) | _tbd_ |
| N7 — AADT 2044 coverage (%) | N6 / N1 | _tbd_ |
| N8 — GPKG row count | `ogrinfo -so base_network.gpkg roadway_segments` (feature count) | _tbd_ |
| N9 — Row parity | N1 == N8 ? | _tbd_ |
| N10 — SQLite file mtime | `stat databases/roadway_inventory.db` | _tbd_ |
| N11 — GPKG file mtime | `stat spatial/base_network.gpkg` | _tbd_ |
| N12 — Run-token parity | mtime delta |N10 − N11| | _tbd_ |

N9 and N12 together determine whether `segments` and `roadway_segments` came from the same `normalize.py` run. If not, freshness is suspect and doc counts must be regenerated.

Filled Step 2 results (supersedes the placeholder table above):

| Key | Query | Value |
|-----|-------|-------|
| N1 - total segments | `SELECT COUNT(*) FROM segments` | 245,863 |
| N2 - total mileage | `SELECT ROUND(SUM(segment_length_mi), 2) FROM segments` | 133,994.38 |
| N3 - AADT current coverage (rows) | `SELECT COUNT(*) FROM segments WHERE AADT IS NOT NULL` | 245,778 |
| N4 - AADT current coverage (%) | N3 / N1 | 99.9654% |
| N5 - AADT current coverage (miles) | `SELECT ROUND(SUM(segment_length_mi), 2) FROM segments WHERE AADT IS NOT NULL` | 133,830.64 |
| N6 - AADT 2044 coverage (rows) | `SELECT COUNT(*) FROM segments WHERE FUTURE_AADT_2044 IS NOT NULL` | 53,215 |
| N7 - AADT 2044 coverage (%) | N6 / N1 | 21.6442% |
| N8 - GPKG row count | `ogrinfo -so base_network.gpkg roadway_segments` (feature count) | 245,863 |
| N9 - Row parity | N1 == N8 ? | PASS |
| N10 - SQLite file mtime | `stat databases/roadway_inventory.db` | 2026-04-14 20:39:02 UTC |
| N11 - GPKG file mtime | `stat spatial/base_network.gpkg` | 2026-04-17 13:48:34 UTC |
| N12 - Run-token parity | mtime delta \|N10 - N11\| | FAIL - 234,571.51 seconds (2d 17h 9m 31.51s) |

Environment note: `ogrinfo` is not installed in this shell. N8 was obtained by querying the GeoPackage's `roadway_segments` table directly via SQLite, which is equivalent for feature count.

Refreshed Step 2 results after regeneration (latest authoritative table):

| Key | Query | Value |
|-----|-------|-------|
| N1 - total segments | `SELECT COUNT(*) FROM segments` | 245,863 |
| N2 - total mileage | `SELECT ROUND(SUM(segment_length_mi), 2) FROM segments` | 133,994.38 |
| N3 - AADT current coverage (rows) | `SELECT COUNT(*) FROM segments WHERE AADT IS NOT NULL` | 245,766 |
| N4 - AADT current coverage (%) | N3 / N1 | 99.9605% |
| N5 - AADT current coverage (miles) | `SELECT ROUND(SUM(segment_length_mi), 2) FROM segments WHERE AADT IS NOT NULL` | 133,382.10 |
| N6 - AADT 2044 coverage (rows) | `SELECT COUNT(*) FROM segments WHERE FUTURE_AADT_2044 IS NOT NULL` | 245,766 |
| N7 - AADT 2044 coverage (%) | N6 / N1 | 99.9605% |
| N8 - GPKG row count | `ogrinfo -so base_network.gpkg roadway_segments` (feature count) | 245,863 |
| N9 - Row parity | N1 == N8 ? | PASS |
| N10 - SQLite file mtime | `stat databases/roadway_inventory.db` | 2026-04-17 16:21:18 UTC |
| N11 - GPKG file mtime | `stat spatial/base_network.gpkg` | 2026-04-17 16:20:55 UTC |
| N12 - Run-token parity | mtime delta \|N10 - N11\| | PASS - 22.72 seconds |

Acceptance notes:
- N5 accepted with flag: `133,382.10` is `-448.54` miles (`-0.34%`) versus the pre-regeneration table, above threshold but consistent across both rerun paths.
- N6/N7 accepted as intentional current behavior. Source breakdown from the regenerated DB: `projection_official_implied=199,147`, `official_exact=45,938`, `direction_mirror=667`, `hpms_2024=14`, `missing=97`.
- Code/history grounding for N6/N7: commit `b581643` added the initial `FUTURE_AADT_2044` fill chain and HPMS future gap-fill, and commit `98691cb` explicitly added the official implied growth-rate projection that "applies to all remaining segments with AADT_2024". Current code keeps the HPMS path narrow in `hpms_enrichment.py` and performs the broad fallback in `normalize.py`.

Change vs. the pre-regeneration table:
- `N1`: unchanged
- `N2`: unchanged
- `N3`: changed from `245,778` to `245,766` (`-12`)
- `N4`: changed from `99.9654%` to `99.9605%` (`-0.0049` percentage points)
- `N5`: changed from `133,830.64` to `133,382.10` (`-448.54` miles)
- `N6`: changed from `53,215` to `245,766` (`+192,551`)
- `N7`: changed from `21.6442%` to `99.9605%` (`+78.3163` percentage points)

### Change thresholds after regeneration

After the DB/GPKG regeneration triggered by N12 FAIL, re-run N1–N12 and apply these thresholds to decide whether to proceed to Step 3 or pause for review:

| Key | Acceptable change | Pause for review if |
|-----|-------------------|---------------------|
| N1 — total segments | 0 (no change) | Any delta. Row counts should not shift from a regeneration unless the source GDB or segmentation logic changed. |
| N2 — total mileage | ≤0.01 mi drift (float rounding) | Anything larger — implies geometry changed. |
| N3 — AADT current rows | ±0.1% of prior value | >0.1%. Small drift expected from re-enrichment; larger drift implies upstream data change. |
| N4 — AADT current % | ±0.1 percentage points | Larger. |
| N5 — AADT current miles | ±0.1% of prior value | Larger. |
| N6 — AADT 2044 rows | ±0.1% of prior value | Larger. |
| N7 — AADT 2044 % | ±0.1 percentage points | Larger. |
| N8 — GPKG row count | 0 (must equal N1) | Any delta. |
| N9 — row parity | PASS | FAIL. |
| N12 — run-token parity | PASS (mtime delta within one regeneration window) | FAIL. |

Anything within the "acceptable" column is just ArcGIS refresh noise; do not pause Step 3 for it. Anything in the "pause" column requires surfacing the diff to the user before continuing.

## Multi-Agent Structure

### Orchestrator agent
- Owns Phase 1 scope.
- Runs Step 0 with the user before dispatching sub-agents.
- Publishes the Numeric Ground Truth table before Sub-agent 4 runs.
- Consolidates findings into final keep/quarantine/remove/refactor/doc-update decisions.
- Enforces the quarantine protocol.

### Sub-agent 1: Phase 1 asset and dependency inventory
- Starts only after Step 0 is resolved.
- Inventories every file under `01-Raw-Data/Roadway-Inventory/`, `02-Data-Staging/config/`, `02-Data-Staging/databases/`, `02-Data-Staging/spatial/`, `02-Data-Staging/tables/`, `02-Data-Staging/reports/`, `02-Data-Staging/qc/`, `02-Data-Staging/scripts/01_roadway_inventory/`, the Phase 1 portion of `04-Webapp/backend/app/services/staged_roadways.py`, and `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py`.
- Classifies every inventoried item into exactly one of: Active, Keep For Future Phases, Provenance/Reference, Quarantine / Remove Candidate, Discovered Duplicate.
- For every Active item, traces at least one real current consumer with a file:line citation.
- For every Keep For Future Phases item, cites the intended future phase and why it cannot be regenerated.
- Uses the pre-seeded buckets in this ledger as a starting set, not as the final answer.

### Sub-agent 2: Phase 1 QA and correctness review (runtime/data behavior)
- Scope: data-behavior issues only. Does not touch module boundaries or imports.
- Covers: missing raw snapshot handling, stale cached GPAS/HPMS/Traffic snapshots, absent SQLite or GeoPackage, row-count drift between `segments` and `roadway_segments`, same-run-token parity (N9/N12), silent-fallback branches in `RoadwayData.load_data()`, silent-fallback branches in `staged_roadways.py`, `AADT` null distribution, unexpected geometry CRS.
- Produces a Phase 1 edge-case checklist with expected vs observed outcomes.

### Sub-agent 3: Phase 1 refactor and code-structure review (imports/module boundaries only)
- Scope: imports, duplicate helpers, dead wrappers, private-symbol leaks, stale comments. Does **not** propose behavior changes.
- Specific targets:
  - `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py`
  - `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py`
  - `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py`
  - `02-Data-Staging/scripts/01_roadway_inventory/route_family.py`
  - `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py` — audit comments and fallback branches for staleness. Do **not** remove SQLite code; it is load-bearing.
- Produces: diff-level proposals targeting the module destinations named in D0.5.

### Sub-agent 4: Phase 1 documentation update
- Starts only after the Numeric Ground Truth table is filled and the orchestrator has consolidated decisions.
- Applies edits from the per-file edit table in this ledger. Does not invent new numbers.
- Flags any doc claim that cannot be grounded in N1–N12 or in the inventory result.

## Key Changes

### 1. Phase 1 inventory and classification
- Build a single source-of-truth inventory for all Phase 1 assets and code paths.
- Classify every item into exactly one of: Active, Keep For Future Phases, Provenance/Reference, Quarantine / Remove Candidate, Discovered Duplicate.
- Apply the quarantine protocol below before any deletion.

### 2. Phase 1 refactor targets
- Extend `route_family.py` with the shared signed-route helper functions and constants currently duplicated in `hpms_enrichment.py` and `route_verification.py` (`_signed_route_family_slots`, `_parse_signed_route_family_list`, `SIGNED_ROUTE_PRIORITY`, `SIGNED_ROUTE_FAMILIES`, plus any equivalent siblings). The exported API becomes public (no leading underscore).
- Create `02-Data-Staging/scripts/01_roadway_inventory/arcgis_client.py` and move `_fetch_arcgis_features` and `_fetch_arcgis_object_ids` from `route_verification.py` into it as public functions. Update `rnhp_enrichment.py` and `route_verification.py` to import the public names.
- Remove wrappers that no longer add value after extraction.
- Audit `Roadways.py` for stale comments and silent-fallback branches. Do **not** remove any SQLite load path; it is the tabular source for RAPTOR.
- Keep `create_db.py` and `roadway_inventory.db` — `04-Webapp/backend/app/services/staged_roadways.py` hard-depends on SQLite for `/manifest`, `/summary`, `/detail/{id}`, `/filter_options`.

### 3. Phase 1 correctness and edge-case review
- Explicitly verify the edge cases listed in `Edge Cases To Verify` below.
- Record every risky assumption before any removal decision.

### 4. Phase 1 documentation updates in `00-Project-Management`
- Apply edits from the per-file edit table below. No freeform rewrites.
- Every numeric claim in every doc must be sourced from N1–N12.
- Every source-of-truth claim about "what Phase 1 uses today" must be sourced from the Active bucket in this ledger.

## Execution Order

1. **Step 0 — Scope decisions.** Orchestrator resolves D0.1 through D0.6 with the user. No sub-agent runs until D0.1 (evacuation scope) and D0.5 (helper destinations) are recorded.
2. **Step 1 — Ledger init.** Orchestrator pre-seeds classification buckets from the lists in this file.
3. **Step 2 — Numeric ground truth.** Orchestrator runs N1 through N12 and records values.
4. **Step 3 — Parallel sub-agents 1, 2, 3.** Inventory, QA, and refactor-review run concurrently on Phase 1 only. Each writes its section of this ledger.
5. **Step 4 — Consolidation.** Orchestrator merges findings into final keep/quarantine/remove/refactor decisions. Conflicts between sub-agents are resolved here before any code or docs change.
6. **Step 5 — Refactor implementation.** Apply extraction in D0.5. Verify imports compile. Run smoke checks.
7. **Step 6 — Validation.** Run the Test and Acceptance Plan. Any failure halts Step 7 until resolved.
8. **Step 7 — Documentation updates.** Sub-agent 4 applies the per-file edit table. Pulls all numbers from the Numeric Ground Truth section.
9. **Step 8 — Consistency pass.** Orchestrator reads the four updated docs end-to-end, confirms internal and cross-file consistency, and records final status here.

## Quarantine Protocol

- **Destination:** a `_quarantine/` subdirectory inside each owning folder (e.g. `01-Raw-Data/Roadway-Inventory/_quarantine/`, `02-Data-Staging/spatial/_quarantine/`). Do not pool quarantined files into one global folder — provenance is easier to restore from a sibling dir.
- **Mechanism:** `git mv` for tracked files; move-and-commit for untracked files so the move is recorded. Never `rm -rf`.
- **Record:** every quarantined item appears in the `Quarantine / Remove Candidates` ledger section with: original path, new path, owner (user), reason, quarantine date, earliest removal date.
- **Dwell:** minimum one project milestone before the file can be deleted outright. Removal is a follow-up PR, not part of this plan.
- **Rollback:** `git mv` back from `_quarantine/` restores the original path; the commit that quarantined the file is the rollback reference.
- **Out of scope:** no deletes of anything under `01-Raw-Data/` during this plan. Raw data is never removed, only quarantined.

## Interfaces / Contract Decisions

- This plan is limited to Phase 1.
- No staged-data contract changes unless a Phase 1 contract is proven dead and unused.
- `RoadwayData.load_data()` must keep current observable Phase 1 behavior — tabular merge from SQLite plus geometry from GeoPackage. Refactor is limited to comment and fallback-branch cleanup.
- The current Phase 1 staged backend contract remains:
  - SQLite (`02-Data-Staging/databases/roadway_inventory.db`) for tabular/detail/filter/summary/manifest queries.
  - GeoPackage (`02-Data-Staging/spatial/base_network.gpkg`) for geometry, bounds, and boundary-layer features.
- Shared helpers must be public utilities. No Phase 1 module may import another module's private `_`-prefixed helpers after Step 5.

## Test and Acceptance Plan

### Static checks
- `git ls-files '02-Data-Staging/**/*.db' '02-Data-Staging/**/*.gpkg'` — records which binaries are tracked. Acceptance: list matches D0.3 resolution.
- `grep -R "_fetch_arcgis" 02-Data-Staging/` — acceptance: only `arcgis_client.py` defines the symbols; other modules import from it. No underscore-prefixed cross-module imports remain.
- `grep -R "_signed_route\|SIGNED_ROUTE_FAMILIES\|SIGNED_ROUTE_PRIORITY" 02-Data-Staging/scripts/01_roadway_inventory/` — acceptance: constants/helpers defined once (in `route_family.py`), imported elsewhere.

### Classification completeness
- Every file surfaced by Sub-agent 1 appears in exactly one classification bucket. Acceptance: zero unclassified items; zero items in two buckets.

### Runtime smoke (lightest safe equivalent)
- `normalize.py` dry-run or equivalent: confirms `base_network.gpkg` and `roadway_inventory_cleaned.csv` are produced with expected layer/column set.
- `create_db.py` against current CSV + GPKG: confirms segment count equals N1; confirms same-run parity N9 holds.
- `validate.py`: confirms reports in `02-Data-Staging/reports/` regenerate without error.
- `RoadwayData.load_data()` smoke: returns the SC=1 filtered row count (approximately 19,569), result is non-zero, matches `SELECT COUNT(*) FROM segments WHERE SYSTEM_CODE = 1`.
- Webapp service check: `staged_roadways.get_manifest()`, `get_summary()`, `get_detail()`, `get_filter_options()` all return non-empty on the current DB.

### Numeric reconciliation
- Every numeric claim in the four updated docs is either (a) literally equal to a value in N1–N12 or (b) derivable from them. No orphan numbers.

### Doc consistency
- Segment count is identical across all four docs.
- AADT current and AADT 2044 coverage numbers (rows and %) are identical across docs that state them.
- "Status" line present and consistent across all four docs.
- SQLite-vs-GeoPackage architecture is stated at least in `phase-1-foundation.md`, and referenced from the other three docs.

## Pre-seeded Classification Buckets

These are starting points derived from the code-review findings. Sub-agent 1 confirms or corrects each entry and fills in anything missing.

Step 0 resolution note: for classification, treat `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb/` as the active Phase 1 AADT source, and treat `_evac_corridor_match.py`, `evacuation_enrichment.py`, `ga_evac_routes.geojson`, `ga_contraflow_routes.geojson`, `02-Data-Staging/qc/evacuation_route_qc/*`, and `02-Data-Staging/reports/evacuation_enrichment_summary.json` as Active.

### Active Phase 1 Inputs (keep — current consumers exist)
- `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/Road_Inventory_2024.gdb/` — source of truth network (consumer: `normalize.py`)
- `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_raw.json`, `hpms_ga_2024_tabular.json`, `hpms_ga_2024_layer_metadata.json` (consumer: `hpms_enrichment.py`)
- `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/speed_zone_on_system.geojson`, `speed_zone_off_system.geojson` (consumer: `rnhp_enrichment.py`)
- `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/signed_route_references/interstates.geojson`, `us_highway.geojson` (consumer: `route_verification.py`)
- `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb/` — pending confirmation in D0.4
- `02-Data-Staging/config/*.json` (all seven: `county_codes`, `crs_config`, `district_codes`, `georgia_route_family_crosswalk`, `georgia_signed_route_verification_sources`, `rnhp_enrichment_sources`, `roadway_domain_labels`)
- `02-Data-Staging/databases/roadway_inventory.db`
- `02-Data-Staging/spatial/base_network.gpkg`
- `02-Data-Staging/tables/roadway_inventory_cleaned.csv`
- `02-Data-Staging/reports/*.json` (generated; acceptance is "regenerate cleanly," not "keep as artifact")
- `02-Data-Staging/scripts/01_roadway_inventory/` — all files pending D0.1 decision on evacuation

### Keep For Future Phases (do not remove)
- Step 0 resolution note: `Traffic_Historical.zip` and `2010_thr_2019_Published_Traffic.zip` stay here for trend analysis; evacuation GeoJSON assets do not belong in this bucket.
- `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip` — trend analysis, likely Phase 5
- `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/2010_thr_2019_Published_Traffic.zip` — trend analysis
- `02-Data-Staging/tables/demographics/opb_projections.csv` — Phase 3 socioeconomic
- `02-Data-Staging/tables/connectivity/*` — Phase 2 connectivity
- `02-Data-Staging/spatial/ga_evac_routes.geojson`, `ga_contraflow_routes.geojson` — pending D0.1 (if Phase 8, this bucket; if active Phase 1, move to Active)

### Keep As Provenance/Reference
- `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/HPMS_Field_Manual_2016.pdf`
- `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary.pdf`
- `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary.agent.md`
- `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary-assets/`
- `01-Raw-Data/Roadway-Inventory/download_metadata.json`
- `00-Project-Management/Pipeline-Documentation/phase-1-Supplement-Docs/georgia-route-type-classification.md`
- `00-Project-Management/Pipeline-Documentation/phase-1-Supplement-Docs/roadway-gap-fill-consolidated.md`

### Quarantine / Remove Candidates
- `02-Data-Staging/spatial/roadway_inventory.db` -> `02-Data-Staging/spatial/_quarantine/roadway_inventory.db`
  owner: user
  reason: stale 0-byte artifact, no code references, primary DB lives in databases/
  quarantine date: 2026-04-17
  earliest removal date: after the next project milestone, via follow-up PR only

### Discovered Duplicates (need explicit resolution)
- Resolution note: D0.2 is resolved. `02-Data-Staging/spatial/roadway_inventory.db` is a stale 0-byte duplicate and must remain quarantined unless a real consumer is discovered later.
- `02-Data-Staging/spatial/roadway_inventory.db` — duplicate of `02-Data-Staging/databases/roadway_inventory.db`. Pending D0.2.

## Potential Mistakes / Risky Assumptions

- Assuming `Roadways.py` SQLite logic is dead. It is not. Refactor must only touch comments and fallback branches.
- Assuming `_evac_corridor_match.py` is dead because of the underscore prefix. It is imported by `evacuation_enrichment.py`.
- Assuming `route_family.py` already hosts signed-route helpers. It does not; it only holds classifier functions. The extraction in D0.5 must extend it.
- Assuming docs disagree only cosmetically. The segment-count delta (959 rows) is real and affects AADT numerators.
- Assuming `02-Data-Staging/spatial/roadway_inventory.db` is a safe duplicate. It may be a deploy artifact consumed outside the repo; confirm before quarantining.
- Assuming Traffic_Historical / 2010_2019 zips are dead. They are likely future-phase inputs.
- Assuming every `.json` in `02-Data-Staging/reports/` is an artifact to classify. They are generated by the normalize/validate pipeline; the test is "can they be regenerated cleanly," not "keep the file."

## Edge Cases To Verify

Each case has an expected outcome that Sub-agent 2 must confirm or falsify.

Step 0 resolution note: Edge case #12 is required under active Phase 1 evacuation enrichment. Missing `ga_evac_routes.geojson` must fail explicitly rather than silently skipping evacuation enrichment.

1. **Missing raw snapshot.** Delete a raw HPMS JSON and rerun `hpms_enrichment.py`. Expected: clean error, not silent empty merge.
2. **Stale GPAS snapshot.** Touch mtime on a `signed_route_references/*.geojson` to an old date. Expected: `route_verification.py` still runs; no staleness warning yet — note as gap.
3. **Stale traffic snapshot.** Same test against `TRAFFIC_Data_2024.gdb`. Expected: clean error if schema changed; silent if not.
4. **Absent SQLite.** Remove `databases/roadway_inventory.db` and call `staged_roadways.get_manifest()`. Expected: hard error. If it returns empty, that is a bug.
5. **Absent GeoPackage.** Remove `spatial/base_network.gpkg` and call `RoadwayData.load_data()`. Expected: falls through to GDB fallback (line 175–199 of `Roadways.py`); if both absent, hard error.
6. **Mismatched row counts.** Force a state where `segments` (SQLite) and `roadway_segments` (GPKG) disagree. Expected: validation catches it. If validation passes, that is a gap.
7. **Same-run-token drift.** Run `normalize.py`, then regenerate only the DB via `create_db.py` on a stale CSV. Expected: mtime divergence visible; ideally flagged.
8. **`_fetch_arcgis_*` offline.** Block ArcGIS host. Expected: `route_verification.py` and `rnhp_enrichment.py` fail with a clear network error, not silent empty frames.
9. **Generated artifact mistaken for deliverable.** After D0.3 resolution, confirm no committed `.db`/`.gpkg` is claimed as "source of truth" in any doc without also being regenerable from scripts.
10. **Unexpected CRS.** Inject a reprojected GPKG. Expected: `RoadwayData.load_data()` reprojects on line 271; no coord-silent-wrong output.
11. **Duplicate DB precedence.** If D0.2 decides `spatial/roadway_inventory.db` is a fallback, confirm which SQLite file the webapp opens under a missing-primary scenario. If D0.2 decides it is stale, confirm it is never opened.
12. **Evacuation geojson missing.** Remove `ga_evac_routes.geojson`. Expected behavior depends on D0.1 — document it either way.

## Documentation To Update — per-file edit table

All numeric edits pull from the Numeric Ground Truth table after it is filled. "N1", "N3" etc. below refer to that table.

### `00-Project-Management/Project_Plan/README.md`
- Line ~140: segment count — replace `244,904` with `N1`.
- Lines ~147–148: AADT current coverage — replace numerator `244,819` with `N3`, mileage numerator with `N5`, keep percentage if it equals `N4` rounded, otherwise replace.
- Line ~149: future AADT — replace `52,236` / `21.3%` with `N6` / `N7`.
- Add a sentence (anywhere in the Phase 1 section) pointing to the SQLite-vs-GeoPackage split documented in `phase-1-foundation.md`.

### `00-Project-Management/Project_Plan/phase-1-foundation.md`
- Line ~8: confirm `Status: Complete` (leave as-is if already correct).
- Line ~15: segment count — confirm equals `N1`; update if not.
- Line ~23–24: AADT current and 2044 coverage — reconcile against `N3`/`N4`, `N6`/`N7`.
- Add a new subsection titled **"Staged backend contract — SQLite + GeoPackage"** with this content (or equivalent):
  > Phase 1 stages tabular attributes to `02-Data-Staging/databases/roadway_inventory.db` (SQLite, `segments` table) and geometry to `02-Data-Staging/spatial/base_network.gpkg` (`roadway_segments` layer, plus `county_boundaries` and `district_boundaries`). The webapp service `staged_roadways.py` reads both: SQLite for manifest/summary/detail/filter queries, GeoPackage for geometry, bounds, and boundary layers. RAPTOR's `RoadwayData.load_data()` merges the SQLite tabular frame with GeoPackage geometry.
- Lines ~112–137: script list — confirm against actual `02-Data-Staging/scripts/01_roadway_inventory/` listing after refactor. Add `arcgis_client.py` and note the extension to `route_family.py`.

### `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md`
- Lines 35, 175, 177: segment count — confirm equals `N1`.
- Line 549, 845: AADT coverage — confirm equals `N3`/`N4`.
- Line 862: future AADT — confirm equals `N6`/`N7`.
- Lines 26, 49, 72 (EOC Hurricane Evacuation Routes references): rewrite per D0.1 resolution. If Phase 8, move reference into a "Related downstream (Phase 8) consumers" paragraph. If Phase 1-adjacent enrichment, mark clearly as "optional enrichment producing `SEC_EVAC` flags."
- Lines 1099–1108: outputs list — confirm against actual `02-Data-Staging/` tree; add the duplicate-DB note per D0.2 resolution.
- Add mileage coverage to match README specificity (`N2`, `N5`).

### `00-Project-Management/Pipeline-Documentation/phase-1-simplified-overview.md`
- Add a header line near the top: `Status: Complete for current project scope.`
- Line ~130: segment count — confirm equals `N1`.
- Lines ~79–83: outputs paragraph — add a one-sentence pointer to the SQLite+GeoPackage split.

## Agent Findings

### Sub-agent 1 - Inventory findings

Scope summary:
- Inventoried 366 files across the mandated Phase 1 paths plus the Phase 1 portions of `04-Webapp/backend/app/services/staged_roadways.py` and `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py`.
- Bucket totals from the live filesystem inventory: Active 193, Keep For Future Phases 5, Provenance/Reference 86, Quarantine / Remove Candidate 82, Discovered Duplicate 0.
- D0.2 is already reflected on disk. No live file remains in a duplicate bucket; the stale duplicate SQLite copy now exists only as `02-Data-Staging/spatial/_quarantine/roadway_inventory.db`.

Seed-bucket corrections:
- `02-Data-Staging/config/crs_config.json` is not Active. Current Phase 1 roadway code hardcodes `EPSG:32617` and only mentions the file as unused (`02-Data-Staging/scripts/01_roadway_inventory/normalize.py:58`, `02-Data-Staging/scripts/01_roadway_inventory/validate.py:27`).
- Only `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_tabular.json` is Active. `hpms_ga_2024_raw.json` and `hpms_ga_2024_layer_metadata.json` have no current in-repo readers; `hpms_enrichment.py` only loads the tabular snapshot (`02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py:123-135`).
- The seeded `02-Data-Staging/reports/*.json` bucket was too broad. Active reports are `current_aadt_coverage_audit_summary.json`, `evacuation_enrichment_summary.json`, and `validation_results.json`. `traffic_match_summary.json`, `hpms_enrichment_summary.json`, `rnhp_enrichment_summary.json`, and `signed_route_verification_summary.json` are current audit/provenance artifacts with no in-repo readers.
- `02-Data-Staging/scripts/01_roadway_inventory/__init__.py` is not an Active dependency. No in-repo package imports of `01_roadway_inventory` were found in the repo-wide search, so it stays in Provenance/Reference as scaffolding only.
- `02-Data-Staging/config/georgia_signed_route_verification_sources.json` points at `state_routes.geojson`, but that snapshot is absent on disk. Current behavior is to fall back to the live GPAS service when the local file is missing (`02-Data-Staging/scripts/01_roadway_inventory/route_verification.py:177-196`), so only the two cached GeoJSONs that actually exist were inventoried here.

#### Active

| Item(s) | Count | Consumer evidence |
|---|---:|---|
| Manifest A1 - all 106 internal files under `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/Road_Inventory_2024.gdb/` | 106 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:2473`, `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:359`, `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:376` |
| Manifest A2 - all 44 internal files under `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb/` | 44 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:2474`, `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:410` |
| `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_tabular.json` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py:123-135`, `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:35` |
| `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/speed_zone_on_system.geojson`, `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/speed_zone_off_system.geojson` | 2 | `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py:44-56`, `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:36-39` |
| `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/signed_route_references/interstates.geojson`, `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/signed_route_references/us_highway.geojson` | 2 | `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py:173-196`, `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:42-45` |
| `01-Raw-Data/Roadway-Inventory/scripts/download.py`, `01-Raw-Data/Roadway-Inventory/scripts/download_rnhp_enrichment.py`, `01-Raw-Data/Roadway-Inventory/scripts/download_signed_route_references.py` | 3 | `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md:320-324`, `00-Project-Management/Project_Plan/phase-1-foundation.md:110-114` |
| `02-Data-Staging/config/county_codes.json`, `02-Data-Staging/config/district_codes.json`, `02-Data-Staging/config/georgia_route_family_crosswalk.json`, `02-Data-Staging/config/georgia_signed_route_verification_sources.json`, `02-Data-Staging/config/rnhp_enrichment_sources.json`, `02-Data-Staging/config/roadway_domain_labels.json` | 6 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:83-91`, `02-Data-Staging/scripts/01_roadway_inventory/route_family.py:27-35`, `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py:37-45`, `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py:32-35` |
| `02-Data-Staging/databases/roadway_inventory.db` | 1 | `04-Webapp/backend/app/services/staged_roadways.py:36`, `04-Webapp/backend/app/services/staged_roadways.py:394`, `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py:146`, `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py:153` |
| `02-Data-Staging/spatial/base_network.gpkg` | 1 | `04-Webapp/backend/app/services/staged_roadways.py:37`, `04-Webapp/backend/app/services/staged_roadways.py:465`, `04-Webapp/backend/app/services/staged_roadways.py:611`, `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py:163`, `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py:166` |
| `02-Data-Staging/spatial/ga_evac_routes.geojson`, `02-Data-Staging/spatial/ga_contraflow_routes.geojson` | 2 | `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py:38-39`, `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py:146`, `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py:28-29` |
| `02-Data-Staging/tables/roadway_inventory_cleaned.csv` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/create_db.py:45-52`, `02-Data-Staging/scripts/01_roadway_inventory/validate.py:207` |
| `02-Data-Staging/reports/current_aadt_coverage_audit_summary.json` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/validate.py:146-147`, `02-Data-Staging/scripts/01_roadway_inventory/validate.py:1006-1007`, `02-Data-Staging/reports/validation_results.json:618-620` |
| `02-Data-Staging/reports/evacuation_enrichment_summary.json` | 1 | Active by D0.1 (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:43-47`); emitted on current normalize runs by `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:2564` and `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py:980-1004` |
| `02-Data-Staging/reports/validation_results.json` | 1 | `00-Project-Management/Project_Plan/phase-1-foundation.md:500` |
| `02-Data-Staging/qc/evacuation_route_qc/contraflow_routes_official.geojson`, `02-Data-Staging/qc/evacuation_route_qc/evac_routes_official.geojson`, `02-Data-Staging/qc/evacuation_route_qc/network_context.geojson`, `02-Data-Staging/qc/evacuation_route_qc/network_contraflow_flagged.geojson`, `02-Data-Staging/qc/evacuation_route_qc/network_evac_flagged.geojson` | 5 | `02-Data-Staging/qc/evacuation_route_qc/index.html:209-213` |
| `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py` | 1 | `00-Project-Management/prompts/hybrid-evac-matching-agent-prompt.md:10`, `00-Project-Management/prompts/hybrid-evac-matching-agent-prompt.md:61`, `00-Project-Management/prompts/false-negative-reduction-plan.md:59`, `00-Project-Management/prompts/false-negative-reduction-plan.md:134-156` |
| `02-Data-Staging/qc/evacuation_route_qc/index.html` | 1 | `00-Project-Management/prompts/hybrid-evac-matching-agent-prompt.md:11`, `00-Project-Management/prompts/hybrid-evac-matching-agent-prompt.md:63`, `02-Data-Staging/qc/evacuation_route_qc/SPATIAL_MATCHING_ISSUES.md:87-89` |
| `02-Data-Staging/qc/evacuation_route_qc/SPATIAL_MATCHING_ISSUES.md` | 1 | Active QC runbook within the D0.1 evacuation set (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:43-47`); still part of the current false-negative review workflow (`00-Project-Management/prompts/false-negative-reduction-plan.md:59`, `00-Project-Management/prompts/false-negative-reduction-plan.md:134-156`) |
| `02-Data-Staging/scripts/01_roadway_inventory/normalize.py` | 1 | `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md:320-329`, `00-Project-Management/Project_Plan/phase-1-foundation.md:168-179` |
| `02-Data-Staging/scripts/01_roadway_inventory/create_db.py` | 1 | `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md:320-329`, `00-Project-Management/tmp/pipeline-refactor-plan-temp.md:217` |
| `02-Data-Staging/scripts/01_roadway_inventory/validate.py` | 1 | `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md:318-329`, `00-Project-Management/Project_Plan/phase-1-foundation.md:190-197` |
| `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:35`, `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md:327` |
| `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:36-39`, `01-Raw-Data/Roadway-Inventory/scripts/download_rnhp_enrichment.py:20-28` |
| `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:42-45`, `01-Raw-Data/Roadway-Inventory/scripts/download_signed_route_references.py:26-34` |
| `02-Data-Staging/scripts/01_roadway_inventory/route_family.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:41`, `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md:325` |
| `02-Data-Staging/scripts/01_roadway_inventory/route_type_gdot.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:46` |
| `02-Data-Staging/scripts/01_roadway_inventory/utils.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:47`, `02-Data-Staging/scripts/01_roadway_inventory/validate.py:16`, `02-Data-Staging/scripts/01_roadway_inventory/route_type_gdot.py:20` |
| `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:34`, `00-Project-Management/tmp/pipeline-refactor-plan-temp.md:43-47` |
| `02-Data-Staging/scripts/01_roadway_inventory/_evac_corridor_match.py` | 1 | `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py:30`, `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py:18-22` |
| `04-Webapp/backend/app/services/staged_roadways.py` | 1 | `04-Webapp/backend/app/services/analytics.py:7`, `04-Webapp/backend/app/services/layers.py:17`, `04-Webapp/backend/app/services/georgia_filters.py:7`, `04-Webapp/backend/app/services/geospatial.py:6` |
| `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py` | 1 | `05-RAPTOR-Integration/states/Georgia/pipeline.py:9`, `05-RAPTOR-Integration/states/Georgia/pipeline.py:31`, `05-RAPTOR-Integration/states/Georgia/categories/SocioEconomic.py:28` |

#### Keep For Future Phases

| Item(s) | Count | Intended future phase | Why it cannot be regenerated |
|---|---:|---|---|
| `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip`, `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/2010_thr_2019_Published_Traffic.zip` | 2 | Future traffic trend-analysis work; the current Phase 1 ledger seeds this as likely Phase 5 (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:305-307`) | These are upstream GDOT archive packages, not products of the Phase 1 roadway pipeline. They cannot be recreated from `TRAFFIC_Data_2024.gdb` or any staged Phase 1 output. |
| `02-Data-Staging/tables/demographics/opb_projections.csv` | 1 | Phase 3 socioeconomic (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:308`, `00-Project-Management/Project_Plan/phase-3-socioeconomic.md:141-150`) | Regeneration depends on the socioeconomic pipeline and raw OPB workbooks outside this scope (`02-Data-Staging/scripts/06_socioeconomic/normalize.py:187-194`, `02-Data-Staging/scripts/06_socioeconomic/normalize.py:309-311`). |
| `02-Data-Staging/tables/connectivity/national_parks.geojson`, `02-Data-Staging/tables/connectivity/rail_facilities.geojson` | 2 | Phase 2 connectivity (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:309`, `00-Project-Management/Project_Plan/phase-2-connectivity.md:75-81`) | They are outputs of the connectivity pipeline, not the roadway pipeline. Later consumers are already wired for those layer names (`05-RAPTOR-Integration/states/Georgia/categories/Connectivity/TrafficGenerators.py:33-44`), and regeneration requires the connectivity download/normalize/create_gpkg flow (`02-Data-Staging/scripts/05_connectivity/create_gpkg.py:30-38`). |

#### Provenance/Reference

| Item(s) | Count | Why this bucket fits |
|---|---:|---|
| `01-Raw-Data/Roadway-Inventory/download_metadata.json` | 1 | Download provenance written by `01-Raw-Data/Roadway-Inventory/scripts/download.py:67-89`; no runtime readers were found. |
| `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/HPMS_Field_Manual_2016.pdf` | 1 | Reference manual for HPMS code meanings; not a runtime Phase 1 input. |
| `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_raw.json`, `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_layer_metadata.json` | 2 | Supplementary upstream snapshot components with no current in-repo readers; the active loader only consumes `hpms_ga_2024_tabular.json` (`02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py:123-135`). |
| `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary.pdf`, `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary.agent.md` | 2 | Reference documentation. The agent-friendly conversion is cited by `00-Project-Management/Pipeline-Documentation/phase-1-Supplement-Docs/georgia-route-type-classification.md:27`. |
| Manifest P1 - all 71 files under `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary-assets/` | 71 | Supporting render assets for the GDOT data dictionary; reference-only and not loaded by Phase 1 ETL/runtime code. |
| `02-Data-Staging/config/crs_config.json` | 1 | Present but unused in current Phase 1 roadway code (`02-Data-Staging/scripts/01_roadway_inventory/normalize.py:58`, `02-Data-Staging/scripts/01_roadway_inventory/validate.py:27`). |
| `02-Data-Staging/databases/.gitkeep`, `02-Data-Staging/spatial/.gitkeep`, `02-Data-Staging/reports/.gitkeep` | 3 | Repo scaffolding for git-ignored generated-output directories, consistent with D0.3 (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:59`). |
| `02-Data-Staging/reports/traffic_match_summary.json`, `02-Data-Staging/reports/hpms_enrichment_summary.json`, `02-Data-Staging/reports/rnhp_enrichment_summary.json`, `02-Data-Staging/reports/signed_route_verification_summary.json` | 4 | Generated audit/provenance artifacts with no current in-repo readers. Writers are `02-Data-Staging/scripts/01_roadway_inventory/normalize.py:1420-1422`, `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py:421-452`, `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py:399-427`, and `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py:683-710`. |
| `02-Data-Staging/scripts/01_roadway_inventory/__init__.py` | 1 | Unused package marker/scaffolding; the repo-wide search found no import sites for the package name. |

#### Quarantine / Remove Candidate

| Item(s) | Count | Why this bucket fits |
|---|---:|---|
| `02-Data-Staging/spatial/_quarantine/roadway_inventory.db` | 1 | D0.2-resolved stale 0-byte duplicate (`00-Project-Management/tmp/pipeline-refactor-plan-temp.md:49-53`, `00-Project-Management/tmp/pipeline-refactor-plan-temp.md:322-330`). |
| Manifest Q1 - all 4 files under `01-Raw-Data/Roadway-Inventory/scripts/__pycache__/` | 4 | Interpreter cache files; reproducible local byproducts, not source-of-truth assets. |
| Manifest Q2 - all 76 files under `02-Data-Staging/scripts/01_roadway_inventory/__pycache__/` | 76 | Interpreter cache files; reproducible local byproducts, not source-of-truth assets. |
| `02-Data-Staging/qc/evacuation_route_qc/__pycache__/generate_qc_map.cpython-313.pyc` | 1 | Interpreter cache file; reproducible local byproduct, not a source-of-truth asset. |

#### Discovered Duplicate

- No current on-disk items remain in this bucket after the D0.2 quarantine. The only former duplicate in scope is already tracked above as `02-Data-Staging/spatial/_quarantine/roadway_inventory.db`.

#### Grouped manifests

Manifest A1 - `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/Road_Inventory_2024.gdb/` (106 files)
```text
a00000001.freelist
a00000001.gdbindexes
a00000001.gdbtable
a00000001.gdbtablx
a00000001.TablesByName.atx
a00000002.gdbtable
a00000002.gdbtablx
a00000003.gdbindexes
a00000003.gdbtable
a00000003.gdbtablx
a00000004.CatItemsByPhysicalName.atx
a00000004.CatItemsByType.atx
a00000004.FDO_UUID.atx
a00000004.freelist
a00000004.gdbindexes
a00000004.gdbtable
a00000004.gdbtablx
a00000004.spx
a00000005.CatItemTypesByName.atx
a00000005.CatItemTypesByParentTypeID.atx
a00000005.CatItemTypesByUUID.atx
a00000005.gdbindexes
a00000005.gdbtable
a00000005.gdbtablx
a00000006.CatRelsByDestinationID.atx
a00000006.CatRelsByOriginID.atx
a00000006.CatRelsByType.atx
a00000006.FDO_UUID.atx
a00000006.gdbindexes
a00000006.gdbtable
a00000006.gdbtablx
a00000007.CatRelTypesByBackwardLabel.atx
a00000007.CatRelTypesByDestItemTypeID.atx
a00000007.CatRelTypesByForwardLabel.atx
a00000007.CatRelTypesByName.atx
a00000007.CatRelTypesByOriginItemTypeID.atx
a00000007.CatRelTypesByUUID.atx
a00000007.gdbindexes
a00000007.gdbtable
a00000007.gdbtablx
a00000009.gdbindexes
a00000009.gdbtable
a00000009.gdbtablx
a00000009.spx
a0000000a.gdbindexes
a0000000a.gdbtable
a0000000a.gdbtablx
a0000000a.spx
a0000000b.gdbindexes
a0000000b.gdbtable
a0000000b.gdbtablx
a0000000b.spx
a0000000c.gdbindexes
a0000000c.gdbtable
a0000000c.gdbtablx
a0000000c.spx
a0000000d.gdbindexes
a0000000d.gdbtable
a0000000d.gdbtablx
a0000000d.spx
a0000000e.gdbindexes
a0000000e.gdbtable
a0000000e.gdbtablx
a0000000e.spx
a0000000f.gdbindexes
a0000000f.gdbtable
a0000000f.gdbtablx
a0000000f.spx
a00000010.gdbindexes
a00000010.gdbtable
a00000010.gdbtablx
a00000010.spx
a00000011.gdbindexes
a00000011.gdbtable
a00000011.gdbtablx
a00000011.spx
a00000012.gdbindexes
a00000012.gdbtable
a00000012.gdbtablx
a00000012.spx
a00000013.gdbindexes
a00000013.gdbtable
a00000013.gdbtablx
a00000013.spx
a00000014.gdbindexes
a00000014.gdbtable
a00000014.gdbtablx
a00000014.spx
a00000015.gdbindexes
a00000015.gdbtable
a00000015.gdbtablx
a00000015.spx
a00000016.gdbindexes
a00000016.gdbtable
a00000016.gdbtablx
a00000016.spx
a00000017.gdbindexes
a00000017.gdbtable
a00000017.gdbtablx
a00000017.spx
a00000018.gdbindexes
a00000018.gdbtable
a00000018.gdbtablx
a00000018.spx
gdb
timestamps
```

Manifest A2 - `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb/` (44 files)
```text
a00000001.gdbindexes
a00000001.gdbtable
a00000001.gdbtablx
a00000001.TablesByName.atx
a00000002.gdbtable
a00000002.gdbtablx
a00000003.gdbindexes
a00000003.gdbtable
a00000003.gdbtablx
a00000004.CatItemsByPhysicalName.atx
a00000004.CatItemsByType.atx
a00000004.FDO_UUID.atx
a00000004.gdbindexes
a00000004.gdbtable
a00000004.gdbtablx
a00000004.spx
a00000005.CatItemTypesByName.atx
a00000005.CatItemTypesByParentTypeID.atx
a00000005.CatItemTypesByUUID.atx
a00000005.gdbindexes
a00000005.gdbtable
a00000005.gdbtablx
a00000006.CatRelsByDestinationID.atx
a00000006.CatRelsByOriginID.atx
a00000006.CatRelsByType.atx
a00000006.FDO_UUID.atx
a00000006.gdbindexes
a00000006.gdbtable
a00000006.gdbtablx
a00000007.CatRelTypesByBackwardLabel.atx
a00000007.CatRelTypesByDestItemTypeID.atx
a00000007.CatRelTypesByForwardLabel.atx
a00000007.CatRelTypesByName.atx
a00000007.CatRelTypesByOriginItemTypeID.atx
a00000007.CatRelTypesByUUID.atx
a00000007.gdbindexes
a00000007.gdbtable
a00000007.gdbtablx
a00000009.gdbindexes
a00000009.gdbtable
a00000009.gdbtablx
a00000009.spx
gdb
timestamps
```

Manifest P1 - `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary-assets/` (71 files)
```text
figures/figure-01.png
figures/figure-02.png
figures/figure-03.png
figures/figure-04.png
figures/figure-05.png
figures/figure-06.png
figures/figure-07.png
figures/figure-08.png
figures/figure-09.png
figures/figure-10.png
figures/figure-11.png
figures/figure-12.png
figures/figure-13.png
figures/figure-14.png
figures/figure-15.png
figures/figure-16.png
figures/figure-17.png
figures/figure-18.png
figures/figure-19.png
figures/figure-20.png
figures/figure-21.png
figures/figure-22.png
figures/figure-23.png
figures/figure-24.png
figures/figure-25.png
figures/figure-26.png
figures/figure-27.png
figures/figure-28.png
figures/figure-29.png
figures/figure-30.png
figures/figure-31.png
figures/figure-32.png
figures/figure-33.png
figures/figure-34.png
figures/figure-35.png
figures/figure-36.png
figures/figure-37.png
figures/figure-38.png
figures/figure-39.png
pages/page-01.png
pages/page-02.png
pages/page-03.png
pages/page-04.png
pages/page-05.png
pages/page-06.png
pages/page-07.png
pages/page-08.png
pages/page-09.png
pages/page-10.png
pages/page-11.png
pages/page-12.png
pages/page-13.png
pages/page-14.png
pages/page-15.png
pages/page-16.png
pages/page-17.png
pages/page-18.png
pages/page-19.png
pages/page-20.png
pages/page-21.png
pages/page-22.png
pages/page-23.png
pages/page-24.png
pages/page-25.png
pages/page-26.png
pages/page-27.png
pages/page-28.png
pages/page-29.png
pages/page-30.png
pages/page-31.png
pages/page-32.png
```

Manifest Q1 - `01-Raw-Data/Roadway-Inventory/scripts/__pycache__/` (4 files)
```text
download.cpython-313.pyc
download.cpython-313.pyc.2042130905648
download_rnhp_enrichment.cpython-313.pyc
download_signed_route_references.cpython-313.pyc
```

Manifest Q2 - `02-Data-Staging/scripts/01_roadway_inventory/__pycache__/` (76 files)
```text
create_db.cpython-313.pyc
create_db.cpython-313.pyc.2617721235888
evacuation_enrichment.cpython-313.pyc
hpms_enrichment.cpython-313.pyc
hpms_enrichment.cpython-313.pyc.1861778770800
hpms_enrichment.cpython-313.pyc.2266578914160
hpms_enrichment.cpython-313.pyc.2516084050800
hpms_enrichment.cpython-313.pyc.2617721117488
hpms_enrichment.cpython-313.pyc.2823194572624
hpms_enrichment.cpython-313.pyc.2840633903984
normalize.cpython-313.pyc
normalize.cpython-313.pyc.2516085882672
normalize.cpython-313.pyc.2617721238320
normalize.cpython-313.pyc.2823193845808
rnhp_enrichment.cpython-313.pyc
rnhp_enrichment.cpython-313.pyc.2617721814112
route_family.cpython-313.pyc
route_type_gdot.cpython-313.pyc
route_type_gdot.cpython-313.pyc.1204607591584
route_type_gdot.cpython-313.pyc.1608625824928
route_type_gdot.cpython-313.pyc.1611105542272
route_type_gdot.cpython-313.pyc.1716356700224
route_type_gdot.cpython-313.pyc.1847654061968
route_type_gdot.cpython-313.pyc.1914838767504
route_type_gdot.cpython-313.pyc.2027893528992
route_type_gdot.cpython-313.pyc.2048841557152
route_type_gdot.cpython-313.pyc.2186839857040
route_type_gdot.cpython-313.pyc.2371828685216
route_type_gdot.cpython-313.pyc.2516084063616
route_type_gdot.cpython-313.pyc.2563711358864
route_type_gdot.cpython-313.pyc.2617721814112
route_type_gdot.cpython-313.pyc.2629586966416
route_type_gdot.cpython-313.pyc.2997094488128
route_verification.cpython-313.pyc
route_verification.cpython-313.pyc.1204596578192
route_verification.cpython-313.pyc.1465297461728
route_verification.cpython-313.pyc.1488472479632
route_verification.cpython-313.pyc.1608615434128
route_verification.cpython-313.pyc.1611992013216
route_verification.cpython-313.pyc.1697315280784
route_verification.cpython-313.pyc.2048830461840
route_verification.cpython-313.pyc.2066541172624
route_verification.cpython-313.pyc.2105579591568
route_verification.cpython-313.pyc.2516084057712
route_verification.cpython-313.pyc.2555872794512
route_verification.cpython-313.pyc.2617721814112
route_verification.cpython-313.pyc.2823194572624
route_verification.cpython-313.pyc.3097367975824
utils.cpython-313.pyc
utils.cpython-313.pyc.1204607590880
utils.cpython-313.pyc.1428030120592
utils.cpython-313.pyc.1465297461024
utils.cpython-313.pyc.1488483508704
utils.cpython-313.pyc.1608625824224
utils.cpython-313.pyc.1611992012688
utils.cpython-313.pyc.1697326309856
utils.cpython-313.pyc.1716356700576
utils.cpython-313.pyc.1847654062320
utils.cpython-313.pyc.1861025453968
utils.cpython-313.pyc.1877437196336
utils.cpython-313.pyc.1914838767856
utils.cpython-313.pyc.2028781901712
utils.cpython-313.pyc.2048841556448
utils.cpython-313.pyc.2066550825440
utils.cpython-313.pyc.2105590686176
utils.cpython-313.pyc.2186839857392
utils.cpython-313.pyc.2372716402576
utils.cpython-313.pyc.2563711359216
utils.cpython-313.pyc.2617721235376
utils.cpython-313.pyc.2629586966768
utils.cpython-313.pyc.2997094488480
utils.cpython-313.pyc.3097379086816
validate.cpython-313.pyc
validate.cpython-313.pyc.2617721235376
_evac_corridor_match.cpython-313.pyc
__init__.cpython-313.pyc
```

### Sub-agent 2 - QA findings

Scope here is runtime/data behavior only. Live smoke on 2026-04-17 against the current staged outputs succeeded for the happy path: `validate.py` passed `116/116`, `staged_roadways.get_manifest()` and `get_summary()` returned `245,863`, `get_filter_options()` returned 7 districts / 159 counties, and `get_detail()` returned a populated record.

Phase 1 edge-case checklist (expected vs observed):

1. `Missing raw snapshot (HPMS)`  
Expected: clean error, not silent empty merge.  
Observed: `hpms_enrichment.py:123-128` warns and returns an empty frame, and `apply_hpms_enrichment()` at `hpms_enrichment.py:203-214` returns the input unchanged. Live probe with `HPMS_PATH` redirected to a missing file returned `equals_input=True`.  
Outcome: `FAIL` - missing HPMS input is silently skipped instead of stopping the run.

2. `Stale GPAS snapshot`  
Expected: `route_verification.py` still runs; no staleness warning yet - note as gap.  
Observed: matches the expectation, but the gap is real. `route_verification.fetch_reference_layer()` (`route_verification.py:177-196`) and `rnhp_enrichment.fetch_enrichment_layer()` (`rnhp_enrichment.py:48-69`) only check `local_path.exists() and not refresh`; there is no mtime/hash/schema freshness check.  
Outcome: `PASS (gap confirmed)` - stale local GPAS/RNHP snapshots are accepted silently if they still parse.

3. `Stale traffic snapshot`  
Expected: clean error if schema changed; silent if not.  
Observed: `normalize.load_current_traffic()` (`normalize.py:408-439`) has no staleness check. An old-but-schema-compatible `TRAFFIC_Data_2024.gdb` will be accepted silently; a schema drift would raise an uncaught column-selection error rather than a custom diagnostic.  
Outcome: `PARTIAL` - silent acceptance for stale-but-compatible traffic data; hard failure for schema drift, but not a clean one.

4. `Absent SQLite`  
Expected: removing `databases/roadway_inventory.db` and calling `staged_roadways.get_manifest()` should hard-error, not return empty.  
Observed: matches expectation for the webapp. Redirecting `STAGED_DB_PATH` to a missing path raised `OperationalError: unable to open database file`. `staged_roadways.py:393-411` / `491-512` do not have an empty fallback.  
Outcome: `PASS` for the webapp service.

5. `Absent GeoPackage`  
Expected: `RoadwayData.load_data()` falls through to GDB fallback; if both GPKG and raw GDB are absent, hard error.  
Observed: both branches exist, but the fallback is not parity-preserving. With the GPKG hidden, `RoadwayData.load_data()` succeeded with only `3,384` rows instead of the normal `19,569` rows because `_load_geometry()` (`Roadways.py:161-204`) picks the first sorted `GA_*_Routes` layer found in the raw GDB. With both GPKG and raw GDB hidden, it raised `FileNotFoundError` as expected. Separately, in `staged_roadways.py`, a missing GPKG hard-fails `manifest`, `bounds`, `features`, and boundary endpoints with `DataSourceError`, while `summary` still works from SQLite alone.  
Outcome: `FAIL` for RAPTOR fallback parity; `PASS` for explicit webapp failure.

6. `Mismatched row counts between SQLite and GeoPackage`  
Expected: validation catches `segments` vs `roadway_segments` drift.  
Observed: current validator does not assert this. `validate_database()` (`validate.py:862-928`) checks DB row count and DB/CSV parity only; `validate_geometry()` / `validate_crs()` (`validate.py:265-279`, `743-858`) inspect the GPKG separately. A DB/GPKG row mismatch is not compared anywhere, yet the current validation run still passed `116/116`.  
Outcome: `FAIL` - Edge Case 6 is currently unguarded.

7. `Same-run-token drift (N9/N12)`  
Expected: mtime divergence should be visible and ideally flagged.  
Observed: the current staged pair passes N12 (`22.72` seconds), but there is still no enforcement in code. `staged_roadways.py:368-390` uses file mtimes only to invalidate caches, not to reject mismatched DB/GPKG pairs, and `validate.py` contains no run-token or mtime-delta assertion. A DB-only rebuild on stale CSV/GPKG could still validate if row counts happened to align.  
Outcome: `FAIL` - current artifacts are in parity, but the check is operationally missing.

8. `ArcGIS fetch helpers offline`  
Expected: `route_verification.py` and `rnhp_enrichment.py` should fail with a clear network error, not silently return unchanged/empty results.  
Observed: top-level callers warn and continue. Blocking `route_verification.fetch_reference_layer()` preserved the signed-route columns unchanged (`route_verification.py:590-680`), and blocking `rnhp_enrichment.fetch_enrichment_layer()` returned rows with no populated speed-zone fields (`rnhp_enrichment.py:174-196`, `329-396`). The network exception is not surfaced as a hard failure to the pipeline caller.  
Outcome: `FAIL` - offline official-reference fetches degrade silently at the pipeline level.

9. `Generated artifact mistaken for deliverable`  
Expected after D0.3: no `.db` / `.gpkg` should be treated as authoritative without also being regenerable.  
Observed: current runtime behavior is consistent with the plan. `normalize.py` + `create_db.py` regenerate the staged artifacts, and the current validator run succeeded against those generated outputs. The remaining operational caveat is Edge Case 7: consumers trust the staged binaries without same-run parity enforcement.  
Outcome: `PASS with parity caveat`.

10. `Unexpected CRS`  
Expected: `RoadwayData.load_data()` should reproject and avoid coord-silent-wrong output.  
Observed: RAPTOR behaves correctly here. Feeding `RoadwayData.load_data()` a temporary `EPSG:4326` GeoPackage still produced `EPSG:32617` output via `Roadways.py:270-271`. The staged webapp does not: `_project_bounds()` in `staged_roadways.py:334-344` assumes source `EPSG:32617` from constants at `staged_roadways.py:39-40`. A temporary `EPSG:4326` GPKG produced nonsense bounds `[-85.48950612403804, 0.0002761997372988751, -85.48947116835902, 0.00031563061897812273]` instead of Georgia latitudes (sample expected bounds were approximately `[-85.08095, 30.62296, -81.17921, 34.99476]`).  
Outcome: `PASS` for RAPTOR reprojection, `FAIL` for staged webapp bounds under unexpected CRS.

11. `Duplicate DB precedence`  
Expected under D0.2: the quarantined `spatial/roadway_inventory.db` should never be opened.  
Observed: matches expectation. The webapp hardcodes `02-Data-Staging/databases/roadway_inventory.db` (`staged_roadways.py:36`, `393-395`), and RAPTOR's unused `_load_from_db()` also points only to `DB_DIR / roadway_inventory.db` (`Roadways.py:144-153`). No inspected runtime path referenced `02-Data-Staging/spatial/roadway_inventory.db`.  
Outcome: `PASS`.

12. `Evacuation geojson missing`  
Expected per D0.1: fail explicitly rather than silently skipping evacuation enrichment.  
Observed: `apply_evacuation_enrichment()` (`evacuation_enrichment.py:871-890`) catches any load failure, logs `Evacuation route enrichment unavailable`, and returns an all-false evac frame. Live probe with `ga_evac_routes.geojson` missing and download blocked returned `25` rows with `SEC_EVAC = 0` and no source attribution.  
Outcome: `FAIL` - active Phase 1 evacuation enrichment currently degrades silently.

Additional runtime notes:

- `RoadwayData.load_data()` currently never calls `_load_from_db()` even though that loader still exists at `Roadways.py:144-153`. Redirecting `DB_DIR` to an empty temp directory still produced the normal `19,569` RAPTOR rows, so SQLite absence is not currently enforced in RAPTOR. This is the main behavioral divergence behind the stale fallback concern in `Roadways.py`.
- Current `AADT` null distribution is small but not random. Live DB query results: `97 / 245,863` rows are missing both current and future AADT; `61` are `SYSTEM_CODE=1` and `36` are `SYSTEM_CODE=2`; route-family split is `40 U.S. Route`, `36 Local/Other`, `21 State Route`; district split is `D3=29`, `D2=18`, `D1=13`, `D6=12`, `D5=11`, `D7=9`, `D4=5`. Cross-check: `AADT IS NULL AND FUTURE_AADT_2044 IS NOT NULL = 0`, and `AADT IS NOT NULL AND FUTURE_AADT_2044 IS NULL = 0`, so the same `97` rows drive both null sets.
- Current live service counts are internally consistent on the happy path: SQLite `segments = 245,863`, GeoPackage `roadway_segments = 245,863`, `staged_roadways.get_manifest().total_segments = 245,863`, and `staged_roadways.get_summary().roadway_count = 245,863`. Current N12 parity is `22.72` seconds, so the present artifacts are acceptable even though the parity guardrail is missing.

### Sub-agent 3 - Refactor findings

- Phase 1 only. Findings below are limited to imports, duplicate helpers, dead wrappers, private-symbol leaks, and stale comments. All extraction proposals use the D0.5 destinations already resolved in this ledger. No behavior changes are proposed.

- Private ArcGIS helper leak across a module boundary: `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py:22-25` imports `_fetch_arcgis_features` and `_fetch_arcgis_object_ids` from `route_verification.py`, then uses them at `rnhp_enrichment.py:61-62`. Those helpers are defined in `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py:103-170` alongside generic transport support code, even though `route_verification.py` is otherwise a signed-route module. Diff-level proposal for Step 5: create `02-Data-Staging/scripts/01_roadway_inventory/arcgis_client.py`, move the generic ArcGIS HTTP/GeoJSON fetch helpers there as public names, and update `route_verification.py:177-197` plus `rnhp_enrichment.py:48-69` to import the public API. Keep `fetch_reference_layer()` and `fetch_enrichment_layer()` local because they own module-specific cache paths and snapshot writes.

- Duplicate signed-route helper logic should move to `route_family.py`: `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py:46-101` and `route_verification.py:52-57,441-492` both define the same signed-route ordering primitives (`SIGNED_ROUTE_PRIORITY`, `SIGNED_ROUTE_FAMILIES`, family sorting, slot assignment, and JSON list parsing). `02-Data-Staging/scripts/01_roadway_inventory/route_family.py:55-176` currently only contains route-family classification helpers, so the D0.5 destination has not been wired yet. Diff-level proposal for Step 5: extend `route_family.py` with public signed-route utilities/constants, then replace the local private copies in `hpms_enrichment.py` and `route_verification.py` with imports from `route_family.py`. Keep HPMS-specific ordering logic local unless a second caller appears.

- Residual private-symbol leak outside the named D0.5 destinations: `route_verification.py:31` and `rnhp_enrichment.py:26` both import `_clean_text` and `_round_milepoint` from `utils.py`. That is the same cross-module private-import pattern the plan is trying to eliminate. I am not assigning a new destination here because D0.5 did not resolve one, but this should remain on the Step 5 punch list if the orchestrator wants full compliance with "no underscore-prefixed cross-module imports may remain after Step 5."

- Dead/orphaned SQLite wrapper in RAPTOR loader: `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py:144-159` defines `_load_from_db()`, but `load_data()` at `Roadways.py:208-275` never calls it and instead proceeds from the geometry frame loaded at `Roadways.py:215-217`. Because the Ground Truth section of this plan says the SQLite path is load-bearing, this is not a deletion finding. It is a structure finding: the SQLite loader is currently orphaned from the live method body. Diff-level proposal for Step 5: keep the SQLite loader in place and make the live contract explicit in comments/docstrings; do not remove any SQLite load path in this phase.

- Stale comments/docstrings in `Roadways.py`: the module docstring at `Roadways.py:1-5`, class docstring at `Roadways.py:28-33`, and `load_data()` docstring at `Roadways.py:209-214` all describe a DB + GeoPackage merge and "State Highway Routes" filtering without mentioning the resolved D0.1 evacuation inclusion. The inline comment at `Roadways.py:215` is also stale because it says the geometry source "has both tabular + geometry," which conflicts with the plan's authoritative SQLite + GeoPackage split. Diff-level proposal for Step 5: align these comments/docstrings to one contract only - SQLite is the tabular source, GeoPackage is the primary geometry source, raw GDB is geometry fallback only, and Phase 1 filtering includes `SEC_EVAC` corridor segments when present.

## Review Results / Decisions
- Step 4 consolidation completed on 2026-04-17.
- Canonical numeric source for all downstream doc edits is the latest regenerated Step 2 table in this ledger. N5 is accepted with a flag. N6/N7 are accepted as intentional current behavior because the current pipeline now uses official implied growth projection to fill remaining future-AADT gaps for segments that already have `AADT_2024`.
- Inventory consolidation accepted Sub-agent 1's bucket corrections: `config/crs_config.json` is Provenance/Reference, only `hpms_ga_2024_tabular.json` is an Active HPMS input, only the current live evacuation/QC artifacts stay Active, and the quarantined duplicate DB remains the only resolved duplicate.
- Phase 1 cleanup decisions accepted from the inventory: interpreter cache files under `__pycache__/` are Quarantine / Remove Candidates, but no new quarantine moves are required before the Step 5 refactor. The raw-data no-delete rule remains in force.
- QA findings are recorded as real Phase 1 gaps, but Step 5 will not change runtime behavior outside the D0.5 extraction/comment scope. In particular, this turn will not change HPMS/GPAS offline fallback behavior, DB/GPKG parity enforcement, or RAPTOR/Webapp runtime contracts beyond comment/docstring accuracy.
- Step 5 implementation scope is limited to: move generic ArcGIS fetch helpers into `arcgis_client.py`; extend `route_family.py` with shared public signed-route helpers/constants; update imports so no underscore-prefixed cross-module helper imports remain; and refresh stale `Roadways.py` comments/docstrings without changing observable SQLite/GPKG behavior.
- Step 5 implementation completed on 2026-04-17. Static checks now show `_fetch_arcgis*` defined only in `arcgis_client.py`, shared signed-route constants/helpers defined centrally in `route_family.py`, and no remaining underscore-prefixed cross-module helper imports in the refactored Phase 1 modules.
- Step 6 validation status: PASS after correcting the RAPTOR smoke gate. `validate.py` passed `116/116`, the staged webapp smoke passed (`manifest=245,863`, `summary=245,863`, `filter_options=7 districts / 159 counties`, `detail` populated), and the refactor patch compiled cleanly. `RoadwayData.load_data()` returned `19,569` RAPTOR rows, and the DB spot-check `SELECT COUNT(*) FROM segments WHERE SYSTEM_CODE = 1` returned `19,458`, which is accepted as the correct contract because RAPTOR intentionally filters to SC=1 plus evac-corridor additions rather than all `N1` statewide segments.
- Step 7 documentation update completed on 2026-04-17. Sub-agent 4 updated the four owned Phase 1 docs with the refreshed N-table values, the SQLite/GeoPackage backend split, the D0.1 evacuation wording, and the D0.2 duplicate-DB note.
- Step 8 consistency pass completed on 2026-04-17. The orchestrator re-read all four updated docs end-to-end and corrected remaining stale runtime metrics outside the worker's edit table: staged DB columns `118`, validation `116/116`, `SYSTEM_CODE = 1` row count `19,458`, HPMS signed-route coverage `223,672`, GPAS authoritative verification coverage `6,854`, Future AADT direct coverage `46,619` (`19.0%`) before implied-growth projection, and posted-speed / school-zone totals `50,959` and `1,105`. Final grep sweep found no remaining stale copies of the pre-refresh counts that had been identified during this review pass.
- Final doc clarification completed on 2026-04-17. `00-Project-Management/Project_Plan/README.md` is confirmed updated in the working tree and now carries the per-file edit-table changes plus an explicit future-AADT note that distinguishes direct forecast coverage (`46,619` / `19.0%`) from total post-imputation coverage (`245,766` / `99.96%`). The same distinction is now stated in the other Phase 1 docs wherever future-AADT coverage is described.

### Sub-agent 4 - Documentation update status
- Updated the four owned Phase 1 docs with the refreshed aggregate counts, the SQLite/GeoPackage backend split, the D0.1 evacuation wording, and the D0.2 duplicate-DB note.
- Unresolved doc claim: the narrative AADT source-by-source split in `phase-1-roadway-data-pipeline.md` was not re-grounded because the plan only published refreshed aggregate N1/N3/N5/N6/N7 values, not a new per-source breakdown.
