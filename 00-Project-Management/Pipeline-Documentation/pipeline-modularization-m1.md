# Pipeline Modularization — M1: Checkpoint Wrapper

Status: DRAFT (scope frozen to M1; M2–M5 explicitly deferred)
Project: Georgia Statewide Data Pipeline
Target pipeline (M1): `02-Data-Staging/scripts/01_roadway_inventory/normalize.py`

## Goal

Stop recomputing unchanged work. After M1, a rerun of `normalize.main()` with no inputs, config, or code changes skips every expensive step and completes in seconds.

Two tiers of invalidation precision, stated honestly:

- **Precise.** Changes to raw inputs, config files, or to any helper module that already lives in its own file (`evacuation_enrichment.py`, `hpms_enrichment.py`, `rnhp_enrichment.py`, `route_verification.py`, `route_type_gdot.py`, `route_family.py`, `admin_breakpoints.py`) invalidate only stages whose helpers they touch, and everything downstream.
- **Coarse — and this is the real limit of M1.** Many stage helpers still live inside the 4,317-line `normalize.py` (segmentation, admin overlay flags, county/district backfill, AADT gap-fill chain, label derivation, boundary fetch, publish-time refetch). If M1 hashed `normalize.py` as a single file, any edit anywhere in it would invalidate most stages. To avoid that, the M1 decorator hashes **the source of each explicitly-declared helper function** via `inspect.getsource()`, not whole files. That keeps invalidation scoped even though the helpers share a file. See the Fingerprint algorithm section below.

Success criterion: on a no-change rerun, wall time drops from "full pipeline" to "load final checkpoint + write outputs" (seconds, not minutes/hours), with byte-identical `roadway_inventory_cleaned.csv` and an attribute-equivalent `base_network.gpkg`.

## Non-goals (explicit M1 exclusions)

- No `--only`, `--from`, `--to` stage selection flags (that is M2).
- No column-wise side-tables; each checkpoint is the whole `segmented` GeoDataFrame at that point (M3).
- No parallel execution of enrichments (M4).
- No change to the connectivity or socioeconomic pipelines (M5).
- No refactor of any existing function body inside `normalize.py` or its enrichment modules. M1 only wraps.

## Architecture

**One idea: a persistent checkpoint after each stage, keyed by a content fingerprint.**

A stage is a single call from `main()` that produces a meaningful intermediate state. For each stage:

1. Compute the stage's fingerprint from: upstream manifest fingerprints, relevant raw inputs (directory-aware content summary for `.gdb` sources; content hash for small files), relevant config-file contents, and **the source text of each explicitly-declared helper function** (not whole files).
2. If a checkpoint with that fingerprint already exists on disk, load it and skip execution.
3. Otherwise, run the inner function, write the output + manifest to disk, and continue.

No orchestration framework. Plain Python + Parquet/FGB files + a JSON manifest per stage.

**One pragmatic side-change to `normalize.main()`.** Today `write_supporting_boundary_layers` refetches county/district from the live GDOT service during the publish step ([normalize.py:4076](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L4076)), and `load_county_boundaries_for_attribute_backfill` prefers the existing staged GPKG and otherwise refetches ([normalize.py:2513](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L2513)). Both paths duplicate boundary fetches that stage 02 already owns. M1 threads the stage-02 county/district GDFs through to both call sites so the cache is authoritative and stages 07 and 15 depend on stage 02 rather than on prior published outputs or redundant live fetches. This is a ~10-line change in `main()`, no behavior change for a clean run.

## Stage decomposition for `01_roadway_inventory`

M1 treats each of these as one checkpoint. Numbering follows [normalize.py:4112](02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L4112) `main()`.

| # | Stage name | Produces | Reads | Cost tier |
|---|---|---|---|---|
| 01 | `load_routes` | `routes.parquet` (post `enrich_routes_with_static_attributes`, prepared, with `unique_id`, reprojected to `TARGET_CRS`) | `Road_Inventory_2024.gdb`, `TRAFFIC_Data_2024.gdb`, `config/*.json` | medium |
| 02 | `fetch_boundaries` | `boundaries/{county,district,mpo,rc,area_office,cities,state_house,state_senate,congressional}.fgb` | live URLs, `01-Raw-Data/Boundaries/cache/*`, area-office config | expensive (network) |
| 03 | `write_admin_snapshots` | `config/mpo_codes.json`, `config/regional_commission_codes.json` (side effect) | stage 02 outputs | trivial (but depends on 02) |
| 04 | `segment` | `segmented_base.parquet` (segmented, with `unique_id`, collision-guarded, reprojected) | stage 01, stage 02, `TRAFFIC_Data_2024.gdb` | **most expensive** |
| 05 | `admin_overlay_flags_and_length` | `with_admin_overlays.parquet` (stamps state house/senate/congressional/city + `SEGMENT_LENGTH`) | stage 04, stage 02 | expensive (spatial overlay) |
| 06 | `speed_zone_enrichment` | `with_speed_zones.parquet` | stage 05, RNHP source | moderate |
| 07 | `county_district_backfill` | `with_cd_backfill.parquet` | stage 06, **stage 02 county boundaries** (threaded in — replaces the current GPKG-first / live-fetch-fallback logic in `load_county_boundaries_for_attribute_backfill`) | expensive (spatial overlay) |
| 08 | `hpms_enrichment` | `with_hpms.parquet` | stage 07, FHWA HPMS tabular + geom | **expensive** (nearest-neighbor match) |
| 09 | `aadt_2024_source_agreement` | `with_aadt_agreement.parquet` | stage 08 | cheap |
| 10 | `off_system_speed_zone_enrichment` | `with_off_system_speed.parquet` | stage 09, RNHP source | moderate |
| 11 | `signed_route_verification` | `with_gpas.parquet` | stage 10, GDOT signed-route ArcGIS layers | expensive (network + overlay) |
| 12 | `route_type_gdot` | `with_route_type.parquet` | stage 11 | cheap |
| 13 | `evacuation_enrichment` | `with_evacuation.parquet` | stage 12, evacuation corridor source | **expensive** (spatial match) |
| 14 | `aadt_gap_fill_and_labels` | `with_aadt_and_labels.parquet` (sync aliases, direction mirror, state-system gap-fill, NN AADT, future AADT fill, official growth, confidence recompute, texas-alignment derive, decoded labels, county_all, column reorder) | stage 13, county boundaries | cheap |
| 15 | `publish` | `tables/roadway_inventory_cleaned.csv`, `spatial/base_network.gpkg` (roadway_segments + 8 boundary layers), `reports/*` (match_summary, current_aadt_coverage_audit, enrichment, hpms, signed_route, evacuation), `.tmp/` audit artifacts written by the current `main()` | stage 14, **stage 02 boundary GDFs threaded into `write_supporting_boundary_layers` so it does not refetch county/district live** | moderate |

15 stages. Stages 04, 07, 08, 11, 13 are the dominant cost.

Stage 14 is intentionally chunky. It contains ~10 cheap derivation calls whose individual caching would add overhead without meaningful savings. M3 will split this one if and when we go column-wise.

Stage 02 keeps the existing `.tmp/rebuild_outputs/*.fgb` cache as its on-disk output — the stage is already half-checkpointed. M1 formalizes it with a manifest.

## Checkpoint format

- Tabular + geometric state after each stage → single **GeoParquet** file (`pyogrio` or `geopandas` native). Fast read/write, single file per stage, preserves dtypes and CRS.
- Boundary layers (stage 02) stay as **FGB** — keeps the existing cache, readable by `pyogrio`.
- Size budget: a full segmented GDF is ~200–300 MB GeoParquet. 15 stages × that = ~3 GB worst case. Will be gitignored — see the gitignore task below.

## Checkpoint + manifest layout

```
02-Data-Staging/staged/checkpoints/
  01_roadway_inventory/
    01_load_routes.parquet
    01_load_routes.manifest.json
    02_fetch_boundaries/
      county.fgb
      district.fgb
      ...
      congressional.fgb
      _manifest.json
    04_segment.parquet
    04_segment.manifest.json
    ...
    14_aadt_gap_fill_and_labels.parquet
    14_aadt_gap_fill_and_labels.manifest.json
    15_publish.manifest.json
```

**Gitignore correction.** The root `.gitignore` does not currently cover `02-Data-Staging/staged/` — only `databases/`, `spatial/`, `tables/`, and `reports/` under `02-Data-Staging/`. Task 1 of this plan adds `02-Data-Staging/staged/` (or at minimum `02-Data-Staging/staged/checkpoints/`) to `.gitignore` before the first checkpoint is written.

## Manifest schema

```json
{
  "stage_name": "08_hpms_enrichment",
  "fingerprint": "sha256:abc123…",
  "produced_at": "2026-04-19T14:12:03Z",
  "runtime_seconds": 142.8,
  "inputs": {
    "upstream_checkpoints": [
      {"stage": "07_county_district_backfill", "fingerprint": "sha256:..."}
    ],
    "raw_files": [
      {"path": "01-Raw-Data/FHWA_HPMS/2024/hpms_ga_2024_tabular.json",
       "kind": "file", "size": 12345678, "mtime": 1713456789.0},
      {"path": "01-Raw-Data/Roadway-Inventory/Road_Inventory_2024.gdb",
       "kind": "gdb_directory",
       "content_summary_sha256": "..."}
    ],
    "config_files": [
      {"path": "02-Data-Staging/config/crs_config.json",
       "sha256": "..."}
    ],
    "code_functions": [
      {"module": "hpms_enrichment",
       "qualname": "apply_hpms_enrichment",
       "source_sha256": "..."},
      {"module": "hpms_enrichment",
       "qualname": "write_hpms_enrichment_summary",
       "source_sha256": "..."}
    ],
    "code_globals": [
      {"module": "normalize",
       "name": "MILEPOINT_TOLERANCE",
       "repr": "0.0001"},
      {"module": "normalize",
       "name": "TARGET_CRS",
       "repr": "'EPSG:32617'"}
    ],
    "code_files": [
      {"path": "02-Data-Staging/scripts/pipeline/stages/roadway_inventory.py",
       "sha256": "..."}
    ]
  },
  "output": {
    "path": "02-Data-Staging/staged/checkpoints/01_roadway_inventory/08_hpms_enrichment.parquet",
    "row_count": 287432,
    "column_count": 178,
    "crs": "EPSG:32617",
    "sha256": "..."
  }
}
```

## Fingerprint algorithm

```
fingerprint = sha256(
    stage_name
    + each upstream manifest.fingerprint (ordered by stage #)
    + each raw_input fingerprint (see below), sorted by path
    + each config_file sha256 (sorted by path)
    + each code_function source_sha256 (sorted by module.qualname)
    + each code_global (module, name, repr(value)) triple (sorted)
    + each code_file sha256 for stage-wrapper modules (sorted by path)
    + a small "stage_params" dict if the stage takes scalar kwargs (sorted JSON)
)
```

Raw-input fingerprinting handles two shapes, because `find_path` resolves both files and `.gdb` directories ([normalize.py:244](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L244)):

- **Regular file.** Fingerprint = `(size, mtime_ns)`. The `mtime` comparison is nanosecond-granular where the filesystem supports it.
- **Directory (including `.gdb`).** Fingerprint = `sha256` over a sorted list of `(relpath, size, mtime_ns)` triples for every regular file inside, recursed. Cheap even for multi-GB geodatabases because we never read file contents — just stat them. Catches edits to internal `.gdbtable` / `.gdbindexes` / `.freelist` / `.atx` files, which is the failure mode Codex flagged.

Small raw files (JSON, CSV under a configurable size cap, e.g. 10 MB) may upgrade to a full `sha256` content hash to catch the "overwrite with identical bytes" edge case (the Open question below lists the candidates).

Design choices:

- **Raw directories: stat-tree summary, not content hash.** The `.gdb` is ~1–2 GB and internal file content hashing would dominate every run. The stat-tree approach catches any internal mutation in O(entries) without reading bytes.
- **Config files: content hash.** Small, fast, the things most likely to change during iterative development.
- **Code fingerprinting is function-level, not file-level.** Each stage's decorator takes an explicit `helpers=[apply_hpms_enrichment, write_hpms_enrichment_summary, ...]` list. The fingerprint component is the concatenation of `inspect.getsource(f)` hashes, sorted by `f.__module__ + f.__qualname__`. This is the mechanism that makes invalidation scoped even though many helpers live inside `normalize.py`. A change to an unrelated function in `normalize.py` does not invalidate the stage.
- **Module-constant fingerprinting.** Each stage also takes an explicit `globals=[(module, "MILEPOINT_TOLERANCE"), (module, "COUNTY_ALL_DELIMITER"), ...]` list. The fingerprint component is the sorted concatenation of `(module, name, repr(value))`. This closes the gap where output-affecting module-level constants (like `MILEPOINT_TOLERANCE` [normalize.py:165](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L165), `COUNTY_ALL_DELIMITER` [normalize.py:168](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L168), `COLUMNS_TO_DROP_FROM_OUTPUT` [normalize.py:174](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L174), `TARGET_CRS`, `ROUTE_MERGE_KEYS`, etc.) are not caught by `inspect.getsource` on functions. Using `repr(value)` keeps this cheap and robust for the primitive types and simple containers these constants hold; complex objects fall back to a conservative `sha256(pickle.dumps(value))`. Per-stage `globals` lists are audited alongside `helpers` in tasks 6–8.
- **Limits of function-level and global-level hashing.**
  - *Indirect callee.* If a stage's helper calls another function in the same module that is *not* listed, an edit to that indirect callee will not be caught. The remedy is to list transitive helpers in the stage's `helpers` declaration.
  - *Indirect constant.* Same risk for module globals read by unlisted helpers. Same remedy.
  - *Dynamic behavior.* Monkey-patching, `importlib` reloads, or env-var-driven branches are out of scope.
  
  Getting the `helpers` and `globals` lists right for each stage is part of tasks 6–8 below, and the regression runbook includes a specific test (case 7) for the constant case.
- **Stage-wrapper modules hashed as whole files.** The thin wrappers we write in `02-Data-Staging/scripts/pipeline/stages/roadway_inventory.py` are small, stable, and declaration-heavy — whole-file hashing is fine.
- **Transitive dependency.** Each manifest includes only its *direct* upstream manifests' fingerprints. Because those fingerprints already encode *their* upstream, transitive invalidation is automatic.

## File layout (new modules)

```
02-Data-Staging/scripts/pipeline/
  __init__.py
  checkpoint.py        # manifest read/write, fingerprint compute, checkpoint IO
  stage.py             # @stage decorator; orchestration glue
  run.py               # CLI entrypoint (see invocation below)
  stages/
    __init__.py
    roadway_inventory.py   # the 15 stage functions for 01_roadway_inventory
```

**Import-path reality.** The existing pipeline scripts live under `02-Data-Staging/scripts/01_roadway_inventory/` and use bare imports (`from admin_breakpoints import ...`, `from evacuation_enrichment import ...`). That only works when that directory is on `sys.path`, which is why today's entrypoint is `python 02-Data-Staging/scripts/01_roadway_inventory/normalize.py`. There is no repo-root `scripts/` package.

The new runner mirrors this: a module script with explicit path setup at the top.

```python
# 02-Data-Staging/scripts/pipeline/run.py
import sys
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "02-Data-Staging" / "scripts" / "01_roadway_inventory"))
```

Invocation:

```
python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory
```

The stage functions in `stages/roadway_inventory.py` are thin wrappers. Each one:
1. Declares its inputs (upstream stage names, raw paths, config-file paths).
2. Declares the list of helper functions whose source participates in its fingerprint.
3. Imports and calls the existing helpers from `01_roadway_inventory/*.py` unchanged.
4. Returns the intermediate object.

The `@stage` decorator handles all manifest / fingerprint / skip logic. The one exception to "no refactor" is the ~10-line change in `main()` to thread stage-02 county/district GDFs through stages 07 and 15 (see Architecture section above). Everything else in `normalize.py` stays untouched in M1 — we leave it as a working reference for the regression check.

## Runner behavior (M1 only)

Flags supported in M1:

- `--pipeline roadway_inventory` — required, identifies which DAG to run.
- `--force-all` — ignore all manifests, rerun everything (safety valve).
- `--output-root <path>` — **worktree redirect.** Overrides the effective root used by stages for every write target the current `main()` produces (see complete list below). Default is `02-Data-Staging/` (with `.tmp/` writes staying under the repo root).

`--output-root` is the mechanism that makes worktree runs safe per AGENT.md §Worktree Data Access. It must redirect **every** write destination the current `main()` touches. The full list:

- `tables/roadway_inventory_cleaned.csv` ([normalize.py:4281](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L4281))
- `spatial/base_network.gpkg` ([normalize.py:4287](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L4287))
- `reports/*.json` (match_summary, enrichment, hpms, signed_route, evacuation, current_aadt_coverage_audit_summary)
- **`config/mpo_codes.json` and `config/regional_commission_codes.json`** via `write_admin_code_snapshots` ([normalize.py:3635](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L3635)) — these are tracked in git, so a worktree run against the main config dir would stomp on the checked-in copies from a potentially different boundary vintage.
- **`.tmp/rebuild_outputs/*.fgb`** — mid-run boundary cache written by `fetch_and_cache_boundary` via the `REBUILD_OUTPUTS_DIR` module constant ([normalize.py:132](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L132)). Rooted at `PROJECT_ROOT / ".tmp"`, not `02-Data-Staging/`.
- **`.tmp/roadway_inventory/current_aadt_audit/*.csv`** — three audit CSVs written inline in `write_current_aadt_coverage_audit` ([normalize.py:2496](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L2496)). The dir is computed locally in the function as `PROJECT_ROOT / ".tmp" / "roadway_inventory" / "current_aadt_audit"`, not via a module constant.

That last bullet is the sharpest edge. Making it obey `--output-root` requires more than the module-attribute injection used for the four `_DIR` constants — the path is built inside the function body. Two options:

1. **Minimal refactor (preferred for M1).** Introduce a module-level `CURRENT_AADT_AUDIT_DIR` constant derived from `PROJECT_ROOT`, have `write_current_aadt_coverage_audit` read from it, and redirect it alongside the other `_DIR` constants. One-line change inside the function body, same pattern as the existing dirs.
2. **Thread the path through** the function signature. More invasive; defer.

Implementation note for the four existing dir constants (`TABLES_DIR`, `SPATIAL_DIR`, `REPORTS_DIR`, `CONFIG_DIR` in `normalize.py`, plus `REBUILD_OUTPUTS_DIR`): override at runtime by setting the module attributes from `run.py` before the first stage call. Matches the "no refactor" M1 spirit for those. `CURRENT_AADT_AUDIT_DIR` needs the one-line extraction in task 1 of the plan — document it explicitly under scaffolding.

That's it. No partial-run flags (M2).

Log output per stage:

```
[stage 08_hpms_enrichment] fingerprint sha256:abc… → CACHE HIT, skipping (manifest age 2h)
[stage 13_evacuation_enrichment] fingerprint sha256:xyz… → MISS, running
[stage 13_evacuation_enrichment] complete in 84.3s, wrote …/13_evacuation_enrichment.parquet
```

## Regression strategy

This is where M1 earns trust. Before declaring done:

1. **Baseline capture.** From a clean main repo checkout, run the current monolithic `normalize.main()`. Snapshot `roadway_inventory_cleaned.csv` (SHA-256), a column-by-column hash of `base_network.gpkg roadway_segments` layer, and row-count + attribute hash of each of the 8 boundary layers inside the GPKG (`county_boundaries`, `district_boundaries`, `area_office_boundaries`, `mpo_boundaries`, `regional_commission_boundaries`, `state_house_boundaries`, `state_senate_boundaries`, `congressional_boundaries`).
2. **Fresh new-runner run.** Delete `02-Data-Staging/staged/checkpoints/`, run `python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory`. Assert the CSV hash matches baseline. Assert each GPKG column hashes match baseline. Assert each of the 8 boundary layers matches baseline row count + attribute hash.
3. **Skip verification.** Rerun immediately. Assert every stage logs CACHE HIT. Assert final outputs are byte-identical and not rewritten (check mtime on the CSV before and after).
4. **Touch-one-file verification.** `touch` the HPMS tabular JSON. Rerun. Assert stages 01–07 are CACHE HIT, stages 08–15 are MISS. Assert outputs still match baseline.
5. **Edit-one-function-in-normalize.py verification.** Add a harmless whitespace-only change to `apply_direction_mirror_aadt` inside `normalize.py`. Rerun. Because that helper is declared only in stage 14's `helpers` list, assert stages 01–13 are CACHE HIT and stages 14–15 are MISS. This is the specific test that function-level hashing works — a whole-file hash would invalidate everything. If this test fails, the scope of `helpers=` lists needs re-auditing before M1 ships.
6. **Edit-one-function-in-enrichment-module verification.** Add a comment to `evacuation_enrichment.py`. Rerun. Assert stages 01–12 are CACHE HIT, stages 13–15 are MISS.
7. **Edit-one-module-global verification.** Change `COUNTY_ALL_DELIMITER` in `normalize.py` from `", "` to `" | "`. Rerun. Because `COUNTY_ALL_DELIMITER` is declared in stage 14's `globals` list, assert stages 01–13 are CACHE HIT and stages 14–15 are MISS. Revert the edit after the test. This is the specific test that module-constant fingerprinting works — without it, this edit would silently hit cache and produce a CSV with the wrong delimiter.

A small pytest under `02-Data-Staging/scripts/pipeline/tests/test_checkpoint_equivalence.py` automates 2–3 against a tiny synthetic input if we can construct one cheaply. Otherwise the checks stay as a documented manual runbook in `00-Project-Management/Pipeline-Documentation/`.

## Concrete task breakdown

Order matters; each task is a separate commit on the feature branch.

1. **Scaffold + gitignore + audit-dir constant** — create `02-Data-Staging/scripts/pipeline/` module, empty stubs for `checkpoint.py`, `stage.py`, `run.py`, `stages/roadway_inventory.py`. Add `02-Data-Staging/staged/` to the root `.gitignore` (currently only `databases/`, `spatial/`, `tables/`, `reports/` are ignored under `02-Data-Staging/`). Extract the inline `PROJECT_ROOT / ".tmp" / "roadway_inventory" / "current_aadt_audit"` path inside `write_current_aadt_coverage_audit` ([normalize.py:2496](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py#L2496)) into a module-level `CURRENT_AADT_AUDIT_DIR` constant, so it can be redirected by `--output-root` via the same module-attribute-injection pattern as the other `_DIR` constants. One-line extraction; no behavior change.
2. **Checkpoint IO + fingerprint** — implement `write_checkpoint(path, gdf)`, `read_checkpoint(path)`, manifest read/write, fingerprint compute (including the directory-aware `.gdb` stat-tree summary and `inspect.getsource()`-based function hashing). Unit tests for fingerprint determinism, a touch-stat-change test, and a function-source-change test.
3. **`@stage` decorator + wiring for output redirection** — wrap a single trivial function first (e.g. `sync_derived_alias_fields`) end-to-end. Prove the skip-on-hit path works. Also prove `--output-root` redirects all four write targets (tables, spatial, reports, config) by running the trivial wrap with a throwaway root.
4. **Boundary threading refactor in `main()`** — the ~10-line change that threads stage-02 county/district into `load_county_boundaries_for_attribute_backfill` and `write_supporting_boundary_layers`. Standalone commit so it can be reverted independently if it breaks something. Verify current `main()` still produces (a) byte-identical CSV, (b) attribute-equivalent `roadway_segments` GPKG layer, and (c) row-count + attribute hash parity for all 8 boundary layers in the published GPKG against the pre-refactor baseline. The boundary-layer check is the one that specifically guards the carveout — a CSV-only check would silently pass if the refactor changed which county/district vintage made it into the published GPKG.
5. **Wrap stages 01, 04, 15** (the load / segment / publish bookends). Run end-to-end once against the post-task-4 baseline snapshot; assert CSV equivalence.
6. **Wrap stages 02, 03, 05–07.** Boundary fetch + early post-segment. Rerun regression suite. Stage 02 should adopt (and replace) the `.tmp/rebuild_outputs/*.fgb` cache.
7. **Wrap stages 08–13** (the expensive enrichments). Rerun regression suite; this is the moment the user-visible speedup appears.
8. **Wrap stage 14** (the chunky derive/labels block). Rerun regression suite — specifically the "edit a function inside `normalize.py`" test — to prove function-level hashing is scoped correctly.
9. **Runner CLI** — add `run.py` with `--pipeline`, `--force-all`, `--output-root`. Finalize help text.
10. **Docs + runbook** — add `00-Project-Management/Pipeline-Documentation/pipeline-checkpointing.md` covering: how skipping works, how to force a rerun, where checkpoints live, when to delete them, how `--output-root` is used for worktree-local runs.
11. **Cutover decision** — `normalize.main()` stays callable as the reference path. Document that the new runner is the recommended entrypoint. Do not delete `normalize.main()` in M1.

**Effort framing (replaces the earlier "2 days" estimate).** The wall-clock cost of M1 is dominated by regression runs, not by writing the wrapper code. Concretely:

- One baseline run of current `normalize.main()` before task 5. This is the slow run we're trying to avoid.
- One cold run of the new runner at the end of task 5 to verify equivalence.
- After that, every subsequent task commit triggers only the stages it touched, because upstream checkpoints are hot.

The wrapping code itself is mechanical. Expect your review/approval attention on roughly 4–6 self-contained commits (scaffold, checkpoint+fingerprint, boundary threading, the three wrap-stages clusters, runner CLI, runbook). If a current full `main()` run is `T` minutes, plan for ~`2T` minutes of pipeline time to get through M1, plus ~10 partial reruns each of which is a small fraction of `T`.

## Open questions

1. **Raw-file staleness — identical-byte rewrites.** Some download scripts may overwrite cache files even when the source bytes are unchanged, which would defeat mtime/size invalidation in the opposite direction — false invalidation, not false cache. Candidates: `download_boundaries.py` cache files, HPMS refresh, RNHP cache. Fix if measured: upgrade those specific paths to content-hash fingerprinting (easy; files are small).
2. **Helpers list audit.** Each stage's `helpers=[...]` declaration is the scope of its function-level hash. For stages whose entry point calls transitive helpers in the same module, the list needs every helper that could change the output. Task 2's unit test for "function-source-change" catches the direct case; each stage's wrap commit (tasks 5–8) should also grep the helper for same-module callees and add them.
3. **Checkpoint directory location.** Default `02-Data-Staging/staged/checkpoints/`. Survives `.tmp/` wipes, stays near the other staging artifacts, and is gitignored by task 1.
4. **Backward-compatibility of stage 15 side effects.** The new runner's stage 15 writes the same `tables/`, `spatial/`, `reports/`, and `.tmp/` artifacts as today's `main()`. No new outputs, no dropped outputs. The equivalence regression check in task 5 enforces this.
5. **Failure mode.** If a stage crashes mid-write, the partial checkpoint must not be trusted. Write checkpoint to `<stage>.parquet.tmp` + rename atomically; write manifest only after the rename succeeds. On startup, delete any stale `.parquet.tmp` or `.manifest.json.tmp` files.
6. **Worktree interaction.** AGENT.md bars full pipeline runs from worktrees against the main staging tree. `--output-root <worktree>/_scratch/staging/` redirects all four write destinations (checkpoints, tables, spatial, reports, config snapshots) so a worktree run cannot stomp on the main tree. Task 10 runbook documents this.
7. **`create_db.py` and `create_gpkg.py` scope.** M1 leaves these out of the runner — they are separate entrypoints today. The runner only owns what `normalize.main()` owns. Revisit in M5 alongside connectivity and socioeconomic.

## Risks

- **Fingerprint false-negative (the worst kind).** A stage is cached but shouldn't be — produces wrong output with no user signal. Two sub-cases specific to this codebase:
  - *Indirect helper edit.* An edit to a same-module callee that wasn't listed in the stage's `helpers=` fires a cache hit on stale logic. Mitigation: the regression runbook's test 5 catches one instance; the per-stage helpers audit in task 6–8 catches the rest.
  - *Raw-file identical-byte rewrite* on a file whose fingerprint is mtime+size. Mitigation: Open question 1's upgrade path.
- **Fingerprint false-positive.** Over-invalidation. Tolerated in M1. Function-level hashing makes this much less likely than whole-file hashing, so it should rarely trigger.
- **Schema drift between monolith and new runner.** Any silent deviation in column order, dtype, or NA encoding breaks the equivalence check. Mitigation: task 5 establishes the bookend check before wrapping any middle stage, so regressions surface on the first wrap commit rather than at the end.
- **Boundary threading regression.** Task 4 changes `main()` — if it breaks, the baseline itself is wrong. Mitigation: task 4 must produce byte-identical CSV against the pre-refactor baseline, and is a standalone revertable commit.
- **Worktree output stomping.** If `--output-root` is forgotten when running from a worktree, the main repo's tracked config snapshots and staged files get overwritten. Mitigation: add a clear startup warning when running from a non-main path without `--output-root`; consider a config flag in the repo-tools worktree bootstrap script to set a default.
- **Checkpoint disk cost.** ~3 GB per full run, persisted. Mitigation: document cleanup (`--force-all` or `rm -rf 02-Data-Staging/staged/checkpoints/`).

## What M1 explicitly leaves for later

- Stage-level CLI selection (`--only`, `--from`, `--to`) → M2.
- Column-wise side-tables per enrichment → M3.
- Parallel tier-2 execution → M4.
- Checkpointing for `05_connectivity` and `06_socioeconomic` → M5.

When M1 ships, the user can rerun `normalize.main()` (via the new runner) with zero real changes and have it finish in seconds. That is the whole point of M1.
