# Pipeline Checkpointing (M1)

## Overview

The checkpoint runner wraps the existing `normalize.main()` pipeline with a fingerprint-based caching layer. Each of the 15 pipeline stages writes a GeoParquet checkpoint and a JSON manifest. On rerun, stages whose fingerprint matches the cached manifest are skipped, loading the checkpoint instead.

## Quick Start

```bash
# Full run (first time, or after deleting checkpoints)
python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory

# Rerun — unchanged stages are skipped
python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory

# Force rerun of everything
python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory --force-all

# Worktree run — redirect all outputs
python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory \
    --output-root /path/to/worktree
```

## How Skipping Works

Each stage declares:
- **Upstream stages** — their manifest fingerprints propagate transitively
- **Raw inputs** — stat-tree fingerprint for directories (`.gdb`), mtime+size for files
- **Config files** — SHA-256 content hash
- **Helper functions** — `inspect.getsource()` hash of each declared helper
- **Module globals** — `repr(value)` of each declared constant

All components are hashed into a single SHA-256 fingerprint. If the manifest on disk matches, the stage is skipped.

## Fingerprint Invalidation

| Change | Stages invalidated |
|---|---|
| Edit a function in `evacuation_enrichment.py` | Stage 13 + downstream (14, 15) |
| Edit `apply_direction_mirror_aadt` in `normalize.py` | Stage 14 + downstream (15) |
| Change `COUNTY_ALL_DELIMITER` constant | Stage 14 + downstream (15) |
| Touch HPMS raw data | Stage 08 + downstream (09-15) |
| Edit a function used only in stage 01 | Stage 01 + all downstream |

## Checkpoint Location

```
02-Data-Staging/staged/checkpoints/01_roadway_inventory/
    01_load_routes.parquet
    01_load_routes.manifest.json
    02_fetch_boundaries.parquet
    02_fetch_boundaries.manifest.json
    ...
    15_publish.manifest.json
```

This directory is gitignored. Total size is ~3 GB for a full run.

## Force Rerun

```bash
# Delete all checkpoints
rm -rf 02-Data-Staging/staged/checkpoints/

# Or use the flag
python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory --force-all
```

## Output Redirection (`--output-root`)

When running from a git worktree, use `--output-root` to prevent writing to the main repo's staging directories. This redirects:

- `TABLES_DIR` → `<root>/02-Data-Staging/tables/`
- `SPATIAL_DIR` → `<root>/02-Data-Staging/spatial/`
- `REPORTS_DIR` → `<root>/02-Data-Staging/reports/`
- `CONFIG_DIR` → `<root>/02-Data-Staging/config/`
- `REBUILD_OUTPUTS_DIR` → `<root>/.tmp/rebuild_outputs/`
- `CURRENT_AADT_AUDIT_DIR` → `<root>/.tmp/roadway_inventory/current_aadt_audit/`
- Checkpoint dir → `<root>/02-Data-Staging/staged/checkpoints/`

The runner warns if it detects a worktree environment without `--output-root`.

## Stage Map

| # | Stage | Cost | Produces |
|---|---|---|---|
| 01 | load_routes | medium | Routes GeoDataFrame |
| 02 | fetch_boundaries | expensive (network) | County boundaries + context dict |
| 03 | write_admin_snapshots | trivial | MPO/RC code JSON snapshots |
| 04 | segment | **most expensive** | Segmented network |
| 05 | admin_overlay_flags_and_length | expensive | +legislative/city flags, segment length |
| 06 | speed_zone_enrichment | moderate | +speed zones |
| 07 | county_district_backfill | expensive | +backfilled county/district |
| 08 | hpms_enrichment | **expensive** | +HPMS attributes |
| 09 | aadt_2024_source_agreement | cheap | +AADT agreement bucket |
| 10 | off_system_speed_zone_enrichment | moderate | +off-system speed |
| 11 | signed_route_verification | expensive | +GPAS verification |
| 12 | route_type_gdot | cheap | +ROUTE_TYPE_GDOT, HWY_NAME |
| 13 | evacuation_enrichment | **expensive** | +evacuation flags |
| 14 | aadt_gap_fill_and_labels | cheap | +AADT gap-fill, labels, county_all |
| 15 | publish | moderate | CSV, GPKG, reports (no checkpoint) |

## Monolith Compatibility

`normalize.main()` remains callable as the reference path. The boundary threading changes (county/district passed through to stages 07 and 15) are backward-compatible — existing call sites work identically when the new parameters are omitted.

## Regression Runbook

See [pipeline-modularization-m1.md](pipeline-modularization-m1.md) § Regression strategy for the 7-case verification protocol.
