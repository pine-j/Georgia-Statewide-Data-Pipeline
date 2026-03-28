# Phase 16 — Environmental Justice

## Goal
Download, clean, and load environmental justice screening data into the Georgia database. Cross-cutting layer usable by multiple RAPTOR categories and for equity analysis.

## Status: Not Started (Post-RAPTOR)
**Depends on**: Phase 1 (base layer), Phase 6 (demographics)

---

## Datasets

### 16.1 EPA EJScreen (v2.3)
- **Source**: U.S. EPA
- **URL**: `https://screening-tools.com/epa-ejscreen`
- **Format**: Interactive tool; download (CSV, SHP)
- **Key fields**: Environmental + demographic indicators at block-group level; includes transportation access index
- **Coverage**: U.S., 2015-2024, annual updates
- **Use**: Primary EJ screening tool; block-group-level EJ indices for corridor equity analysis
- **Place in**: `01-Raw-Data/ej/ejscreen/`

### 16.2 CDC/ATSDR Environmental Justice Index (EJI)
- **Source**: CDC/ATSDR
- **URL**: `https://www.atsdr.cdc.gov/place-health/php/eji/index.html`
- **Format**: Interactive; downloads
- **Key fields**: Cumulative EJ rankings combining environmental burden and social vulnerability
- **Coverage**: U.S., 2022 (first edition)
- **Use**: Complementary EJ ranking to EJScreen; health-focused vulnerability indicators
- **Place in**: `01-Raw-Data/ej/eji/`

---

## ETL Pipeline

**`02-Data-Staging/scripts/16_ej/`**:

1. `download.py` — Download EJScreen block-group data, EJI tract-level data
2. `normalize.py` — Filter to Georgia, standardize FIPS codes, align geography levels
3. `create_db.py` — Load into `ej.db` (tables: `ejscreen_block_groups`, `eji_tracts`)
4. `create_gpkg.py` — Write to `ej.gpkg` layers: `ejscreen`, `eji` (EPSG:32617)
5. `validate.py` — Block group/tract counts match Georgia totals, index ranges valid

---

## Key Metrics (weight=0 initially, cross-cutting layer)

| Metric | Type | Description |
|--------|------|-------------|
| EJScreen_EJ_Index | min_max_standard | EPA EJ index for block groups within buffer of road segment |
| EJScreen_Transport_Access | min_max_standard | Transportation access indicator from EJScreen |
| EJI_Cumulative_Rank | min_max_standard | CDC/ATSDR cumulative EJ ranking for nearby tracts |
| Is_EJ_Community | boolean | Whether segment passes through an EJ-flagged community (top 20th percentile) |

**Cross-cutting use**: EJ metrics can be applied as equity overlays across any RAPTOR category (e.g., safety in EJ communities, mobility gaps in disadvantaged areas, connectivity deserts).

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| EJScreen v2.3 (GA block groups) | EPA | Yes | ⬜ Download |
| EJI Tract-Level Data | CDC/ATSDR | Yes | ⬜ Download |

## Deliverables
- `02-Data-Staging/databases/ej.db`
- `02-Data-Staging/spatial/ej.gpkg` (layers: `ejscreen`, `eji`)
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] Georgia block group count matches Census (~5,500 block groups)
- [ ] EJScreen indices are in expected 0-100 percentile range
- [ ] EJI tracts cover all 159 Georgia counties
- [ ] High-EJ areas include known environmental justice communities (e.g., parts of South Atlanta, Augusta, Savannah industrial areas)
- [ ] No duplicate geographies after merge
