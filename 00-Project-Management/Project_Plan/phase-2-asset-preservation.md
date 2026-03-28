# Phase 2 — Asset Preservation (RAPTOR Category — Weight: 0.20)

## Goal
Download, clean, and load bridge and pavement data into the Georgia database. Build the RAPTOR `AssetPreservation` category class.

## Status: Not Started
**Depends on**: Phase 1 (Roadways base layer)

---

## Texas Comparison

> **Texas uses**: `Pavement_data.csv` (TxDOT, with Condition/Ride/Distress scores by HWY+DFO) + `Bridges.shp` (TxDOT, with sufficiency rating, vertical clearance, deck width). Both are TxDOT-specific datasets.
>
> **Georgia equivalents**:
> - **Bridges**: NBI (FHWA) — public, annual release, good field compatibility with Texas metrics
> - **Pavement**: COPACES ratings from GDOT GAMS — **requires data request from GDOT**. This is a **critical path blocker** for this phase. Fallback is FHWA HPMS pavement data (IRI, cracking, rutting) but with fewer fields than Texas.

---

## Datasets

### 2.1 NBI Bridge Data
- **Source**: FHWA National Bridge Inventory
- **URL**: `https://www.fhwa.dot.gov/bridge/nbi.cfm`
- **Filter**: State Code = 13 (Georgia), ~15,090 bridges
- **Format**: CSV/ASCII (annual release)
- **Key fields**:
  - Sufficiency Rating (Item 68, 0-100)
  - Deck Condition Rating (Item 58, 0-9)
  - Superstructure Condition Rating (Item 59, 0-9)
  - Substructure Condition Rating (Item 60, 0-9)
  - Vertical Clearance (Item 10)
  - Year Built (Item 27)
  - ADT on bridge (Item 29)
  - Latitude/Longitude (Items 16-17)
  - Route Carried (Items 5A-5E)
- **Place in**: `01-Raw-Data/bridge/`

### 2.2 COPACES Pavement Data
- **Source**: GDOT GAMS (internal)
- **Status**: Requires data-sharing agreement with GDOT
- **Format**: CSV (when available)
- **Key fields**: COPACES Rating (0-100 composite), individual distress scores
- **Fallback**: FHWA HPMS pavement data (IRI, cracking, rutting)
- **Place in**: `01-Raw-Data/pavement/`

---

## ETL Pipeline

**`02-Data-Staging/scripts/02_asset_preservation/`**:

1. `download.py` — Download NBI from FHWA, filter to Georgia
2. `normalize.py` — Clean column names, parse vertical clearance format, validate ratings
3. `create_db.py` — Load into `asset_preservation.db` (table: `bridges`)
4. `create_gpkg.py` — Write bridge points to `assets.gpkg` layer `bridges` (EPSG:32617)
5. `validate.py` — Row count, null checks, rating ranges, coordinate validity

**Config files** (`02-Data-Staging/config/`):
- `nbi_column_mapping.json` — Item numbers to readable names
- `bridge_condition_codes.json` — Condition rating definitions

---

## RAPTOR Category Class

**File**: `scripts/states/Georgia/categories/AssetPreservation.py`

**Join method**: NBI has lat/lon → spatial join to nearest roadway segment within 50m buffer. Additionally cross-reference route carried (Items 5A-5E) to RCLINK.

**Metrics**:
| Metric | Type | Weight |
|--------|------|--------|
| Bridge_Sufficiency_Rating | min_max_inverted | 0.33 |
| Is_Sufficiency_Rating_Low (<50) | boolean | 0.17 |
| Is_Vertical_Clearance_Low (<16ft) | boolean | 0.17 |
| COPACES_Rating (when available) | min_max_inverted | 0.33 |

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| NBI Bridge Data (GA) | FHWA | Yes | ⬜ Download |
| COPACES Pavement | GDOT GAMS | No | ⬜ Request from GDOT |

## Deliverables
- `02-Data-Staging/databases/asset_preservation.db`
- `02-Data-Staging/spatial/assets.gpkg` (layer: `bridges`)
- `scripts/states/Georgia/categories/AssetPreservation.py`
- Updated `Georgia_Data_Inventory_GDOT.csv`

## Verification
- [ ] NBI bridge count ~15,090 for Georgia
- [ ] Sufficiency ratings in 0-100 range
- [ ] Bridges spatially join to correct road segments
- [ ] No row duplication after merge
