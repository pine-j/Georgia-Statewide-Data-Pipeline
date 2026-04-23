# Phase 2 — Federal & Public Transportation Data

## Goal
Download, stage, and score all publicly available federal/state transportation datasets in a single pass. This phase produces metrics for four RAPTOR categories: **Connectivity** (0.15), **Freight** (0.10), **Asset Preservation** (0.20, bridges only), and **Mobility** (0.20, railroad crossings only).

## Status: Not Started
**Depends on**: Phase 1 (roadway base layer), Phase 1b (SRP derivation)

---

## Organizing Principle

This phase groups all publicly downloadable federal transportation datasets. If a dataset feeds multiple RAPTOR categories, it is downloaded and processed once here. The key shared datasets:

- **FAF5**: Energy corridor flags (Connectivity) + freight tonnage/trips scoring (Freight)
- **NTAD**: Traffic generators (Connectivity) + railroad crossings (Mobility) + intermodal facilities (Freight)
- **NBI**: Bridge sufficiency (Asset Preservation) — downloaded alongside other federal data

---

## Datasets

### Connectivity Datasets

#### 2.1 GDOT SRP Priority Routes (State Route Prioritization)
- **Source**: GDOT MapServer (Layers 13-16 at `maps.itos.uga.edu`)
- **Full name**: State Route Prioritization Network (est. 2014-2015 by GDOT Office of Transportation Data)
- **Levels**: Critical, High, Medium, Low
- **Format**: ArcGIS REST (query/download as GeoJSON)
- **Fields**: `ROUTE_ID`, `FROM_MEASURE`, `TO_MEASURE`, `DISTRICT`, `COUNTY_CODE`, `STATE_RTE_PRI`, `SRP_EDITS`
- **CRS**: EPSG:3857 (reproject to 32617)
- **Join**: ROUTE_ID + milepoint match to staged roadway inventory
- **Note**: Phase 1b produces a derived SRP from 2024 data. The official SRP is downloaded here for comparison/validation. Both `SRP_GDOT_OFFICIAL` and `SRP_DERIVED` fields will be available.
- **Place in**: `01-Raw-Data/connectivity/srp_priority_routes/`

#### 2.2 NEVI Corridors
- **Source**: GDOT ArcGIS Hub
- **Routes**: I-75, I-85, I-95, I-20, I-16, I-185, US-82, US-441, I-985/US-23, I-575/GA-515
- **Place in**: `01-Raw-Data/connectivity/`

#### 2.3 Traffic Generators (6 types, filtered to Georgia)

| Type | Source | Format | Predicate | NTAD? |
|------|--------|--------|-----------|-------|
| Airports | FAA NPIAS / BTS NTAD | Point | contains | Yes |
| Seaports | BTS NTAD Principal Ports | Point | contains | Yes |
| Universities | NCES IPEDS | Point | intersects | No |
| Military Bases | NTAD Military Installations | Polygon | intersects | Yes |
| National Parks | NPS boundaries | Polygon | intersects | No |
| Intermodal Rail | NTAD | Point | contains | Yes |

- **Place in**: `01-Raw-Data/connectivity/generators/`

#### 2.4 AFDC Alternative Fueling Stations
- **Source**: DOE Alternative Fuels Data Center
- **Format**: JSON API (developer.nrel.gov)
- **Place in**: `01-Raw-Data/connectivity/`

#### 2.5 GDOT Aviation Planning
- **Source**: GDOT
- **Use**: Context for airport traffic generators; supplement FAA NPIAS data
- **Place in**: `01-Raw-Data/connectivity/`

### Freight Datasets

#### 2.6 FAF5 Network Links + Assignment Flow Tables
- **Source**: FHWA / BTS Freight Analysis Framework v5
- **Reference implementation**: Erik Martinez's `Freight.py` (Massachusetts/Virginia, branch `_eml`)
- **Downloads**:
  - FAF5 Network Links GDB (`Freight_Analysis_Framework__FAF5__Network_Links`)
  - 12 Assignment Flow Table CSVs (6 flow types × 2 years):
    - Domestic, Export, Import, Total CU, Total SU, Total All
    - 2022 (observed) + 2050 Baseline (forecast)
  - Each CSV has Trips + Tons per link, broken down by commodity (SCTG codes)
- **Filter**: `STATE == 'GA'` for network links
- **Processing**: 
  - Join all 12 CSVs to geometry on link `ID`
  - CAGR interpolation from 2022→2050 to target year
  - Perpendicular-line spatial conflation (20 perp lines/segment, 200m) to transfer from FAF5 links to roadway inventory
  - Route-number matching for disambiguation
- **Dual use**:
  - **Freight scoring**: Total tonnage/trips per segment (all commodities)
  - **Energy corridor extraction**: Filter SCTG 15-19 (coal, crude, gasoline, fuel oils, natural gas) → flag links exceeding tonnage threshold
- **Place in**: `01-Raw-Data/freight/`

#### 2.7 Commodity Flow Survey (CFS) 2022
- **Source**: Census Bureau / BTS
- **Format**: Tables, PUMS microdata
- **Use**: Validate FAF5 estimates at Georgia subarea level
- **Place in**: `01-Raw-Data/freight/cfs/`

#### 2.8 Georgia Statewide Freight & Logistics Plan
- **Source**: GDOT
- **Format**: PDF, maps
- **Use**: Designated freight corridors, critical corridors, bottleneck analysis
- **Place in**: `01-Raw-Data/freight/ga_freight_plan/`

#### 2.9 Georgia State Rail Plan & Rail Map
- **Source**: GDOT
- **Use**: Rail network context, intermodal facility locations
- **Place in**: `01-Raw-Data/freight/rail/`

#### 2.10 ARC Freight Dashboard & Regional Truck Routes
- **Source**: Atlanta Regional Commission
- **Format**: GIS feature layer + interactive dashboard
- **Use**: Designated truck routes for Atlanta metro
- **Place in**: `01-Raw-Data/freight/arc/`

#### 2.11 Port Data
- **Sources**: USACE WCSC (port tonnage), BTS PPFS (performance), Georgia Ports Authority (monthly TEUs)
- **Use**: Port volume metrics, validate FAF5 freight at Savannah/Brunswick
- **Place in**: `01-Raw-Data/freight/ports/`

#### 2.12 Freight Generators
- **Source**: NTAD Freight Analysis Framework
- **Filter**: `STATE == 'GA'`
- **Place in**: `01-Raw-Data/connectivity/generators/`

### Asset Preservation Datasets

#### 2.13 NBI Bridge Data
- **Source**: FHWA National Bridge Inventory
- **Filter**: State Code = 13 (Georgia), ~15,090 bridges
- **Format**: CSV/ASCII (annual release)
- **Key fields**: Sufficiency Rating (0-100), Deck/Super/Sub Condition (0-9), Vertical Clearance, Year Built, ADT, Lat/Lon, Route Carried
- **Join**: Lat/lon spatial join to nearest roadway segment within 50m + route cross-reference
- **Place in**: `01-Raw-Data/bridge/`

### Mobility Datasets

#### 2.14 NTAD Railroad Grade Crossings
- **Source**: BTS / FRA National Highway-Rail Crossing Inventory
- **Filter**: `STATENAME='GEORGIA'`, `POSXING=1` (at-grade)
- **Format**: CSV with lat/lon
- **Join**: Spatial join — count crossings within 0.25-mile buffer of each segment
- **Place in**: `01-Raw-Data/mobility/`

---

## ETL Pipeline

### Downloads (`01-Raw-Data/`)

| Script | Datasets | Location |
|--------|----------|----------|
| `connectivity/scripts/download.py` | SRP, NEVI, generators (6 types), AFDC | Exists — needs SRP removed (handled by Phase 1b) |
| `connectivity/scripts/validate_endpoints.py` | Health-check all 9 endpoints | Exists |
| `freight/scripts/download_faf5.py` | FAF5 network GDB + 12 flow CSVs | New |
| `freight/scripts/download_freight_support.py` | CFS, ARC truck routes, port data | New |
| `bridge/scripts/download_nbi.py` | NBI Georgia bridges | New |
| `mobility/scripts/download_railroad_crossings.py` | NTAD railroad crossings | New |

### Staging (`02-Data-Staging/scripts/`)

| Script | Purpose | Output |
|--------|---------|--------|
| `05_connectivity/normalize.py` | Filter to GA, standardize CRS, clean attributes | Exists |
| `05_connectivity/create_db.py` | Load into `connectivity.db` | Exists |
| `05_connectivity/create_gpkg.py` | Write to `connectivity.gpkg` (10 layers) | Exists |
| `05_connectivity/validate.py` | Layer counts, geometry validity | Exists |
| `08_freight/normalize.py` | Load FAF5, join flow tables, CAGR interpolation | New |
| `08_freight/conflate.py` | Perpendicular-line conflation FAF5 → roadway | New (adapted from Erik's Freight.py) |
| `08_freight/extract_energy_corridors.py` | Filter SCTG 15-19, flag energy corridor links | New |
| `08_freight/create_db.py` | Load into `freight.db` | New |
| `08_freight/create_gpkg.py` | Write to `freight.gpkg` | New |
| `08_freight/validate.py` | Tonnage totals, corridor coverage | New |
| `02_asset_preservation/download_nbi.py` | Download + normalize NBI | New |
| `02_asset_preservation/create_db.py` | Load into `asset_preservation.db` | New |
| `02_asset_preservation/create_gpkg.py` | Write bridges to `assets.gpkg` | New |

---

## RAPTOR Category Classes

### Connectivity (Weight: 0.15)

**Files**:
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/Connectivity.py`
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/TrafficGenerators.py`
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/PriorityRoutes.py`

**Metrics** (UI allows weight override):

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| Connections_(I)_to_I,_US,_SR | min_max_inverted | 0.00 | 1-mi buffer, unique hwy names |
| Connections_(US)_to_I,_US,_SR | min_max_inverted | 0.00 | Same |
| Connections_(SR)_to_I,_US,_CR | min_max_inverted | 0.00 | Same |
| Connections_(CR)_to_I,_US,_CR | min_max_inverted | 0.00 | Same |
| Number_of_Connections | min_max_inverted | **0.25** | Total (all types) |
| Proximity_to_Major_Traffic_Generators | min_max_inverted | **0.25** | Count within 5-mi buffer |
| Density_of_Major_Traffic_Generators | min_max_inverted | 0.00 | Per square mile |
| Is_Seg_On_Hurricane_Evacuation_Route | boolean | 0.00 | Phase 1 `SEC_EVAC` |
| Is_Seg_On_NHFN | boolean | 0.00 | HPMS `nhfn` (Phase 1b) |
| Is_Seg_On_Energy_Sector_Corridor | boolean | 0.00 | FAF5 SCTG 15-19 |
| Is_Seg_On_SRP_Critical_or_High | boolean | **0.50** | Phase 1b derived SRP |

### Freight (Weight: 0.10)

**File**: `05-RAPTOR-Integration/states/Georgia/categories/Freight.py`

**Processing**: Adapted from Erik Martinez's Massachusetts Freight.py. Load FAF5 network links filtered to Georgia, join flow tables, CAGR interpolation to target year, perpendicular-line spatial conflation to roadway inventory.

**Metrics**:

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| Truck_AADT | min_max_standard | 0.25 | Phase 1 roadway inventory |
| FAF5_Total_Tonnage | min_max_standard | 0.25 | FAF5 total truck flows |
| FAF5_Total_Tonnage_2050 | min_max_standard | 0.25 | FAF5 2050 forecast |
| Is_Freight_Corridor | boolean | 0.25 | GA Freight Plan designation |
| FAF5_Total_Trips | min_max_standard | 0.00 | FAF5 total truck trips |
| FAF5_Total_Value | min_max_standard | 0.00 | FAF5 commodity value |
| Is_Designated_Truck_Route | boolean | 0.00 | ARC truck routes |
| Port_Proximity_Flag | boolean | 0.00 | Near Savannah/Brunswick |
| Num_Intermodal_Facilities | min_max_standard | 0.00 | NTAD intermodal rail |

### Asset Preservation — Bridges (Weight: 0.20, partial)

**File**: `05-RAPTOR-Integration/states/Georgia/categories/AssetPreservation.py`

**Note**: Only bridge metrics are produced in Phase 2. Pavement metrics (COPACES) are added in Phase 5 when the GDOT DSA is resolved.

**Metrics** (Phase 2):

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| Bridge_Sufficiency_Rating | min_max_inverted | 0.33 | NBI Item 68 |
| Is_Sufficiency_Rating_Low | boolean | 0.17 | NBI < 50 |
| Is_Vertical_Clearance_Low | boolean | 0.17 | NBI < 16 ft |

**Metrics** (added in Phase 5):

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| COPACES_Rating | min_max_inverted | 0.33 | GDOT GAMS |

### Mobility — Railroad Crossings (Weight: 0.20, partial)

**Note**: Only railroad crossing count is produced in Phase 2. V/C, AADT 2050, LOS, and congestion metrics come from Phase 4. LOTTR/TTTR come from Phase 5.

**Metric** (Phase 2):

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| Num_Railroad_Crossings | min_max_standard | 0.34 | NTAD at-grade crossings |

---

## Data Needed

| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| SRP Priority Routes | GDOT MapServer | Yes | ⬜ Download (comparison only) |
| NEVI Corridors | GDOT Hub | Yes | ⬜ Download |
| Airports (GA) | NTAD | Yes | ⬜ Download |
| Seaports (GA) | BTS NTAD | Yes | ⬜ Download |
| Universities (GA) | NCES IPEDS | Yes | ⬜ Download |
| Military Bases (GA) | NTAD | Yes | ⬜ Download |
| National Parks (GA) | NPS | Yes | ⬜ Download |
| Rail Facilities (GA) | NTAD | Yes | ⬜ Download |
| Freight Generators (GA) | NTAD FAF | Yes | ⬜ Download |
| AFDC Alt. Fuel Stations | DOE | Yes | ⬜ Download |
| FAF5 Network Links | FHWA | Yes | ⬜ Download |
| FAF5 Flow Tables (12 CSVs) | FHWA | Yes | ⬜ Download |
| CFS 2022 | Census/BTS | Yes | ⬜ Download |
| GA Freight & Logistics Plan | GDOT | Yes | ⬜ Download |
| GA State Rail Plan | GDOT | Yes | ⬜ Download |
| ARC Truck Routes | ARC | Yes | ⬜ Download |
| Port Tonnage (WCSC) | USACE | Yes | ⬜ Download |
| GA Ports Authority Stats | GPA | Yes | ⬜ Download |
| NBI Bridge Data (GA) | FHWA | Yes | ⬜ Download |
| Railroad Grade Crossings | NTAD/FRA | Yes | ⬜ Download |
| NHFN field | HPMS 2024 | Yes | ✅ Phase 1b |
| Evacuation routes | Phase 1 | N/A | ✅ `SEC_EVAC` |
| SRP Derived | Phase 1b | N/A | ✅ `SRP_DERIVED` |

## Deliverables
- `02-Data-Staging/databases/connectivity.db`
- `02-Data-Staging/databases/freight.db`
- `02-Data-Staging/databases/asset_preservation.db`
- `02-Data-Staging/spatial/connectivity.gpkg` (10 layers)
- `02-Data-Staging/spatial/freight.gpkg` (6 layers)
- `02-Data-Staging/spatial/assets.gpkg` (layer: `bridges`)
- `02-Data-Staging/spatial/mobility.gpkg` (layer: `railroad_crossings`)
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/` (3 files)
- `05-RAPTOR-Integration/states/Georgia/categories/Freight.py`
- `05-RAPTOR-Integration/states/Georgia/categories/AssetPreservation.py` (bridges only)
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] SRP official routes include major interstates
- [ ] Hartsfield-Jackson shows up as generator for nearby segments
- [ ] Port of Savannah shows up for I-16/I-95 segments
- [ ] Generator counts are plausible per segment
- [ ] NHFN flag populated on I-75, I-95, I-16, I-20
- [ ] Energy corridor flag covers Port of Savannah access routes
- [ ] FAF5 tonnage highest on I-75, I-16 (Savannah corridor)
- [ ] FAF5 Georgia total tonnage plausible vs published FAF5 state totals
- [ ] Freight corridors cover I-75, I-85, I-16, I-95, I-20
- [ ] Intermodal terminals include Savannah, Cordele, Atlanta facilities
- [ ] NBI bridge count ~15,090 for Georgia
- [ ] Sufficiency ratings in 0-100 range
- [ ] Railroad crossing count for Georgia is plausible
- [ ] No row duplication after merges
- [ ] All Connectivity, Freight, bridge, and railroad metrics present in RAPTOR output
