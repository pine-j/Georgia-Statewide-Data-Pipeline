# Phase 14 — Freight & Logistics

## Goal
Download, clean, and load freight data into the Georgia database. Build the RAPTOR `Freight` category class. Georgia's freight significance (Savannah = 3rd busiest U.S. container port) makes this a high-priority category.

## Status: Not Started
**Depends on**: Phase 1 (base layer)

---

## Datasets

### 14.1 Freight Analysis Framework (FAF5)
- **Source**: FHWA / BTS
- **URL**: `https://www.bts.gov/faf`
- **Format**: Download (CSV, Access)
- **Key fields**: Freight flows by state/metro, 42 commodities, all modes; includes projections to 2050
- **Coverage**: U.S., 2012-2024, annual updates
- **Use**: Primary source for commodity tonnage/value by corridor; 2050 projections for future demand
- **Place in**: `01-Raw-Data/freight/faf5/`

### 14.2 Commodity Flow Survey (CFS) 2022
- **Source**: Census Bureau / BTS
- **URL**: `https://www.bts.gov/cfs`
- **Format**: Tables; PUMS microdata
- **Key fields**: Commodity type, value, weight, origin/destination, mode; Georgia subarea estimates
- **Coverage**: U.S., every 5 years (1993-2022)
- **Use**: Detailed commodity flows at Georgia subarea level; validates FAF5 estimates
- **Place in**: `01-Raw-Data/freight/cfs/`

### 14.3 NTAD (National Transportation Atlas Database) — Multimodal
- **Source**: BTS
- **URL**: `https://www.bts.gov/ntad`
- **Format**: Download (GDB, SHP, CSV, GeoJSON)
- **Key fields**: ~90 datasets: railroad crossings, intermodal terminals, freight facilities, ports, airports
- **Coverage**: U.S., varies by layer, quarterly updates
- **Use**: Intermodal terminal locations; freight facility inventory
- **Place in**: `01-Raw-Data/freight/ntad/`

### 14.4 Georgia State Rail Plan & Rail Map
- **Source**: Georgia DOT (GDOT)
- **URL**: `https://www.dot.ga.gov/GDOT/pages/Rail.aspx`
- **Format**: PDF reports; GIS data
- **Key fields**: 28 freight railroads (CSX, Norfolk Southern + short lines); intermodal facilities
- **Coverage**: Georgia, 2015 (periodic updates)
- **Use**: Rail network context; intermodal facility locations for freight scoring
- **Place in**: `01-Raw-Data/freight/rail/`

### 14.5 Georgia Statewide Freight and Logistics Plan
- **Source**: Georgia DOT
- **URL**: `https://www.dot.ga.gov/GDOT/pages/freight.aspx`
- **Format**: PDF; maps
- **Key fields**: 2023 BIL-compliant plan; freight corridors, critical corridors, bottleneck analysis
- **Coverage**: Georgia, 2023
- **Use**: Designated freight corridors and bottlenecks; critical urban/rural freight corridors
- **Place in**: `01-Raw-Data/freight/ga_freight_plan/`

### 14.6 ARC Freight Dashboard & Regional Truck Routes
- **Source**: Atlanta Regional Commission (ARC)
- **URL**: `https://opendata.atlantaregional.com/maps/28b5c124dc3f4a8e8449c999b6e1ad74`
- **Format**: Interactive dashboard; downloads
- **Key fields**: Truck routes, freight clusters, rail networks, EJ scores for Atlanta region (2024 ARFMP)
- **Place in**: `01-Raw-Data/freight/arc/`

### 14.7 ARC Regional Truck Routes
- **Source**: Atlanta Regional Commission
- **URL**: `https://data-hub.gio.georgia.gov/datasets/GARC::regional-truck-routes`
- **Format**: GIS feature layer
- **Key fields**: Designated truck routes for the Atlanta metropolitan region
- **Place in**: `01-Raw-Data/freight/arc/`

### 14.8 Waterborne Commerce Statistics (WCSC)
- **Source**: USACE
- **URL**: `https://www.iwr.usace.army.mil/About/Technical-Centers/WCSC-Waterborne-Commerce-Statistics-Center/WCSC-Waterborne-Commerce/`
- **Format**: Data portal; tools & downloads
- **Key fields**: Port tonnage, vessel movements for Georgia ports (Savannah, Brunswick)
- **Coverage**: U.S., 2001-2023, annual
- **Place in**: `01-Raw-Data/freight/ports/`

### 14.9 Port Performance Freight Statistics (PPFS)
- **Source**: USDOT Bureau of Transportation Statistics
- **URL**: `https://www.bts.gov/ports`
- **Format**: Dashboards & annual reports
- **Key fields**: Performance indicators; filter to Georgia ports
- **Use**: Port efficiency metrics for freight corridor analysis

### 14.10 Georgia Ports Authority Statistics
- **Source**: Georgia Ports Authority
- **URL**: `https://gaports.com/sales/by-the-numbers/`
- **Format**: Web reports; PDF
- **Key fields**: Monthly/annual TEUs, RoRo cargo, vessel calls, rail lifts for Savannah & Brunswick
- **Use**: Most current port volume data; validates WCSC and FAF5
- **Place in**: `01-Raw-Data/freight/ports/`

---

## ETL Pipeline

**`02-Data-Staging/scripts/14_freight/`**:

1. `download.py` — Download FAF5, CFS, NTAD freight layers, WCSC port data
2. `download_truck_routes.py` — Download ARC truck routes, GA freight corridors
3. `normalize.py` — Filter to Georgia flows/facilities, standardize CRS, clean attributes
4. `create_db.py` — Load into `freight.db` (tables: `faf5_flows`, `cfs_flows`, `intermodal_terminals`, `port_tonnage`, `freight_corridors`, `truck_routes`)
5. `create_gpkg.py` — Write to `freight.gpkg` layers: `freight_corridors`, `truck_routes`, `intermodal_terminals`, `port_facilities`, `rail_network` (all EPSG:32617)
6. `validate.py` — FAF5 flow totals match published, port tonnage reasonable, corridor coverage

---

## RAPTOR Category Class

**File**: `scripts/states/Georgia/categories/Freight.py`

**Processing**:
1. Load Truck AADT from roadway inventory (already in Phase 1 via TRUCK_PCT * AADT)
2. Load FAF5 commodity flows — assign tonnage/value to road segments via FAF5 network
3. Load freight corridor designations from GA Freight Plan
4. Spatial join: intermodal terminals and port facilities within buffer of segments
5. Flag designated truck routes

**Metrics**:
| Metric | Type | Weight |
|--------|------|--------|
| Truck_AADT | min_max_standard | 0.25 |
| FAF5_Tonnage | min_max_standard | 0.25 |
| FAF5_Value | min_max_standard | 0.00 |
| Is_Freight_Corridor | boolean | 0.25 |
| Is_Designated_Truck_Route | boolean | 0.00 |
| Num_Intermodal_Facilities | min_max_standard | 0.25 |
| Port_Proximity_Flag | boolean | 0.00 |

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| FAF5 Freight Flows | FHWA / BTS | Yes | ⬜ Download |
| CFS 2022 | Census / BTS | Yes | ⬜ Download |
| NTAD Freight Layers | BTS | Yes | ⬜ Download |
| GA State Rail Plan | GDOT | Yes | ⬜ Download |
| GA Freight & Logistics Plan | GDOT | Yes | ⬜ Download |
| ARC Freight Dashboard | ARC | Yes | ⬜ Download |
| ARC Regional Truck Routes | ARC | Yes | ⬜ Download |
| WCSC Port Tonnage | USACE | Yes | ⬜ Download |
| Port Performance Stats | BTS | Yes | ⬜ Download |
| GA Ports Authority Stats | GPA | Yes | ⬜ Download |

## Deliverables
- `02-Data-Staging/databases/freight.db`
- `02-Data-Staging/spatial/freight.gpkg` (5 layers)
- `scripts/states/Georgia/categories/Freight.py`
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] FAF5 Georgia origin/destination flows are plausible vs published totals
- [ ] Truck AADT on I-75, I-16 (Savannah corridor) is among highest
- [ ] Port of Savannah tonnage matches GPA published figures
- [ ] Freight corridors cover I-75, I-85, I-16, I-95, I-20
- [ ] Intermodal terminals include Savannah, Cordele, Atlanta facilities
- [ ] No row duplication after merges
