# Phase 2 — Connectivity (RAPTOR Category — Weight: 0.15)

## Goal
Download, clean, and load priority route and traffic generator data into the Georgia database. Build the RAPTOR `Connectivity` category class.

## Status: Not Started
**Depends on**: Phase 1 (Roadways base layer), **Phase 1b** (SRP derivation — NHFN enrichment, GRIP corridors, nuclear EPZ, sole county-seat connections, derived SRP classification)

See [phase-1b-srp-derivation.md](phase-1b-srp-derivation.md) for the pre-requisite gap-fill plan.

---

## Texas Comparison

> **Texas uses**: Energy Sector Corridors, Trade Corridors, 7 traffic generator types (airports, seaports, universities, national parks, border crossings, freight generators, intermodal rail), evacuation route flag (`SEC_EVAC`), NHFN flag, trunk system flag (`SEC_TRUNK`), and SLRTP project data. Degrees of Connection: count unique highway names within 1-mile buffer.
>
> **Georgia differences**:
> - **Energy Sector Corridors**: Georgia has significant energy infrastructure — Port of Savannah (energy imports), Colonial Pipeline terminus (Atlanta), Savannah River Site (nuclear). Use GA Freight & Logistics Plan + FAF5 commodity data to designate energy corridors.
> - **Trade Corridors**: No GDOT equivalent found — investigate Georgia Freight & Logistics Plan for designated freight corridors
> - **Border Crossings**: Not relevant to Georgia (no international border)
> - **Traffic Generators**: 6 of 7 Texas types have Georgia equivalents (all except border crossings)
> - **Evacuation routes**: ✅ Already implemented in Phase 1 — `SEC_EVAC`, `SEC_EVAC_CONTRAFLOW` fields via `evacuation_enrichment.py`
> - **NHFN**: HPMS 2024 has `nhfn` field — needs to be added to `HPMS_GAP_FILL_FIELDS` in `hpms_enrichment.py` (pre-requisite task)
> - **NEVI EV Corridors**: Georgia-specific addition not in Texas pipeline
> - **SLRTP → GDOT Projects Portal**: Available via ArcGIS Hub

---

## Datasets

### 2.1 GDOT SRP Priority Routes
- **Source**: GDOT MapServer (Layers 13-16 at `maps.itos.uga.edu`)
- **Levels**: Critical, High, Medium, Low priority
- **Format**: GIS service (query/download)
- **Join**: ROUTE_ID + milepoint match
- **Place in**: `01-Raw-Data/connectivity/`
- **TODO**: Investigate what "SRP" stands for (Strategic Route Plan?), confirm acronym with GDOT documentation. Determine if GDOT publishes versioned release dates for the priority route layers and whether more recent data is available beyond what the MapServer currently serves.

### 2.2 NEVI Corridors
- **Source**: GDOT ArcGIS Hub
- **URL**: `https://nevi-gdot.hub.arcgis.com/`
- **Routes**: I-75, I-85, I-95, I-20, I-16, I-185, US-82, US-441, I-985/US-23, I-575/GA-515
- **Place in**: `01-Raw-Data/connectivity/`

### 2.3 Traffic Generators (7 types, all filtered to Georgia)

| Type | Source | Format | Predicate |
|------|--------|--------|-----------|
| Airports | FAA NPIAS / BTS NTAD | Point shapefile | contains |
| Seaports | BTS + Georgia Ports Authority | Point | contains |
| Universities | NCES IPEDS / Board of Regents | Point/Polygon | intersects |
| Military Bases | HIFLD | Polygon | intersects |
| National Parks | NPS boundaries | Polygon | intersects |
| Intermodal Rail | NTAD | Point | contains |
| Freight Generators | FAF5 / GA Freight Plan | Point | contains |

- **Place in**: `01-Raw-Data/connectivity/generators/`

### 2.4 AFDC Alternative Fueling Station Locator
- **Source**: DOE Alternative Fuels Data Center
- **URL**: `https://afdc.energy.gov/corridors`
- **Format**: Download (CSV, GeoJSON, SHP)
- **Key fields**: EV charging and alternative fuel stations along designated corridors
- **Coverage**: U.S. (current snapshot), continuous updates
- **Use**: Supplement NEVI corridor data with actual station locations; gap analysis

### 2.5 GDOT Aviation Planning
- **Source**: Georgia DOT (GDOT)
- **URL**: `https://www.dot.ga.gov/GDOT/pages/AviationPlanning.aspx`
- **Format**: Reports; plans
- **Key fields**: Georgia Statewide Aviation System Plan (GSASP) and airport data
- **Use**: Context for airport traffic generators; supplement FAA NPIAS data

### 2.6 Energy Sector Corridors
- **Source**: Georgia Statewide Freight & Logistics Plan (2023) + FAF5 commodity flow data
- **Approach**: Identify highway segments carrying significant energy-related commodity tonnage
- **Key infrastructure**: Port of Savannah (energy imports), Colonial Pipeline terminus (Atlanta area), Savannah River Site (nuclear), power plant access routes
- **Primary corridors (expected)**: I-75, I-95, I-16 (port access), I-20 (Atlanta distribution)
- **Format**: CSV of designated corridor segments (highway name + milepoint ranges), matching Texas format
- **Place in**: `01-Raw-Data/connectivity/`
- **Note**: Overlaps with Phase 14 (Freight) — FAF5 data may be shared. Energy corridors are pulled forward into Phase 2 as a boolean flag; detailed commodity analysis remains in Phase 14.

### 2.7 NHFN (National Highway Freight Network)
- **Source**: Already in HPMS 2024 submission (`nhfn` field) — no separate download needed
- **Enrichment**: Add `nhfn` and `strahnet_type` to `HPMS_GAP_FILL_FIELDS` in `hpms_enrichment.py`
- **Field values**: NHFN designation codes per FHWA
- **Note**: This is a Phase 1 staging update (pre-requisite), not a new download

---

## ETL Pipeline

**`01-Raw-Data/connectivity/scripts/`**:

1. `download.py` — Download SRP, NEVI, all generator shapefiles

**`02-Data-Staging/scripts/05_connectivity/`**:

1. `normalize.py` — Filter to Georgia, standardize CRS, clean attributes
2. `create_db.py` — Load tabular attributes into `connectivity.db`
3. `create_gpkg.py` — Write to `connectivity.gpkg` layers: `priority_routes`, `nevi_corridors`, `alt_fuel_stations`, `airports`, `seaports`, `universities`, `military_bases`, `national_parks`, `rail_facilities`, `freight_generators` (all EPSG:32617)
4. `validate.py` — Layer counts, geometry validity, known feature spot checks

---

## RAPTOR Category Class

**Files**:
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/Connectivity.py`
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/TrafficGenerators.py`
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/PriorityRoutes.py`

**Degrees of Connection**: 1-mile buffer, count unique intersecting highway names (same as Texas).

**Traffic Generators**: Count within 5-mile buffer per segment.

**Metrics** (expanded to match Texas field coverage — UI allows weight override):

| Metric | Type | Default Weight | Notes |
|--------|------|----------------|-------|
| Connections_(I)_to_I,_US,_SR | min_max_inverted | 0.00 | Interstate segments |
| Connections_(US)_to_I,_US,_SR | min_max_inverted | 0.00 | US Route segments |
| Connections_(SR)_to_I,_US,_CR | min_max_inverted | 0.00 | State Route segments |
| Connections_(CR)_to_I,_US,_CR | min_max_inverted | 0.00 | County Road segments |
| Number_of_Connections | min_max_inverted | **0.25** | Total (all types combined) |
| Proximity_to_Major_Traffic_Generators | min_max_inverted | **0.25** | Count within 5-mi buffer |
| Density_of_Major_Traffic_Generators | min_max_inverted | 0.00 | Per square mile |
| Is_Seg_On_Hurricane_Evacuation_Route | boolean | 0.00 | From Phase 1 `SEC_EVAC` |
| Is_Seg_On_NHFN | boolean | 0.00 | From HPMS `nhfn` field |
| Is_Seg_On_Energy_Sector_Corridor | boolean | 0.00 | From GA Freight Plan + FAF5 |
| Is_Seg_On_SRP_Critical_or_High | boolean | **0.50** | GA analog to TX SLRTP |

**Active weights**: 0.25 + 0.25 + 0.50 = 1.0
**Inactive (0.00)**: Available for analyst override via RAPTOR WebApp UI

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| SRP Priority Routes | GDOT MapServer | Yes | ⬜ Query — confirm acronym, check for latest release |
| NEVI Corridors | GDOT Hub | Yes | ⬜ Download |
| Airports (GA) | NTAD | Yes | ⬜ Download |
| Seaports (GA) | BTS + GPA | Yes | ⬜ Download |
| Universities (GA) | NCES IPEDS | Yes | ⬜ Download |
| Military Bases (GA) | HIFLD | Yes | ⬜ Download |
| National Parks (GA) | NPS | Yes | ⬜ Download |
| Rail Facilities (GA) | NTAD | Yes | ⬜ Download |
| Freight Generators (GA) | FAF5 | Yes | ⬜ Download |
| AFDC Alt. Fuel Stations | DOE AFDC | Yes | ⬜ Download |
| GDOT Aviation Planning | GDOT | Yes | ⬜ Download |
| Energy Sector Corridors | GA Freight Plan + FAF5 | Yes | ⬜ Research + build corridor CSV |
| NHFN field | HPMS 2024 | Yes | ✅ Already downloaded — needs `hpms_enrichment.py` update |
| Evacuation routes | Phase 1 staging | N/A | ✅ Already in `SEC_EVAC` / `SEC_EVAC_CONTRAFLOW` |

## Deliverables
- `02-Data-Staging/databases/connectivity.db`
- `02-Data-Staging/spatial/connectivity.gpkg` (9 layers)
- `05-RAPTOR-Integration/states/Georgia/categories/Connectivity/` (3 files)
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] SRP Critical routes include major interstates
- [ ] Hartsfield-Jackson shows up as generator for nearby segments
- [ ] Port of Savannah shows up for I-16/I-95 segments
- [ ] Generator counts are plausible per segment
- [ ] No row duplication after merges
- [ ] NHFN flag populated on expected freight corridors (I-75, I-95, I-16, I-20)
- [ ] Energy corridor flag covers Port of Savannah access routes
- [ ] Evacuation flag carried through from Phase 1 without data loss
- [ ] All 11 metrics present in RAPTOR output (active + inactive)
