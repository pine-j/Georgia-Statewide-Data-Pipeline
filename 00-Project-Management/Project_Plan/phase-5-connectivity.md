# Phase 5 — Connectivity (RAPTOR Category — Weight: 0.15)

## Goal
Download, clean, and load priority route and traffic generator data into the Georgia database. Build the RAPTOR `Connectivity` category class.

## Status: Not Started
**Depends on**: Phase 1 (Roadways base layer)

---

## Texas Comparison

> **Texas uses**: Energy Sector Corridors, Trade Corridors, 7 traffic generator types (airports, seaports, universities, national parks, border crossings, freight generators, intermodal rail), evacuation route flag (`SEC_EVAC`), NHFN flag, trunk system flag (`SEC_TRUNK`), and SLRTP project data. Degrees of Connection: count unique highway names within 1-mile buffer.
>
> **Georgia differences**:
> - **Energy Sector Corridors**: Not relevant to Georgia — no equivalent needed
> - **Trade Corridors**: No GDOT equivalent found — investigate Georgia Freight & Logistics Plan for designated freight corridors
> - **Border Crossings**: Not relevant to Georgia (no international border)
> - **Traffic Generators**: 6 of 7 Texas types have Georgia equivalents (all except border crossings)
> - **Evacuation routes**: Georgia is hurricane-prone — need to verify if GDOT GDB has an evacuation flag
> - **NHFN**: Georgia has `NHS` flag; need to check for freight network designation
> - **NEVI EV Corridors**: Georgia-specific addition not in Texas pipeline
> - **SLRTP → GDOT Projects Portal**: Available via ArcGIS Hub

---

## Datasets

### 5.1 GDOT SRP Priority Routes
- **Source**: GDOT MapServer (Layers 13-16 at `maps.itos.uga.edu`)
- **Levels**: Critical, High, Medium, Low priority
- **Format**: GIS service (query/download)
- **Join**: ROUTE_ID + milepoint match
- **Place in**: `01-Raw-Data/connectivity/`

### 5.2 NEVI Corridors
- **Source**: GDOT ArcGIS Hub
- **URL**: `https://nevi-gdot.hub.arcgis.com/`
- **Routes**: I-75, I-85, I-95, I-20, I-16, I-185, US-82, US-441, I-985/US-23, I-575/GA-515
- **Place in**: `01-Raw-Data/connectivity/`

### 5.3 Traffic Generators (7 types, all filtered to Georgia)

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

### 5.4 AFDC Alternative Fueling Station Locator
- **Source**: DOE Alternative Fuels Data Center
- **URL**: `https://afdc.energy.gov/corridors`
- **Format**: Download (CSV, GeoJSON, SHP)
- **Key fields**: EV charging and alternative fuel stations along designated corridors
- **Coverage**: U.S. (current snapshot), continuous updates
- **Use**: Supplement NEVI corridor data with actual station locations; gap analysis

### 5.5 GDOT Aviation Planning
- **Source**: Georgia DOT (GDOT)
- **URL**: `https://www.dot.ga.gov/GDOT/pages/AviationPlanning.aspx`
- **Format**: Reports; plans
- **Key fields**: Georgia Statewide Aviation System Plan (GSASP) and airport data
- **Use**: Context for airport traffic generators; supplement FAA NPIAS data

---

## ETL Pipeline

**`02-Data-Staging/scripts/05_connectivity/`**:

1. `download.py` — Download SRP, NEVI, all generator shapefiles
2. `normalize.py` — Filter to Georgia, standardize CRS, clean attributes
3. `create_db.py` — Load tabular attributes into `connectivity.db`
4. `create_gpkg.py` — Write to `connectivity.gpkg` layers: `priority_routes`, `nevi_corridors`, `alt_fuel_stations`, `airports`, `seaports`, `universities`, `military_bases`, `national_parks`, `rail_facilities`, `freight_generators` (all EPSG:32617)
5. `validate.py` — Layer counts, geometry validity, known feature spot checks

---

## RAPTOR Category Class

**Files**:
- `scripts/states/Georgia/categories/Connectivity/Connectivity.py`
- `scripts/states/Georgia/categories/Connectivity/TrafficGenerators.py`
- `scripts/states/Georgia/categories/Connectivity/PriorityRoutes.py`

**Degrees of Connection**: 1-mile buffer, count unique intersecting highway names (same as Texas).

**Traffic Generators**: Count within 5-mile buffer per segment.

**Metrics**:
| Metric | Type | Weight |
|--------|------|--------|
| Degrees_of_Connection_Count | min_max_inverted | 0.25 |
| Total_Traffic_Generators | min_max_inverted | 0.25 |
| Is_SRP_Critical_or_High | boolean | 0.50 |

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| SRP Priority Routes | GDOT MapServer | Yes | ⬜ Query |
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

## Deliverables
- `02-Data-Staging/databases/connectivity.db`
- `02-Data-Staging/spatial/connectivity.gpkg` (9 layers)
- `scripts/states/Georgia/categories/Connectivity/` (3 files)
- Updated `Georgia_Data_Inventory_GDOT.csv`

## Verification
- [ ] SRP Critical routes include major interstates
- [ ] Hartsfield-Jackson shows up as generator for nearby segments
- [ ] Port of Savannah shows up for I-16/I-95 segments
- [ ] Generator counts are plausible per segment
- [ ] No row duplication after merges
