# Phase 13 — Transit

## Goal
Download, clean, and load transit data (GTFS feeds, ridership, regional plans) into the Georgia database. Potential future RAPTOR category for multimodal connectivity scoring.

## Status: Not Started (Post-RAPTOR)
**Depends on**: Phase 1 (base layer for spatial joins)

---

## Datasets

### 13.1 MARTA GTFS Feed
- **Source**: MARTA
- **URL**: `https://itsmarta.com/app-developer-resources.aspx`
- **Format**: Download (GTFS zip)
- **Key fields**: Routes, stops, stop_times, trips, shapes for Metropolitan Atlanta Rapid Transit Authority
- **Coverage**: Georgia (current schedule), updated per service change
- **Place in**: `01-Raw-Data/transit/gtfs/marta/`

### 13.2 ARC Regional GTFS Feeds (Consolidated)
- **Source**: Atlanta Regional Commission (ARC)
- **URL**: `https://opendata.atlantaregional.com/datasets/61dca73994b746ac97eae8c0f83fdedc`
- **Format**: Download (GTFS)
- **Key fields**: Consolidated GTFS for all fixed-route transit in the Atlanta region
- **Coverage**: Georgia (current schedule), updated per service change
- **Place in**: `01-Raw-Data/transit/gtfs/arc_regional/`

### 13.3 GRTA Xpress GTFS Feed
- **Source**: SRTA / GRTA
- **Format**: GTFS (ZIP) via ARC Regional GTFS & Transitland
- **Key fields**: Commuter bus between 12 Atlanta-region counties and major employment centers
- **Place in**: `01-Raw-Data/transit/gtfs/grta/`

### 13.4 CobbLinc GTFS Feed
- **Source**: Cobb County DOT
- **Format**: GTFS (ZIP) via ARC Regional GTFS & Transitland
- **Key fields**: Local and commuter bus routes in Cobb County with Atlanta connections
- **Place in**: `01-Raw-Data/transit/gtfs/cobblinc/`

### 13.5 Gwinnett County Transit (Ride Gwinnett) GTFS
- **Source**: Gwinnett County Transit
- **Format**: GTFS (ZIP) via ARC Regional GTFS & Transitland
- **Key fields**: Local and commuter bus within Gwinnett County and connections to Atlanta
- **Place in**: `01-Raw-Data/transit/gtfs/gwinnett/`

### 13.6 Chatham Area Transit (CAT) GTFS Feed
- **Source**: Chatham Area Transit
- **URL**: `https://www.transit.land/feeds/f-djwq-chathamareatransit`
- **Format**: GTFS (ZIP)
- **Key fields**: Bus routes and schedules for Savannah metro area
- **Place in**: `01-Raw-Data/transit/gtfs/cat_savannah/`

### 13.7 Transitland — Georgia Transit Feeds
- **Source**: MobilityData / Transitland
- **URL**: `https://www.transit.land/feeds`
- **Format**: GTFS (ZIP); API
- **Key fields**: Aggregated GTFS feeds from all Georgia transit operators; archived versions
- **Use**: Discovery and download of all GA transit feeds; historical schedule archives

### 13.8 GDOT Regional Transit Development Plans
- **Source**: Georgia DOT (GDOT)
- **URL**: `https://regionaltdp-gdot.hub.arcgis.com/search`
- **Format**: Interactive hub; downloads
- **Key fields**: Statewide regional transit development plan data and maps
- **Use**: Planning context for transit corridors and future service expansion

### 13.9 National Transit Database (NTD)
- **Source**: FTA
- **URL**: `https://www.transit.dot.gov/ntd/ntd-data`
- **Format**: Excel; CSV
- **Key fields**: Ridership, operating stats, financials, asset condition for all GA transit agencies
- **Coverage**: U.S., 2002-2024, annual updates
- **Use**: Ridership and performance data for Georgia transit agencies; agency-level metrics

---

## ETL Pipeline

**`02-Data-Staging/scripts/13_transit/`**:

1. `download.py` — Download all GTFS feeds, NTD data; use Transitland API for discovery
2. `parse_gtfs.py` — Parse GTFS into stops, routes, shapes tables; compute route-level metrics (headways, span of service)
3. `normalize.py` — Standardize agency names, route types, CRS; merge multi-agency stops
4. `create_db.py` — Load into `transit.db` (tables: `stops`, `routes`, `stop_times`, `ntd_ridership`, `ntd_financials`)
5. `create_gpkg.py` — Write to `transit.gpkg` layers: `transit_stops`, `transit_routes`, `transit_service_areas` (EPSG:32617)
6. `validate.py` — Stop counts per agency, route geometry validity, schedule consistency checks

---

## Key Metrics (weight=0 initially, available for future RAPTOR category)

| Metric | Type | Description |
|--------|------|-------------|
| Transit_Stop_Density | min_max_standard | Number of transit stops within 0.5-mile buffer of road segment |
| Transit_Route_Count | min_max_standard | Number of unique transit routes serving the corridor |
| Headway_Minutes | min_max_inverted | Average weekday peak headway for nearest transit service |
| Annual_Ridership_Nearby | min_max_standard | NTD annual ridership for agencies serving the corridor |

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| MARTA GTFS | MARTA | Yes | ⬜ Download |
| ARC Regional GTFS | ARC | Yes | ⬜ Download |
| GRTA Xpress GTFS | SRTA/GRTA | Yes | ⬜ Download |
| CobbLinc GTFS | Cobb County DOT | Yes | ⬜ Download |
| Gwinnett Transit GTFS | Gwinnett County | Yes | ⬜ Download |
| CAT (Savannah) GTFS | CAT | Yes | ⬜ Download |
| Transitland Feed Index | MobilityData | Yes | ⬜ Explore |
| GDOT Regional Transit Plans | GDOT | Yes | ⬜ Explore |
| NTD Ridership/Financials | FTA | Yes | ⬜ Download |

## Deliverables
- `02-Data-Staging/databases/transit.db`
- `02-Data-Staging/spatial/transit.gpkg` (layers: `transit_stops`, `transit_routes`, `transit_service_areas`)
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] MARTA stop count is plausible (~5,000+ bus stops + 38 rail stations)
- [ ] All GTFS feeds parse without errors
- [ ] Transit routes render correctly on map
- [ ] NTD ridership totals match published MARTA/agency reports
- [ ] No duplicate stops across consolidated feeds
- [ ] Savannah (CAT) stops are geographically separate from Atlanta agencies
