# Phase 4 — Mobility (RAPTOR Category — Weight: 0.20)

## Goal
Download, clean, and load traffic and railroad crossing data into the Georgia database. Build the RAPTOR `Mobility` category class.

## Status: Not Started
**Depends on**: Phase 1 (Roadways base layer)

---

## Texas Comparison

> **Texas uses**: Current AADT (`ADT_CUR`), design AADT (`AADT_DESGN`) for 2050 projection, V/C ratio from HCM formula, NTAD railroad crossings, SAM congestion model output (Fort Worth, current + 2050), and Top 100 Congested Roadways list.
>
> **Georgia gaps**:
> - **No design AADT** (`AADT_DESGN`): Texas uses this for exponential 2050 projection. Georgia must compute growth rates from historic AADT trend data instead. See "Historic AADT Sources" below.
> - **No congestion model output**: Texas uses SAM (Fort Worth travel demand model) for LOS/V/C. Georgia has GSTDM but output is not publicly available. Workaround: compute V/C from HCM formula + use NPMRDS for reliability.
> - **No Top 100 congested list**: Workaround: derive from computed V/C ratios or NPMRDS congestion ranking.
>
> **Georgia advantage**: NPMRDS travel time reliability (LOTTR, TTTR) — not currently used by Texas RAPTOR. Georgia plan already includes these metrics.

---

## Datasets

### 4.1 HPMS Georgia
- **Source**: FHWA Highway Performance Monitoring System
- **URL**: `https://catalog.data.gov/dataset/highway-performance-monitoring-system-hpms-geospatial-data`
- **Format**: Shapefile (national, filter to Georgia)
- **Key fields**: AADT, K-factor, D-factor, truck percent, speed limit, capacity, functional class
- **Place in**: `01-Raw-Data/mobility/`

### 4.2 GDOT Traffic Counts
- **Source**: GDOT TADA application
- **URL**: `https://gdottrafficdata.drakewell.com/`
- **Format**: CSV (historical counts by station)
- **Place in**: `01-Raw-Data/mobility/`

### 4.3 NTAD Railroad Grade Crossings
- **Source**: BTS / FRA National Highway-Rail Crossing Inventory
- **URL**: `https://geodata.bts.gov/`
- **Filter**: STATENAME='GEORGIA', POSXING=1 (at-grade)
- **Format**: CSV with lat/lon
- **Place in**: `01-Raw-Data/mobility/`

### 4.4 NPMRDS (National Performance Management Research Data Set)
- **Source**: FHWA / RITIS
- **URL**: `https://npmrds.ritis.org/analytics/`
- **Format**: Download (CSV); requires Data Sharing Agreement
- **Key fields**: Probe-based travel times at 5-min intervals on 400K+ NHS segments; supports LOTTR, TTTR measures
- **Coverage**: 2017-present, continuous (weekly downloads)
- **Use**: Primary source for travel time reliability metrics (LOTTR, TTTR); congestion validation against V/C
- **Place in**: `01-Raw-Data/mobility/npmrds/`

### 4.5 ARC Historical Traffic Counts (2008-2017)
- **Source**: Atlanta Regional Commission / GDOT
- **URL**: `https://opendata.atlantaregional.com/datasets/c9ce7fe9c5f94f338422e4d5c7119158_0`
- **Format**: Download (shapefile)
- **Key fields**: Historical AADT and truck percentage data
- **Coverage**: 2008-2017 (static/archived)
- **Use**: Historical trend analysis for Metro Atlanta; supplement TADA counts
- **Place in**: `01-Raw-Data/mobility/`

### 4.8 GDOT Traffic_Historical.zip (Statewide)
- **Source**: GDOT file server
- **URL**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/Traffic_Historical.zip`
- **Format**: ~591 MB zip (contents unknown — need to download and inspect)
- **Coverage**: Statewide, years TBD
- **Use**: Primary source for statewide historic AADT trend analysis; needed for 2050 growth projection workaround
- **Place in**: `01-Raw-Data/mobility/`

### 4.9 GDOT 2010-2019 Published Traffic
- **Source**: GDOT file server
- **URL**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/2010_thr_2019_Published_Traffic.zip`
- **Format**: ~516 MB zip (contents unknown — need to download and inspect)
- **Coverage**: 2010-2019, statewide
- **Use**: Decade of published traffic counts; supplement Traffic_Historical for trend analysis
- **Place in**: `01-Raw-Data/mobility/`

### 4.10 HPMS Georgia Shapefiles (Yearly, 2011-2023)
- **Source**: FHWA HPMS
- **URL**: `https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles.cfm`
- **Format**: Shapefiles by year (georgia2011.zip through georgia2017.zip; 2018+ via FeatureServer/USDOT Open Data)
- **Coverage**: 2011-2023, Federal-Aid roads only (subset of full inventory)
- **Use**: Yearly AADT snapshots for growth rate computation; fallback if GDOT historical traffic files are insufficient
- **Place in**: `01-Raw-Data/mobility/hpms/`

### 4.6 Georgia Traffic Monitoring Program Guide
- **Source**: Georgia DOT
- **URL**: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/2025_Georgia_Traffic_Monitoring_Program.pdf`
- **Format**: PDF
- **Key fields**: ~35 Continuous Count Stations, WIM sites, short-term portable counts across ~18,000 mi
- **Coverage**: 2025
- **Use**: Reference for count station locations and methodology; WIM data for truck factors

### 4.7 GEOCOUNTS — Georgia Traffic Counts
- **Source**: GEOCOUNTS
- **URL**: `https://geocounts.com/gdot/`
- **Format**: Interactive web app
- **Key fields**: Third-party interface for exploring Georgia DOT traffic count data
- **Use**: Alternative interface for GDOT count data; cross-reference with TADA

---

## ETL Pipeline

**`02-Data-Staging/scripts/04_mobility/`**:

1. `download.py` — Download HPMS, GDOT traffic counts, NTAD railroad crossings, NPMRDS (requires DSA)
2. `normalize.py` — Filter to Georgia, standardize columns, validate coordinates
3. `create_db.py` — Load into `mobility.db` (tables: `hpms_segments`, `traffic_counts`, `railroad_crossings`, `npmrds_travel_times`)
4. `create_gpkg.py` — Write to `mobility.gpkg` layers: `railroad_crossings`, `hpms_segments` (EPSG:32617)
5. `validate.py` — Row counts, coordinate validity, crossing count sanity check, NPMRDS TMC coverage

---

## RAPTOR Category Class

**File**: `scripts/states/Georgia/categories/Mobility.py`

**AADT**: From base roadway inventory (already loaded in Phase 1). Future 2050 via growth factor computed from historic AADT trend data (no design AADT available — see Texas comparison above).

**V/C Ratio**:
```python
Volume = K_FACTOR * D_FACTOR * AADT * 0.01 * 0.01
fhv = 1 / (1 + TRUCK_PCT * 0.01 * 0.5)
Lane_peak = LANES // 2 (minimum 1)
Capacity = 2200 * fhv * Lane_peak
VC_Ratio = Volume / Capacity
```

**Railroad Crossings**: Spatial join — count crossings within 0.25-mile buffer of each segment.

**Road Category**: 2L / 4U+ / 4D+ from lane count + median type.

**Metrics**:
| Metric | Type | Weight |
|--------|------|--------|
| AADT_Current | min_max_standard | 0.00 |
| AADT_2050 | min_max_standard | 0.00 |
| Volume_to_Capacity_Ratio | threshold | 0.00 |
| Top_Congested_Flag | boolean | 0.33 |
| Num_Railroad_Crossings | min_max_standard | 0.34 |
| Roadway_Cross_Section | categorical | 0.00 |
| Num_Lanes | min_max_standard | 0.00 |
| LOS_Model | threshold | 0.33 |
| LOTTR (Level of Travel Time Reliability) | threshold | 0.00 |
| TTTR (Truck Travel Time Reliability) | threshold | 0.00 |

---

## Data Needed
| Dataset | Source | Public? | Status | Priority |
|---------|--------|---------|--------|----------|
| GDOT Traffic_Historical.zip | GDOT file server | Yes | ⬜ Download & inspect | **High** — needed for 2050 growth |
| GDOT 2010-2019 Published Traffic | GDOT file server | Yes | ⬜ Download & inspect | **High** — needed for 2050 growth |
| HPMS Georgia (yearly 2011-2023) | FHWA | Yes | ⬜ Download | High — fallback for growth rates |
| NTAD Railroad Crossings | BTS | Yes | ⬜ Download | High |
| NPMRDS Travel Times | FHWA / RITIS | Yes (DSA required) | ⬜ Request DSA | Medium |
| GDOT Traffic Counts | GDOT TADA | Yes | ⬜ Download | Medium |
| ARC Historical Counts (2008-2017) | ARC / GDOT | Yes | ⬜ Download | Medium |
| GEOCOUNTS | GEOCOUNTS | Yes | ⬜ Explore | Low |
| Traffic Monitoring Guide | GDOT | Yes | ⬜ Download | Low (reference) |

## Deliverables
- `02-Data-Staging/databases/mobility.db`
- `02-Data-Staging/spatial/mobility.gpkg`
- `scripts/states/Georgia/categories/Mobility.py`
- Updated `Georgia_Data_Inventory_GDOT.csv`

## Verification
- [ ] Railroad crossing count for Georgia is plausible
- [ ] V/C > 1.0 on known congested segments (I-285, I-75/85)
- [ ] Road category distribution is reasonable
- [ ] NPMRDS TMC segments cover NHS routes in Georgia
- [ ] LOTTR values plausible on known unreliable corridors (I-285, GA-400)
- [ ] No row duplication after merges
