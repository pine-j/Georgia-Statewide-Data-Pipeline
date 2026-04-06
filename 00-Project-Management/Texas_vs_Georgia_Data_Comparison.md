# Texas vs Georgia — RAPTOR Data Comparison (by RAPTOR Category)

**Date**: 2026-03-28
**Purpose**: Compare data available for the Georgia RAPTOR pipeline against what the Texas pipeline currently uses, identify gaps, and propose workarounds.

---

## How to Read This Document

- **Available** = Public data source identified and accessible
- **Partial** = Some data available but missing fields or coverage compared to Texas
- **GAP** = No equivalent data source identified; workaround needed
- **Restricted** = Data exists but requires agreement/request
- **Need to verify** = Likely in GDOT GDB but must confirm after download

---

## 1. Roadways — Base Layer (Not a Scored Category)

> **Note**: In RAPTOR, the base roadway network is loaded by the `RoadwayData` class and serves as the foundation for all 6 scored categories. It is not itself a scored category. Texas loads from `states/Texas/categories/Roadways.py`; Georgia will load from `states/Georgia/categories/Roadways.py`.

### Texas Approach
- **Source**: TxDOT Roadway Inventory GDB — yearly snapshots (e.g., `2023_Roadway_Inventory.gdb`, `2024_Roadway_Inventory.gdb`)
- **Class**: `RoadwayData` in `states/Texas/categories/Roadways.py`
- **Size**: ~125,000+ centerline miles
- **Key fields**: `RDBD_ID`, `HWY`, `FRM_DFO`, `TO_DFO`, `HSYS`, `DI`, `CO`, `NUM_LANES`, `ADT_CUR`, `AADT_DESGN`, `K_FAC`, `D_FAC`, `TRK_DHV_PCT`, `AADT_TRUCKS`, `PCT_SADT`, `PCT_CADT`, `DVMT`, `SEC_EVAC`, `NHFN`, `SEC_TRUNK`, `MED_TYPE`, `HWY_DES`, `geometry`
- **Segment ID**: `HWY + FRM_DFO + TO_DFO`
- **CRS**: EPSG:3081 (Texas Centric)
- **Temporal**: Yearly archives allow year-over-year comparison

### Georgia Equivalent
- **Source**: GDOT Road Inventory GDB (`Road_Inventory_Geodatabase.zip`) — current snapshot only
- **Class**: `RoadwayData` in `states/Georgia/categories/Roadways.py`
- **Size**: 125,000+ centerline miles
- **Key fields**: `RCLINK`, `ROUTE_ID`, `FROM_MEASURE`, `TO_MEASURE`, `FUNCTIONAL_CLASS`, `NHS`, `SYSTEM_CODE`, `AADT`, `AADT_YEAR`, `LANES`, `SPEED_LIMIT`, `SURFACE_TYPE`, `MEDIAN_TYPE`, `SHOULDER_TYPE`, `URBAN_CODE`, `DISTRICT`, `COUNTY_CODE`, `TRUCK_PCT`, `K_FACTOR`, `D_FACTOR`, `geometry`
- **Segment ID**: `ROUTE_ID + FROM_MEASURE + TO_MEASURE`
- **CRS**: Reproject to EPSG:32617 (UTM Zone 17N)
- **Temporal**: Rolling live dataset only — **no yearly archives** (GDOT confirmed)

### Gaps & Action Items

| Field | Texas | Georgia | Status |
|-------|-------|---------|--------|
| Current AADT | `ADT_CUR` | `AADT` | Available |
| Design AADT (20-yr) | `AADT_DESGN` | None found | **GAP** |
| Traffic factors | `K_FAC`, `D_FAC` | `K_FACTOR`, `D_FACTOR` | Available (verify names) |
| Truck AADT | `AADT_TRUCKS` | Unknown | Need to verify |
| Truck % (total) | `TRK_AADT_PCT` | `TRUCK_PCT` | Available |
| Single-unit truck % | `PCT_SADT` | Unknown | Need to verify |
| Combination truck % | `PCT_CADT` | Unknown | Need to verify |
| Truck design % | `TRK_DHV_PCT` | Unknown | Need to verify |
| DVMT | `DVMT` | Unknown | Need to verify |
| Evacuation route flag | `SEC_EVAC` | Unknown | Need to verify |
| Freight network flag | `NHFN` | `NHS` | Partial |
| Trunk system flag | `SEC_TRUNK` | N/A | Not applicable to GA |
| Yearly snapshots | Yes (2023, 2024) | No | **GAP** — no historical GDB |

> **Design AADT Gap**: Texas uses `AADT_DESGN` (20-year projection built into the GDB) to forecast 2050 traffic. Georgia does not appear to have this. **Workaround**: Compute growth rate from historical AADT trend data (see Mobility section) or use HPMS growth factors.

---

## 2. Asset Preservation (RAPTOR Category — Weight: 0.20)

### Texas Approach
| Data | Source | Key Fields |
|------|--------|------------|
| Pavement condition | `Pavement_data.csv` (TxDOT) | Condition Score, Ride Score, Distress Score by HWY + DFO |
| Bridge data | `Bridges.shp` (TxDOT) | Sufficiency Rating, Vertical Clearance, Structure Length, Deck Width |

### Georgia Equivalent
| Data | Source | Key Fields | Status |
|------|--------|------------|--------|
| Pavement condition | GDOT pavement management system | Unknown | **GAP** — not in data inventory; need to investigate |
| Bridge data | National Bridge Inventory (FHWA) | Sufficiency Rating, Vertical Clearance, Structure Length | **Available** (public, filter State=13) |

### Action Items
- [ ] Investigate GDOT pavement condition data availability (contact GDOT or check internal sources)
- [ ] Download NBI data for Georgia and verify field compatibility with Texas bridge metrics

---

## 3. Safety (RAPTOR Category — Weight: 0.20)

### Texas Approach
| Data | Source | Key Fields |
|------|--------|------------|
| Crash incidents | TxDOT crash files by year (`safety/{YEAR}/`) | Crash_ID, severity, fatality flag, DFO location, date |
| Time windows | 5-year, 3-year, 1-year lookback | Crash counts and rates per 100M VMT |

### Georgia Equivalent
| Data | Source | Key Fields | Status |
|------|--------|------------|--------|
| Crash data | GEARS (GA Electronic Accident Reporting System) | Crash location, severity, date | **Restricted** — requires DSA with GDOT |
| Fatal crashes only | FARS (NHTSA) | Fatal crashes, State=13 | **Available** (public) |
| Crash dashboard | GDOT Crash Data Dashboard | Interactive only | Not downloadable in bulk |

### Action Items
- [ ] Initiate data-sharing agreement (DSA) with GDOT for GEARS crash data — **critical path item**
- [ ] Download FARS as interim source (fatalities only, not full severity spectrum)
- [ ] Determine if GDOT crash data includes DFO/milepoint location for segment matching

---

## 4. Mobility (RAPTOR Category — Weight: 0.20)

### Texas Approach
| Data | Source | Key Fields |
|------|--------|------------|
| Current AADT | GDB (`ADT_CUR`) | Traffic volume |
| 2050 AADT | Projected from `AADT_DESGN` | Exponential growth |
| V/C Ratio | Computed from K_FAC, D_FAC, ADT, lanes | HCM formula |
| Railroad crossings | NTAD (BTS/FRA) | At-grade crossings within 0.25mi buffer |
| SAM congestion model | Fort Worth TDM output (current + 2050) | PM_AB_VOC |
| Top 100 congested | TxDOT list | Boolean flag |

### Georgia Equivalent
| Data | Source | Status |
|------|--------|--------|
| Current AADT | GDOT GDB (`AADT`) | **Available** |
| 2050 AADT | No design AADT; need growth rate | **GAP** — see workarounds below |
| V/C Ratio | Compute from GDB fields | **Available** (same HCM formula) |
| Railroad crossings | NTAD (BTS/FRA) | **Available** (same national source) |
| Congestion model output | GSTDM exists but not publicly available | **GAP** |
| Top congested list | No GDOT equivalent | **GAP** |
| NPMRDS travel time | FHWA / RITIS | **Available** (requires DSA) — not used by Texas |

### Historic AADT Sources (for Growth Rate Workaround)

Since Georgia lacks design AADT, historic AADT data could be used to compute growth trends:

| Source | Years | Coverage | Format |
|--------|-------|----------|--------|
| `Traffic_Historical.zip` (GDOT server) | Unknown — need to download | Statewide | ~591 MB zip |
| `2010_thr_2019_Published_Traffic.zip` (GDOT server) | 2010-2019 | Statewide | ~516 MB zip |
| ARC Open Data (GDOT traffic counts) | 2008-2017 | Metro Atlanta only | Shapefile |
| HPMS shapefiles (FHWA) | 2011-2023 | Federal-Aid roads only | Shapefiles |
| TADA interactive app | ~15 years | Per-station (not bulk) | CSV export |

### Action Items
- [ ] Download `Traffic_Historical.zip` and `2010_thr_2019_Published_Traffic.zip` to determine exact content and year coverage
- [ ] Evaluate whether historic AADT can produce reliable growth rates for 2050 projection
- [ ] Request NPMRDS DSA for travel time reliability metrics (LOTTR, TTTR)
- [ ] Investigate whether GSTDM output can be obtained from GDOT

---

## 5. Connectivity (RAPTOR Category — Weight: 0.15)

### Texas Approach
| Data | Source |
|------|--------|
| Energy sector corridors | TxDOT Energy_Sector_Corridors.csv |
| Trade corridors | TxDOT Trade_Corridor.csv |
| 7 traffic generator types | Shapefiles (airports, seaports, universities, national parks, border crossings, freight generators, intermodal rail) |
| Evacuation routes | SEC_EVAC flag in GDB |
| NHFN / Trunk system | Flags in GDB |
| SLRTP projects | State long-range plan data |

### Georgia Equivalent
| Data | Source | Status |
|------|--------|--------|
| Energy sector corridors | N/A | **Not relevant** to Georgia |
| Trade corridors | Georgia Freight & Logistics Plan may identify | **GAP** — investigate |
| Traffic generators (6 types) | FAA airports, GPA ports, NCES universities, NPS parks, NTAD freight, intermodal rail | **Available** (no border crossings — not relevant) |
| Evacuation routes | Unknown — check GDOT GDB | **Need to verify** |
| NHFN | NHS flag in GDB | **Available** |
| NEVI EV corridors | GDOT NEVI Hub | **Available** (Georgia-specific addition) |
| SLRTP / project data | GDOT Projects Portal (ArcGIS Hub) | **Available** — needs investigation |

### Action Items
- [ ] Check GDOT GDB for evacuation route designation after download
- [ ] Review Georgia Freight & Logistics Plan for designated trade/freight corridors
- [ ] Download traffic generator shapefiles (airports, ports, universities, parks, freight facilities)

---

## 6. Freight (RAPTOR Category — Weight: 0.10)

### Texas Approach
| Data | Source | Key Fields |
|------|--------|------------|
| Transearch commodity data | `TX_Transearch_{YEAR}.accdb` (proprietary, IHS Markit) | Tons, Units, Value by truck network link |
| ORNL Truck Network | `ORNL_Truck_Links.shp` | LID for Transearch conflation |
| Truck AADT breakdown | GDB fields | `AADT_TRUCKS`, `PCT_SADT`, `PCT_CADT` |
| 2050 truck projections | Transearch 2050 + growth from AADT_DESGN | Tonnage and truck AADT forecasts |

### Georgia Equivalent
| Data | Source | Status |
|------|--------|--------|
| Transearch commodity data | Not available for Georgia | **MAJOR GAP** — proprietary; would need to purchase |
| ORNL Truck Network | Same national source | **Available** (filter to Georgia) |
| Truck AADT | `TRUCK_PCT` in GDB | **Partial** — may lack single/combination split |
| FAF5 commodity flows | FHWA / BTS | **Available** — coarser (state/metro, not link-level) |
| GA Ports Authority stats | GPA website | **Available** — port-level tonnage/TEUs |
| State Rail Plan | GDOT | **Available** — rail network and intermodal facilities |

### Action Items
- [ ] Determine if Transearch purchase for Georgia is feasible/budgeted
- [ ] Evaluate FAF5 as Transearch alternative — can flows be assigned to network links?
- [ ] Verify truck breakdown fields in GDOT GDB after download
- [ ] Download ORNL truck network and filter to Georgia

---

## 7. Socioeconomic (RAPTOR Category — Weight: 0.15)

### Texas Approach
| Data | Source | Key Fields |
|------|--------|------------|
| TAZ demographics | SAMv5 TAZ shapefile + Demographics Excel | Population, Employment by TAZ for 2021, 2030, 2040, 2050 |
| Density metrics | Computed from TAZ area | Pop density, employment density (current + 2050) |
| Interpolation | Linear regression across 4 forecast years | Predict current year from trend |

### Georgia Equivalent
| Data | Source | Status |
|------|--------|--------|
| TAZ demographics | No statewide TAZ data available | **GAP** |
| Population projections | OPB Projections (2020-2060) | **Available** — county-level, not TAZ |
| Employment projections | BLS / GA DOL | **Available** — state/region level, not TAZ |
| Census block group data | ACS 5-Year Estimates | **Available** — income, poverty, commute, education |
| LEHD/LODES employment | Census Bureau | **Available** — block-level employment by NAICS |

### Proposed Georgia Workaround
Instead of TAZ-based demographics (which require a travel demand model), Georgia can use:
1. **Census block groups** (ACS) for current population and employment density
2. **LEHD/LODES** for block-level employment data (WAC = Workplace Area Characteristics)
3. **OPB county projections** for 2050 population growth factors applied to block-group data
4. **BLS/GA DOL** for employment growth factors

This gives finer spatial resolution than county but coarser than TAZ for projections.

### Action Items
- [ ] Download ACS 5-Year Estimates for Georgia (block group level)
- [ ] Download LEHD/LODES WAC data for Georgia
- [ ] Download OPB population projections (2020-2060)
- [ ] Design methodology for applying county growth factors to block-group base data

---

## Summary: Gap Analysis

### Critical Gaps (Affect Scoring)

| # | Gap | Impact | Proposed Workaround | Priority |
|---|-----|--------|---------------------|----------|
| 1 | **No design AADT** (AADT_DESGN) | Cannot project 2050 traffic using TxDOT method | Compute growth rate from historic AADT trend data | High |
| 2 | **No Transearch data** | No link-level commodity tonnage/value | FAF5 (state/metro level) + ORNL truck network | High |
| 3 | **Crash data restricted** (GEARS requires DSA) | Cannot score safety without crash locations | Initiate DSA with GDOT; FARS as interim (fatal only) | High |
| 4 | **No pavement condition data** | Cannot score asset preservation (pavement) | Investigate GDOT pavement management data; may require data request | High |
| 5 | **No TAZ demographics** | Cannot compute TAZ-level population/employment density | Use Census block groups + LEHD + county projections | Medium |

### Moderate Gaps

| # | Gap | Impact | Proposed Workaround | Priority |
|---|-----|--------|---------------------|----------|
| 6 | No congestion model output (GSTDM) | No model-based V/C or LOS | Compute V/C from HCM formula; use NPMRDS for reliability | Medium |
| 7 | No Top 100 congested list | Missing boolean flag metric | Derive from computed V/C or NPMRDS congestion ranking | Low |
| 8 | No trade corridor designations | Missing connectivity flag | Review GA Freight & Logistics Plan for corridor designations | Low |
| 9 | No yearly GDB snapshots | Cannot compare year-over-year road attributes | Document download dates; begin archiving | Low |

### Georgia Advantages (Data Texas Doesn't Use)

| Data | Source | Potential Use |
|------|--------|---------------|
| NPMRDS travel time reliability | FHWA / RITIS | LOTTR and TTTR metrics (Georgia Phase 4 plan already includes these) |
| NEVI EV corridor data | GDOT NEVI Hub | EV infrastructure connectivity scoring |
| ACS block-group demographics | Census Bureau | Finer spatial resolution than TAZ for some areas |
| LEHD/LODES block-level employment | Census Bureau | More granular than SAMv5 for employment |
| OPB projections to 2060 | GA Governor's Office | Longer forecast horizon than Texas SAMv5 (2050) |

---

## Next Steps

1. **Download GDOT Road Inventory GDB** and catalog all columns — many "need to verify" items will be resolved
2. **Download `Traffic_Historical.zip`** and `2010_thr_2019_Published_Traffic.zip` — determine if historic AADT is usable for growth projections
3. **Initiate GEARS DSA** with GDOT for crash data
4. **Investigate pavement data** — contact GDOT or check if available through internal Jacobs channels
5. **Evaluate Transearch purchase** for Georgia or confirm FAF5 as alternative
6. **Update data inventory CSV** with data type, temporal coverage, and gap status columns
