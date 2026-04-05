# Phase 3 — Socioeconomic (RAPTOR Category — Weight: 0.15)

## Goal
Build a **nationwide** U.S. Census database (reusable for Georgia, Tennessee, and future states), then build the RAPTOR `SocioEconomic` category class for Georgia.

## Status: Not Started
**Depends on**: Phase 1 (Roadways base layer for RAPTOR class)

---

## Texas Comparison

> **Texas uses**: SAMv5 Transportation Analysis Zones (TAZ) with population and employment projections for 2021, 2030, 2040, 2050. Demographics are at TAZ level from the state travel demand model. Density computed as value / TAZ area, with 1-mile buffer overlay on road segments.
>
> **Georgia gap**: No statewide TAZ demographics available (GSTDM output not public).
>
> **Georgia workaround** (already reflected in this plan):
> - Use **Census block groups** (ACS 5-Year) for current population and employment density — finer spatial resolution than county, comparable to TAZ
> - Use **LEHD/LODES** WAC data for block-level employment by NAICS sector
> - Apply **OPB county-level growth factors** (2020-2060) to block-group base data for 2050 projections
> - Use **BLS/GA DOL** employment projections for employment growth rates
>
> **Georgia advantage**: ACS block-group data and LEHD/LODES provide more granular and more standardized data than SAMv5 TAZs. This approach is also reusable for any state (Tennessee, etc.) without needing state-specific travel demand model output.

---

## Datasets

### 3.1 Decennial Census 2020
- **Source**: U.S. Census Bureau
- **URL**: `https://data.census.gov/`
- **Scope**: Nationwide (all states)
- **Granularity**: Census block (finest)
- **Key fields**: Total population, housing units, race/ethnicity
- **Frequency**: Every 10 years (2020 latest)

### 3.2 ACS 5-Year Estimates
- **Source**: U.S. Census Bureau
- **URL**: `https://data.census.gov/`
- **Scope**: Nationwide
- **Granularity**: Block group (finest for ACS)
- **Key fields**: Median household income, education, age distribution, poverty rate, commute mode
- **Frequency**: Annual releases (latest: 2019-2023)

### 3.3 LEHD LODES
- **Source**: U.S. Census Bureau, Longitudinal Employer-Household Dynamics
- **URL**: `https://lehd.ces.census.gov/data/`
- **Scope**: Nationwide (by state file)
- **Granularity**: Census block
- **Tables**:
  - **WAC** (Workplace Area Characteristics) — employment count by 20 NAICS sectors, worker age/earnings/education
  - **RAC** (Residence Area Characteristics) — where workers live
  - **OD** (Origin-Destination) — commute flows
- **Frequency**: Annual, ~2-3 year lag (2021 latest as of 2024)

### 3.4 Economic Census
- **Source**: U.S. Census Bureau
- **URL**: `https://www.census.gov/programs-surveys/economic-census/data.html`
- **Scope**: Nationwide
- **Granularity**: County (some ZIP code)
- **Key fields**: Business establishments, revenue, payroll by industry
- **Frequency**: Every 5 years (2022 latest)

### 3.5 OPB Population Projections (Georgia-specific)
- **Source**: Georgia Governor's Office of Planning and Budget
- **URL**: `https://opb.georgia.gov/census-data/population-projections`
- **Granularity**: County (159 counties)
- **Key fields**: Population projections 2020-2060

### 3.6 Census Population Estimates Program (PEP) 2020-2025
- **Source**: U.S. Census Bureau
- **URL**: `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/`
- **Scope**: Nationwide
- **Granularity**: County
- **Key fields**: Annual county-level population estimates, April 2020 to July 2025
- **Use**: Bridge between Decennial Census and ACS; most current population figures

### 3.7 ACS Commuting & Transportation Tables
- **Source**: U.S. Census Bureau
- **URL**: `https://data.census.gov/`
- **Scope**: Nationwide
- **Granularity**: State/county/tract/block group
- **Key fields**: Mode share, travel time, vehicles per HH, work-from-home rates
- **Use**: Commute mode context for RAPTOR corridor analysis; supplements LODES OD flows

### 3.8 BLS National Employment Projections (2024-2034)
- **Source**: Bureau of Labor Statistics
- **URL**: `https://www.bls.gov/emp/data.htm`
- **Scope**: National
- **Format**: Download (CSV; Excel)
- **Key fields**: National employment projections by industry and occupation; 10-year horizon
- **Use**: National baseline for 2050 employment growth factors

### 3.9 Projections Central — State Long-Term Occupational Projections
- **Source**: State LMI Agencies / DOL
- **URL**: `https://projectionscentral.org/Projections/LongTerm`
- **Scope**: All states
- **Format**: Download (Excel; CSV)
- **Key fields**: State-level long-term employment projections by occupation; ~10-year horizon
- **Use**: Georgia-specific employment growth rates to complement OPB population projections

### 3.10 Georgia DOL Labor Market Explorer
- **Source**: Georgia Department of Labor
- **URL**: `https://explorer.gdol.ga.gov/`
- **Format**: Interactive; PDF reports
- **Key fields**: Long-term industry and occupation projections for Georgia statewide and workforce development regions
- **Use**: Georgia-specific employment projections; validate BLS/Projections Central data

### 3.11 Opportunity Zones
- **Source**: U.S. Treasury CDFI Fund
- **URL**: `https://www.cdfifund.gov/opportunity-zones`
- **Granularity**: Census tract
- **Format**: Lists, maps, shapefiles (via Data.gov/HUD)

---

## Research Notes — Geography & Year Availability

> **Important**: When we start this phase, we need to research and document the exact availability matrix. Below is our current understanding:

| Dataset | Block | Block Group | Tract | County | Years Available |
|---------|-------|-------------|-------|--------|----------------|
| Decennial Census | **Yes** | Yes | Yes | Yes | 2020, 2010, 2000 |
| ACS 5-Year | No | **Yes** (finest) | Yes | Yes | Annual (2009-2023) |
| ACS 1-Year | No | No | No | 65K+ pop counties | Annual (2005-2023) |
| LEHD LODES (WAC/RAC) | **Yes** | Yes | Yes | Yes | 2002-2021 (varies by state) |
| LEHD LODES (OD) | **Yes** | Yes | Yes | Yes | 2002-2021 |
| Economic Census | No | No | No | Yes | 2017, 2022 |

**Key decisions for implementation**:
1. Download nationwide — reusable for Tennessee and future states
2. Start at finest granularity: block (Decennial + LODES), block group (ACS)
3. Aggregate up to tract → county → district → state as needed
4. For 2050 projections: apply OPB county growth rates to block-level base (GA-specific); need equivalent source for other states
5. LODES latest year varies by state — verify for GA and TN

---

## ETL Pipeline

**`02-Data-Staging/scripts/06_socioeconomic/`**:

1. `download_census.py` — Download Decennial, ACS, LODES for all states (or GA + TN initially)
2. `download_economic_census.py` — Download Economic Census
3. `download_opb.py` — Download OPB projections (GA-specific)
4. `download_opportunity_zones.py` — Download OZ shapefiles
5. `normalize.py` — Standardize geography codes (FIPS), join tables, validate
6. `create_db.py` — Load into `socioeconomic.db` (tables: `decennial_blocks`, `acs_block_groups`, `lodes_wac`, `lodes_rac`, `economic_census`, `opportunity_zones`)
7. `create_gpkg.py` — Write to `demographics.gpkg` layers: `census_blocks`, `block_groups`, `tracts`, `opportunity_zones` (EPSG:32617 for GA; consider national CRS for multi-state)
8. `validate.py` — Population totals match published state totals, employment counts reasonable

---

## RAPTOR Category Class

**File**: `05-RAPTOR-Integration/states/Georgia/categories/SocioEconomic.py`

**Processing**:
1. Load block-level data from `socioeconomic.db` (filter to Georgia)
2. Load geometry from `demographics.gpkg`
3. Create 1-mile buffer around each road segment
4. Spatial overlay: intersect blocks/block groups with buffers
5. Aggregate population and employment per segment
6. Compute density = value / buffer area
7. For 2050: apply OPB county growth rates to block-level base values

**Metrics**:
| Metric | Type | Weight |
|--------|------|--------|
| Population_Density | min_max_standard | 0.25 |
| Employment_Density | min_max_standard | 0.25 |
| Population_Density_2050 | min_max_standard | 0.25 |
| Employment_Density_2050 | min_max_standard | 0.25 |

Additional (weight=0, for future categories or reference):
- Median household income
- Employment by NAICS sector
- Opportunity Zone flag
- Worker commute patterns (LODES OD)

---

## Data Needed
| Dataset | Source | Public? | Scope | Status |
|---------|--------|---------|-------|--------|
| Decennial Census 2020 | Census Bureau | Yes | Nationwide | ⬜ Download |
| ACS 5-Year (2019-2023) | Census Bureau | Yes | Nationwide | ⬜ Download |
| LEHD LODES WAC | Census Bureau | Yes | Nationwide | ⬜ Download |
| LEHD LODES RAC | Census Bureau | Yes | Nationwide | ⬜ Download |
| Economic Census 2022 | Census Bureau | Yes | Nationwide | ⬜ Download |
| Census PEP (2020-2025) | Census Bureau | Yes | Nationwide | ⬜ Download |
| ACS Commuting Tables | Census Bureau | Yes | Nationwide | ⬜ Download |
| OPB Pop. Projections | GA Gov Office | Yes | Georgia | ⬜ Download |
| BLS Employment Projections | BLS | Yes | National | ⬜ Download |
| Projections Central | State LMI / DOL | Yes | All states | ⬜ Download |
| GA DOL Labor Market Explorer | GA DOL | Yes | Georgia | ⬜ Download |
| Opportunity Zones | Treasury CDFI | Yes | Nationwide | ⬜ Download |
| TIGER/Line shapefiles | Census Bureau | Yes | Nationwide | ⬜ Download |

## Deliverables
- `02-Data-Staging/databases/socioeconomic.db` (nationwide)
- `02-Data-Staging/spatial/demographics.gpkg` (GA layers initially)
- `05-RAPTOR-Integration/states/Georgia/categories/SocioEconomic.py`
- Updated `Georgia_Data_Inventory_GDOT.csv`

## Verification
- [ ] Georgia total population matches published Census figure
- [ ] Employment totals are reasonable vs BLS published data
- [ ] Metro Atlanta (District 7) has highest population/employment density
- [ ] Rural districts have lower density
- [ ] Opportunity Zones flag correctly assigned
- [ ] No row duplication after merges
