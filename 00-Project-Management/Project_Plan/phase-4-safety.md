# Phase 4 — Safety (RAPTOR Category — Weight: 0.20)

## Goal
Download, clean, and load crash data into the Georgia database. Build the RAPTOR `Safety` category class.

## Status: Not Started
**Depends on**: Phase 1 (Roadways base layer)

---

## Texas Comparison

> **Texas uses**: TxDOT crash incident files by year, with Crash_ID, severity (Crash_Sev_ID), fatality flag, DFO location, and date. Metrics use 5-year/3-year/1-year lookback windows. Crash rates computed using DVMT from the roadway GDB.
>
> **Georgia equivalent**: GEARS (Georgia Electronic Accident Reporting System) — **requires data-sharing agreement (DSA) with GDOT**. This is a **critical path blocker** for this phase. FARS (NHTSA) is available as public interim but only covers fatal crashes, not the full KABCO severity spectrum.
>
> **Key difference**: Texas crash data includes DFO (distance from origin) for precise segment matching. Need to confirm whether GEARS data includes RCLINK + milepoint for the same purpose.
>
> **DVMT for crash rates**: Texas has `DVMT` in the GDB. If Georgia lacks this field, it must be computed as `AADT × segment_length_miles`.

---

## Datasets

### 4.1 GEARS Crash Data (Primary)
- **Source**: GDOT Georgia Electronic Accident Reporting System
- **Status**: Requires data-sharing agreement with GDOT
- **Years**: 5-year rolling window (2021-2025 if available, else 2020-2024)
- **Format**: CSV
- **Key fields**: Crash_ID, Crash_Date, Severity (KABCO), RCLINK, Milepoint, Lat/Lon, vehicles/peds/bikes involved
- **Place in**: `01-Raw-Data/safety/{year}/`

### 4.2 FARS Fatal Crashes (Interim/Supplement)
- **Source**: NHTSA Fatality Analysis Reporting System
- **URL**: `https://www.nhtsa.gov/research-data/fatality-analysis-reporting-system-fars`
- **Years**: 2020-2024 (latest available)
- **Format**: CSV (from FTP/API)
- **Key fields**: Fatal crash details, lat/lon, route info
- **Place in**: `01-Raw-Data/safety/fars/`

### 4.3 GDOT Crash Data Dashboard
- **Source**: Georgia DOT (GDOT)
- **URL**: `https://gdot.aashtowaresafety.net/crash-data`
- **Format**: Interactive dashboard
- **Key fields**: Crash and vehicle data on Georgia public roads; filtering by city, county, region
- **Coverage**: Multi-year
- **Use**: Cross-reference with GEARS data; supplemental crash counts if GEARS agreement delayed

### 4.4 GA Governor's Office of Highway Safety — Traffic Data
- **Source**: GA GOHS
- **URL**: `https://www.gahighwaysafety.org/traffic-records-data-data-sources/`
- **Format**: Reports; PDF; county data sheets
- **Key fields**: Georgia traffic safety facts; fatalities, serious injuries, impaired driving stats by county
- **Coverage**: Multi-year
- **Use**: Validation source for crash totals; county-level safety context

---

## ETL Pipeline

**`02-Data-Staging/scripts/03_safety/`**:

1. `download.py` — Download FARS from NHTSA (GEARS requires manual delivery)
2. `extract_and_merge.py` — Concatenate multi-year crash files into single file per analysis year
3. `normalize.py` — Standardize severity codes (KABCO), parse dates, clean route IDs
4. `create_db.py` — Load into `safety.db` (table: `crashes`)
5. `create_gpkg.py` — Write crash points to `safety.gpkg` layer `crash_points` (EPSG:32617)
6. `validate.py` — Row counts per year, severity distribution, coordinate validity

**Config files**:
- `severity_codes.json` — KABCO mapping
- `crash_type_codes.json` — Crash type definitions

---

## RAPTOR Category Class

**File**: `05-RAPTOR-Integration/states/Georgia/categories/Safety/Safety.py`

**Processing**: Chunk-based (100K rows), join by RCLINK + milepoint (crash milepoint within segment FROM_MEASURE to TO_MEASURE).

**VMT**: `AADT × segment_length_miles × 365 × num_years / 100,000,000`

**Metrics**:
| Metric | Type | Weight |
|--------|------|--------|
| Num_Incap_Fatal_Crashes_5yr | min_max_standard | 0.33 |
| Overall_Crash_Rate_5yr | min_max_standard | 0.34 |
| Incap_Fatal_Crash_Rate_5yr | min_max_standard | 0.33 |

Additional (weight=0, available for tuning):
- Total crashes, fatal crashes (5yr/3yr/1yr)
- Crash rates for 3yr and 1yr windows
- Pedestrian/bicycle crash counts

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| GEARS Crash Data (5yr) | GDOT | No | ⬜ Request |
| FARS Fatal Crashes | NHTSA | Yes | ⬜ Download |
| GDOT Crash Data Dashboard | GDOT | Yes | ⬜ Explore |
| GA GOHS Traffic Safety Data | GA GOHS | Yes | ⬜ Download |

## Deliverables
- `02-Data-Staging/databases/safety.db`
- `02-Data-Staging/spatial/safety.gpkg` (layer: `crash_points`)
- `05-RAPTOR-Integration/states/Georgia/categories/Safety/Safety.py`
- `05-RAPTOR-Integration/states/Georgia/categories/Safety/Extract_and_Merge.py`
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] Crash counts per year are plausible
- [ ] Severity distribution makes sense (K < A < B < C < O)
- [ ] Crashes join to correct road segments by RCLINK + milepoint
- [ ] Crash rates capped at 96th percentile (like Texas)
- [ ] No row duplication after merge
