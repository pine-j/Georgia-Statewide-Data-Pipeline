# Phase 1 — Roadways Base Layer + Project Setup

> **RAPTOR context**: This phase builds the `RoadwayData` class (`states/Georgia/categories/Roadways.py`). This is not a scored category — it provides the foundation data that all 6 scored categories (Asset Preservation, Safety, Mobility, Connectivity, Freight, Socioeconomic) process.

## Goal
Download the complete Georgia roadway inventory, clean and normalize it into a SQLite database, then build the `RoadwayData` class that loads from it. This database becomes the foundational source of truth for all road segment data.

## Status: Complete

## Current Implementation Snapshot

Phase 1 is implemented, validated, and ready to treat as the closed roadway-foundation phase for the current project scope. The core ETL, staged database, staged GeoPackage, official boundary layers, `RoadwayData` loader, and staged web-app data path are all working. Remaining questions about supplemental roadway sources and richer route-family labeling are deferred follow-on improvements, not Phase 1 blockers.

As of the current staged build:
- `roadway_inventory.db` contains `622,255` segmented roadway records
- `base_network.gpkg` contains:
  - `roadway_segments` (`622,255` features)
  - `county_boundaries` (`159` features)
  - `district_boundaries` (`7` features)
- Boundary layers are sourced from the official GDOT-hosted `GDOT_Boundaries` service and are now consumed by the staged web-app path

Current traffic coverage in the staged roadway network:
- Current AADT (`AADT` / `AADT_2024`) is available on `185,748` of `622,255` segments (`29.85%`)
- Current AADT covers `38,359.71` of `133,992.56` staged segment miles (`28.63%`)
- Historical AADT is currently present for:
  - `2010` through `2020`
- Historical route-segment traffic is currently unavailable for:
  - `2021`
  - `2022`
  - `2023`
- Important clarification: the current staged build does include `2020`; the missing route-segment years are `2021-2023`, not `2020-2023`

Selected historical coverage highlights:
- `AADT_2019` covers `535,080` segments (`85.99%`)
- `AADT_2020` covers `548,343` segments (`88.12%`)

Related exploratory note:

- [Roadway Supplement Options](../Assessment_and_Options/roadway-supplement-options.md)

Related exploratory memo:
- [Roadway Gap-Fill Exploratory Analysis](../Assessment_and_Options/roadway-gap-fill-options.md)

Current working note:

- A `2026-04-04` Playwright re-check of the local web app after full roadway load found no obvious planning-relevant network gaps in sampled Columbus / Dinglewood, Atlanta, and Savannah views.
- Phase 1 should therefore proceed on the assumption that the current GDOT-based staged network is good enough for initial statewide planning and prototype scoring.
- Supplemental TIGER / OSM / additional GDOT gap-fill work remains valuable as a validation and improvement track, but it is not currently treated as a Phase 1 blocker.

Phase 1 closeout decision:

- Close Phase 1 using the current GDOT-based staged roadway network and documented validation results.
- Defer roadway supplementation, expanded route-family taxonomy, and any optional archival-source integration to later targeted work only if downstream QA or scoring needs justify it.

---

## Key Differences from Texas Pipeline

> **No yearly snapshots**: GDOT publishes only a single rolling/current Road Inventory GDB — no archived annual versions (unlike TxDOT which has `{YEAR}_Roadway_Inventory.gdb`). We must document the download date and begin archiving snapshots ourselves.
>
> **No design AADT**: Texas has `AADT_DESGN` (20-year projection) built into the GDB, used for 2050 traffic forecasting. Georgia does not appear to have this field. Workaround is to compute growth rates from historic AADT data (see Phase 4).
>
> **AADT is split from the full roadway geometry**: GDOT's full `GA_2024_Routes` geometry layer does not carry AADT fields directly. Current-year AADT and truck traffic fields are available in the GDOT traffic products, and historic traffic files are available separately.
>
> **Future AADT is canonical only in 2024**: older historical segment files sometimes include `Future_AADT` / `FUTURE_AAD`, but we treat those as legacy projections and do not carry them forward. The normalized network keeps `FUTURE_AADT` only from the current 2024 GDOT traffic record.
>
> **Fields to verify after download**: The following Texas fields have unknown Georgia equivalents that must be checked once the GDB is downloaded:
> | Texas Field | Purpose | Georgia Equivalent |
> |---|---|---|
> | `AADT_TRUCKS` | Truck AADT count | Unknown — may be derivable from TRUCK_PCT × AADT |
> | `PCT_SADT` | Single-unit truck % | Unknown |
> | `PCT_CADT` | Combination truck % | Unknown |
> | `TRK_DHV_PCT` | Truck design hour % | Unknown |
> | `DVMT` | Daily vehicle-miles traveled | Unknown — may need to compute from AADT × segment length |
> | `SEC_EVAC` | Hurricane evacuation route flag | Unknown |
> | `NHFN` | National Highway Freight Network | `NHS` is likely available |
> | `SEC_TRUNK` | State trunk system flag | Not applicable to Georgia |
> | `TOP100ID` | Top 100 congested segment ID | No Georgia equivalent |

---

## ETL Approach

Every dataset in this project follows the same pattern (inspired by BTS-TransBorder pipeline):

```
01-Raw-Data/         →  02-Data-Staging/       →  03-Processed-Data/
(sacred, never edit)    (scripts + cleaned DB)     (final outputs for RAPTOR)
```

- **01-Raw-Data/**: Downloaded files exactly as received. Never modify.
- **02-Data-Staging/**: ETL scripts, config files, and per-dataset SQLite databases.
- **03-Processed-Data/**: Analysis-ready outputs that RAPTOR category classes consume.

Each dataset gets its own SQLite database (tabular) in `02-Data-Staging/databases/` and spatial data goes into themed GeoPackage files (all EPSG:32617) in `02-Data-Staging/spatial/`.

---

## Tasks

### 1.1 Create directory structure
```
scripts/
  states/
    Georgia/
      __init__.py
      pipeline.py                    # Stub for now
      categories/
        __init__.py
        Roadways.py                  # Base layer loader (reads from DB)
      utils/
        __init__.py

02-Data-Staging/
  scripts/
    01_roadway_inventory/
      download.py                    # Download GDB from GDOT
      normalize.py                   # Clean, normalize, build unique_id
      create_db.py                   # Load into SQLite
      validate.py                    # Verify row counts, nulls, CRS
    requirements.txt                 # Shared Python dependencies
  config/
    crs_config.json                  # {"georgia": "EPSG:32617"}
    district_codes.json              # 7 GDOT districts
    county_codes.json                # 159 GA counties
    system_codes.json                # Route system codes
  databases/                         # Per-dataset SQLite DBs (gitignored)
    roadway_inventory.db
  spatial/                           # Themed GeoPackage files (gitignored)
    base_network.gpkg                # Layers: roadway_segments, district_boundaries, county_boundaries
```

### 1.2 Download Georgia Road Inventory GDB
- **Source**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/Road_Inventory_Geodatabase.zip` (~492 MB, Jul 2025 version)
- **Data Dictionary**: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf`
- **Place in**: `01-Raw-Data/GA_RDWY_INV/` (raw, unmodified)
- **IMPORTANT**: GDOT only publishes a single rolling snapshot — no yearly archives. Document the download date in a `download_metadata.json` file alongside the GDB.

### 1.2b Download all available GDOT Road and Traffic Data directory files
- **Directory**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/`
- **Directory contents to download**:
  - `2010_thr_2019_Published_Traffic.zip`
  - `Road_Inventory_Excel.zip`
  - `Road_Inventory_Geodatabase.zip`
  - `Traffic_GeoDatabase.zip`
  - `Traffic_Historical.zip`
  - `Traffic_Tabular.zip`
- **Action**: Download every file in the directory into `01-Raw-Data/GA_RDWY_INV/` and record the source URLs, directory timestamps, and file sizes in `download_metadata.json`
- **Why**: GDOT provides the full roadway geometry and the traffic/AADT products in separate packages, and the historic traffic archives are needed for later growth analysis

### 1.2a Catalog all GDB columns
After download, run a column inventory script to document every field in the GDB. This resolves many "need to verify" items from the Texas comparison:
- Truck fields: `AADT_TRUCKS`, `PCT_SADT`, `PCT_CADT`, `TRK_DHV_PCT` equivalents?
- DVMT field?
- System flags: evacuation route, NHFN, freight network?
- Design AADT (`AADT_DESGN` equivalent)?
- Output: `02-Data-Staging/config/gdb_column_inventory.json`

Also inventory the GDOT traffic products separately:
- `Traffic_GeoDatabase.zip` / `TRAFFIC_Data_2024.gdb`
- `Traffic_Tabular.zip` / `TRAFFIC_DataYear2024.csv`
- `Traffic_Historical.zip`
- `2010_thr_2019_Published_Traffic.zip`

Confirmed current-year GDOT traffic fields include:
- `AADTRound`
- `Single_Unit_AADT`
- `Combo_Unit_AADT`
- `Future_AADT`
- `VMT`
- `TruckVMT`

Confirmed historical traffic fields retained in the normalized network are actual AADT series only. Legacy future-projection fields from 2010-2020 are intentionally dropped.

### 1.3 Install Python dependencies
```
geopandas, pyogrio, shapely, pandas, numpy, pyarrow, tqdm, python-dotenv, openpyxl, scikit-learn
```

### 1.4 Build ETL scripts for Roadway Inventory

**`02-Data-Staging/scripts/01_roadway_inventory/normalize.py`**:
1. Load the official full roadway geometry from `Road_Inventory_2024.gdb` layer `GA_2024_Routes`
2. Document ALL columns and their types (don't filter yet — keep the complete dataset)
3. Clean column names (standardize case, remove spaces)
4. Parse RCLINK route IDs into component fields (county, route type, number, suffix, direction)
5. Build `unique_id`: `{ROUTE_ID}_{FROM_MEASURE:.3f}_{TO_MEASURE:.3f}`
6. Compute segment length in miles from geometry
7. Reproject to `EPSG:32617` (UTM Zone 17N)
8. Join or map in traffic attributes from GDOT traffic products where a defensible relationship exists
9. Export cleaned data as CSV/GeoPackage to `02-Data-Staging/cleaned/`

**`02-Data-Staging/scripts/01_roadway_inventory/create_db.py`**:
1. Read cleaned data
2. Create `roadway_inventory.db` with table `segments` (**tabular columns only — no geometry**)
3. Create indexes on: ROUTE_ID, DISTRICT, COUNTY_CODE, FUNCTIONAL_CLASS, SYSTEM_CODE
4. Create `load_summary` metadata table (row count, date, source URL)
5. Write geometry to `02-Data-Staging/spatial/base_network.gpkg` layer `roadway_segments` (EPSG:32617)
6. Optionally add `district_boundaries` and `county_boundaries` layers to same GPKG

> **Why separate tabular and spatial?** The SQLite DB stores complete tabular attributes as a source of truth — fast to query, lightweight, easy to version. Geometry lives in themed GeoPackage files (EPSG:32617) — only loaded when spatial operations are needed. This keeps the DB small and lets geometry update independently.

**`02-Data-Staging/scripts/01_roadway_inventory/validate.py`**:
- Row count check
- Unique_id uniqueness
- Null checks on critical fields
- CRS verification
- District value range (1-7)
- Geometry validity

### 1.4a Field Lineage for Staged `roadway_segments`

The staged roadway network is built from the official GDOT route geometry, then enriched with selected roadway-inventory attribute layers, current traffic attributes, historical traffic attributes, and ETL-derived fields.

**Directly from `Road_Inventory_2024.gdb` layer `GA_2024_Routes`**:
- `FUNCTION_TYPE`
- `COUNTY`
- `SYSTEM_CODE`
- `DIRECTION`
- `ROUTE_ID`
- `Comments`
- `StateID`
- `BeginDate`
- `START_M`
- `END_M`
- `FROM_MILEPOINT`
- `TO_MILEPOINT`
- `BeginPoint`
- `EndPoint`
- `RouteId`
- `Shape_Length`
- `geometry`

**Joined from other layers in `Road_Inventory_2024.gdb`**:
- `COUNTY_ID` from layer `COUNTY_ID`
- `F_SYSTEM` from layer `F_SYSTEM`
- `NHS` from layer `NHS`
- `FACILITY_TYPE` from layer `FACILITY_TYPE`
- `THROUGH_LANES` from layer `THROUGH_LANES`
- `MEDIAN_TYPE` from layer `MEDIAN_TYPE`
- `SHOULDER_TYPE` from layer `SHOULDER_TYPE`
- `SURFACE_TYPE` from layer `SURFACE_TYPE`
- `URBAN_ID` from layer `URBAN_ID`

**Mapped from `TRAFFIC_Data_2024.gdb` / `TRAFFIC_DataYear2024`**:
- `AADT_2024`
- `SINGLE_UNIT_AADT_2024`
- `COMBO_UNIT_AADT_2024`
- `FUTURE_AADT_2024`
- `K_FACTOR`
- `D_FACTOR`
- `VMT_2024`
- `TRUCK_VMT_2024`
- `TRAFFIC_CLASS_2024`
- `TC_NUMBER`

**Mapped from `Traffic_Historical.zip`**:
- `AADT_2010` through `AADT_2020`
- `TRUCK_AADT_2010` through `TRUCK_AADT_2020`
- `TRUCK_PCT_2010` through `TRUCK_PCT_2020`

**Derived by ETL**:
- Parsed route ID fields:
  `PARSED_FUNCTION_TYPE`, `PARSED_COUNTY_CODE`, `PARSED_SYSTEM_CODE`,
  `PARSED_ROUTE_NUMBER`, `PARSED_SUFFIX`, `PARSED_DIRECTION`
- Standardized or recoded fields:
  `COUNTY_CODE`, `GDOT_District`, `DISTRICT`, `FUNCTIONAL_CLASS`,
  `NUM_LANES`, `URBAN_CODE`, `NHS_IND`, `ROUTE_TYPE`, `ROUTE_NUMBER`,
  `ROUTE_SUFFIX`, `ROUTE_DIRECTION`
- Segment identifiers and metrics:
  `unique_id`, `segment_length_m`, `segment_length_mi`
- Current-traffic summary fields:
  `AADT`, `AADT_YEAR`, `TRUCK_AADT`, `TRUCK_PCT`, `FUTURE_AADT`,
  `VMT`, `TruckVMT`, `current_aadt_covered`, `Traffic_Class`
- Historical coverage summary:
  `historical_aadt_years_available`

**Important note on segmentation**:
- The ETL uses `GA_2024_Routes` as the canonical geometry source.
- Traffic data is attached by `ROUTE_ID` and milepoint intervals rather than by direct geometry overlay.
- When a route is split to accommodate traffic interval changes, the route-level fields from the original roadway inventory are copied to each child segment.
- The fields that change during splitting are the interval and geometry fields:
  `FROM_MILEPOINT`, `TO_MILEPOINT`, `BeginPoint`, `EndPoint`, `geometry`, and `unique_id`.
- The staged output therefore preserves the selected original roadway-inventory attributes, but only for the layers explicitly joined above. Any roadway GDB layer not joined in the ETL is not carried into the staged network.

### 1.4b Current Roadway Classification in the Staged Data

The staged roadway network currently supports multiple kinds of classification, but they answer different questions:

**System / ownership classification**:
- Field: `SYSTEM_CODE`
- Current values present in the staged build:
  - `1` = State Highway Route
  - `2` = County Road
- Current segment counts:
  - `SYSTEM_CODE = 1`: `109,314` segments
  - `SYSTEM_CODE = 2`: `512,941` segments
- GDOT's code table also defines:
  - `3` = City Street
  - `6` = Ramp
  - `7` = Private Road
  - `8` = Public Road
  - `9` = Collector-Distributor
- Those codes are documented in config, but they are not currently present in the staged `segments` table

**Functional classification**:
- Source field: `F_SYSTEM`
- Derived field: `FUNCTIONAL_CLASS`
- Current values present in the staged build:
  - `1` through `7` in the GDOT roadway inventory
- This is the clearest current classification for arterial / collector / local-road hierarchy, but it is numeric and still needs a finalized Georgia-specific label mapping in project documentation

**Route identity / route-family parsing**:
- Fields derived from `ROUTE_ID`:
  - `PARSED_FUNCTION_TYPE`
  - `PARSED_SYSTEM_CODE`
  - `ROUTE_TYPE`
  - `ROUTE_NUMBER`
  - `ROUTE_SUFFIX`
  - `ROUTE_DIRECTION`
- These fields help distinguish route identifiers and route-family encoding in the GDOT network, but the current Phase 1 docs do not yet provide a formal crosswalk for categories like Interstate vs U.S. Highway vs State Route

**Current closeout position on classification**:
- Phase 1 does have a clear system classification (`SYSTEM_CODE`) and functional classification (`F_SYSTEM` / `FUNCTIONAL_CLASS`)
- Phase 1 does not yet have a finished, documented statewide crosswalk that cleanly labels every segment as Interstate, U.S. Route, State Route, county road, city street, ramp, etc.
- If that route-family classification is needed for downstream reporting or scoring, it should be added as an explicit follow-on task using the route ID schema and GDOT data dictionary

### 1.5 Build config JSON files

**`02-Data-Staging/config/district_codes.json`**:
```json
{
  "1": "Gainesville",
  "2": "Tennille",
  "3": "Thomaston",
  "4": "Tifton",
  "5": "Jesup",
  "6": "Cartersville",
  "7": "Chamblee"
}
```

**`02-Data-Staging/config/system_codes.json`**:
```json
{
  "1": "State Highway Route",
  "2": "County Road",
  "3": "City Street",
  "6": "Ramp",
  "7": "Private Road",
  "8": "Public Road",
  "9": "Collector-Distributor"
}
```

### 1.6 Implement `Roadways.py` (RAPTOR category class)

**Class**: `RoadwayData` — reads from `roadway_inventory.db` (not directly from GDB)

**Georgia Route ID System (RCLINK)** — 11-character identifier:
| Chars | Component | Example |
|-------|-----------|---------|
| 1-3 | County Code (odd 001-321) | `123` |
| 4 | Route Type (1=SR, 2=CR, 3=City, 6=Ramp) | `1` |
| 5-8 | Route Number | `0036` |
| 9-10 | Suffix (00-99 or AL,BU,BY,NO,SO,etc.) | `NO` |
| 11 | Direction (I=inventory, D=divided opposite) | `I` |

**Key columns for RAPTOR** (subset of the full DB):
```python
COLUMNS_TO_KEEP = [
    'ROUTE_ID', 'FROM_MEASURE', 'TO_MEASURE', 'unique_id',
    'FUNCTIONAL_CLASS', 'NHS', 'SYSTEM_CODE',
    'AADT', 'AADT_YEAR', 'LANES', 'SPEED_LIMIT',
    'SURFACE_TYPE', 'MEDIAN_TYPE', 'SHOULDER_TYPE',
    'URBAN_CODE', 'DISTRICT', 'COUNTY_CODE', 'COUNTY_NAME',
    'TRUCK_PCT', 'K_FACTOR', 'D_FACTOR',
    'geometry'
]
```

**Processing steps** (in `load_data()`):
1. Read tabular data from `roadway_inventory.db` (already cleaned and indexed)
2. Filter to `SYSTEM_CODE = 1` (State Highway Routes, ~18,000 centerline miles)
3. Load geometry from the source GDB and join by ROUTE_ID + milepoints
4. Filter by district if `district_id` specified
5. Select RAPTOR-relevant columns
6. Reproject to EPSG:32617
7. Assign to `self.GA_RDWY_INV`

### 1.7 Explore and validate
- Verify segment count, CRS, column names
- Check that district filtering works (District 7 = Metro Atlanta)
- Compare column names against data dictionary — document discrepancies
- Check for nulls in critical fields (AADT, LANES, geometry)
- Confirm what share of the full roadway network receives AADT from current GDOT traffic products
- Document all columns in the full dataset (useful for future categories)

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| Road Inventory GDB | GDOT website | Yes | ⬜ Download |
| Road Inventory Excel | GDOT website | Yes | ⬜ Download |
| Traffic GeoDatabase | GDOT website | Yes | ⬜ Download |
| Traffic Tabular | GDOT website | Yes | ⬜ Download |
| Traffic Historical | GDOT website | Yes | ⬜ Download |
| 2010-2019 Published Traffic | GDOT website | Yes | ⬜ Download |
| Data Dictionary PDF | GDOT website | Yes | ⬜ Download |
| GDOT District Boundaries | ITOS UGA FeatureServer | Yes | ⬜ Optional |

---

## Deliverables

### ETL Pipeline
- `02-Data-Staging/scripts/01_roadway_inventory/` — download, normalize, create_db, validate scripts
- `02-Data-Staging/databases/roadway_inventory.db` — complete roadway inventory SQLite database
- `02-Data-Staging/config/` — district_codes.json, county_codes.json, system_codes.json, crs_config.json

### RAPTOR Category Class
- `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py` — `RoadwayData` class that reads from the database and returns a filtered GeoDataFrame as `self.GA_RDWY_INV`

## Verification
- [x] GDB downloads and unpacks without errors
- [x] `roadway_inventory.db` created with staged source columns and load metadata
- [x] Row count validation passes: `622,255` staged rows
- [x] `unique_id` exists and is unique across all staged segments
- [x] CRS validation passes: `EPSG:32617`
- [x] Staged database tables and indexes are implemented through the ETL load step
- [x] `RoadwayData` class is implemented and loads staged roadway geometry for RAPTOR use
- [x] SYSTEM_CODE filtering is supported in `Roadways.py`
- [x] District filtering is supported in `Roadways.py`
- [x] Critical-field null checks pass within the validation thresholds
- [x] Validation script passes all recorded checks in `02-Data-Staging/config/validation_results.json`

## Deferred From Phase 1

- statewide roadway supplementation from TIGER / OSM / alternate GDOT services
- expanded route-family labeling beyond the current system and functional classifications
- any decision to operationalize currently archival-only raw source packages beyond the active ETL inputs
