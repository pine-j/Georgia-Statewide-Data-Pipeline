# Phase 1 — Roadways Base Layer + Project Setup

> **RAPTOR context**: This phase builds the `RoadwayData` class (`states/Georgia/categories/Roadways.py`). This is not a scored category — it provides the foundation data that all 6 scored categories (Asset Preservation, Safety, Mobility, Connectivity, Freight, Socioeconomic) process.

## Goal
Download the complete Georgia roadway inventory, clean and normalize it into a SQLite database, then build the `RoadwayData` class that loads from it. This database becomes the foundational source of truth for all road segment data.

## Status: Complete

## Current Implementation Snapshot

Phase 1 is implemented, validated, and ready to treat as the closed roadway-foundation phase for the current project scope. The core ETL, staged database, staged GeoPackage, official boundary layers, `RoadwayData` loader, staged web-app data path, Georgia-specific `ROUTE_ID` route-family crosswalk, HPMS-based signed-route verification, posted speed limit enrichment, FHWA HPMS 2024 enrichment, and multi-source AADT fill chain are all working. `82/82` validation checks pass. Remaining questions about supplemental roadway sources and optional GDOT live-layer corroboration are deferred follow-on improvements, not Phase 1 blockers.

As of the current staged build:
- `roadway_inventory.db` contains `244,904` segmented roadway records with `128` columns
- `base_network.gpkg` contains:
  - `roadway_segments` (`244,904` features)
  - `county_boundaries` (`159` features)
  - `district_boundaries` (`7` features)
- Boundary layers are sourced from the official GDOT-hosted `GDOT_Boundaries` service and are now consumed by the staged web-app path

Current traffic coverage in the staged roadway network:
- Current AADT (`AADT` / `AADT_2024`) is available on `244,819` of `244,904` segments (`99.97%`) via five-tier fill chain
- Future AADT 2044 is available on `52,236` of `244,904` segments (`21.3%`)
- Historical AADT columns (2010-2020) have been removed from pipeline output; raw source files retained in `01-Raw-Data/`

Related exploratory note:

- [Roadway Supplement Options](../Assessment_and_Options/roadway-supplement-options.md)

Related exploratory memo:
- [Roadway Gap-Fill Exploratory Analysis](../Assessment_and_Options/roadway-gap-fill-options.md)
- [Georgia Route-Family Classification Strategy](../Assessment_and_Options/2026-04-07-georgia-route-family-classification-strategy.md)
- [Georgia Signed-Route Verification Strategy](../Assessment_and_Options/2026-04-07-georgia-signed-route-verification-strategy.md)

Official Georgia sources used for the roadway base layer and route-family crosswalk:

- GDOT Road & Traffic Data: `https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx`
- GDOT Understanding Route IDs: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/UnderstandingRouteIDs_Doc.pdf`
- GDOT Road Inventory Data Dictionary: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf`
- GDOT live LRS metadata: `https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Network_LRSN/MapServer/exts/LRSServer/layers`

Current working note:

- A `2026-04-04` Playwright re-check of the local web app after full roadway load found no obvious planning-relevant network gaps in sampled Columbus / Dinglewood, Atlanta, and Savannah views.
- Phase 1 should therefore proceed on the assumption that the current GDOT-based staged network is good enough for initial statewide planning and prototype scoring.
- Supplemental TIGER / OSM / additional GDOT gap-fill work remains valuable as a validation and improvement track, but it is not currently treated as a Phase 1 blocker.

Phase 1 closeout decision:

- Close Phase 1 using the current GDOT-based staged roadway network and documented validation results.
- Defer roadway supplementation, optional GDOT live-layer corroboration, and any optional archival-source integration to later targeted work only if downstream QA or scoring needs justify it.

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
      download_signed_route_references.py   # Snapshot official GDOT signed-route verifier layers
      route_family.py                # Georgia ROUTE_ID family crosswalk helper
      route_verification.py          # Official GDOT signed-route verification (GPAS layers)
      rnhp_enrichment.py             # RNHP enrichment (speed zones)
      download_rnhp_enrichment.py    # Download RNHP enrichment layers
      create_db.py                   # Load into SQLite
      validate.py                    # Verify row counts, nulls, CRS
    requirements.txt                 # Shared Python dependencies
  config/
    crs_config.json                  # {"georgia": "EPSG:32617"}
    district_codes.json              # 7 GDOT districts
    county_codes.json                # 159 GA counties
    system_codes.json                # Route system codes
    georgia_route_family_crosswalk.json   # GDOT-based Interstate / US / State / Local rule tables
    georgia_signed_route_verification_sources.json   # GDOT GPAS signed-route reference layer config
    rnhp_enrichment_sources.json     # RNHP enrichment layer config (speed zones)
  databases/                         # Per-dataset SQLite DBs (gitignored)
    roadway_inventory.db
  spatial/                           # Themed GeoPackage files (gitignored)
    base_network.gpkg                # Layers: roadway_segments, district_boundaries, county_boundaries
```

### 1.2 Download Georgia Road Inventory GDB
- **Source**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/Road_Inventory_Geodatabase.zip` (~492 MB, Jul 2025 version)
- **Data Dictionary**: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf`
- **Place in**: `01-Raw-Data/Roadway-Inventory/` (raw, unmodified)
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
- **Action**: Download every file in the directory into `01-Raw-Data/Roadway-Inventory/` and record the source URLs, directory timestamps, and file sizes in `download_metadata.json`
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
4. Parse GDOT 16-character `ROUTE_ID` values into component fields (function type, county, system code, route code, suffix, direction)
5. Apply the Georgia route-family crosswalk to derive `BASE_ROUTE_NUMBER`, `ROUTE_SUFFIX_LABEL`, `ROUTE_FAMILY`, `ROUTE_FAMILY_DETAIL`, `ROUTE_FAMILY_CONFIDENCE`, and `ROUTE_FAMILY_SOURCE`
6. Build `unique_id`: `{ROUTE_ID}_{FROM_MEASURE:.3f}_{TO_MEASURE:.3f}`
7. Compute segment length in miles from geometry
8. Reproject to `EPSG:32617` (UTM Zone 17N)
9. Join or map in traffic attributes from GDOT traffic products where a defensible relationship exists
10. Export cleaned data as CSV/GeoPackage to `02-Data-Staging/cleaned/`

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
- Georgia route-family fields:
  `BASE_ROUTE_NUMBER`, `ROUTE_SUFFIX_LABEL`, `ROUTE_FAMILY`,
  `ROUTE_FAMILY_DETAIL`, `ROUTE_FAMILY_CONFIDENCE`,
  `ROUTE_FAMILY_SOURCE`
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

### 1.4b Georgia Route-Family Classification in the Staged Data

The staged roadway network currently supports multiple kinds of classification, but they answer different questions:

**System / ownership classification**:
- Field: `SYSTEM_CODE`
- Current values present in the staged build:
  - `1` = State Highway Route
  - `2` = Public Road
- Current segment counts:
  - `SYSTEM_CODE = 1`: `18,499` segments
  - `SYSTEM_CODE = 2`: `226,405` segments
- Official GDOT LRS metadata also documents:
  - `3` = Private
  - `4` = Federal
- The current staged build only contains `1` and `2`

**Functional classification**:
- Source field: `F_SYSTEM`
- Derived field: `FUNCTIONAL_CLASS`
- Current values present in the staged build:
  - `1` through `7` in the GDOT roadway inventory
- This remains the clearest statewide arterial / collector / local-road hierarchy and should not be replaced by route-family labels

**Route identity / route-family parsing**:
- Existing fields derived from `ROUTE_ID`:
  - `PARSED_FUNCTION_TYPE`
  - `PARSED_SYSTEM_CODE`
  - `ROUTE_TYPE`
  - `ROUTE_NUMBER`
  - `ROUTE_SUFFIX`
  - `ROUTE_DIRECTION`
- New Georgia route-family fields:
  - `BASE_ROUTE_NUMBER`
  - `ROUTE_SUFFIX_LABEL`
  - `ROUTE_FAMILY`
  - `ROUTE_FAMILY_DETAIL`
  - `ROUTE_FAMILY_CONFIDENCE`
  - `ROUTE_FAMILY_SOURCE`
- Official Georgia classification priority:
  - `Interstate`
  - `U.S. Route`
  - `State Route`
  - `Local/Other`
- Detailed rules and source notes are documented in:
  - [Georgia Route-Family Classification Strategy](../Assessment_and_Options/2026-04-07-georgia-route-family-classification-strategy.md)

**Current closeout position on classification**:
- Phase 1 now has:
  - a clear system classification (`SYSTEM_CODE`)
  - a clear functional classification (`F_SYSTEM` / `FUNCTIONAL_CLASS`)
  - a documented Georgia-specific route-family crosswalk grounded in GDOT route-ID documentation
- Important limitation:
  - `U.S. Route` versus `State Route` remains a medium-confidence interpretation because Georgia `ROUTE_ID` values encode state route numbers and concurrency can still exist
- Use `FUNCTION_TYPE`, `F_SYSTEM`, `NHS`, and `STRAHNET` as separate dimensions rather than forcing those concepts into a single route-family field

**Signed-route verification (operational)**:
- Current ETL implementation:
  - initializes signed-route verification fields from the existing `ROUTE_ID` crosswalk
  - uses FHWA HPMS `routesigning` codes as the operational verifier for `Interstate`, `U.S. Route`, and `State Route`
  - falls back to the baseline crosswalk only where HPMS does not provide a signed-route classification
- Proposed staged verification fields:
  - `SIGNED_INTERSTATE_FLAG`
  - `SIGNED_US_ROUTE_FLAG`
  - `SIGNED_STATE_ROUTE_FLAG`
  - `SIGNED_ROUTE_FAMILY_PRIMARY`
  - `SIGNED_ROUTE_FAMILY_ALL`
  - `SIGNED_ROUTE_VERIFY_SOURCE`
  - `SIGNED_ROUTE_VERIFY_METHOD`
  - `SIGNED_ROUTE_VERIFY_CONFIDENCE`
  - `SIGNED_ROUTE_VERIFY_SCORE`
  - `SIGNED_ROUTE_VERIFY_NOTES`
- Current verification coverage:
  - HPMS `routesigning` coverage on `223,136` segments
  - `SIGNED_ROUTE_FAMILY_PRIMARY` distribution:
    - `Interstate`: `3,659`
    - `U.S. Route`: `10,169`
    - `State Route`: `4,671`
    - `Local/Other`: `226,405`
- Detailed design:
  - [Georgia Signed-Route Verification Strategy](../Assessment_and_Options/2026-04-07-georgia-signed-route-verification-strategy.md)

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
  "2": "Public",
  "3": "Private",
  "4": "Federal"
}
```

### 1.6 Implement `Roadways.py` (RAPTOR category class)

**Class**: `RoadwayData` — reads from `roadway_inventory.db` (not directly from GDB)

**Georgia Route ID System (`ROUTE_ID`)** — 16-character identifier:
| Chars | Component | Example |
|-------|-----------|---------|
| 1 | Function Type | `1` |
| 2-4 | County Code | `000` |
| 5 | System Code | `1` |
| 6-11 | Route Code | `000401` |
| 12-13 | Suffix | `00` |
| 14-16 | Direction | `INC` |

Example: `1000100040100INC` = state-system mainline route code `000401` in increasing inventory direction.

Important note:

- For `FUNCTION_TYPE` `2` through `4`, GDOT uses a special route-code layout where digits `6` through `8` are reference post and digits `9` through `11` are the underlying route number.

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
7. Assign to `self.Roadway_Inventory`

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
- `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py` — `RoadwayData` class that reads from the database and returns a filtered GeoDataFrame as `self.Roadway_Inventory`

## Verification
- [x] GDB downloads and unpacks without errors
- [x] `roadway_inventory.db` created with staged source columns and load metadata
- [x] Row count validation passes: `244,904` staged rows
- [x] `unique_id` exists and is unique across all staged segments
- [x] CRS validation passes: `EPSG:32617`
- [x] Staged database tables and indexes are implemented through the ETL load step
- [x] `RoadwayData` class is implemented and loads staged roadway geometry for RAPTOR use
- [x] SYSTEM_CODE filtering is supported in `Roadways.py`
- [x] District filtering is supported in `Roadways.py`
- [x] Georgia-specific route-family fields are implemented and staged
- [x] Signed-route verification is operational via FHWA HPMS `routesigning` codes (`223,136` segments with signed-route coverage)
- [x] Speed limit enrichment is operational via GDOT GPAS SpeedZone OnSystem (102,335 segments)
- [x] Critical-field null checks pass within the validation thresholds
- [x] Validation script passes all recorded checks in `02-Data-Staging/config/validation_results.json`

## Deferred From Phase 1

- statewide roadway supplementation from TIGER / OSM / alternate GDOT services
- optional GDOT live-layer corroboration against `egisp.dot.ga.gov` or similar official services
- any decision to operationalize currently archival-only raw source packages beyond the active ETL inputs
