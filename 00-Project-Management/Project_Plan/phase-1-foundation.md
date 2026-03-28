# Phase 1 — Roadways Base Layer + Project Setup

> **RAPTOR context**: This phase builds the `RoadwayData` class (`states/Georgia/categories/Roadways.py`). This is not a scored category — it provides the foundation data that all 6 scored categories (Asset Preservation, Safety, Mobility, Connectivity, Freight, Socioeconomic) process.

## Goal
Download the complete Georgia roadway inventory, clean and normalize it into a SQLite database, then build the `RoadwayData` class that loads from it. This database becomes the foundational source of truth for all road segment data.

## Status: Not Started

---

## Key Differences from Texas Pipeline

> **No yearly snapshots**: GDOT publishes only a single rolling/current Road Inventory GDB — no archived annual versions (unlike TxDOT which has `{YEAR}_Roadway_Inventory.gdb`). We must document the download date and begin archiving snapshots ourselves.
>
> **No design AADT**: Texas has `AADT_DESGN` (20-year projection) built into the GDB, used for 2050 traffic forecasting. Georgia does not appear to have this field. Workaround is to compute growth rates from historic AADT data (see Phase 4).
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

### 1.2a Catalog all GDB columns
After download, run a column inventory script to document every field in the GDB. This resolves many "need to verify" items from the Texas comparison:
- Truck fields: `AADT_TRUCKS`, `PCT_SADT`, `PCT_CADT`, `TRK_DHV_PCT` equivalents?
- DVMT field?
- System flags: evacuation route, NHFN, freight network?
- Design AADT (`AADT_DESGN` equivalent)?
- Output: `02-Data-Staging/config/gdb_column_inventory.json`

### 1.3 Install Python dependencies
```
geopandas, pyogrio, shapely, pandas, numpy, pyarrow, tqdm, python-dotenv, openpyxl, scikit-learn
```

### 1.4 Build ETL scripts for Roadway Inventory

**`02-Data-Staging/scripts/01_roadway_inventory/normalize.py`**:
1. Load full GDB with `gpd.read_file(..., engine='pyogrio', use_arrow=True)`
2. Document ALL columns and their types (don't filter yet — keep the complete dataset)
3. Clean column names (standardize case, remove spaces)
4. Parse RCLINK route IDs into component fields (county, route type, number, suffix, direction)
5. Build `unique_id`: `{ROUTE_ID}_{FROM_MEASURE:.3f}_{TO_MEASURE:.3f}`
6. Compute segment length in miles from geometry
7. Reproject to `EPSG:32617` (UTM Zone 17N)
8. Export cleaned data as CSV/GeoPackage to `02-Data-Staging/cleaned/`

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
- Document all columns in the full dataset (useful for future categories)

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| Road Inventory GDB | GDOT website | Yes | ⬜ Download |
| Data Dictionary PDF | GDOT website | Yes | ⬜ Download |
| GDOT District Boundaries | ITOS UGA FeatureServer | Yes | ⬜ Optional |

---

## Deliverables

### ETL Pipeline
- `02-Data-Staging/scripts/01_roadway_inventory/` — download, normalize, create_db, validate scripts
- `02-Data-Staging/databases/roadway_inventory.db` — complete roadway inventory SQLite database
- `02-Data-Staging/config/` — district_codes.json, county_codes.json, system_codes.json, crs_config.json

### RAPTOR Category Class
- `scripts/states/Georgia/categories/Roadways.py` — `RoadwayData` class that reads from the database and returns a filtered GeoDataFrame as `self.GA_RDWY_INV`

## Verification
- [ ] GDB downloads and unpacks without errors
- [ ] `roadway_inventory.db` created with all columns from source
- [ ] Row count matches source GDB
- [ ] unique_id is unique across all segments
- [ ] CRS is EPSG:32617 after reprojection
- [ ] Indexes created on key columns
- [ ] `RoadwayData` class loads from DB successfully
- [ ] SYSTEM_CODE=1 filter gives ~18,000-27,000 segments
- [ ] District 7 filter returns Metro Atlanta area only
- [ ] No critical columns have >50% nulls
- [ ] Validation script passes all checks
