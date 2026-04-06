# Phase 1 Roadway Data Pipeline

## Purpose

This document explains the current Phase 1 Georgia roadway data pipeline in operational terms. It is intended to serve as the technical reference for how the foundational roadway layer is assembled, what source files are used, what each source contributes, how the staged database is built, and what limitations remain in the current implementation.

Phase 1 is the base layer for later RAPTOR categories. It is not a scoring category by itself. Its job is to create a reliable, queryable statewide roadway foundation with geometry, roadway attributes, district and county boundaries, and as much current and historical traffic coverage as can be matched to the official GDOT network.

Current closeout position:

- this Phase 1 pipeline is treated as complete for the current project scope
- the GDOT-based staged network is accepted as the initial statewide planning baseline
- supplementation remains a separate validation/improvement track rather than part of the closed Phase 1 build

---

## Pipeline Goal

The Phase 1 pipeline produces two primary staged outputs:

- `02-Data-Staging/databases/roadway_inventory.db`
- `02-Data-Staging/spatial/base_network.gpkg`

Together, these provide:

- the statewide roadway segment inventory
- roadway geometry
- joined roadway-inventory attributes from GDOT
- current traffic fields from GDOT 2024 traffic data
- historical traffic fields from GDOT route-segment traffic archives
- county and district boundary layers

The staged outputs are then consumed by:

- `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py`
- the local web application in `04-Webapp/`

---

## Directory Structure

The roadway pipeline follows the project-wide ETL pattern:

```text
01-Raw-Data/         ->  02-Data-Staging/       ->  03-Processed-Data/
(raw source files)       (ETL + staged DB/GPKG)    (future analysis outputs)
```

For Phase 1 specifically:

```text
01-Raw-Data/GA_RDWY_INV/
02-Data-Staging/scripts/01_roadway_inventory/
02-Data-Staging/databases/roadway_inventory.db
02-Data-Staging/spatial/base_network.gpkg
02-Data-Staging/cleaned/roadway_inventory_cleaned.csv
02-Data-Staging/config/
```

---

## Source Files

### Core roadway and traffic source directory

The main raw source directory is:

- `01-Raw-Data/GA_RDWY_INV/`

The download metadata file is:

- `01-Raw-Data/GA_RDWY_INV/download_metadata.json`

That directory contains the GDOT roadway and traffic packages downloaded from:

- `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/`

Downloaded files currently include:

- `Road_Inventory_Geodatabase.zip`
- `Road_Inventory_Excel.zip`
- `Traffic_GeoDatabase.zip`
- `Traffic_Tabular.zip`
- `Traffic_Historical.zip`
- `2010_thr_2019_Published_Traffic.zip`
- `DataDictionary.pdf`

### Extracted and directly used roadway source

The foundational roadway geometry comes from:

- `01-Raw-Data/GA_RDWY_INV/Road_Inventory_2024.gdb`
- layer: `GA_2024_Routes`

This is the canonical route geometry for the staged roadway network.

### Extracted and directly used current traffic source

Current traffic fields come from:

- `01-Raw-Data/GA_RDWY_INV/Traffic_2024_Geodatabase/TRAFFIC_Data_2024.gdb`
- layer: `TRAFFIC_DataYear2024`

This source contributes current AADT and related traffic measures.

### Directly used historical traffic source

Historical route-segment traffic is read from:

- `01-Raw-Data/GA_RDWY_INV/Traffic_Historical.zip`

The current ETL reads historical route-segment traffic records from this archive and aligns them back to the official route geometry by route ID and milepoint interval.

### Official boundary source

County and district polygons currently come from the GDOT-hosted ArcGIS boundary service:

- `https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Boundaries/MapServer/1` for counties
- `https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Boundaries/MapServer/3` for GDOT districts

These are written into `base_network.gpkg` during the Phase 1 build.

---

## What Each Source Contributes

### `GA_2024_Routes`

This layer contributes the base route geometry and core route-level attributes, including:

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

This layer is the backbone of the staged network. All segment geometry in the staged output originates from this source.

### Additional roadway-inventory layers in `Road_Inventory_2024.gdb`

The ETL also joins selected attribute layers from the same GDOT geodatabase:

- `COUNTY_ID`
- `F_SYSTEM`
- `NHS`
- `FACILITY_TYPE`
- `THROUGH_LANES`
- `MEDIAN_TYPE`
- `SHOULDER_TYPE`
- `SURFACE_TYPE`
- `URBAN_ID`

These layers are joined to the base route layer using:

- `ROUTE_ID`
- `BeginPoint`
- `EndPoint`

This is an exact interval-based join within the official roadway-inventory GDB.

### `TRAFFIC_DataYear2024`

The current traffic source contributes:

- `AADTRound`
- `Single_Unit_AADT`
- `Combo_Unit_AADT`
- `Future_AADT`
- `K_Factor`
- `D_Factor`
- `VMT`
- `TruckVMT`
- `Traffic_Class`
- `TC_NUMBER`
- county and district references used during ETL backfilling

These fields are matched to the official GDOT route geometry by:

- `ROUTE_ID`
- `FROM_MILEPOINT`
- `TO_MILEPOINT`

### `Traffic_Historical.zip`

Historical route-segment traffic contributes actual historical AADT and truck-related measures, normalized year by year. The staged dataset currently includes:

- `AADT_2010` through `AADT_2020`
- `TRUCK_AADT_2010` through `TRUCK_AADT_2020`
- `TRUCK_PCT_2010` through `TRUCK_PCT_2020`

Legacy future-projection fields in older historical datasets are intentionally not carried forward. The only canonical future AADT kept in the staged output is the 2024 current-year `FUTURE_AADT` field.

---

## High-Level ETL Flow

The roadway pipeline consists of four main steps:

1. Download and stage raw GDOT source files
2. Normalize the roadway network onto the official `GA_2024_Routes` geometry
3. Write staged tabular and spatial outputs
4. Validate the staged outputs

The key scripts are:

- `02-Data-Staging/scripts/01_roadway_inventory/download.py`
- `02-Data-Staging/scripts/01_roadway_inventory/catalog_columns.py`
- `02-Data-Staging/scripts/01_roadway_inventory/normalize.py`
- `02-Data-Staging/scripts/01_roadway_inventory/create_db.py`
- `02-Data-Staging/scripts/01_roadway_inventory/validate.py`

---

## Detailed Transformation Logic

### 1. Load the canonical roadway geometry

The pipeline starts with `GA_2024_Routes` from `Road_Inventory_2024.gdb`.

This layer is treated as the official statewide network geometry. It is not replaced by traffic geometries. Instead, traffic is matched back to this route layer.

### 2. Join official roadway-inventory attributes

Selected roadway-inventory attribute layers are joined from the same GDB:

- `COUNTY_ID`
- `F_SYSTEM`
- `NHS`
- `FACILITY_TYPE`
- `THROUGH_LANES`
- `MEDIAN_TYPE`
- `SHOULDER_TYPE`
- `SURFACE_TYPE`
- `URBAN_ID`

These joins use exact route-interval matching inside the roadway-inventory GDB.

### 3. Parse route identifiers and derive route attributes

The ETL parses `ROUTE_ID` into component parts and creates route-identity helper fields such as:

- `PARSED_FUNCTION_TYPE`
- `PARSED_COUNTY_CODE`
- `PARSED_SYSTEM_CODE`
- `PARSED_ROUTE_NUMBER`
- `PARSED_SUFFIX`
- `PARSED_DIRECTION`

It also derives standardized fields used downstream:

- `COUNTY_CODE`
- `GDOT_District`
- `DISTRICT`
- `FUNCTIONAL_CLASS`
- `NUM_LANES`
- `URBAN_CODE`
- `NHS_IND`
- `ROUTE_TYPE`
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
- `ROUTE_DIRECTION`

### 4. Load current traffic data

The current traffic dataset is loaded from `TRAFFIC_DataYear2024` and normalized into a route/milepoint interval table.

Important design choice:

- the pipeline does not use the current traffic geometry as the canonical roadway network
- it treats traffic records as interval-based attributes along the official route geometry

### 5. Load historical traffic data

Historical route-segment traffic is loaded from `Traffic_Historical.zip`.

The ETL reads the yearly records, normalizes field names across years, and attempts to resolve route identifiers back to the official route network. Historical records are then treated as interval-based attributes, similar to current traffic.

### 6. Split official routes where traffic intervals change

The route geometry from `GA_2024_Routes` is segmented whenever traffic intervals introduce breakpoints.

The segmentation logic is:

1. take the official route geometry for one route interval
2. collect all relevant breakpoints from:
   - current traffic intervals
   - historical traffic intervals
3. sort those breakpoints along the route
4. cut the official geometry into smaller subsegments between adjacent breakpoints
5. assign the traffic record that fully covers each resulting interval

This means the staged network keeps the official route geometry as the base geometry but subdivides it where traffic changes occur.

### 7. Preserve route-level attributes during segmentation

When a route is split into smaller segments:

- route-level attributes from `GA_2024_Routes` are copied to each child segment
- joined roadway-inventory attributes are also copied to each child segment
- traffic values are then assigned interval by interval

The fields that change during splitting are:

- `FROM_MILEPOINT`
- `TO_MILEPOINT`
- `BeginPoint`
- `EndPoint`
- `geometry`
- `unique_id`

The pipeline therefore does not intentionally discard the selected original network attributes during segmentation. The main limitation is that only the roadway-inventory layers explicitly joined by the ETL are preserved downstream.

### 8. Reproject and compute segment length

After segmentation, the roadway network is reprojected to:

- `EPSG:32617`

The ETL then computes:

- `segment_length_m`
- `segment_length_mi`

### 9. Write staged outputs

The ETL writes:

- `02-Data-Staging/cleaned/roadway_inventory_cleaned.csv`
- `02-Data-Staging/spatial/base_network.gpkg`

The roadway GeoPackage currently contains:

- `roadway_segments`
- `county_boundaries`
- `district_boundaries`

The database load step writes:

- `02-Data-Staging/databases/roadway_inventory.db`

with:

- `segments`
- `load_summary`

---

## Field Lineage Summary

### Directly from `GA_2024_Routes`

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

### Joined from other roadway GDB layers

- `COUNTY_ID`
- `F_SYSTEM`
- `NHS`
- `FACILITY_TYPE`
- `THROUGH_LANES`
- `MEDIAN_TYPE`
- `SHOULDER_TYPE`
- `SURFACE_TYPE`
- `URBAN_ID`

### From current traffic

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

### From historical traffic

- `AADT_2010` through `AADT_2020`
- `TRUCK_AADT_2010` through `TRUCK_AADT_2020`
- `TRUCK_PCT_2010` through `TRUCK_PCT_2020`

### Derived by ETL

- parsed route fields
- recoded route and district fields
- `unique_id`
- `AADT`
- `AADT_YEAR`
- `TRUCK_AADT`
- `TRUCK_PCT`
- `FUTURE_AADT`
- `VMT`
- `TruckVMT`
- `current_aadt_covered`
- `historical_aadt_years_available`
- `segment_length_m`
- `segment_length_mi`

---

## Current Staged Outputs

### SQLite database

File:

- `02-Data-Staging/databases/roadway_inventory.db`

Tables:

- `segments`
- `load_summary`

Current contents:

- `622,255` roadway segment rows
- `94` columns in `segments`

The SQLite database is the staged tabular source of truth. It contains roadway and traffic attributes but no geometry.

### GeoPackage

File:

- `02-Data-Staging/spatial/base_network.gpkg`

Current layers:

- `roadway_segments` (`622,255` features)
- `county_boundaries` (`159` features)
- `district_boundaries` (`7` features)

The GeoPackage is the staged spatial source used by the web application and other geometry-aware workflows.

---

## Current Traffic Coverage

### Current-year coverage

Current AADT coverage in the staged roadway network:

- `185,748` of `622,255` segments have current AADT
- coverage rate by segment count: `29.85%`
- `38,359.71` of `133,992.56` staged segment miles have current AADT
- coverage rate by staged miles: `28.63%`

This is important for interpretation. The staged roadway network is much broader than the traffic-covered subset.

### Historical coverage

The staged network currently includes route-segment historical AADT for:

- `2010`
- `2011`
- `2012`
- `2013`
- `2014`
- `2015`
- `2016`
- `2017`
- `2018`
- `2019`
- `2020`

Notable high-coverage years:

- `2019`: `535,080` segments (`85.99%`)
- `2020`: `548,343` segments (`88.12%`)

### Historical gap

The current staged build does not have route-segment historical traffic for:

- `2021`
- `2022`
- `2023`

Important clarification:

- `2020` is present in the current staged build
- the missing route-segment years are `2021-2023`

The current documentation should therefore refer to the gap as `2021-2023`, not `2020-2023`

---

## Roadway Classification Available in Phase 1

The staged network currently supports several distinct classification concepts.

### 1. System / ownership classification

Field:

- `SYSTEM_CODE`

Documented GDOT system codes include:

- `1` = State Highway Route
- `2` = County Road
- `3` = City Street
- `6` = Ramp
- `7` = Private Road
- `8` = Public Road
- `9` = Collector-Distributor

Current values actually present in the staged Phase 1 build:

- `SYSTEM_CODE = 1`: `109,314` segments
- `SYSTEM_CODE = 2`: `512,941` segments

This means the current statewide staged network is mostly composed of state highway and county road records in the current build.

### 2. Functional classification

Fields:

- `F_SYSTEM`
- `FUNCTIONAL_CLASS`

Current values present in the staged build:

- `1`
- `2`
- `3`
- `4`
- `5`
- `6`
- `7`

This is the most direct hierarchy for roadway functional class in the staged data, but the current project documentation does not yet include a finalized descriptive label table for all GDOT functional-class codes.

### 3. Route-family parsing

Fields:

- `PARSED_FUNCTION_TYPE`
- `PARSED_SYSTEM_CODE`
- `ROUTE_TYPE`
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
- `ROUTE_DIRECTION`

These fields are useful for route interpretation and can support a more explicit route-family classification later.

### 4. Current limitation

Phase 1 currently has:

- a clear system classification
- a clear functional classification
- route-identity parsing

Phase 1 does not yet have a finalized, project-documented crosswalk that cleanly labels each route as:

- Interstate
- U.S. Route
- State Route
- county road
- city street
- ramp
- other route-family categories

If that route-family classification is needed for reporting or downstream scoring, it should be added as an explicit follow-on enhancement.

---

## Boundaries in the Current Build

### County boundaries

The county layer currently includes:

- `COUNTYFP`
- `NAME`
- `GDOT_DISTRICT`
- `DISTRICT_NAME`
- `DISTRICT_LABEL`

This allows counties to be filtered and labeled consistently with district metadata.

### District boundaries

The district layer currently includes:

- `GDOT_DISTRICT`
- `DISTRICT_NAME`
- `DISTRICT_LABEL`

District names have been normalized to a consistent format:

- `District 1 - Gainesville`
- `District 2 - Tennille`
- `District 3 - Thomaston`
- `District 4 - Tifton`
- `District 5 - Jesup`
- `District 6 - Cartersville`
- `District 7 - Chamblee`

---

## Web Application Use of the Staged Outputs

The local web application uses the staged outputs in `02-Data-Staging`.

### Roadway segments

The staged roadway path reads:

- tabular detail from `roadway_inventory.db`
- geometry from `base_network.gpkg`, layer `roadway_segments`

### Boundary layers

The staged application path now also reads:

- `base_network.gpkg`, layer `county_boundaries`
- `base_network.gpkg`, layer `district_boundaries`

The backend exposes these through boundary endpoints, and the frontend map renders them as map layers in staged mode.

The default backend setting is:

- `data_mode = "staged"`

which means the staged GPKG/SQLite path is the normal local-development path unless the app is intentionally switched to another data mode.

---

## Source Packages Downloaded but Not Fully Used in Current ETL

The raw download step captures all GDOT files in the directory, but not every downloaded package is currently used directly by the normalization workflow.

Downloaded but not central to the current normalization path:

- `Road_Inventory_Excel.zip`
- `Traffic_Tabular.zip`
- `2010_thr_2019_Published_Traffic.zip`

Current role of these files:

- archival / reference value
- schema inspection
- alternate tabular delivery formats
- potential future ETL enhancements

Current direct ETL inputs remain:

- `Road_Inventory_2024.gdb`
- `TRAFFIC_Data_2024.gdb`
- `Traffic_Historical.zip`
- GDOT boundary service

---

## Known Limitations and Open Questions

### 1. Traffic coverage is partial

Only about `29.85%` of staged segments currently have current AADT. This is not necessarily an ETL error. It reflects the fact that the full staged roadway network is broader than the subset of routes with matched current traffic coverage.

### 2. Historical route-segment gap for 2021-2023

The staged network currently lacks route-segment historical traffic records for `2021-2023`.

### 3. Not every roadway-inventory layer is preserved

The staged network retains only the base route layer plus the roadway-inventory attribute layers explicitly joined by the ETL. Other layers in the roadway GDB are not automatically carried forward.

### 4. Route-family classification is not fully documented

The project currently has system and functional classification, but not a finished descriptive crosswalk for a full Interstate / U.S. Route / State Route taxonomy.

### 5. Base-network coverage versus commercial basemap coverage

The staged roadway layer is based on the official GDOT route inventory. That is a defensible official source, but it may still omit some local-road detail visible in commercial or consumer-facing basemaps. That is a known coverage question and should be evaluated separately from the core Phase 1 data-pipeline implementation.

Current practical note:

- a `2026-04-04` Playwright visual re-check of the fully loaded local web app found no obvious planning-relevant roadway gaps in sampled Columbus / Dinglewood, Atlanta, and Savannah views
- this reduces the urgency of statewide supplementation for immediate planning use, though it does not eliminate the possibility of localized future gap-fill work

---

## Current Assessment

Phase 1 is complete and usable as the foundation for downstream work.

Closed with Phase 1:

- statewide staged roadway ETL
- staged SQLite and GeoPackage outputs
- county and district boundaries in the staged build
- RAPTOR `RoadwayData` loader
- documented validation results
- visual confirmation that the staged web map is adequate for initial planning use

Deferred beyond Phase 1:

- statewide roadway supplementation from TIGER / OSM / alternate GDOT services unless later QA shows a planning-relevant omission pattern
- richer route-family classification documentation if needed by downstream scoring or reporting
- expanded use of archival/reference raw packages that are not central to the current normalization path
