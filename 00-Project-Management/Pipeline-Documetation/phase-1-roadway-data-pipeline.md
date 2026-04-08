# Phase 1 Roadway Data Pipeline

## Purpose

This document explains the current Phase 1 Georgia roadway data pipeline in operational terms. It is intended to serve as the technical reference for how the foundational roadway layer is assembled, what source files are used, what each source contributes, how the staged database is built, and what limitations remain in the current implementation.

Phase 1 is the base layer for later RAPTOR categories. It is not a scoring category by itself. Its job is to create a reliable, queryable statewide roadway foundation with geometry, roadway attributes, district and county boundaries, and as much current and historical traffic coverage as can be matched to the official GDOT network.

Current closeout position:

- this Phase 1 pipeline is treated as complete for the current project scope
- the GDOT-based staged network is accepted as the initial statewide planning baseline
- supplementation remains a separate validation/improvement track rather than part of the closed Phase 1 build

Official Georgia sources used for the roadway base layer, route-family crosswalk, and enrichment:

- GDOT Road & Traffic Data: `https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx`
- GDOT Understanding Route IDs: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/UnderstandingRouteIDs_Doc.pdf`
- GDOT Road Inventory Data Dictionary: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf`
- GDOT live LRS metadata: `https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Network_LRSN/MapServer/exts/LRSServer/layers`
- GDOT GPAS Interstates: `https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/7`
- GDOT GPAS US Routes: `https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/6`
- GDOT GPAS SpeedZone OnSystem: `https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/10`
- FHWA HPMS Georgia 2024: `https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_GA_2024/FeatureServer/0`

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
- future AADT projection (2044) from GDOT and HPMS sources
- official signed-route verification from GDOT GPAS layers
- posted speed limits from GDOT SpeedZone OnSystem permits
- county and district boundary layers

Note: historical AADT (2010-2020) has been removed from pipeline output to produce a cleaner network. Raw historical source files are retained in `01-Raw-Data/` for future use.

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

### Historical traffic source (archived, not in pipeline output)

Historical route-segment traffic files are available at:

- `01-Raw-Data/GA_RDWY_INV/Traffic_Historical.zip`

These files are retained for future use but are no longer loaded into the pipeline output. Removing historic traffic breakpoints reduced the segment count from 622,255 to 244,904, producing a cleaner network that segments only on current-year traffic intervals.

### Official signed-route reference snapshots

The signed-route verification downloads official GDOT GPAS reference layers to:

- `01-Raw-Data/GA_RDWY_INV/signed_route_references/interstates.geojson`
- `01-Raw-Data/GA_RDWY_INV/signed_route_references/us_highway.geojson`

These snapshots are cached locally and only re-downloaded when `download_signed_route_references.py` is run with `refresh=True`.

### RNHP enrichment snapshots

The speed zone enrichment downloads from GDOT GPAS to:

- `01-Raw-Data/GA_RDWY_INV/rnhp_enrichment/speed_zone_on_system.geojson`

This snapshot is cached locally and only re-downloaded when `download_rnhp_enrichment.py` is run with `refresh=True`.

### FHWA HPMS 2024 data

The HPMS (Highway Performance Monitoring System) dataset is GDOT's annual submission to FHWA. It uses the same GDOT `ROUTE_ID` and milepoint system as our base network, enabling direct interval-overlap matching without spatial joins.

Source: `https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_GA_2024/FeatureServer/0`

Downloaded snapshot:

- `01-Raw-Data/GA_RDWY_INV/HPMS_2024/hpms_ga_2024_tabular.json`

Key finding: HPMS AADT values are 99.7% identical to GDOT official values where both sources have data, confirming HPMS is the same GDOT data repackaged for federal reporting. HPMS covers 498,214 of 499,372 segments with AADT (99.8% coverage), compared to the GDOT traffic GDB which only attaches AADT to 185,748 of our 622,255 segments.

HPMS contributes:

- AADT gap-fill for segments not covered by the GDOT traffic GDB
- Pavement condition: IRI, PSR, rutting, cracking percent
- Safety attributes: access control, terrain type
- Route signing labels

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
- `LANE_WIDTH`
- `MEDIAN_TYPE`
- `MEDIAN_WIDTH`
- `SHOULDER_TYPE`
- `SHOULDER_WIDTH_L`
- `SHOULDER_WIDTH_R`
- `OWNERSHIP`
- `STRAHNET`
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
- `02-Data-Staging/scripts/01_roadway_inventory/download_signed_route_references.py`
- `02-Data-Staging/scripts/01_roadway_inventory/download_rnhp_enrichment.py`
- `02-Data-Staging/scripts/01_roadway_inventory/normalize.py`
- `02-Data-Staging/scripts/01_roadway_inventory/route_family.py`
- `02-Data-Staging/scripts/01_roadway_inventory/route_verification.py`
- `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py`
- `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py`
- `02-Data-Staging/scripts/01_roadway_inventory/audit_current_aadt_coverage.py`
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
- `LANE_WIDTH`
- `MEDIAN_TYPE`
- `MEDIAN_WIDTH`
- `SHOULDER_TYPE`
- `SHOULDER_WIDTH_L`
- `SHOULDER_WIDTH_R`
- `OWNERSHIP`
- `STRAHNET`
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
- `BASE_ROUTE_NUMBER`
- `ROUTE_SUFFIX_LABEL`
- `ROUTE_FAMILY`
- `ROUTE_FAMILY_DETAIL`
- `ROUTE_FAMILY_CONFIDENCE`
- `ROUTE_FAMILY_SOURCE`

The route-family crosswalk is Georgia-specific and grounded in GDOT
`ROUTE_ID` documentation plus GDOT Interstate and U.S.-route appendix tables.

### 4. Load current traffic data

The current traffic dataset is loaded from `TRAFFIC_DataYear2024` and normalized into a route/milepoint interval table.

Important design choice:

- the pipeline does not use the current traffic geometry as the canonical roadway network
- it treats traffic records as interval-based attributes along the official route geometry

### 5. Split official routes where traffic intervals change

The route geometry from `GA_2024_Routes` is segmented whenever current-year traffic intervals introduce breakpoints.

The segmentation logic is:

1. take the official route geometry for one route interval
2. collect all relevant breakpoints from current traffic intervals
3. sort those breakpoints along the route
4. cut the official geometry into smaller subsegments between adjacent breakpoints
5. assign the traffic record that fully covers each resulting interval

This means the staged network keeps the official route geometry as the base geometry but subdivides it where current traffic changes occur. Historical traffic breakpoints are no longer used for segmentation.

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

### 8. Apply official signed-route verification

After segmentation, the ETL initializes signed-route verification fields from
the baseline `ROUTE_ID` crosswalk and then runs an official-source
verification pass using GDOT GPAS layers.

Official reference sources (from `rnhp.dot.ga.gov`):

- GPAS/7 `GDOT Interstates` (22 features) — maps `ROUTE_ID` + milepoint intervals to `INTERSTATE_NAME`
- GPAS/6 `US Route` (674 features) — maps `ROUTE_ID` + milepoint intervals to `US_ROUTE_NUM`

Verification method:

- match segments to official references by `ROUTE_ID` base (first 13 characters, direction-stripped) plus milepoint interval overlap
- also try derived 10-character `RCLINK` candidates as fallback
- segments matching an official reference are upgraded to `high` confidence with source `gdot_interstates` or `gdot_us_highway`
- segments without an official match retain the baseline `route_id_crosswalk` values

Current verification results:

- `1,074` segments verified by `gdot_interstates`
- `5,516` segments verified by `gdot_us_highway`
- `238,314` segments retain baseline `route_id_crosswalk`

Note: State Route signed labels are not available from GPAS. The GDOT ArcWeb
`State Routes` layer on `egisp.dot.ga.gov` has the data but its query endpoint
returns HTTP 500 errors. State Route classification remains at medium confidence
via the route-family crosswalk fallback.

Current signed-route verification fields:

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

If the official references are unavailable at runtime, the ETL keeps the
baseline crosswalk-derived values and logs a warning rather than failing hard.

### 8a. Apply RNHP enrichment (speed zones)

After signed-route verification, the ETL enriches state highway segments with
posted speed limit data from the GDOT GPAS SpeedZone OnSystem layer.

Source: `https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/10`

Matching method:

- filter to active speed zone records (`RECORD_STATUS_CD = 'ACTV'`)
- match by `RNH_ROUTE_ID` base (first 13 characters) plus statewide milepoint interval overlap
- when multiple speed zones cover a segment, take the best-covering (longest overlap)

Current enrichment results:

- `14,766` segments with posted speed limits
- `494` school zone segments flagged

Speed limit distribution:

- 25 mph: `484` segments
- 35 mph: `1,352` segments
- 45 mph: `1,836` segments
- 55 mph: `8,316` segments (most common)
- 65 mph: `940` segments
- 70 mph: `629` segments

Current enrichment fields:

- `SPEED_LIMIT`
- `IS_SCHOOL_ZONE`
- `SPEED_LIMIT_SOURCE`

### 8b. Apply HPMS 2024 enrichment

After speed zone enrichment, the ETL joins FHWA HPMS 2024 data to fill AADT
gaps and add pavement/safety attributes.

Source: `https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_GA_2024/FeatureServer/0`

HPMS uses the same GDOT `ROUTE_ID` and milepoint system. Matching is done by
direct `ROUTE_ID` + milepoint interval overlap — no spatial matching needed.

AADT gap-fill priority chain (each step only fills segments not yet covered):

1. **GDOT official exact** (44,983 segments) — direct traffic GDB match, confidence `high`
2. **HPMS 2024** (196,247 segments) — FHWA HPMS route_id + milepoint match, confidence `medium`
3. **Direction mirror** (3,091 segments) — INC→DEC copy for all routes, confidence `high`
4. **Analytical gap-fill** (498 segments) — interpolation + nearest-neighbor on state highways, confidence `medium`

Final 2024 AADT coverage: 244,819 / 244,904 segments (99.97%).

Future AADT 2044 fill chain uses the same multi-source pattern:

1. **GDOT official** — direct FUTURE_AADT from GDOT traffic GDB
2. **HPMS future_aadt** — FHWA HPMS future AADT values
3. **Direction mirror** — INC→DEC copy
4. **Interpolation** — linear interpolation on state highways
5. **Nearest-neighbor** — nearest covered segment on same route (20-mile cap)

Current Future AADT 2044 coverage: 52,236 / 244,904 segments (21.3%).

HPMS also contributes pavement and safety attributes:

- `HPMS_IRI` — International Roughness Index (10,410 segments)
- `HPMS_RUTTING` — pavement rutting depth (9,591 segments)
- `HPMS_CRACKING_PCT` — pavement cracking percent (10,410 segments)
- `HPMS_ACCESS_CONTROL` — access control type (10,519 segments)
- `HPMS_TERRAIN_TYPE` — terrain classification (891 segments)
- `HPMS_ROUTE_SIGNING` — route signing type (223,136 segments)
- `HPMS_ROUTE_NUMBER` — signed route number
- `HPMS_ROUTE_NAME` — road name from HPMS

### 9. Reproject and compute segment length

After segmentation, the roadway network is reprojected to:

- `EPSG:32617`

The ETL then computes:

- `segment_length_m`
- `segment_length_mi`

### 10. Write staged outputs

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
- `LANE_WIDTH`
- `MEDIAN_TYPE`
- `MEDIAN_WIDTH`
- `SHOULDER_TYPE`
- `SHOULDER_WIDTH_L`
- `SHOULDER_WIDTH_R`
- `OWNERSHIP`
- `STRAHNET`
- `SURFACE_TYPE`
- `URBAN_ID`

### From current traffic

- `AADT_2024`
- `AADT_2024_OFFICIAL`
- `SINGLE_UNIT_AADT_2024`
- `COMBO_UNIT_AADT_2024`
- `FUTURE_AADT_2024`
- `K_FACTOR`
- `D_FACTOR`
- `VMT_2024`
- `TRUCK_VMT_2024`
- `TRAFFIC_CLASS_2024`
- `TC_NUMBER`

### Future AADT provenance fields

- `FUTURE_AADT_2044` — canonical 20-year projection AADT
- `FUTURE_AADT_2044_OFFICIAL` — direct GDOT traffic match only, never overwritten
- `FUTURE_AADT_2044_SOURCE` — `official_exact`, `hpms_2024`, `direction_mirror`, `analytical_gap_fill`, `nearest_neighbor`, or `missing`
- `FUTURE_AADT_2044_CONFIDENCE` — `high`, `medium`, or `low`
- `FUTURE_AADT_2044_FILL_METHOD` — method used for non-official values
- `future_aadt_covered` — boolean: has any FUTURE_AADT_2044

### From GDOT GPAS signed-route verification

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

### From GDOT GPAS SpeedZone OnSystem

- `SPEED_LIMIT`
- `IS_SCHOOL_ZONE`
- `SPEED_LIMIT_SOURCE`

### From FHWA HPMS 2024

- `HPMS_IRI`
- `HPMS_PSR`
- `HPMS_RUTTING`
- `HPMS_CRACKING_PCT`
- `HPMS_ACCESS_CONTROL`
- `HPMS_TERRAIN_TYPE`
- `HPMS_ROUTE_SIGNING`
- `HPMS_ROUTE_NUMBER`
- `HPMS_ROUTE_NAME`

### AADT provenance fields

- `AADT_2024` — canonical 2024 AADT (official, HPMS, or estimated)
- `AADT_2024_OFFICIAL` — direct GDOT traffic match only, never overwritten
- `AADT_2024_SOURCE` — `official_exact`, `hpms_2024`, `direction_mirror`, `analytical_gap_fill`, `nearest_neighbor`, or `missing`
- `AADT_2024_CONFIDENCE` — `high`, `medium`, or `low`
- `AADT_2024_FILL_METHOD` — method used for non-official values
- `current_aadt_official_covered` — boolean: has direct GDOT match
- `current_aadt_covered` — boolean: has any AADT_2024

### Derived by ETL

- parsed route fields
- recoded route and district fields
- decoded label fields for staged coded attributes
- route-family fields
- `unique_id`
- `AADT`
- `AADT_2024_SOURCE`
- `AADT_2024_CONFIDENCE`
- `AADT_2024_FILL_METHOD`
- `AADT_YEAR`
- `TRUCK_AADT`
- `TRUCK_PCT`
- `FUTURE_AADT`
- `FUTURE_AADT_2044`
- `FUTURE_AADT_2044_OFFICIAL`
- `FUTURE_AADT_2044_SOURCE`
- `FUTURE_AADT_2044_CONFIDENCE`
- `FUTURE_AADT_2044_FILL_METHOD`
- `future_aadt_covered`
- `VMT`
- `TruckVMT`
- `current_aadt_official_covered`
- `current_aadt_covered`
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

- `244,904` roadway segment rows
- `128` columns in `segments`

The SQLite database is the staged tabular source of truth. It contains roadway and traffic attributes but no geometry.

### GeoPackage

File:

- `02-Data-Staging/spatial/base_network.gpkg`

Current layers:

- `roadway_segments` (`244,904` features)
- `county_boundaries` (`159` features)
- `district_boundaries` (`7` features)

The GeoPackage is the staged spatial source used by the web application and other geometry-aware workflows.

### Coverage of newly added roadway event fields

The Phase 1 ETL now stages six additional official roadway-inventory attributes from `Road_Inventory_2024.gdb`:

- `LANE_WIDTH`
- `MEDIAN_WIDTH`
- `OWNERSHIP`
- `SHOULDER_WIDTH_L`
- `SHOULDER_WIDTH_R`
- `STRAHNET`

Current populated segment counts in the staged database:

- `OWNERSHIP`: `452,912`
- `STRAHNET`: `2,228`
- `LANE_WIDTH`: `713`
- `SHOULDER_WIDTH_R`: `390`
- `MEDIAN_WIDTH`: `9`
- `SHOULDER_WIDTH_L`: `9`

This is operationally important:

- `OWNERSHIP` came through at meaningful statewide coverage
- `STRAHNET` is present but limited to a small subset of nationally strategic facilities
- the width-related event layers are currently very sparse in the staged output under the current exact route-interval join strategy

---

## Current Traffic Coverage

### Current-year coverage

The staged network now treats `AADT_2024` as the single canonical 2024 traffic value.

- `AADT_2024_OFFICIAL` preserves the direct GDOT exact-match value
- `AADT_2024` is the final 2024 value used downstream
- `AADT_2024_SOURCE`, `AADT_2024_CONFIDENCE`, and `AADT_2024_FILL_METHOD` document whether the final value came from an official match or a conservative analytical fill
- `AADT` is retained as the current-year convenience alias and now mirrors the final `AADT_2024` value

Current 2024 AADT coverage in the staged roadway network:

- **Final canonical coverage: `244,819` of `244,904` segments (`99.97%`)**
- Only `85` segments remain uncovered (unmaintained local roads with no traffic data in any source)

Coverage by source (priority order):

| Source | Segments | Confidence | Method |
|--------|----------|-----------|--------|
| GDOT official exact | 44,983 | high | Direct GDOT traffic GDB match |
| FHWA HPMS 2024 | 196,247 | medium | Route_id + milepoint match |
| Direction mirror | 3,091 | high | INC→DEC copy for all routes |
| Analytical gap-fill | 498 | medium | Interpolation + nearest-neighbor on same route (20-mile cap) |

Key validation: HPMS AADT values are 99.7% identical to GDOT official values where both sources overlap, confirming HPMS is the same GDOT data submitted to FHWA.

### Future AADT 2044 coverage

- **Current coverage: `52,236` of `244,904` segments (`21.3%`)**
- Future AADT is only available where GDOT or HPMS report a future projection value
- The same fill chain (direction mirror, interpolation, nearest-neighbor) extends coverage beyond official sources

### State-system null county and district diagnosis

The current-year audit also identified a distinct data-quality bucket inside the state-system tail:

- `8,698` uncovered segments had null `COUNTY_CODE` and `DISTRICT`

Investigation showed these are primarily statewide GDOT routes whose `ROUTE_ID` structure uses parsed county code `000`. Those rows do not pick up county or district through the normal non-spatial joins because the route ID itself does not resolve to a single county.

The ETL now includes a spatial county/district backfill step for those segments:

- use county polygons from the staged `county_boundaries` layer when available, with official GDOT county boundaries as fallback
- generate a representative point for each affected roadway segment
- spatially assign `COUNTY_ID`, `COUNTY_CODE`, `COUNTY_NAME`, `GDOT_District`, and `DISTRICT`

This fixes the null district/county issue at the roadway-segment level without changing the underlying statewide route identity.

### Historical AADT

Historical AADT columns (2010-2020) have been removed from the pipeline output to produce a cleaner network. This eliminated historic traffic breakpoints as segmentation drivers, reducing the segment count from 622,255 to 244,904.

Raw historical source files remain available in `01-Raw-Data/GA_RDWY_INV/Traffic_Historical.zip` for future use. HPMS 2020 and 2022 snapshots are also planned for download to `01-Raw-Data/GA_RDWY_INV/HPMS_2020/` and `HPMS_2022/`.

---

## Roadway Classification Available in Phase 1

The staged network currently supports several distinct classification concepts.

### 1. System / ownership classification

Fields:

- `SYSTEM_CODE`
- `OWNERSHIP`

Documented GDOT system codes include:

- `1` = State Highway Route
- `2` = Public
- `3` = Private

Current values actually present in the staged Phase 1 build:

- `SYSTEM_CODE = 1`: `18,499` segments
- `SYSTEM_CODE = 2`: `226,405` segments

`SYSTEM_CODE` is the broadest route-system bucket. `OWNERSHIP` is a separate legal/jurisdiction classification and is now also carried into the staged outputs with `OWNERSHIP_LABEL`.

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

This is the most direct hierarchy for roadway functional class in the staged data, and the staged outputs now include readable label columns for those codes.
The staged outputs also carry:

- `F_SYSTEM_LABEL`
- `FUNCTIONAL_CLASS_LABEL`

### 3. Route-family parsing

Fields:

- `PARSED_FUNCTION_TYPE`
- `PARSED_SYSTEM_CODE`
- `ROUTE_TYPE`
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
- `ROUTE_DIRECTION`
- `BASE_ROUTE_NUMBER`
- `ROUTE_SUFFIX_LABEL`
- `ROUTE_FAMILY`
- `ROUTE_FAMILY_DETAIL`
- `ROUTE_FAMILY_CONFIDENCE`
- `ROUTE_FAMILY_SOURCE`

The route-family crosswalk now uses official Georgia rules with this priority:

- `Interstate`
- `U.S. Route`
- `State Route`
- `Local/Other`

Operationally:

- `ROUTE_FAMILY` is the coarse reporting field
- `ROUTE_FAMILY_DETAIL` captures Georgia-specific subtypes such as `Business`, `Spur`, `County Road`, `City Street`, `Ramp`, and `Frontage Road`
- `ROUTE_FAMILY_CONFIDENCE` keeps `U.S. Route` vs `State Route` separation explicit as a medium-confidence interpretation

### 4. Network significance classification

Fields:

- `NHS`
- `NHS_IND`
- `STRAHNET`

These fields indicate whether a route segment is part of the National Highway System or the Strategic Highway Network.

Important limitation:

- `NHS` does not identify signed route family
- it can help identify nationally significant facilities
- it does not by itself tell us whether a segment is an `Interstate`, `U.S. Route`, or `State Route`

The route-family classification comes from a dedicated crosswalk based primarily on `ROUTE_ID` parsing and GDOT route-code interpretation.

### 5. Current limitation

Phase 1 currently has:

- a clear system classification
- a clear functional classification
- route-identity parsing
- a Georgia-specific route-family crosswalk grounded in GDOT route documentation

Important limitation:

- `U.S. Route` versus `State Route` remains a medium-confidence interpretation because Georgia `ROUTE_ID` values encode state route numbers and concurrency can still exist

Reference note:

- [Georgia Route-Family Classification Strategy](../Assessment_and_Options/2026-04-07-georgia-route-family-classification-strategy.md)

### 6. Signed-route verification (operational)

Official verification is operational for Interstates and US Routes using GDOT GPAS layers from `rnhp.dot.ga.gov`:

- GPAS/7 `GDOT Interstates` — 22 features mapping `ROUTE_ID` to signed interstate numbers
- GPAS/6 `US Route` — 674 features mapping `ROUTE_ID` to signed US route numbers

Current verification coverage:

- `1,074` segments verified by `gdot_interstates` (high confidence)
- `5,516` segments verified by `gdot_us_highway` (high confidence)
- `238,314` segments retain `route_id_crosswalk` baseline (medium or high confidence depending on family)

Matching method:

1. `ROUTE_ID` base (13-character, direction-stripped) plus milepoint interval overlap
2. derived 10-character `RCLINK` candidates plus milepoint interval overlap as fallback

State Route signed labels remain unavailable from GPAS. The GDOT ArcWeb
`State Routes` layer on `egisp.dot.ga.gov` has the data but its query endpoint
returns HTTP 500 errors. State Route classification remains at medium confidence.

### 7. Speed limit enrichment (operational)

Official posted speed limits are enriched from GDOT GPAS SpeedZone OnSystem
permits from `rnhp.dot.ga.gov`:

- GPAS/10 `SpeedZone OnSystem` — 22,265 features with active speed zone permits

Current enrichment coverage:

- `14,766` segments with posted speed limits
- `494` school zone segments flagged

Detailed design note:

- [Georgia Signed-Route Verification Strategy](../Assessment_and_Options/2026-04-07-georgia-signed-route-verification-strategy.md)

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
- GDOT GPAS `Interstates` and `US Route` signed-route reference layers (from `rnhp.dot.ga.gov`)
- GDOT GPAS `SpeedZone OnSystem` enrichment layer (from `rnhp.dot.ga.gov`)
- FHWA HPMS Georgia 2024 (from `geo.dot.gov`)

---

## Known Limitations and Open Questions

### 1. Traffic coverage is near-complete

Current 2024 AADT coverage is `244,819` of `244,904` segments (`99.97%`). Only `85` segments remain uncovered — these are unmaintained local roads with no traffic data in any source (GDOT, HPMS, or adjacent segments).

AADT source distribution:

- `official_exact`: `44,983` (GDOT traffic GDB, high confidence)
- `hpms_2024`: `196,247` (FHWA HPMS, medium confidence — same GDOT data via federal reporting)
- `direction_mirror`: `3,091` (INC→DEC copy for all routes, high confidence)
- `analytical_gap_fill`: `498` (interpolation + nearest-neighbor, medium confidence)

The `AADT_2024_SOURCE` and `AADT_2024_CONFIDENCE` fields distinguish these sources. The former null `COUNTY_CODE` / `DISTRICT` state-system rows are handled by spatial backfill.

Future AADT 2044 coverage is `52,236` of `244,904` segments (`21.3%`). Future projections are only available where GDOT or HPMS report them; the fill chain extends coverage via direction mirror, interpolation, and nearest-neighbor.

### 2. Historical AADT removed from output

Historical AADT columns (2010-2020) have been removed from the pipeline output. Raw source files are retained. HPMS 2020 and 2022 data is available for future integration if multi-year trend analysis is needed.

### 3. Some official roadway event layers remain sparse after staging

The staged network now retains the main roadway-inventory event layers used for widths, ownership, STRAHNET, surface, shoulder, median, lanes, NHS, and functional class. However, some of those event layers remain sparsely populated after the current exact route-interval join, especially:

- `LANE_WIDTH`
- `MEDIAN_WIDTH`
- `SHOULDER_WIDTH_L`
- `SHOULDER_WIDTH_R`
- `STRAHNET`

That may reflect sparse source publication, segmentation differences, or both. If those attributes become important for downstream scoring, they should be evaluated as a dedicated refinement task.

### 4. State Route signed-label verification is not yet available

Official signed-route verification is operational for Interstates and US Routes
via GDOT GPAS layers on `rnhp.dot.ga.gov`. State Route signed labels are not
yet available because:

- the GDOT ArcWeb `State Routes` layer on `egisp.dot.ga.gov` has the data but
  its query endpoint returns HTTP 500 errors
- no equivalent State Route label layer exists on `rnhp.dot.ga.gov`

State Route classification remains at medium confidence via the route-family
crosswalk. This could be upgraded if GDOT restores the `egisp.dot.ga.gov`
query endpoint or if the data is obtained through a direct file export.

### 4a. RNHP data source exploration results

An inventory of all queryable layers on `rnhp.dot.ga.gov` was completed.
Several layers were evaluated but not integrated:

- **Bridges (FCRS/2)**: 15,478 features — RCLINK field is blank on all records, unusable for matching
- **Railroad Crossings (FCRS/3)**: 12,154 features — all key fields (RCLINK, INT_MILEPT, SPEED_LMT) are null
- **Traffic Counters (FCRS/4)**: 17,145 features — stuck at 2013 data, superseded by 2024 AADT
- **Crashes (FCRS/6)**: 1,827,102 features — 2016-2021 data, available for future safety analysis
- **Fatalities (FCRS/5)**: 41,869 features — 2024-2026 data, available for future safety analysis
- **SpeedZone OffSystem (GPAS/9)**: 81,778 features — only 1,468 have ROUTE_ID, rest need spatial matching
- **Scenic Byways (GPAS/16)**: 112 features — available for future integration
- **Estimated ROW (GDOT_Estimated_ROW/1)**: 7,460 features — available for future integration

### 5. Base-network coverage versus commercial basemap coverage

The staged roadway layer is based on the official GDOT route inventory. That is a defensible official source, but it may still omit some local-road detail visible in commercial or consumer-facing basemaps. That is a known coverage question and should be evaluated separately from the core Phase 1 data-pipeline implementation.

Current practical note:

- a `2026-04-04` Playwright visual re-check of the fully loaded local web app found no obvious planning-relevant roadway gaps in sampled Columbus / Dinglewood, Atlanta, and Savannah views
- this reduces the urgency of statewide supplementation for immediate planning use, though it does not eliminate the possibility of localized future gap-fill work

---

## Current Assessment

Phase 1 is complete and usable as the foundation for downstream work.

Closed with Phase 1:

- statewide staged roadway ETL with 244,904 segments and 128 columns
- 2024 AADT coverage at 99.97% (244,831 segments) via five-tier fill chain
- Future AADT 2044 coverage at 21.3% (52,181 segments) via same fill chain
- FHWA HPMS 2024 enrichment with pavement condition (IRI, rutting, cracking) and safety attributes
- official signed-route verification for Interstates and US Routes
- posted speed limit enrichment from GDOT SpeedZone OnSystem
- staged SQLite database, GeoPackage, and cleaned CSV outputs
- county and district boundaries with spatial backfill for statewide routes
- RAPTOR `RoadwayData` loader
- 79/79 validation checks passing

Deferred beyond Phase 1:

- State Route signed-label verification (blocked by `egisp.dot.ga.gov` query endpoint returning HTTP 500)
- historical AADT integration from HPMS 2020/2022 snapshots (data to be downloaded)
- crash and fatality data enrichment from FCRS layers (data available on `rnhp.dot.ga.gov`)
- off-system speed zone enrichment (requires spatial matching, 80,310 records without ROUTE_ID)
- statewide roadway supplementation from TIGER / OSM unless later QA shows a planning-relevant omission pattern
