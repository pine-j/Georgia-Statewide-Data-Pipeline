# Phase 1 Roadway Data Pipeline

## Purpose

This document explains the current Phase 1 Georgia roadway data pipeline in operational terms. It is intended to serve as the technical reference for how the foundational roadway layer is assembled, what source files are used, what each source contributes, how the staged database is built, and what limitations remain in the current implementation.

Phase 1 is the base layer for later RAPTOR categories. It is not a scoring category by itself. Its job is to create a reliable, queryable statewide roadway foundation with geometry, roadway attributes, district and county boundaries, and as much current and historical traffic coverage as can be matched to the official GDOT network.

Current closeout position:

- this Phase 1 pipeline is treated as complete for the current project scope
- the GDOT-based staged network is accepted as the initial statewide planning baseline
- supplementation remains a separate validation/improvement track rather than part of the closed Phase 1 build

### Data Sources (ordered by pipeline importance)

| # | Source | URL | What it provides |
|---|--------|-----|------------------|
| 1 | **GDOT Road Inventory 2024 GDB** | [GDOT Road & Traffic Data](https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx) | Base route geometry (`GA_2024_Routes`), route IDs, milepoints, and 15 attribute event layers (COUNTY_ID, F_SYSTEM, NHS, FACILITY_TYPE, THROUGH_LANES, LANE_WIDTH, MEDIAN_TYPE/WIDTH, SHOULDER_TYPE/WIDTH, OWNERSHIP, STRAHNET, SURFACE_TYPE, URBAN_ID). This is the foundation — every segment starts here. |
| 2 | **GDOT Traffic Data 2024 GDB** | [GDOT Road & Traffic Data](https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx) | Current AADT, future AADT, truck AADT, VMT, K-factor, D-factor, traffic class, and count station numbers. The traffic intervals define the segment boundaries — routes are sliced at traffic milepoint breaks to assign per-segment traffic values. |
| 3 | **FHWA HPMS Georgia 2024** | [HPMS Feature Server](https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_GA_2024/FeatureServer/0) | Parallel GDOT-official AADT, future AADT, and roadway attributes — particularly for federally-reportable segments outside the state 2024 GDB scope (e.g., off-system roads). HPMS is GDOT's annual federal submission, not a secondary fallback. Pavement condition (IRI, PSR, rutting, cracking). Broad-coverage initial signed-route classification via `routesigning` codes. Access control and terrain type. |
| 4 | **GDOT GPAS SpeedZone OnSystem** | [GPAS MapServer/10](https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/10) | Posted speed limits and school zone flags for state highway routes, matched by route ID and milepoint overlap. |
| 5 | **GDOT GPAS SpeedZone OffSystem** | [GPAS MapServer/9](https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/9) | Posted speed limits and school zone flags for non-state-highway roads (81,778 features). Most records lack geometry; matched by normalized road name + county FIPS code. |
| 6 | **GDOT GPAS Reference Layers** | [GPAS MapServer](https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer) (layers 5, 6, 7) | Authoritative signed-route verification for Interstate, U.S. Highway, and State Route designations via RCLINK + milepoint matching. GPAS has priority over HPMS for signed-route family where it has coverage. |
| 7 | **GDOT Boundaries Service** | [GDOT Boundaries MapServer](https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Boundaries/MapServer) | County (159) and district (7) boundary polygons used for spatial backfill of COUNTY_ID and GDOT_District where route attributes are missing. |
| 8 | **GDOT EOC Hurricane Evacuation Routes** | [EOC MapServer/7](https://rnhp.dot.ga.gov/hosting/rest/services/EOC/EOC_RESPONSE_LAYERS/MapServer/7) | Optional Phase 1 enrichment that flags roadway segments as `SEC_EVAC` via spatial overlay. Hurricane evacuation route polylines (268 features) and contraflow routes ([Layer 8](https://rnhp.dot.ga.gov/hosting/rest/services/EOC/EOC_RESPONSE_LAYERS/MapServer/8), 12 features) are used when present. |

### How the sources come together

```text
GDOT Road Inventory GDB -> Base geometry + route attributes (206,994 routes)
         |
         v
GDOT Traffic GDB -> Segment at traffic intervals -> assign AADT, VMT, factors (46,029 traffic records)
         |
         v
GDOT GPAS SpeedZone -> Enrich: posted speed limits (OnSystem by route + milepoint; OffSystem by road name + county)
         |
         v
GDOT Boundaries -> Backfill: county/district assignment from spatial overlay
         |
         v
FHWA HPMS 2024 -> Add: parallel GDOT-official AADT for federally-reportable segments (state 2024 GDB and HPMS are two parallel GDOT sources) + roadway attribute fill where state 2024 GDB is null + pavement/safety attributes; set initial signed-route family from routesigning
         |
         v
GDOT GPAS Reference -> Verify: signed-route family (Interstate / US / State); override HPMS where GPAS matches
         |
         v
Optional Phase 1 evacuation enrichment -> Flag: `SEC_EVAC` from spatial overlay with hurricane evacuation routes (268 + 12 contraflow)
         |
         v
Route Classification -> Derive: ROUTE_FAMILY, ROUTE_TYPE_GDOT, HWY_NAME from ROUTE_ID structure + verification
         |
         v
Official Growth Projection -> Fill: FUTURE_AADT_2044 gaps using GDOT's implied growth rate (~1.17% annual) from known official pairs
         |
         v
Texas RAPTOR Alignment -> Derive: PCT_SADT, PCT_CADT, HWY_DES for downstream RAPTOR scoring compatibility
         |
         v
Decoded Labels -> Add: human-readable _LABEL columns for all coded fields
         |
         v
Final staged output -> roadway_inventory.db + base_network.gpkg
```

Note: GPAS has final authority for signed-route classification because it is the direct GDOT source. HPMS signed-route data is a federal derivative that originates from GDOT reporting.

### Reference documentation

- [GDOT Understanding Route IDs](https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/UnderstandingRouteIDs_Doc.pdf) — explains the 16-character ROUTE_ID structure
- [GDOT Road Inventory Data Dictionary](https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf) — field definitions for the source GDB
- [GDOT live LRS metadata](https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Network_LRSN/MapServer/exts/LRSServer/layers) — linear referencing service metadata
- [Staged data dictionary](./data_dictionary.csv) — column-by-column reference for the final staged output

---

## Pipeline Goal

The Phase 1 pipeline produces two primary staged outputs:

- `02-Data-Staging/databases/roadway_inventory.db`
- `02-Data-Staging/spatial/base_network.gpkg`

Note: `02-Data-Staging/spatial/_quarantine/roadway_inventory.db` is a quarantined stale duplicate and is not part of the active Phase 1 output contract.

Together, these provide:

- the statewide roadway segment inventory
- roadway geometry
- joined roadway-inventory attributes from GDOT
- current traffic fields from GDOT 2024 traffic data
- future AADT projection (2044) from GDOT and HPMS sources
- official signed-route verification from GDOT GPAS reference layers, seeded by HPMS `routesigning` coverage
- posted speed limits from GDOT SpeedZone OnSystem and OffSystem permits
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
01-Raw-Data/Roadway-Inventory/
01-Raw-Data/Roadway-Inventory/scripts/
02-Data-Staging/scripts/01_roadway_inventory/
02-Data-Staging/databases/roadway_inventory.db
02-Data-Staging/spatial/base_network.gpkg
02-Data-Staging/tables/roadway_inventory_cleaned.csv
02-Data-Staging/config/
```

---

## Source Files

### Core roadway and traffic source directory

The main raw source directory is:

- `01-Raw-Data/Roadway-Inventory/`

The download metadata file is:

- `01-Raw-Data/Roadway-Inventory/download_metadata.json`

That directory is organized by source:

```text
Roadway-Inventory/
├── GDOT_Road_Inventory/   (Road_Inventory_2024.gdb, DataDictionary)
├── GDOT_Traffic/           (TRAFFIC_Data_2024.gdb, Traffic_Historical.zip, 2010_thr_2019)
├── GDOT_GPAS/              (rnhp_enrichment — speed zones on-system & off-system)
├── FHWA_HPMS/              (2024 snapshot, field manual, metadata)
└── download_metadata.json
```

GDOT data is downloaded from `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/`.

### Extracted and directly used roadway source

The foundational roadway geometry comes from:

- `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/Road_Inventory_2024.gdb`
- layer: `GA_2024_Routes`

This is the canonical route geometry for the staged roadway network.

### Extracted and directly used current traffic source

Current traffic fields come from:

- `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/TRAFFIC_Data_2024.gdb`
- layer: `TRAFFIC_DataYear2024`

This source contributes current AADT and related traffic measures.

### Historical traffic source (archived, not in pipeline output)

Historical route-segment traffic files are available at:

- `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip`

These files are retained for future use but are no longer loaded into the pipeline output. Removing historic traffic breakpoints reduced the segment count from 622,255 to 245,863, producing a cleaner network that segments only on current-year traffic intervals.

### Signed-route verification (HPMS first, GPAS authoritative)

HPMS `routesigning` codes provide the broad-coverage first pass for signed-route family classification, covering 223,672 segments (91.0%). GPAS reference layers remain the final authoritative verifier where they match, even though their direct coverage is narrower at 6,854 segments (2.8%).

### RNHP enrichment snapshots

The speed zone enrichment downloads from GDOT GPAS to:

- `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/speed_zone_on_system.geojson`
- `01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/speed_zone_off_system.geojson`

These snapshots are cached locally and only re-downloaded when
`01-Raw-Data/Roadway-Inventory/scripts/download_rnhp_enrichment.py` is run.

### FHWA HPMS 2024 data

The HPMS (Highway Performance Monitoring System) dataset is GDOT's annual federal submission to FHWA — a parallel GDOT-official source, not a secondary fallback. It uses the same GDOT `ROUTE_ID` and milepoint system as our base network, enabling direct interval-overlap matching without spatial joins. HPMS is the canonical AADT source for federally-reportable segments that fall outside the state 2024 GDB scope (e.g., off-system roads).

Source: `https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_GA_2024/FeatureServer/0`

Downloaded snapshot:

- `01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_tabular.json`

**Key finding: HPMS AADT values are 99.7% identical to GDOT state-system values where both sources have data.** This is direct evidence that HPMS *is* the GDOT data — packaged for federal reporting — rather than an independent estimate. The 2024 hygiene pass treats HPMS as a parallel GDOT-official source and cross-validates the two wherever they overlap. Direct state 2024 GDB current-year coverage is `45,938` of `245,863` segments; HPMS adds GDOT-official AADT for the federally-reportable segments outside that scope, raising combined GDOT-official coverage to roughly `242,033` segments (`96.5%`). The remaining `~3.5%` is filled by pipeline-derived methods (direction mirror, analytical interpolation, nearest neighbor), and `~0.04%` remains truly missing.

HPMS contributes:

- Parallel GDOT-official AADT for segments outside the state 2024 GDB scope (the operational behavior is the same as before: HPMS populates segments the state 2024 GDB skipped — it just isn't a "fallback", it's the canonical source for federally-reportable off-system segments)
- Cross-validation against the state 2024 GDB on overlap segments (captured in `AADT_2024_HPMS` and `AADT_2024_SOURCE_AGREEMENT`)
- Pavement condition: IRI, PSR, rutting, cracking percent
- Safety attributes: access control, terrain type
- Initial signed-route classification via `routesigning` codes (223,672 segments, 91.0%)
- GPAS reference layers provide the final authoritative signed-route family where they match
- Roadway attribute fill for 13 GDOT fields where the state 2024 GDB values are null (never overwrites existing values)

#### AADT 2024 cross-validation columns and confidence tiers

The 2024 hygiene pass added four columns that surface the two-source picture explicitly:

- `AADT_2024_HPMS` — raw 2024 AADT from the GDOT HPMS submission, captured for every HPMS-matched segment regardless of which source wins the canonical AADT.
- `AADT_2024_SOURCE_AGREEMENT` — `state_only`, `hpms_only`, `both_agree` (within ±15% or ±200 veh/day), `both_disagree`, or null when neither source has a value.
- `AADT_2024_STATS_TYPE` — pass-through of the GDOT `Statistics_Type` field on the matched 2024 traffic record (`Actual`, `Estimated`, `Calculated`).
- `AADT_2024_SAMPLE_STATUS` — pass-through of the GDOT `SampleStatus` free-text descriptor for the most recent sample-adequacy pass.

`AADT_2024_CONFIDENCE` semantics changed in this pass and now use a four-tier scheme: `high` when `Statistics_Type = Actual` or the two GDOT sources agree; `medium` when `Statistics_Type` is `Estimated`/`Calculated` or the segment has a single GDOT-official source with no disagreement; `low` when the value is pipeline-derived (`direction_mirror`, `analytical_gap_fill`, `nearest_neighbor`) or the two GDOT sources disagree; `missing` when no value is available. Note that `direction_mirror` was previously tagged `high` and `analytical_gap_fill` was previously `medium`; both are now `low` so that the confidence tier reflects measurement provenance rather than fill method.

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
- `Traffic_Class` (staged as `TRAFFIC_CLASS_2024`)
- `TC_NUMBER`
- county and district references used during ETL backfilling

These fields are matched to the official GDOT route geometry by:

- `ROUTE_ID`
- `FROM_MILEPOINT`
- `TO_MILEPOINT`

### `Traffic_Historical.zip`

Historical traffic source files are archived in raw sources and retained for potential future use, but they are not normalized into or included in the staged output.

The archived files remain available in `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip`.

Historical AADT and truck-related measures from these files are intentionally excluded from the staged dataset. Legacy future-projection fields in older historical datasets are also not carried forward. The only canonical future AADT kept in the staged output is the 2024 current-year `FUTURE_AADT` field.

---

## High-Level ETL Flow

The roadway pipeline consists of four main steps:

1. Download and stage raw GDOT source files
2. Normalize the roadway network onto the official `GA_2024_Routes` geometry
3. Write staged tabular and spatial outputs
4. Validate the staged outputs

The key scripts are:

- `01-Raw-Data/Roadway-Inventory/scripts/download.py`
- `01-Raw-Data/Roadway-Inventory/scripts/download_rnhp_enrichment.py`
- `02-Data-Staging/scripts/01_roadway_inventory/normalize.py`
- `02-Data-Staging/scripts/01_roadway_inventory/route_family.py`
- `02-Data-Staging/scripts/01_roadway_inventory/rnhp_enrichment.py`
- `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py`
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
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
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
the baseline `ROUTE_ID` crosswalk. HPMS enrichment then applies a
broad-coverage first pass using FHWA `routesigning` codes, and GPAS reference
layers run after that as the final authoritative verifier for signed-route
family.

Current verification results:

- `223,672` segments seeded by `hpms_2024` (broad coverage)
- `6,854` segments overridden or confirmed by `gdot_*` GPAS references (authoritative where matched)
- remaining segments retain baseline `route_id_crosswalk`

Current signed-route verification fields:

- `SIGNED_INTERSTATE_FLAG`
- `SIGNED_US_ROUTE_FLAG`
- `SIGNED_STATE_ROUTE_FLAG`
- `SIGNED_ROUTE_FAMILY_PRIMARY`
- `SECONDARY_SIGNED_ROUTE_FAMILY`
- `TERTIARY_SIGNED_ROUTE_FAMILY`
- `SIGNED_ROUTE_FAMILY_ALL`
- `SIGNED_ROUTE_VERIFY_SOURCE`
- `SIGNED_ROUTE_VERIFY_METHOD`
- `SIGNED_ROUTE_VERIFY_CONFIDENCE`
- `SIGNED_ROUTE_VERIFY_SCORE`
- `SIGNED_ROUTE_VERIFY_NOTES`

### 8a. Apply RNHP enrichment (speed zones)

The ETL enriches roadway segments with posted speed limit data from two GDOT
GPAS SpeedZone layers before the HPMS and GPAS signed-route passes.

**OnSystem** (state highway routes):

Source: [GPAS MapServer/10](https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/10)

Matching method:

- filter to active speed zone records (`RECORD_STATUS_CD = 'ACTV'`)
- match by `RNH_ROUTE_ID` base (first 13 characters) plus statewide milepoint interval overlap
- when multiple speed zones cover a segment, take the best-covering (longest overlap)
- applies to state highway segments only (`PARSED_SYSTEM_CODE = 1`)

Current enrichment results:

- `15,709` segments with posted speed limits
- `523` school zone segments flagged

Speed limit distribution:

- 25 mph: `563` segments
- 35 mph: `1,378` segments
- 45 mph: `2,078` segments
- 55 mph: `8,847` segments (most common)
- 65 mph: `940` segments
- 70 mph: `629` segments

**OffSystem** (county roads, city streets):

Source: [GPAS MapServer/9](https://rnhp.dot.ga.gov/hosting/rest/services/GPAS/MapServer/9) — 81,778 features

Layer 9 records mostly lack geometry (only 1,696/81,778 have it) and have no
milepoint fields.  All 80,199 active records have `ROAD_NAME` and
`COUNTY_FIPS_CD`, so matching is done by normalized road name + county code:

- Filter to active records (`RECORD_STATUS_CD = 'ACTV'`)
- Normalize road names: canonicalize suffixes (STREET->ST, DRIVE->DR, etc.), strip "SCHOOL ZONE" tags, remove parenthetical route codes, remove slash alternatives, normalize punctuation
- Build a `(normalized_name, county_fips)` lookup from speed zone records
- Join against segment `HPMS_ROUTE_NAME` (same normalization) + `COUNTY_CODE`
- Only fills segments where `SPEED_LIMIT` is still null after the OnSystem pass
- When multiple speed zones share the same (name, county) key but disagree on speed limit, the key is skipped as ambiguous (3,270 ambiguous keys skipped)
- Runs after HPMS enrichment (requires `HPMS_ROUTE_NAME` column)

Current enrichment results:

- `29,672` segments with posted speed limits
- `582` school zone segments flagged
- `31,674` unambiguous (name, county) keys used; `3,270` ambiguous keys skipped

~30.7% of speed zone road names (10,546 unique roads) do not appear in the
GDOT Road Inventory at all — these are local streets not digitized by GDOT
and cannot be matched regardless of approach.

Current enrichment fields:

- `SPEED_LIMIT`
- `IS_SCHOOL_ZONE`
- `SPEED_LIMIT_SOURCE` — `gdot_speed_zone_on_system` or `gdot_speed_zone_off_system`

### 8b. Apply HPMS 2024 enrichment

After OnSystem speed zone enrichment, the ETL joins FHWA HPMS 2024 data — GDOT's
parallel federal submission — to extend GDOT-official AADT coverage to
federally-reportable segments outside the state 2024 GDB scope, derive
signed-route flags, fill GDOT roadway attributes where the state 2024 GDB is
null, and add pavement/safety attributes. The OffSystem speed zone pass runs
immediately after HPMS (it requires `HPMS_ROUTE_NAME` for name matching).

Source: `https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_GA_2024/FeatureServer/0`

HPMS uses the same GDOT `ROUTE_ID` and milepoint system. Matching is done by
direct `ROUTE_ID` + milepoint interval overlap — no spatial matching needed.

AADT priority chain (each step only fills segments not yet covered). Steps 1
and 2 are two parallel GDOT-official sources; steps 3-5 are pipeline-derived
fills for the remaining ~3.5%:

1. **State 2024 GDB exact** (45,938 segments) — direct GDOT traffic GDB match, GDOT-official
2. **HPMS 2024 federal submission** (196,095 segments) — GDOT's annual federal submission, matched by route_id + milepoint, GDOT-official (canonical for federally-reportable segments outside the state 2024 GDB scope)
3. **Direction mirror** (3,085 segments) — INC→DEC copy for all routes, pipeline-derived, confidence `low`
4. **Analytical gap-fill** (511 segments) — interpolation on the same route, pipeline-derived, confidence `low`
5. **Nearest-neighbor** (137 segments) — same-route nearest-neighbor fill, pipeline-derived, confidence `low`

Combined GDOT-official coverage from steps 1+2: ~242,033 segments (~96.5%).
Pipeline-derived fill from steps 3-5: ~3,733 segments (~1.5%; the 8,739 figure
in the hygiene plan is the broader derived-slice count). Final 2024 AADT
coverage: 245,766 / 245,863 segments (99.9605%); the residual ~0.04% remains
truly missing.

The new cross-validation columns `AADT_2024_HPMS` and
`AADT_2024_SOURCE_AGREEMENT` capture the relationship between the two
GDOT-official sources on segments where both have a value, and the new
`AADT_2024_STATS_TYPE` / `AADT_2024_SAMPLE_STATUS` columns pass through GDOT's
own measurement-provenance fields. `AADT_2024_CONFIDENCE` now uses the updated
four-tier scheme described in the FHWA HPMS 2024 data section above.

Future AADT 2044 fill chain:

1. **GDOT official** — direct FUTURE_AADT from GDOT traffic GDB, confidence `high`
2. **HPMS future_aadt** — FHWA HPMS future AADT values, confidence `medium`
3. **Direction mirror** — INC→DEC copy for divided highways, confidence `high`
4. **Official implied growth projection** — applies GDOT's own implied growth rate to AADT_2024 for all remaining gaps. The rate is back-derived from the ~53K segments where GDOT/HPMS provides both AADT_2024 and FUTURE_AADT_2044 (statewide median ~1.17% annual). Grouped by county+system code, district+system code, system code, or statewide median. Formula: `FUTURE_AADT_2044 = AADT_2024 × (1 + rate)^20`. Confidence `low`.

Steps 1-3 provide direct or proximate official values on `46,619` segments (`19.0%`). Step 4 extends total `FUTURE_AADT_2044` coverage to `245,766` segments (`99.96%`) wherever `AADT_2024` exists.

HPMS signed-route enrichment:

The `SIGNED_INTERSTATE_FLAG`, `SIGNED_US_ROUTE_FLAG`, and `SIGNED_STATE_ROUTE_FLAG` fields are seeded from HPMS `routesigning` codes to provide broad statewide coverage. GPAS reference verification runs after HPMS and has final authority where GPAS matches.

HPMS gap-fills 13 GDOT roadway attributes where GDOT values are null (never overwrites existing values):

- `THROUGH_LANES`: +74,935 segments
- `OWNERSHIP`: +31,915 segments
- `F_SYSTEM`: +26,328 segments
- `URBAN_ID`: +21,917 segments
- `FACILITY_TYPE`: +19,684 segments
- `SURFACE_TYPE`: +10,462 segments
- `NHS`: +6,680 segments
- `SPEED_LIMIT`: +5,578 segments (backfills gaps left by GPAS SpeedZone OnSystem + OffSystem)
- `LANE_WIDTH`: +3,805 segments
- `SHOULDER_TYPE` / `SHOULDER_WIDTH_R`: +3,866 segments
- `MEDIAN_TYPE`: +3,866 segments
- `SHOULDER_WIDTH_L` / `MEDIAN_WIDTH`: +1,073 segments

HPMS also contributes pavement and safety attributes:

- `HPMS_IRI` — International Roughness Index (10,410 segments)
- `HPMS_RUTTING` — pavement rutting depth (9,591 segments)
- `HPMS_CRACKING_PCT` — pavement cracking percent (10,410 segments)
- `HPMS_ACCESS_CONTROL` — access control type (10,519 segments)
- `HPMS_TERRAIN_TYPE` — terrain classification (891 segments)
- `HPMS_ROUTE_SIGNING` — route signing type (223,672 segments)
- `HPMS_ROUTE_NUMBER` — signed route number
- `HPMS_ROUTE_NAME` — road name from HPMS

### 9. Reproject and compute segment length

After segmentation, the roadway network is reprojected to:

- `EPSG:32617`

The ETL then computes:

- `segment_length_m`
- `segment_length_mi`
- `county_all`

### 10. Write staged outputs

The ETL writes:

- `02-Data-Staging/tables/roadway_inventory_cleaned.csv`
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
- `FUTURE_AADT_2044_SOURCE` — `official_exact`, `hpms_2024`, `direction_mirror`, `projection_official_implied`, or `missing`
- `FUTURE_AADT_2044_CONFIDENCE` — `high`, `medium`, or `low`
- `FUTURE_AADT_2044_FILL_METHOD` — method used for non-official values
- `future_aadt_covered` — boolean: has any FUTURE_AADT_2044

### From signed-route enrichment and verification

- `SIGNED_INTERSTATE_FLAG`
- `SIGNED_US_ROUTE_FLAG`
- `SIGNED_STATE_ROUTE_FLAG`
- `SIGNED_ROUTE_FAMILY_PRIMARY`
- `SECONDARY_SIGNED_ROUTE_FAMILY`
- `TERTIARY_SIGNED_ROUTE_FAMILY`
- `SIGNED_ROUTE_FAMILY_ALL`
- `SIGNED_ROUTE_VERIFY_SOURCE`
- `SIGNED_ROUTE_VERIFY_METHOD`
- `SIGNED_ROUTE_VERIFY_CONFIDENCE`
- `SIGNED_ROUTE_VERIFY_SCORE`
- `SIGNED_ROUTE_VERIFY_NOTES`

### From GDOT GPAS SpeedZone (OnSystem + OffSystem)

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

### From spatial county overlap

- `county_all` - comma-separated county names crossed by the segment, keeping counties that cover at least 1% of segment length, starting with `COUNTY_NAME`, then listing any additional counties by descending overlap share

### AADT provenance fields

- `AADT_2024` — canonical 2024 AADT chosen by tie-breaker: state 2024 GDB > HPMS 2024 federal submission > pipeline-derived (mirror / interpolation / nearest)
- `AADT_2024_OFFICIAL` — audit trail for the state 2024 GDB match only, never overwritten by HPMS or any pipeline-derived fill
- `AADT_2024_HPMS` — raw HPMS 2024 AADT captured for every HPMS-matched segment regardless of which source wins (cross-validation column)
- `AADT_2024_SOURCE_AGREEMENT` — `state_only`, `hpms_only`, `both_agree` (within ±15% or ±200 veh/day), `both_disagree`, or null
- `AADT_2024_STATS_TYPE` — pass-through of GDOT `Statistics_Type` (`Actual`, `Estimated`, `Calculated`)
- `AADT_2024_SAMPLE_STATUS` — pass-through of GDOT `SampleStatus` free-text descriptor
- `AADT_2024_SOURCE` — `official_exact`, `hpms_2024`, `direction_mirror`, `analytical_gap_fill`, `nearest_neighbor`, or `missing`
- `AADT_2024_CONFIDENCE` — `high`, `medium`, `low`, or `missing` (updated four-tier semantics, see HPMS section above)
- `AADT_2024_FILL_METHOD` — method used when neither GDOT-official source had a value
- `current_aadt_official_covered` — boolean: has direct state 2024 GDB match
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
- `PCT_SADT`
- `PCT_CADT`
- `HWY_DES`
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
- `county_all`

---

## Current Staged Outputs

### SQLite database

File:

- `02-Data-Staging/databases/roadway_inventory.db`

Tables:

- `segments`
- `load_summary`

Current contents:

- `245,863` roadway segment rows
- `118` columns in `segments`

The SQLite database is the staged tabular source of truth. It contains roadway and traffic attributes but no geometry.

### GeoPackage

File:

- `02-Data-Staging/spatial/base_network.gpkg`

Current layers:

- `roadway_segments` (`245,863` features)
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

- `OWNERSHIP`: near-complete statewide coverage after HPMS gap-fill
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

- **Final canonical coverage: `245,766` of `245,863` segments (`99.9605%`)**
- Current AADT covers `133,382.10` of `133,994.38` staged segment miles
- Only `97` segments remain uncovered (unmaintained local roads with no traffic data in any source)

Coverage by source (priority order). Steps 1 and 2 are two parallel
GDOT-official sources; steps 3-5 are pipeline-derived fill for the remaining
~3.5%. Confidence tiers reflect the updated four-tier scheme — see the HPMS
2024 data section for the full rules:

| Source | Segments | Provenance | Method |
|--------|----------|-----------|--------|
| State 2024 GDB exact | 45,938 | GDOT-official | Direct GDOT traffic GDB match |
| HPMS 2024 federal submission | 196,095 | GDOT-official (parallel) | Route_id + milepoint match (canonical for federally-reportable segments outside the state 2024 GDB scope) |
| Direction mirror | 3,085 | pipeline-derived | INC→DEC copy for all routes |
| Analytical gap-fill | 511 | pipeline-derived | Interpolation on the same route |
| Nearest-neighbor | 137 | pipeline-derived | Same-route nearest-neighbor fill (20-mile cap) |

Key validation: HPMS AADT values are 99.7% identical to state 2024 GDB values where both sources overlap. This is direct evidence that HPMS *is* the GDOT data, packaged for federal reporting — not an independent estimate.

### Future AADT 2044 coverage

- Direct coverage (GDOT official + HPMS + direction mirror): `46,619` of `245,863` segments (`19.0%`)
- Total post-imputation coverage after the official implied growth projection: `245,766` of `245,863` segments (`99.96%`)
- **Official implied growth projection (step 4) extends coverage to near-complete** — applies GDOT's own implied growth rate (~1.17% annual, back-derived from known official pairs) to AADT_2024 for all remaining segments

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

Historical AADT columns (2010-2020) have been removed from the pipeline output to produce a cleaner network. This eliminated historic traffic breakpoints as segmentation drivers, reducing the segment count from 622,255 to 245,863.

Raw historical source files remain available in `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip` for future use.

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

- `SYSTEM_CODE = 1`: `19,458` segments
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
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
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

Note on baseline confidence:

- `U.S. Route` versus `State Route` is a medium-confidence interpretation when derived from `ROUTE_ID` alone, because Georgia route IDs encode state route numbers and concurrency can still exist. In practice, this baseline is largely superseded by the HPMS/GPAS signed-route verification pass (see section 6 below), which provides federal or GDOT-authoritative family classification for 91%+ of the network.

Reference note:

- [Georgia Route Type Classification](./phase-1-Supplement-Docs/georgia-route-type-classification.md)

### 6. Signed-route verification (operational)

Official verification is operational for Interstates, US Routes, and State Routes using a two-stage process:

- HPMS `routesigning` provides the broad-coverage first pass
- GDOT GPAS reference layers provide the final authoritative family where GPAS matches

Current verification coverage:

- `223,672` segments seeded by `hpms_2024` (91.0% of network)
- `6,854` segments overridden or confirmed by `gdot_*` GPAS references (2.8% of network, authoritative where matched)
- remaining segments retain `route_id_crosswalk` baseline

GPAS has final authority because it is the direct GDOT live reference source. HPMS signed-route values are a federal derivative of GDOT reporting and are used first for coverage, not final precedence.

### 7. Speed limit enrichment (operational)

Official posted speed limits are enriched from GDOT GPAS SpeedZone permits
from `rnhp.dot.ga.gov`:

- GPAS/10 `SpeedZone OnSystem` — 22,265 features with active speed zone permits (state highways, route ID + milepoint matching)
- GPAS/9 `SpeedZone OffSystem` — 81,778 features (county roads, city streets; name + county matching)

Current enrichment coverage (combined OnSystem + OffSystem + HPMS backfill):

- `50,959` segments with posted speed limits (20.7% of network)
- OnSystem: `15,709` segments (route ID + milepoint)
- OffSystem: `29,672` segments (road name + county)
- HPMS backfill: `5,578` segments
- `1,105` school zone segments flagged (OnSystem + OffSystem)

Detailed design note:

- Signed-route verification in the current build uses HPMS `routesigning` as the initial pass, followed by authoritative GPAS verification where GDOT live reference layers have coverage.

---

## Boundaries in the Current Build

### County boundaries

The county layer currently includes:

- `COUNTYFP`
- `NAME`
- `GDOT_DISTRICT`
- `DISTRICT_NAME`

This allows counties to be filtered and labeled consistently with district metadata.

### District boundaries

The district layer currently includes:

- `GDOT_DISTRICT`
- `DISTRICT_NAME`

District names use the short office name:

- `Gainesville`
- `Tennille`
- `Thomaston`
- `Tifton`
- `Jesup`
- `Cartersville`
- `Chamblee`

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

## Source Packages and Current ETL Inputs

Current direct ETL inputs:

- `GDOT_Road_Inventory/Road_Inventory_2024.gdb` — base route geometry and roadway attributes
- `GDOT_Traffic/TRAFFIC_Data_2024.gdb` — current AADT and traffic measures
- `GDOT_GPAS/rnhp_enrichment/` — speed zone enrichment geojson (on-system + off-system)
- GDOT GPAS live reference layers (MapServer layers 5, 6, 7) — final authoritative signed-route verification
- `FHWA_HPMS/2024/hpms_ga_2024_tabular.json` — HPMS enrichment, initial signed-route classification, and GDOT attribute gap-fill
- GDOT boundary service (live download at runtime)

Archived but retained for future use:

- `GDOT_Traffic/Traffic_Historical.zip` — historical AADT 2010-2020
- `GDOT_Traffic/2010_thr_2019_Published_Traffic.zip` — published traffic archive

---

## Known Limitations and Open Questions

### 1. Traffic coverage is near-complete

Current 2024 AADT coverage is `245,766` of `245,863` segments (`99.9605%`). Only `97` segments remain uncovered — these are unmaintained local roads with no traffic data in any source (GDOT, HPMS, or adjacent segments).

AADT source distribution:

- `official_exact`: `45,938` (state 2024 GDB direct match — GDOT-official)
- `hpms_2024`: `196,238` (GDOT's HPMS federal submission — GDOT-official, parallel source, canonical for federally-reportable segments outside the state 2024 GDB scope; values are 99.7% identical to the state 2024 GDB on overlap)
- `direction_mirror`: `3,091` (INC→DEC copy for all routes — pipeline-derived)
- `analytical_gap_fill`: `511` (interpolation + nearest-neighbor — pipeline-derived)

The `AADT_2024_SOURCE` and `AADT_2024_CONFIDENCE` fields distinguish these sources, and `AADT_2024_HPMS` / `AADT_2024_SOURCE_AGREEMENT` capture the cross-validation between the two GDOT-official sources where both are present. The former null `COUNTY_CODE` / `DISTRICT` state-system rows are handled by spatial backfill.

Future AADT 2044 direct coverage (GDOT official + HPMS + direction mirror) is `46,619` of `245,863` segments (`19.0%`). Total post-imputation coverage is `245,766` of `245,863` segments (`99.96%`) after the official implied growth projection applies GDOT's own implied growth rate (~1.17% annual, back-derived from known official pairs) to `AADT_2024` for the remaining covered segments.

### 2. Historical AADT removed from output

Historical AADT columns (2010-2020) have been removed from the pipeline output. Raw source files are retained for future use if multi-year trend analysis is needed.

### 3. Some official roadway event layers remain sparse after staging

The staged network now retains the main roadway-inventory event layers used for widths, ownership, STRAHNET, surface, shoulder, median, lanes, NHS, and functional class. However, some of those event layers remain sparsely populated after the current exact route-interval join, especially:

- `LANE_WIDTH`
- `MEDIAN_WIDTH`
- `SHOULDER_WIDTH_L`
- `SHOULDER_WIDTH_R`
- `STRAHNET`

That may reflect sparse source publication, segmentation differences, or both. If those attributes become important for downstream scoring, they should be evaluated as a dedicated refinement task.

### 4. Signed-route verification now uses HPMS coverage with GPAS final authority

Signed-route verification for Interstates, US Routes, and State Routes is now
operational through HPMS `routesigning` coverage plus authoritative GPAS
reference verification. HPMS covers 223,672 segments (91.0% of the network) and
GPAS directly verifies 6,854 segments (2.8%) with final precedence where it
matches. Segments not matched by either source retain the baseline
`route_id_crosswalk` values.

### 4a. RNHP data source exploration results

An inventory of all queryable layers on `rnhp.dot.ga.gov` was completed.
Several layers were evaluated but not integrated:

- **Bridges (FCRS/2)**: 15,478 features — RCLINK field is blank on all records, unusable for matching
- **Railroad Crossings (FCRS/3)**: 12,154 features — all key fields (RCLINK, INT_MILEPT, SPEED_LMT) are null
- **Traffic Counters (FCRS/4)**: 17,145 features — stuck at 2013 data, superseded by 2024 AADT
- **Crashes (FCRS/6)**: 1,827,102 features — 2016-2021 data, available for future safety analysis
- **Fatalities (FCRS/5)**: 41,869 features — 2024-2026 data, available for future safety analysis
- **SpeedZone OffSystem (GPAS/9)**: 81,778 features — now integrated via normalized road name + county matching (only 1,696 have geometry; 80,199 active records matched by name)
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

- statewide staged roadway ETL with 245,863 segments and 118 columns
- 2024 AADT coverage at 99.9605% (`245,766` segments): ~96.5% from two parallel GDOT-official sources (state 2024 GDB + HPMS federal submission), ~3.5% from pipeline-derived fill (direction mirror, analytical interpolation, nearest neighbor), ~0.04% truly missing. Cross-validation captured in `AADT_2024_HPMS` / `AADT_2024_SOURCE_AGREEMENT`
- Future AADT 2044 coverage extended from `46,619` direct-forecast segments (`19.0%`) to `245,766` total post-imputation segments (`99.96%`) via four-step fill chain: GDOT official, HPMS, direction mirror, then official implied growth projection (~1.17% annual rate) for all remaining segments with `AADT_2024`
- FHWA HPMS 2024 enrichment with pavement condition (IRI, rutting, cracking) and safety attributes
- signed-route verification for Interstates, US Routes, and State Routes via HPMS first-pass coverage with GPAS final authority
- HPMS gap-fill for 13 GDOT roadway attributes
- posted speed limit enrichment from GDOT SpeedZone OnSystem and OffSystem
- staged SQLite database, GeoPackage, and table CSV outputs
- county and district boundaries with spatial backfill for statewide routes
- RAPTOR `RoadwayData` loader
- `116/116` validation checks passing

Deferred beyond Phase 1:

- crash and fatality data enrichment from FCRS layers (data available on `rnhp.dot.ga.gov`)
- statewide roadway supplementation from TIGER / OSM unless later QA shows a planning-relevant omission pattern

