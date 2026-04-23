# Phase 1 Roadway Data Pipeline — Simplified Overview

Status: Complete for current project scope.

## What This Pipeline Does

Phase 1 builds the foundational roadway layer that RAPTOR scoring runs on top of. It takes raw GDOT data sources, combines them into a single statewide roadway network with traffic and roadway attributes, and outputs a database and spatial file ready for analysis.

---

## Data Sources

| Source | What We Get From It |
|--------|-------------------|
| **GDOT Road Inventory GDB** | The base road network — route geometry, route IDs, milepoints, and roadway attributes (lanes, surface type, functional class, median, shoulders, etc.) |
| **GDOT Traffic GDB** | Current-year AADT, truck AADT, VMT, K-factor, D-factor |
| **FHWA HPMS 2024** | Parallel GDOT-official AADT for federally-reportable segments (HPMS is GDOT's annual federal submission, not a fallback — it's the canonical source for segments outside the state 2024 GDB scope, e.g., off-system roads); pavement condition (IRI, rutting, cracking); initial signed-route classification |
| **GDOT GPAS SpeedZone** | Posted speed limits — on-system (state highways) matched by route ID, off-system (local roads) matched by road name + county |
| **GDOT GPAS Reference Layers** | Authoritative verification of signed-route family (Interstate / US / State Route) |
| **GDOT Boundaries Service** | County (159) and district (7) boundary polygons — **split-driving**: routes are segmented at every boundary crossing |
| **GDOT EOC Evacuation Routes** | Hurricane evacuation route flags |

---

## How It Works (Step by Step)

### 1. Start with the official road network

Load the `GA_2024_Routes` layer from the GDOT Road Inventory GDB. This gives us 206,994 routes with geometry — the backbone of everything downstream.

### 2. Attach roadway attributes

Join 15 attribute layers from the same GDB onto the routes (functional class, number of lanes, surface type, median type, shoulder widths, NHS status, ownership, etc.). These join by route ID and milepoint intervals.

### 3. Segment by traffic intervals + administrative geography

Load the GDOT Traffic GDB (46,029 traffic records) and five sets of administrative boundary polygons. The pipeline computes breakpoints from **two sources simultaneously** and splits the official route geometry at every one:

- **Traffic interval boundaries** — wherever AADT, truck AADT, or other traffic values change along a route
- **Administrative boundary crossings** — wherever a route crosses a county, GDOT district, area office, MPO, or regional commission boundary

For each resulting segment the pipeline:
- slices the official geometry between adjacent breakpoints using Shapely `substring()`
- stamps the covering traffic record (AADT, truck AADT, VMT, K/D factors)
- stamps administrative attributes by querying which polygon contains the segment midpoint (county, district, area office, MPO, regional commission)

This dual-source segmentation produces ~245,863 segments, each with a consistent set of traffic values **and** unambiguous administrative geography. A segment never straddles a county or district boundary.

**Post-split overlay flags** are applied to the already-split segments without further splitting:
- **Legislative districts** (State House, State Senate, Congressional) — assigned by majority-length intersection
- **City** — assigned only when a single city covers ≥50% of the segment length

### 4. Parse route identity

Break down the 16-character GDOT `ROUTE_ID` into component parts (county code, system code, route number, suffix, direction). Derive the route family classification: Interstate, U.S. Route, State Route, or Local/Other.

### 5. Enrich with speed limits

Match GDOT SpeedZone permits to segments:
- **On-system** (state highways): match by route ID + milepoint overlap → ~15,000 segments
- **Off-system** (local roads): match by normalized road name + county code → ~30,000 segments

### 6. Add HPMS (parallel GDOT-official source)

Join FHWA HPMS 2024 — GDOT's annual federal submission — by route ID + milepoint overlap. HPMS is a parallel GDOT-official source, not a secondary fallback: it is the canonical AADT source for federally-reportable segments outside the state 2024 GDB scope. Together, the state 2024 GDB and HPMS cover ~96.5% of segments with GDOT-official AADT; the remaining ~3.5% gets pipeline-derived fill (mirror / interpolation / nearest neighbor).

This step:
- Adds GDOT-official AADT for federally-reportable segments not covered by the state 2024 GDB
- Captures cross-validation between the two GDOT-official sources (`AADT_2024_HPMS`, `AADT_2024_SOURCE_AGREEMENT`); HPMS values match the state 2024 GDB on 99.7% of overlap segments, confirming HPMS is the same GDOT data repackaged for federal reporting
- Fills missing roadway attributes (lanes, ownership, functional class, etc.) where the state 2024 GDB is null
- Adds pavement condition metrics (IRI, rutting, cracking)
- Sets initial signed-route flags from HPMS `routesigning` codes (91% coverage)

### 7. Verify signed-route classification

Run GDOT GPAS reference layers as the final authority on whether a segment is signed as Interstate, US Route, or State Route. GPAS overrides HPMS where it has coverage.

### 8. Project future AADT

Build `FUTURE_AADT_2044` through a four-step fill chain:
1. Direct GDOT official future AADT values
2. HPMS future AADT values
3. Direction mirror (copy from opposite travel direction)
4. Apply GDOT's implied growth rate (~1.17%/year) to current AADT for remaining gaps

Direct GDOT/HPMS/direction-mirror forecast coverage is `46,619` segments (`19.0%`); the official implied-growth step raises total `FUTURE_AADT_2044` coverage to `245,766` segments (`99.96%`).

### 9. Backfill remaining county/district gaps

County and district are normally stamped during segmentation (step 3) via boundary-crossing splits. A small tail of ~8,698 statewide routes with parsed county code `000` still has null values after that step. For these, use a representative-point spatial overlay against GDOT boundary polygons to assign the correct county and district.

### 10. Flag evacuation routes

Spatial overlay with GDOT EOC hurricane evacuation route polylines to flag segments as evacuation routes.

### 11. Derive RAPTOR compatibility fields

Calculate fields that align with the Texas RAPTOR format: `PCT_SADT`, `PCT_CADT`, `HWY_DES`, `TRUCK_PCT`, `HWY_NAME`.

### 12. Write outputs

- `roadway_inventory.db` — SQLite database with all 245,863 segments (tabular, no geometry)
- `base_network.gpkg` — GeoPackage with segment geometry + county/district boundary layers
- `roadway_inventory_cleaned.csv` — flat table export

The tabular attributes live in SQLite and the geometry/boundary layers live in GeoPackage, so the staged backend cleanly separates the two storage roles.

---

## What RAPTOR Gets

The RAPTOR `RoadwayData` loader reads from these outputs and keeps the fields it needs for scoring:

| Category | Key Fields |
|----------|-----------|
| **Identity** | `unique_id`, `ROUTE_ID`, `ROUTE_FAMILY`, `HWY_NAME`, `ROUTE_NUMBER` |
| **Location** | `COUNTY_CODE`, `COUNTY_NAME`, `DISTRICT`, `FROM_MILEPOINT`, `TO_MILEPOINT`, `geometry` |
| **Classification** | `FUNCTIONAL_CLASS`, `SYSTEM_CODE`, `ROUTE_TYPE_GDOT`, `SIGNED_ROUTE_FAMILY_PRIMARY` |
| **Traffic** | `AADT`, `TRUCK_AADT`, `TRUCK_PCT`, `K_FACTOR`, `D_FACTOR`, `VMT`, `FUTURE_AADT_2044` |
| **RAPTOR-specific** | `PCT_SADT`, `PCT_CADT`, `HWY_DES` |
| **Roadway characteristics** | `NUM_LANES`, `SPEED_LIMIT`, `SURFACE_TYPE`, `MEDIAN_TYPE`, `FACILITY_TYPE`, `OWNERSHIP` |
| **Pavement condition** | `HPMS_IRI`, `HPMS_PSR`, `HPMS_RUTTING`, `HPMS_CRACKING_PCT` |
| **Network significance** | `NHS_IND`, `URBAN_CODE` |
| **Data quality** | `AADT_2024_SOURCE`, `AADT_2024_CONFIDENCE` (4-tier: high/medium/low/missing), `AADT_2024_HPMS`, `AADT_2024_SOURCE_AGREEMENT`, `AADT_2024_STATS_TYPE`, `AADT_2024_SAMPLE_STATUS`, `current_aadt_covered` |

---

## Pipeline Flow Diagram

```
GDOT Road Inventory GDB
  → Load route geometry + join 15 attribute layers
  → Segment at traffic intervals + admin boundary crossings
      Traffic: GDOT Traffic GDB (46,029 records)
      Geography: County, District, Area Office, MPO, Regional Commission
      → 245,863 segments, each with consistent traffic + unambiguous geography
  → Post-split overlay flags (Legislative districts, City — no further splitting)
  → Parse ROUTE_ID → derive route family
  → Enrich speed limits (GPAS SpeedZone on/off system)
  → Gap-fill AADT + attributes (HPMS 2024)
  → Verify signed-route family (GPAS Reference)
  → Project FUTURE_AADT_2044 (growth rate fill)
  → Backfill remaining county/district gaps (statewide route code 000)
  → Flag evacuation routes (GDOT EOC) — secondary split at corridor boundaries
  → Derive RAPTOR fields (PCT_SADT, PCT_CADT, HWY_DES)
  → Write: roadway_inventory.db + base_network.gpkg

  ↓

RAPTOR RoadwayData loader → scoring categories
```

---

## Key Numbers

- **245,863** roadway segments in the final output
- **99.9605%** AADT coverage (Only 97 segments have no traffic data at all)
- **8 data sources** combined into one network
- **153 columns** in the staged database
- **7 GDOT districts**, **159 counties** covered statewide


