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
| **FHWA HPMS 2024** | Gap-fill for AADT where GDOT is missing; pavement condition (IRI, rutting, cracking); initial signed-route classification |
| **GDOT GPAS SpeedZone** | Posted speed limits — on-system (state highways) matched by route ID, off-system (local roads) matched by road name + county |
| **GDOT GPAS Reference Layers** | Authoritative verification of signed-route family (Interstate / US / State Route) |
| **GDOT Boundaries Service** | County and district polygons for spatial assignment |
| **GDOT EOC Evacuation Routes** | Hurricane evacuation route flags |

---

## How It Works (Step by Step)

### 1. Start with the official road network

Load the `GA_2024_Routes` layer from the GDOT Road Inventory GDB. This gives us 206,994 routes with geometry — the backbone of everything downstream.

### 2. Attach roadway attributes

Join 15 attribute layers from the same GDB onto the routes (functional class, number of lanes, surface type, median type, shoulder widths, NHS status, ownership, etc.). These join by route ID and milepoint intervals.

### 3. Segment by traffic intervals

Load the GDOT Traffic GDB (46,029 traffic records). Split the road geometry wherever traffic values change along a route. This produces ~245,863 segments, each with a consistent set of traffic values (AADT, truck AADT, VMT, K/D factors).

### 4. Parse route identity

Break down the 16-character GDOT `ROUTE_ID` into component parts (county code, system code, route number, suffix, direction). Derive the route family classification: Interstate, U.S. Route, State Route, or Local/Other.

### 5. Enrich with speed limits

Match GDOT SpeedZone permits to segments:
- **On-system** (state highways): match by route ID + milepoint overlap → ~15,000 segments
- **Off-system** (local roads): match by normalized road name + county code → ~30,000 segments

### 6. Gap-fill with HPMS

Join FHWA HPMS 2024 data by route ID + milepoint overlap:
- Fill AADT gaps (raises coverage from 19% to 99.9605%)
- Fill missing roadway attributes (lanes, ownership, functional class, etc.)
- Add pavement condition metrics (IRI, rutting, cracking)
- Set initial signed-route flags from HPMS `routesigning` codes (91% coverage)

### 7. Verify signed-route classification

Run GDOT GPAS reference layers as the final authority on whether a segment is signed as Interstate, US Route, or State Route. GPAS overrides HPMS where it has coverage.

### 8. Project future AADT

Build `FUTURE_AADT_2044` through a four-step fill chain:
1. Direct GDOT official future AADT values
2. HPMS future AADT values
3. Direction mirror (copy from opposite travel direction)
4. Apply GDOT's implied growth rate (~1.17%/year) to current AADT for remaining gaps

Direct GDOT/HPMS/direction-mirror forecast coverage is `46,619` segments (`19.0%`); the official implied-growth step raises total `FUTURE_AADT_2044` coverage to `245,766` segments (`99.96%`).

### 9. Backfill county/district gaps

For segments with null county or district (mostly statewide routes with county code `000`), use spatial overlay with GDOT boundary polygons to assign the correct county and district.

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
| **Data quality** | `AADT_2024_SOURCE`, `AADT_2024_CONFIDENCE`, `current_aadt_covered` |

---

## Pipeline Flow Diagram

```
GDOT Road Inventory GDB
  → Load route geometry + join 15 attribute layers
  → Segment at traffic intervals (GDOT Traffic GDB)
  → Parse ROUTE_ID → derive route family
  → Enrich speed limits (GPAS SpeedZone on/off system)
  → Gap-fill AADT + attributes (HPMS 2024)
  → Verify signed-route family (GPAS Reference)
  → Project FUTURE_AADT_2044 (growth rate fill)
  → Backfill county/district (GDOT Boundaries)
  → Flag evacuation routes (GDOT EOC)
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
- **118 columns** in the staged database
- **7 GDOT districts**, **159 counties** covered statewide


