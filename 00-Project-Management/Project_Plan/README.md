# Georgia RAPTOR Pipeline — Overview

## What We're Building

A Georgia state pipeline matching the Texas pipeline in the [RAPTOR_Pipeline](https://github.com/People-Places-Solutions/RAPTOR_Pipeline) repo. Texas scores every road segment (0-10) across weighted categories and produces a Total Needs Score.

### RAPTOR Scoring Categories (matching Texas pipeline structure)
| Category | Weight | RAPTOR Class | What It Measures | Data Readiness |
|----------|--------|-------------|-----------------|----------------|
| Connectivity | 0.15 | `Connectivity` | Priority routes, traffic generators, network connections, EV corridors | Ready |
| Socioeconomic | 0.15 | `SocioEconomic` | Population & employment density (Census block level) | Ready (Census + LEHD + OPB projections) |
| Safety | 0.20 | `Safety` | Crash counts and rates (5yr/3yr/1yr) | Public interim data available. Full build still needs GEARS DSA |
| Asset Preservation | 0.20 | `AssetPreservation` | Pavement condition, bridge sufficiency/clearance | Bridges: NBI (public). Pavement: GDOT request still needed |
| Mobility | 0.20 | `Mobility` | AADT, V/C ratio, railroad crossings, congestion, travel time reliability | Mostly available. NPMRDS and 2050 projection inputs need extra unblocking |
| Freight | 0.10 | `Freight` | Truck AADT, commodity tonnage/value, freight corridors | **Major gap — no Transearch data**. FAF5 workaround |

### Database-Only Categories (Post-RAPTOR)
| Category | Datasets | Status |
|----------|----------|--------|
| Climate & Resilience | FEMA NFHL Flood Maps, 3DEP DEMs | Post-RAPTOR |
| Land Use | NLCD, GLUT, Fulton County Parcels, GA DCA | Post-RAPTOR |
| Environmental | NWI Wetlands, Critical Habitat, GA EPD GIS, GOMAS Water Quality | Post-RAPTOR |
| Cultural / Historic | National Register of Historic Places | Post-RAPTOR |
| Transit | GTFS Feeds (MARTA, ARC, CAT, etc.), NTD, Regional Transit Plans | Post-RAPTOR |
| Bicycle & Pedestrian | GDOT Bike/Ped, ARC Bike Data, Atlanta Trails, City of Atlanta layers | Post-RAPTOR |
| Environmental Justice | EPA EJScreen, CDC/ATSDR EJI | Post-RAPTOR |

These are built into the Georgia database but not wired into RAPTOR scoring initially. They can become RAPTOR categories later — see [Adding New Categories](#adding-new-categories).

---

### Data Architecture — Database-First ETL

Each dataset follows the same pipeline:
```
01-Raw-Data/              02-Data-Staging/              03-Processed-Data/
(sacred, never edit)      (ETL scripts + SQLite DBs)    (RAPTOR-ready outputs)
```

- **01-Raw-Data/**: Downloaded files exactly as received. Never modify.
- **02-Data-Staging/**: ETL scripts, JSON config lookups, **per-dataset SQLite databases** (tabular), and **themed GeoPackage files** (spatial). Each DB/GPKG is a complete source of truth.
- **03-Processed-Data/**: Analysis-ready outputs that RAPTOR category classes consume.

Download-script convention:

- Keep dataset-specific download scripts under `01-Raw-Data/<dataset>/`, preferably `01-Raw-Data/<dataset>/scripts/`.
- Keep `02-Data-Staging/` limited to post-download ETL steps such as normalization, enrichment, validation, SQLite loading, and GeoPackage generation.

**Tabular** → SQLite in `02-Data-Staging/databases/`
**Spatial** → GeoPackage in `02-Data-Staging/spatial/` (EPSG:32617), split by theme:

| GPKG File | Layers | Phase |
|-----------|--------|-------|
| `base_network.gpkg` | roadway_segments, district_boundaries, county_boundaries | 1 |
| `connectivity.gpkg` | priority_routes, nevi_corridors, alt_fuel_stations, airports, seaports, universities, military_bases, national_parks, rail_facilities, freight_generators | 2 |
| `demographics.gpkg` | tract_aggregated_blocks, block_groups, tracts, opportunity_zones | 3 |
| `safety.gpkg` | crash_points | 4 |
| `assets.gpkg` | bridges, pavement_sections | 5 |
| `mobility.gpkg` | railroad_crossings, hpms_segments | 6 |
| `climate.gpkg` | flood_zones, elevation | 9 |
| `land_use.gpkg` | land_cover, parcels_fulton | 10 |
| `environmental.gpkg` | wetlands, critical_habitat, protected_areas, water_quality_sites | 11 |
| `cultural.gpkg` | historic_places | 12 |
| `transit.gpkg` | transit_stops, transit_routes, transit_service_areas | 13 |
| `freight.gpkg` | freight_corridors, truck_routes, intermodal_terminals, port_facilities, rail_network | 14 |
| `bike_ped.gpkg` | bike_lanes, trails, sidewalks | 15 |
| `ej.gpkg` | ejscreen, eji | 16 |

### Data Inventory
All known datasets are tracked in `01-Raw-Data/Georgia_Data_Inventory.csv`. This is a living document — add new datasets as they're discovered. Currently tracking **88 datasets** across 20+ categories.

### Assessment And Options

- [Roadway Gap-Fill and Supplement Strategy](../Pipeline-Documentation/phase-1-Supplement-Docs/roadway-gap-fill-consolidated.md)

---

### Two-Repo Workflow
| Repo | Purpose |
|------|---------|
| **Georgia-Statewide-Data-Pipeline** (this repo) | ETL pipelines, per-dataset databases, RAPTOR category classes, data management |
| **RAPTOR_Pipeline** (clone later) | Integration — wire Georgia into framework, scoring engine, output. Clone in Phase 8. |

### Transfer Strategy
Code lives under `05-RAPTOR-Integration/states/Georgia/` here, mirroring the RAPTOR layout. When ready, copy to RAPTOR clone, push branch, open PR.

---

### Georgia Web App Shape

The Georgia web app should stay local-first:

- Frontend: Vite + React + TypeScript
- Backend: FastAPI
- Database: local PostGIS
- Mapping: MapLibre
- Runtime: local Docker Compose or direct local development commands

This keeps the app aligned with the current project scope while preserving a clear frontend/backend split.

---

### Phases

**RAPTOR-Required:**
| Phase | Focus | Depends On |
|-------|-------|------------|
| [Phase 1](phase-1-foundation.md) | Roadways Base Layer (Road Inventory, boundaries) | Road Inventory GDB download |
| [Phase 2](phase-2-connectivity.md) | Connectivity (SRP, NEVI, AFDC, Traffic Generators) | Phase 1 |
| [Phase 3](phase-3-socioeconomic.md) | Socioeconomic (Nationwide Census, LEHD, OPB, Employment Projections) | Phase 1 |
| [Phase 4](phase-4-safety.md) | Safety (Crash Data, FARS, GOHS) | Phase 1 |
| [Phase 5](phase-5-asset-preservation.md) | Asset Preservation (NBI Bridges, COPACES) | Phase 1 |
| [Phase 6](phase-6-mobility.md) | Mobility (HPMS, Traffic Counts, NPMRDS, Railroad Crossings) | Phase 1 |
| [Phase 7](phase-7-sharepoint.md) | SharePoint Data Organization | Phases 1-6 |
| [Phase 8](phase-8-raptor-integration.md) | RAPTOR Integration + Output | Phases 1-7 |

**Post-RAPTOR (Archived — revisit later):**
| Phase | Focus |
|-------|-------|
| [Phase 9](archive/phase-9-climate-resilience.md) | Climate & Resilience (FEMA Flood, DEMs) |
| [Phase 10](archive/phase-10-land-use.md) | Land Use (NLCD, GLUT, Parcels, DCA) |
| [Phase 11](archive/phase-11-environmental.md) | Environmental (Wetlands, Critical Habitat, GA EPD) |
| [Phase 12](archive/phase-12-cultural-historic.md) | Cultural & Historic (NRHP) |
| [Phase 13](archive/phase-13-transit.md) | Transit (GTFS Feeds, NTD, Regional Plans) |
| [Phase 14](archive/phase-14-freight.md) | Freight & Logistics (FAF5, CFS, Ports, Truck Routes, Rail) |
| [Phase 15](archive/phase-15-bicycle-pedestrian.md) | Bicycle & Pedestrian (GDOT, ARC, Atlanta) |
| [Phase 16](archive/phase-16-environmental-justice.md) | Environmental Justice (EJScreen, EJI) |

Phases 2-6 are ordered by current data availability after Phase 1: Connectivity, Socioeconomic, Safety, Asset Preservation, then Mobility. Post-RAPTOR plans are in `archive/` for future reference.

---

### Georgia-Specific Notes

### Phase 1 Status Snapshot

Phase 1 is complete for the current project scope and serves as the closed roadway-foundation baseline for downstream phases.

Current staged outputs:
- `02-Data-Staging/databases/roadway_inventory.db`: `244,904` segmented roadway rows
- `02-Data-Staging/spatial/base_network.gpkg`:
  - `roadway_segments` (`244,904` features)
  - `county_boundaries` (`159` features)
  - `district_boundaries` (`7` features)

Current traffic coverage in the staged roadway network:
- Current AADT is present on `244,819` of `244,904` segments (`99.97%`)
- Current AADT covers `133,830.64` of `133,994.38` staged segment miles (`99.88%`)
- Future AADT 2044 is present on `52,236` of `244,904` segments (`21.3%`)
- Historical AADT columns were removed from staged output; raw source files are retained for future trend work

Current classification available in Phase 1:
- `SYSTEM_CODE` for roadway system / ownership class
- `F_SYSTEM` / `FUNCTIONAL_CLASS` for functional class
- Route-ID-derived fields for route parsing

Phase 1 closeout position:
- the GDOT-based staged roadway network is accepted as the working statewide baseline
- validation results and web-app inspection are sufficient to proceed into later phases without first doing a statewide roadway supplement merge
- supplementation and richer route-family labeling remain optional follow-on improvements, not closeout blockers

Current working note for Phase 1:
- A `2026-04-04` Playwright visual check of the fully loaded staged roadway layer found no obvious planning-relevant gaps in sampled Columbus / Dinglewood, Atlanta, and Savannah views.
- The current GDOT-based network should be treated as adequate for initial statewide planning, with roadway supplementation kept as a follow-on validation/improvement track unless later QA shows planning-relevant omissions.

**Key Differences from Texas** (see [Texas_vs_Georgia_Data_Comparison.md](../Texas_vs_Georgia_Data_Comparison.md) for full analysis):
| Aspect | Texas | Georgia |
|--------|-------|---------|
| Route ID | HWY + DFO | RCLINK (11-char) + Milepoint |
| Roadway GDB | Yearly snapshots (2023, 2024) | Rolling live snapshot only — no yearly archives |
| Design AADT | `AADT_DESGN` (20-yr projection) | **Not available** — must compute growth from historic AADT |
| Pavement | TxDOT Condition/Ride/Distress CSV | COPACES (0-100) — **requires GDOT data request** |
| Bridge source | TxDOT state shapefile | NBI federal (State=13) |
| Crash data | TxDOT crash files (public) | GEARS — **requires DSA with GDOT** |
| Freight commodity | Transearch (proprietary, link-level) | **Not available** — FAF5 workaround (state/metro level) |
| Congestion model | SAM (Fort Worth TDM output) | No public GSTDM output — compute V/C from HCM formula |
| Socioeconomic source | SAMv5 TAZ (Texas-specific) | U.S. Census nationwide (block level) + OPB projections |
| Port significance | Moderate | Very high (Savannah, 3rd busiest US port) |
| Districts | 25 | 7 |
| State routes | ~80,000+ miles | ~18,000 miles |
| CRS | EPSG:3081 | EPSG:32617 (UTM Zone 17N) |

**GDOT Districts**:
| ID | HQ | Coverage |
|----|-----|----------|
| 1 | Gainesville | NE Georgia |
| 2 | Tennille | East-central |
| 3 | Thomaston | West-central |
| 4 | Tifton | South-central |
| 5 | Jesup | SE/coast |
| 6 | Cartersville | NW Georgia |
| 7 | Chamblee | Metro Atlanta |

---

### Adding New Categories

The pipeline is designed to grow. Post-RAPTOR phases (Transit, Bike/Ped, EJ, Climate, Land Use, Environmental, Cultural) are already documented and can be promoted to active RAPTOR categories when needed. When the team adds a new category:

1. **Build the ETL** — download, normalize, create_db, validate scripts in `02-Data-Staging/scripts/`
2. **Build the RAPTOR class** — `05-RAPTOR-Integration/states/Georgia/categories/NewCategory.py` following the `generate_metrics(roadways)` pattern
3. **Update the scoring schema** — add category with weight (all weights must sum to 1.0)
4. **Wire into `pipeline.py`** — import, instantiate, call `generate_metrics()`
5. **Update data inventory** — add datasets to `Georgia_Data_Inventory.csv`
6. **Re-run** — the scoring engine picks up new categories automatically

---

### Assessment And Options

- [Roadway Gap-Fill and Supplement Strategy](../Pipeline-Documentation/phase-1-Supplement-Docs/roadway-gap-fill-consolidated.md)
  - Consolidates the GDOT gap analysis, supplemental source comparison, and recommended provenance-preserving merge strategy.
