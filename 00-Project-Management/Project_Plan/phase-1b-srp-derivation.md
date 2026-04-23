# Phase 1b — Derived State Route Prioritization (SRP)

## Goal
Fill data gaps and build a reproducible, 2024-data-driven State Route Prioritization classification that mirrors GDOT's official SRP criteria (established 2015, last reviewed biennially). The derived SRP replaces reliance on the static 2015 MapServer layers with a transparent, updatable classification.

## Status: Complete
**Depends on**: Phase 1 (staged roadway inventory with 263,947 segments)
**Blocks**: Phase 2 (Connectivity — `Is_Seg_On_SRP_Critical_or_High` metric) — **unblocked**

### Pipeline Run Results (2026-04-23)
- **NHFN**: 1,174 segments (concentrated on Interstates)
- **STRAHNET_TYPE**: 2,155 segments filled
- **GRIP corridors**: 2,662 segments across 20 corridors
- **Nuclear EPZ**: 52 segments (15 Vogtle, 37 Hatch)
- **Sole connections**: 3,548 segments across 156 county seats
- **SRP distribution**: Critical=5,530 (2.1%), High=33,969 (12.9%), Medium=90 (0.0%), Low=224,358 (85.0%)
- **41/41 tests pass**

---

## Background

GDOT's Office of Transportation Data created the State Route Prioritization Network in 2014-2015, classifying ~18,000 centerline miles into four tiers. The criteria are well-documented but the published GIS layers (MapServer at `maps.itos.uga.edu`, layers 13-16) have not been independently versioned or dated, and the underlying data (AADT, NHS, STRAHNET) has changed substantially since 2015.

Our staged roadway inventory (2024 GDOT GDB + HPMS enrichment) already contains ~11 of 13 SRP criteria fields. This phase fills the remaining gaps and implements the classification logic.

**Sources**:
- [Roads & Bridges: Setting Priorities (2016)](https://www.roadsbridges.com/maintenance/highway-maintenance/article/10648431/setting-priorities)
- [GDOT TAM: State Route Prioritization PDF](https://www.dot.ga.gov/InvestSmart/TAM/GeorgiaStateRoutePrioritization.pdf)
- [GDOT GRIP Fact Sheet](https://www.dot.ga.gov/InvestSmart/GRIP/Resources/GRIPSystemSummaryFactSheet.pdf)

---

## SRP Criteria → Staged Field Mapping

### Critical
| Criterion | Staged Field | Status |
|---|---|---|
| Interstates | `FUNCTIONAL_CLASS = 1` or `SIGNED_INTERSTATE_FLAG` | Ready |
| National freight corridors | `NHFN` from HPMS 2024 | **Gap 1 — needs enrichment** |
| State freight corridors | `STRAHNET` (1=Regular, 2=Connector) | Ready |
| Intermodal connectors | `NHS_IND` codes 2-9 | Ready |

### High
| Criterion | Staged Field | Status |
|---|---|---|
| STRAHNET / STRAHNET connectors | `STRAHNET` | Ready |
| NHS principal arterials (AADT > 3,000) | `NHS_IND` + `AADT_2024_HPMS` | Ready |
| US routes | `SIGNED_US_ROUTE_FLAG` or `ROUTE_TYPE_GDOT = 'US'` | Ready |
| GRIP corridors | — | **Gap 2 — needs GIS data** |
| Nuclear plant evacuation zones | — | **Gap 3 — needs EPZ route data** |
| GEMA evacuation routes | `SEC_EVAC` (hurricane only) | **Gap 4 — partial coverage** |
| Sole county-seat connections | — | **Gap 5 — needs network analysis** |
| AADT > 3,000 (non-NHS) | `AADT_2024_HPMS` | Ready |

### Medium
| Criterion | Staged Field | Status |
|---|---|---|
| Hurricane evacuation routes | `SEC_EVAC`, `SEC_EVAC_CONTRAFLOW` | Ready |
| NHS with AADT < 3,000 | `NHS_IND` + `AADT_2024_HPMS` | Ready |
| US highways with 4+ lanes | `SIGNED_US_ROUTE_FLAG` + `THROUGH_LANES` | Ready |

### Low (default — everything not classified above)
| Criterion | Staged Field | Status |
|---|---|---|
| AADT < 3,000 | `AADT_2024_HPMS` | Ready |
| Speed limit < 35 mph | `SPEED_LIMIT` | Ready |
| Segment length < 5 mi | Computable from geometry | Ready |
| Limited connectivity | Derived from network analysis | Covered by Gap 5 |

---

## Gap Fills

### Gap 1: NHFN Enrichment
- **What**: Add `nhfn` and `strahnet_type` fields from HPMS 2024 to the staged output
- **Where**: `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py`, `HPMS_GAP_FILL_FIELDS` dict (~line 91)
- **Action**: Add `"nhfn": ("NHFN", "int")` and `"strahnet_type": ("STRAHNET_TYPE", "int")` to the dict
- **Rerun**: Execute HPMS enrichment step to populate fields in the staged DB/CSV
- **Validation**: NHFN flags should appear on major freight corridors (I-75, I-95, I-16, I-20)
- **Effort**: Small (code change + rerun)

### Gap 2: GRIP Corridors
- **What**: Governor's Road Improvement Program — 19 economic development corridors + 3 truck access routes, 3,323 miles total, connecting 95% of GA cities (pop >2,500) to the Interstate system. Established by GA General Assembly in 1989.
- **Source options** (investigate in order):
  1. GDOT Route Network MapServer (`rnhp.dot.ga.gov`) — may have a GRIP layer
  2. GDOT FunctionalClass MapServer — check for GRIP-related layers beyond 0-22
  3. GDOT ArcGIS Hub / Open Data portal
  4. Manual: GRIP corridors use state route numbers in the 500-599 range — filter `ROUTE_TYPE_GDOT` + route number
  5. Digitize from GDOT GRIP corridor map if no GIS source available
- **Join**: ROUTE_ID + milepoint or spatial overlay
- **Place in**: `01-Raw-Data/connectivity/grip_corridors/`
- **Effort**: Medium

### Gap 3: Nuclear Plant Evacuation Zones
- **What**: Emergency Planning Zone (EPZ) routes for Georgia's two nuclear facilities:
  - **Plant Vogtle** (Burke County) — 4 operating reactors (Units 3-4 are newest US reactors, online 2023-2024)
  - **Plant Hatch** (Appling County) — 2 operating reactors
- **EPZ radius**: NRC standard is 10-mile plume exposure pathway zone
- **Source options**:
  1. GEMA (Georgia Emergency Management & Homeland Security Agency) — may publish EPZ route maps
  2. NRC public documents for each plant's emergency plan
  3. County emergency management offices (Burke, Appling, and adjacent counties)
  4. Derive: buffer 10 miles around each plant, flag all state routes within buffer
- **Join**: Spatial overlay (buffer intersection)
- **Place in**: `01-Raw-Data/connectivity/nuclear_epz/`
- **Effort**: Medium

### Gap 4: GEMA Evacuation Routes (beyond hurricane)
- **What**: GDOT's SRP High criteria reference "Georgia Emergency Management Agency Evacuation Routes" separately from hurricane evacuation routes. Our `SEC_EVAC` field comes from GDOT EOC layers 7-8 (hurricane-specific only).
- **Investigation needed**:
  1. Does GEMA publish a broader evacuation route network beyond hurricane routes?
  2. Are GEMA routes a superset of the hurricane routes we already have?
  3. Check GEMA's public GIS portal or data catalog
- **Likely outcome**: GEMA routes may largely overlap with `SEC_EVAC`. If no separate data is available, document the gap and use `SEC_EVAC` as the best available proxy.
- **Effort**: Low (research) to Medium (if new data source found)

### Gap 5: Sole County-Seat Connections
- **What**: Identify state routes that are the only road connection between a county seat and the broader state route network. Removing these routes would isolate the county seat.
- **Approach**:
  1. Obtain county seat locations (159 Georgia counties) — Census Bureau or Georgia GIS Clearinghouse
  2. Build a network graph of the state route system (nodes = intersections, edges = route segments)
  3. For each county seat, identify the set of state routes connecting it to the network
  4. Flag routes where the county seat has only one connecting state route (single point of failure)
  5. Alternatively: identify bridge/cut edges in the subgraph around each county seat
- **Libraries**: networkx for graph analysis, shapely for spatial ops
- **Validation**: Rural counties in south Georgia are most likely to have sole connections
- **Place in**: `02-Data-Staging/scripts/01_roadway_inventory/sole_county_seat_connections.py`
- **Effort**: High

---

## Implementation Steps

### Step 1: NHFN Enrichment (Gap 1)
- [ ] Add `nhfn` and `strahnet_type` to `HPMS_GAP_FILL_FIELDS`
- [ ] Rerun HPMS enrichment
- [ ] Validate NHFN flags on expected corridors
- [ ] Update staged DB, GPKG, and CSV outputs

### Step 2: GRIP Corridor Data (Gap 2)
- [ ] Investigate GDOT GIS sources for GRIP layer
- [ ] Download or build GRIP corridor dataset
- [ ] Spatial join to staged roadway segments
- [ ] Add `IS_GRIP_CORRIDOR` boolean field to staged output

### Step 3: Nuclear EPZ Routes (Gap 3)
- [ ] Source Plant Vogtle and Plant Hatch locations (lat/lon)
- [ ] Obtain or derive 10-mile EPZ boundaries
- [ ] Flag state routes within EPZ
- [ ] Add `IS_NUCLEAR_EPZ_ROUTE` boolean field to staged output

### Step 4: GEMA Route Investigation (Gap 4)
- [ ] Research GEMA evacuation route data availability
- [ ] If available: download, join, add `IS_GEMA_EVAC_ROUTE` field
- [ ] If not available: document gap, use `SEC_EVAC` as proxy

### Step 5: Sole County-Seat Connections (Gap 5)
- [ ] Download Georgia county seat locations (159 points)
- [ ] Build state route network graph
- [ ] Identify sole-connection routes via cut-edge analysis
- [ ] Add `IS_SOLE_COUNTY_SEAT_CONNECTION` boolean field
- [ ] Validate against known rural isolated county seats

### Step 6: Derive SRP Classification
- [ ] Implement `derive_srp_priority()` in `02-Data-Staging/scripts/01_roadway_inventory/srp_derivation.py`
- [ ] Apply criteria in priority order: Critical → High → Medium → Low
- [ ] Add `SRP_DERIVED` field (values: Critical, High, Medium, Low) to staged output
- [ ] Add `SRP_DERIVED_REASONS` field (pipe-delimited list of matching criteria)

### Step 7: Validation
- [ ] Download official SRP layers from GDOT MapServer (layers 13-16) for comparison
- [ ] Compare derived vs official SRP at segment level — compute confusion matrix
- [ ] Document discrepancies (expected: derived should promote some segments due to newer AADT/NHS data)
- [ ] Generate summary report: segment counts and route-miles per priority level (derived vs official)
- [ ] Spot-check: I-75, I-95, I-16 should be Critical; GRIP corridors should be High; rural county roads should be Low

---

## Deliverables
- Updated `hpms_enrichment.py` with NHFN/STRAHNET_TYPE fields
- `01-Raw-Data/connectivity/grip_corridors/` — GRIP corridor GIS data
- `01-Raw-Data/connectivity/nuclear_epz/` — Nuclear EPZ boundary data
- `02-Data-Staging/scripts/01_roadway_inventory/srp_derivation.py` — Classification logic
- `02-Data-Staging/scripts/01_roadway_inventory/sole_county_seat_connections.py` — Network analysis
- Updated staged outputs with new fields: `NHFN`, `STRAHNET_TYPE`, `IS_GRIP_CORRIDOR`, `IS_NUCLEAR_EPZ_ROUTE`, `IS_GEMA_EVAC_ROUTE` (if available), `IS_SOLE_COUNTY_SEAT_CONNECTION`, `SRP_DERIVED`, `SRP_DERIVED_REASONS`
- Validation report: derived vs official SRP comparison

## Verification
- [ ] All 245,863 segments have an `SRP_DERIVED` value (no nulls)
- [ ] Critical count is plausible (~2,000-4,000 segments for interstates + freight corridors)
- [ ] High count includes all GRIP corridor segments
- [ ] NHFN flag populated on I-75, I-95, I-16, I-20
- [ ] Plant Vogtle and Plant Hatch EPZ routes flagged
- [ ] At least some sole county-seat connections identified in rural south Georgia
- [ ] Derived SRP agrees with official SRP on >80% of segments
- [ ] Discrepancies are explainable (newer data, AADT shifts, NHS reclassifications)
