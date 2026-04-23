# Phase 2a — Energy Sector Corridors

## Goal
Identify and designate Georgia highway segments that serve as energy sector transportation corridors, producing an `IS_ENERGY_SECTOR_CORRIDOR` boolean flag derived from FAF5 link-level energy commodity tonnage data.

## Status: Not Started
**Depends on**: Phase 1 (staged roadway inventory)
**Blocks**: Phase 2 (Connectivity — energy corridor metric)

---

## Approach: FAF5 Link-Level Commodity Filtering

### Key Discovery
Erik Martinez's Massachusetts Freight implementation (`Freight.py`) demonstrates that **FAF5 has link-level assignment data** — not just aggregate state/metro flows. FHWA publishes "Assignment Flow Tables" that are pre-assigned to the FAF5 highway network, with Trips and Tons per link, broken down by commodity.

This means we can:
1. Load FAF5 network links for Georgia
2. Load FAF5 flow-by-commodity CSVs
3. Filter to energy commodity codes (SCTG 15-19)
4. Flag FAF5 links with significant energy tonnage
5. Spatially transfer those flags to our roadway inventory using the perpendicular-line conflation method

This replaces the earlier manual/research-heavy approaches with a **data-driven, reproducible method** using the same architecture Erik built for Massachusetts freight.

---

## Reference: Erik's Massachusetts Freight.py

**Source**: Shared via Teams by MartinezLuna, Erik (branch `_eml`; also applied to Virginia)

**Architecture**:
1. FAF5 Network Links geometry (`.gdb`) — `Freight_Analysis_Framework__FAF5__Network_Links`, filtered by `STATE`
2. 12 CSV flow tables (6 flow types x 2 years):
   - Domestic, Export, Import, Total CU, Total SU, Total All
   - Each has Trips + Tons per link `ID`
3. Join CSVs to geometry on link `ID`
4. CAGR interpolation from 2022→2050 to target year
5. Perpendicular-line spatial conflation (20 perp lines per segment, 200m length) to transfer from FAF5 links to state roadway network
6. Route-number matching to disambiguate spatial overlaps

**Data paths** (Massachusetts):
```
.\DATA\Freight\Geom\freight_geom.gdb\Freight_Analysis_Framework__FAF5__Network_Links
.\DATA\Freight\{2022,2050}\Assignment Flow Tables\CSV Format\FAF5 * Truck Flows by Commodity_*.csv
```

---

## Georgia Energy Infrastructure

| Facility | Location | Significance |
|---|---|---|
| Port of Savannah | Chatham County | 3rd busiest US container port; energy commodity imports |
| Colonial Pipeline terminus | Atlanta metro | Largest refined petroleum pipeline in the US |
| Plant Vogtle | Burke County | 4 nuclear reactors (Units 3-4 online 2023-2024) |
| Plant Hatch | Appling County | 2 nuclear reactors |
| Savannah River Site | Aiken, SC (adjacent) | Nuclear materials processing; GA routes serve as access |
| Fuel distribution terminals | Statewide | Pipeline endpoints, tank farms, distribution hubs |

### Expected Primary Energy Corridors
| Corridor | Routes | Rationale |
|---|---|---|
| Port of Savannah access | I-16, I-95, US-17, GA-21 | Energy commodity import/distribution from port |
| Atlanta fuel distribution | I-75, I-85, I-20, I-285 | Colonial Pipeline terminus; metro fuel distribution |
| Nuclear plant access | US-1, GA-56 (Vogtle); US-1, GA-292 (Hatch) | Heavy equipment transport, fuel rod shipments |
| North-south energy spine | I-75 full length | Connects port access to Atlanta distribution hub |
| East-west distribution | I-20 | Cross-state fuel/energy equipment movement |

---

## FAF5 Data Required

### 1. FAF5 Network Links (geometry)
- **Source**: FHWA Freight Analysis Framework
- **URL**: `https://ops.fhwa.dot.gov/freight/freight_analysis/faf/`
- **Format**: File Geodatabase (`.gdb`), layer `Freight_Analysis_Framework__FAF5__Network_Links`
- **Filter**: `STATE == 'GA'`
- **Key fields**: `ID` (link ID), `Rte_Number`, `Road_Name`, `STATE`, `geometry`
- **Place in**: `01-Raw-Data/freight/geom/`

### 2. FAF5 Assignment Flow Tables — by Commodity
- **Source**: Same FHWA FAF5 download
- **Years**: 2022 (observed) + 2050 Baseline (forecast)
- **Files needed** (same as Erik's script — 12 CSVs):

| File | Description |
|---|---|
| `FAF5 Domestic Truck Flows by Commodity_2022.csv` | Domestic flows, base year |
| `FAF5 Domestic Truck Flows by Commodity_2050Base.csv` | Domestic flows, forecast |
| `FAF5 Export Truck Flows by Commodity_2022.csv` | Export flows, base year |
| `FAF5 Export Truck Flows by Commodity_2050Base.csv` | Export flows, forecast |
| `FAF5 Import Truck Flows by Commodity_2022.csv` | Import flows, base year |
| `FAF5 Import Truck Flows by Commodity_2050Base.csv` | Import flows, forecast |
| `FAF5 Total CU Truck Flows by Commodity_2022.csv` | Combination unit totals, base year |
| `FAF5 Total CU Truck Flows by Commodity_2050Base.csv` | CU totals, forecast |
| `FAF5 Total SU Truck Flows by Commodity_2022.csv` | Single unit totals, base year |
| `FAF5 Total SU Truck Flows by Commodity_2050Base.csv` | SU totals, forecast |
| `FAF5 Total Truck Flows by Commodity_2022.csv` | All truck totals, base year |
| `FAF5 Total Truck Flows by Commodity_2050Base.csv` | All truck totals, forecast |

- **Place in**: `01-Raw-Data/freight/flow_tables/`

### 3. Energy Commodity SCTG Codes
The Standard Classification of Transported Goods (SCTG) codes for energy commodities:

| SCTG | Description |
|---|---|
| 15 | Coal |
| 16 | Crude petroleum |
| 17 | Gasoline, aviation turbine fuel, and ethanol |
| 18 | Fuel oils |
| 19 | Natural gas and other fossil products |

The "by Commodity" CSVs should have per-commodity columns. Filter/sum columns for SCTG 15-19 to isolate energy tonnage per link.

---

## Implementation Steps

### Step 1: Download FAF5 data
- [ ] Download FAF5 Network Links GDB from FHWA
- [ ] Download all 12 Assignment Flow Table CSVs (2022 + 2050)
- [ ] Place in `01-Raw-Data/freight/`
- [ ] Verify Georgia links exist in the network (`STATE == 'GA'`)
- [ ] Document download metadata (URL, date, version)

### Step 2: Explore CSV column structure
- [ ] Identify how commodity columns are named in the flow CSVs
- [ ] Confirm SCTG 15-19 columns exist (may be named like `SCTG15`, `Coal`, etc.)
- [ ] Document the column-to-commodity mapping
- [ ] Check if "by Commodity" means columns per commodity or rows per commodity

### Step 3: Build energy corridor extraction script
- [ ] Create `01-Raw-Data/freight/scripts/extract_energy_corridors.py`
- [ ] Load FAF5 network links, filter to Georgia
- [ ] Load flow-by-commodity CSVs, join to links on `ID`
- [ ] Sum energy commodity tonnage (SCTG 15-19) per link for 2022 and 2050
- [ ] Compute energy tonnage share (energy tons / total tons) per link
- [ ] Flag links exceeding a tonnage or share threshold as energy corridors
- [ ] Export flagged links as GeoDataFrame

### Step 4: Threshold determination
- [ ] Analyze distribution of energy tonnage across Georgia FAF5 links
- [ ] Set threshold: e.g., top quartile of energy tonnage, or absolute minimum (e.g., >10,000 tons/year)
- [ ] Validate against expected corridors (I-16, I-75, I-95, I-20 should be flagged)
- [ ] Adjust threshold if too permissive (flags rural roads) or too strict (misses known corridors)

### Step 5: Spatial conflation to roadway inventory
- [ ] Adapt Erik's perpendicular-line conflation method for Georgia:
  - Reproject FAF5 links and roadway inventory to `EPSG:32617`
  - 20 perpendicular lines per roadway segment, 200m length
  - Route-number matching (`ROUTE_TYPE_GDOT` + route number vs FAF5 `Rte_Number`)
  - Best-match by intersection count
- [ ] Transfer `IS_ENERGY_SECTOR_CORRIDOR` boolean to roadway segments
- [ ] Optionally transfer `ENERGY_TONS_{year}` numeric field for analyst reference

### Step 6: Validation
- [ ] I-16 and I-95 near Savannah should be flagged (port access)
- [ ] I-75 through Georgia should be flagged (north-south spine)
- [ ] I-20 should be flagged (east-west distribution)
- [ ] I-85 Atlanta area should be flagged (Colonial Pipeline terminus distribution)
- [ ] Rural county roads should generally NOT be flagged
- [ ] Total flagged mileage is plausible (likely 1,000-3,000 miles)
- [ ] Cross-reference with expected corridors table above

---

## Relationship to Other Phases

### Phase 2 (Connectivity)
- Consumes `IS_ENERGY_SECTOR_CORRIDOR` as a boolean metric (default weight 0.00, UI-overridable)
- No other Phase 2 dependency on the FAF5 flow tables

### Phase 14 (Freight) — future
- Will reuse the same FAF5 data download (network links + all flow tables)
- Will use ALL commodity flows (not just energy), following Erik's full Freight.py pattern
- Will produce: `Truck_Tonnage`, `Truck_Trips`, growth projections, freight corridor flags
- Energy corridor work done here will be a subset of Phase 14's scope
- **Shared data**: `01-Raw-Data/freight/` directory serves both Phase 2a and Phase 14

### Phase 1b (SRP Derivation)
- SRP does not directly use energy corridors, but energy corridor data validates that Critical-priority freight corridors align with energy infrastructure

---

## Deliverables
- `01-Raw-Data/freight/geom/` — FAF5 Network Links GDB
- `01-Raw-Data/freight/flow_tables/` — 12 FAF5 Assignment Flow CSVs
- `01-Raw-Data/freight/scripts/extract_energy_corridors.py`
- `IS_ENERGY_SECTOR_CORRIDOR` boolean field in staged roadway output
- Optional: `ENERGY_TONS_{year}` numeric field for analyst reference

## Open Questions
1. How are commodity columns structured in the "by Commodity" CSVs — per-column or per-row? (Need to inspect after download)
2. What tonnage threshold defines "energy corridor"? (Data-driven — inspect distribution first)
3. Should we include corridors serving renewable energy infrastructure (solar farm equipment, wind turbine components) or focus on fossil fuel/nuclear only? (SCTG 15-19 covers fossil fuels; renewable equipment falls under other SCTG codes)
4. Erik's script is for Massachusetts — confirm the FAF5 download is national (covers all states) so we don't need a state-specific download
