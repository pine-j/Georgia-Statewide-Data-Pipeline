# Phase 5 — GDOT Restricted Data (DSA-Dependent)

## Goal
Obtain, stage, and score datasets that require a data-sharing agreement (DSA) with GDOT or other agencies. This phase completes three RAPTOR categories: **Safety** (0.20), **Asset Preservation** (0.20, pavement), and **Mobility** (0.20, travel time reliability).

## Status: Blocked on DSA
**Depends on**: Phase 1 (roadway base layer)
**Blocked by**: GDOT data-sharing agreements for GEARS, COPACES, and NPMRDS

---

## Absorbed From
- Old Phase 4 (Safety) — GEARS crash data, FARS
- Old Phase 5 (Asset Preservation) — COPACES pavement data
- Old Phase 6 (Mobility) — NPMRDS travel time reliability

These three datasets share a common blocker: they all require formal data-sharing agreements with GDOT or partner agencies. Grouping them ensures a single DSA negotiation can unblock all three RAPTOR categories.

---

## Datasets

### 5.1 GEARS Crash Data (Primary — Safety)
- **Source**: GDOT Georgia Electronic Accident Reporting System
- **Status**: Requires DSA with GDOT
- **Years**: 5-year rolling window (2021-2025 if available)
- **Format**: CSV
- **Key fields**: Crash_ID, Crash_Date, Severity (KABCO), RCLINK, Milepoint, Lat/Lon
- **Join**: RCLINK + milepoint (crash within segment FROM_MEASURE to TO_MEASURE)
- **Place in**: `01-Raw-Data/safety/{year}/`

### 5.2 FARS Fatal Crashes (Interim — Safety)
- **Source**: NHTSA Fatality Analysis Reporting System
- **Status**: Public (can download without DSA)
- **Years**: 2020-2024
- **Format**: CSV
- **Use**: Fatal-only crash data as interim while GEARS DSA is in progress
- **Place in**: `01-Raw-Data/safety/fars/`

### 5.3 GDOT Crash Data Dashboard (Supplement)
- **Source**: GDOT
- **Format**: Interactive dashboard
- **Use**: Cross-reference; supplemental crash counts if GEARS delayed

### 5.4 GA GOHS Traffic Safety Data (Supplement)
- **Source**: GA Governor's Office of Highway Safety
- **Format**: Reports, PDF, county data sheets
- **Use**: Validation source for crash totals

### 5.5 COPACES Pavement Data (Asset Preservation)
- **Source**: GDOT GAMS (internal)
- **Status**: Requires DSA with GDOT
- **Format**: CSV
- **Key fields**: COPACES Rating (0-100 composite), individual distress scores
- **Fallback**: FHWA HPMS pavement data (IRI, cracking, rutting) — already in Phase 1 HPMS download, fields just need mapping
- **Place in**: `01-Raw-Data/pavement/`

### 5.6 NPMRDS Travel Times (Mobility)
- **Source**: FHWA / RITIS
- **Status**: Requires separate DSA
- **Format**: CSV (probe-based travel times, 5-min intervals)
- **Coverage**: NHS segments, 2017-present
- **Key metrics**: LOTTR (Level of Travel Time Reliability), TTTR (Truck Travel Time Reliability)
- **Place in**: `01-Raw-Data/mobility/npmrds/`

---

## RAPTOR Category Classes

### Safety (Weight: 0.20)

**File**: `05-RAPTOR-Integration/states/Georgia/categories/Safety/Safety.py`

**VMT**: `AADT × segment_length_miles × 365 × num_years / 100,000,000`

**Metrics**:

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| Num_Incap_Fatal_Crashes_5yr | min_max_standard | 0.33 | GEARS (or FARS interim) |
| Overall_Crash_Rate_5yr | min_max_standard | 0.34 | GEARS |
| Incap_Fatal_Crash_Rate_5yr | min_max_standard | 0.33 | GEARS |

Additional (weight=0): Total crashes, fatal crashes (5yr/3yr/1yr), pedestrian/bicycle counts

### Asset Preservation — Pavement (Weight: 0.20, completing Phase 2 bridges)

**File**: `05-RAPTOR-Integration/states/Georgia/categories/AssetPreservation.py` (update)

**Metric added in Phase 5**:

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| COPACES_Rating | min_max_inverted | 0.33 | GDOT GAMS |

**HPMS fallback** (if COPACES unavailable): IRI, PSR, rutting, cracking from HPMS 2024 (already downloaded in Phase 1, fields need mapping in `hpms_enrichment.py`)

### Mobility — Travel Time Reliability (Weight: 0.20, completing Phase 2 + 4)

**Metrics added in Phase 5**:

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| LOTTR | threshold | 0.00 | NPMRDS |
| TTTR | threshold | 0.00 | NPMRDS |

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| GEARS Crash Data (5yr) | GDOT | No | ⬜ Request DSA |
| FARS Fatal Crashes | NHTSA | Yes | ⬜ Download (interim) |
| GDOT Crash Dashboard | GDOT | Yes | ⬜ Explore |
| GA GOHS Safety Data | GA GOHS | Yes | ⬜ Download |
| COPACES Pavement | GDOT GAMS | No | ⬜ Request DSA |
| NPMRDS Travel Times | FHWA/RITIS | DSA | ⬜ Request DSA |

## Deliverables
- `02-Data-Staging/databases/safety.db`
- `02-Data-Staging/spatial/safety.gpkg` (layer: `crash_points`)
- `05-RAPTOR-Integration/states/Georgia/categories/Safety/Safety.py`
- Updated `AssetPreservation.py` with COPACES metrics
- Updated `Mobility.py` with LOTTR/TTTR metrics

## Verification
- [ ] Crash counts per year are plausible
- [ ] Severity distribution makes sense (K < A < B < C < O)
- [ ] Crashes join to correct segments by RCLINK + milepoint
- [ ] Crash rates capped at 96th percentile
- [ ] COPACES ratings in 0-100 range (or HPMS IRI fallback populated)
- [ ] NPMRDS TMC segments cover NHS routes
- [ ] LOTTR values plausible on known unreliable corridors (I-285, GA-400)
