# Unblocking Phases 4, 5, and 6

**Date**: 2026-03-28
**Purpose**: Steps the team needs to take to unblock data acquisition for three RAPTOR categories that depend on restricted GDOT data.

---

## Summary

| Phase | Category (Weight) | Blocker | Who Needs to Act | Urgency |
|-------|-------------------|---------|------------------|---------|
| 4 | Safety (0.20) | GEARS crash data — requires Data Sharing Agreement | GDOT Office of Transportation Data | **Critical** |
| 5 | Asset Preservation (0.20) | COPACES pavement data — not publicly available | GDOT Office of Maintenance (GAMS system) | High |
| 6 | Mobility (0.20) | Historic AADT data format unknown; NPMRDS requires DSA | GDOT OTD + FHWA/RITIS | Medium |

Combined, these three categories account for **60% of the RAPTOR Total Needs Score**. Without them, we can only score Connectivity (0.15), Socioeconomic (0.15), and Freight (0.10) = 40%.

---

## Phase 4 — Safety: GEARS Crash Data

### What We Need
Access to **GEARS** (Georgia Electronic Accident Reporting System) crash records. Specifically:
- 5 years of crash data (2021-2025, or most recent 5-year window)
- Per-crash fields: Crash ID, date, severity (KABCO scale), location (RCLINK + milepoint or lat/lon), fatality flag
- Vehicle/pedestrian/bicycle involvement flags (for future metrics)

### Why We Need It
The Safety category computes crash counts and crash rates per road segment using a 5-year rolling window. This is **the most critical blocker** — Safety is weighted at 0.20 and requires incident-level data with route location for segment matching. Texas gets crash files directly from TxDOT.

### Who to Contact
- **GDOT Office of Transportation Data**
- Email: OTDCustomerService@dot.ga.gov
- Phone: (770) 986-1436
- May also need to go through **LexisNexis** (GEARS portal operator): https://www.gearsportal.com/

### What to Ask
> We are conducting a statewide roadway needs assessment for GDOT using the RAPTOR framework. We need to establish a Data Sharing Agreement (DSA) for access to GEARS crash data. Specifically:
>
> - **Scope**: All reportable crashes on Georgia public roads
> - **Years**: Most recent 5-year period (2021-2025 preferred)
> - **Fields needed**: Crash ID, crash date, crash severity (KABCO), route identifier (RCLINK or highway name), milepoint or lat/lon, fatality flag, number of vehicles/pedestrians/bicyclists involved
> - **Format**: CSV or database export preferred
> - **Use**: Non-public internal analysis for GDOT roadway prioritization
>
> What is the process for establishing a DSA? What is the typical turnaround time?

### Fallback If Delayed
- **FARS** (Fatality Analysis Reporting System, NHTSA): Public download at https://www.nhtsa.gov/research-data/fatality-analysis-reporting-system-fars — covers fatal crashes only (K in KABCO), not the full severity spectrum. Allows partial safety scoring.
- **GDOT Crash Data Dashboard**: https://gdot.aashtowaresafety.net/crash-data — interactive, may allow some data export for validation
- **GA GOHS county-level data**: Aggregate safety stats, not incident-level

### What We Can Do Now (While Waiting)
- Download FARS fatal crash data for Georgia and build the ETL pipeline
- Build Safety.py class with full metrics structure, tested against FARS
- When GEARS data arrives, swap in the full dataset — the code won't need major changes

---

## Phase 5 — Asset Preservation: COPACES Pavement Data

### What We Need
GDOT uses the **COPACES** (Computerized Pavement Condition Evaluation System) rating system. We need:
- COPACES composite rating (0-100) per route segment
- Individual distress scores if available (load cracking, block cracking, rutting, raveling, etc.)
- Route ID (RCLINK) and milepoint range for each record

### Why We Need It
The Asset Preservation category scores pavement condition and bridge sufficiency. Bridges are covered by the public NBI dataset, but pavement is half the category weight. Texas gets `Pavement_data.csv` directly from TxDOT with Condition Score, Ride Score, and Distress Score.

### Who to Contact
- **GDOT Office of Maintenance** — manages the GAMS (Georgia Asset Management System)
- **GDOT Office of Transportation Data** — may also have access
- Email: OTDCustomerService@dot.ga.gov
- Phone: (770) 986-1436

### What to Ask
> We are working on a statewide roadway needs assessment for GDOT using the RAPTOR prioritization framework. We need COPACES pavement condition ratings by route segment (RCLINK + milepoint range) for the most recent available year. Ideally:
> - COPACES composite rating (0-100)
> - Individual distress type scores if available
> - Route identification (RCLINK or equivalent) and milepoint FROM/TO
> - CSV or Excel format preferred
>
> If COPACES data is not available externally, can you provide IRI (International Roughness Index) values by segment?

### Fallback If Unavailable
- **FHWA HPMS pavement data**: Contains IRI, cracking percentage, and rutting for Federal-Aid roads. Available at https://www.fhwa.dot.gov/policyinformation/hpms.cfm
- This covers fewer roads and fewer metrics than COPACES, but allows partial scoring

### What We Can Do Now (While Waiting)
- Download NBI bridge data and build the bridge half of Asset Preservation
- Build the AssetPreservation.py class with bridge metrics active and pavement metrics stubbed (weight=0 until data arrives)

---

## Phase 6 — Mobility: Historic AADT + NPMRDS

Phase 4 has **two sub-blockers**, neither as severe as Phases 2-3.

### 4A: Historic AADT Data (for 2050 Growth Projection)

#### The Problem
Texas has `AADT_DESGN` (a 20-year design AADT) built into their roadway GDB, used for 2050 traffic projection. Georgia does not have this field. We need historic AADT by segment to compute growth rates ourselves.

#### What to Do

**Step 1 — Download and inspect these files from the GDOT file server** (no permission needed, public):

| File | URL | Size |
|------|-----|------|
| Traffic_Historical.zip | https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/Traffic_Historical.zip | ~591 MB |
| 2010-2019 Published Traffic | https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/2010_thr_2019_Published_Traffic.zip | ~516 MB |

**Step 2 — Evaluate the contents**:
- What format? (CSV, Excel, GDB?)
- What years are covered?
- Is AADT by segment (with RCLINK/route ID)?
- Can we match to current road inventory segments?

**Step 3 — If insufficient, supplement with**:
- HPMS yearly shapefiles (2011-2023): https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles.cfm — Federal-Aid roads only but have AADT per year
- ARC historical counts (2008-2017): https://opendata.atlantaregional.com/datasets/c9ce7fe9c5f94f338422e4d5c7119158_0 — Metro Atlanta only

**Step 4 — If all public sources are insufficient, ask GDOT**:
> Do you maintain historical AADT records by route segment (RCLINK + milepoint) going back 5-10 years? We need year-over-year AADT to compute traffic growth rates for a 2050 projection in the RAPTOR needs assessment.

#### What We Can Do Now
- V/C ratio computation, railroad crossings, and road classification metrics all work without historic AADT
- The 2050 AADT projection is the only metric that depends on this data
- If no historic data is found, a flat statewide growth rate (from GDOT Traffic Monitoring Program Guide) can serve as a rough fallback

---

### 4B: NPMRDS Travel Time Data (DSA Required)

#### What We Need
Access to **NPMRDS** (National Performance Management Research Data Set) for Georgia. This provides:
- Probe-based travel times at 5-minute intervals on NHS segments
- Used to compute LOTTR (Level of Travel Time Reliability) and TTTR (Truck Travel Time Reliability)

#### Who to Contact
- **FHWA / RITIS**: https://npmrds.ritis.org/analytics/
- GDOT likely already has a state-level DSA — check with GDOT Office of Transportation Data

#### What to Ask
> Does GDOT have an existing NPMRDS Data Sharing Agreement through RITIS? If so, can we access Georgia travel time data for the RAPTOR needs assessment? We need segment-level travel times for LOTTR and TTTR computation on NHS routes.

#### Priority
**Medium** — NPMRDS metrics (LOTTR, TTTR) are included in the Georgia plan but weighted at 0.00 initially. They are enhancements beyond what Texas currently uses. The core Mobility metrics (V/C, railroad crossings, AADT) work without NPMRDS.

---

## Action Item Tracker

| # | Action | Owner | Status | Target Date |
|---|--------|-------|--------|-------------|
| 1 | Email GDOT OTD requesting GEARS DSA | | ⬜ Not started | |
| 2 | Email GDOT Maintenance requesting COPACES data | | ⬜ Not started | |
| 3 | Download Traffic_Historical.zip and inspect | | ⬜ Not started | |
| 4 | Download 2010-2019 Published Traffic and inspect | | ⬜ Not started | |
| 5 | Check with GDOT re: existing NPMRDS/RITIS DSA | | ⬜ Not started | |
| 6 | Download FARS fatal crash data as interim | | ⬜ Not started | |
| 7 | Download NBI bridge data for Georgia | | ⬜ Not started | |
| 8 | Download HPMS yearly shapefiles (2011-2023) | | ⬜ Not started | |

---

## Contact Reference

| Office | Email | Phone | For |
|--------|-------|-------|-----|
| GDOT Office of Transportation Data | OTDCustomerService@dot.ga.gov | (770) 986-1436 | GEARS crash data, traffic data, NPMRDS |
| GDOT Office of Maintenance | (find contact via GDOT directory) | | COPACES pavement data |
| FHWA RITIS | https://npmrds.ritis.org/ | | NPMRDS DSA |
| NHTSA FARS | https://www.nhtsa.gov/research-data/fatality-analysis-reporting-system-fars | | Fatal crash data (public) |
| FHWA NBI | https://www.fhwa.dot.gov/bridge/nbi.cfm | | Bridge data (public) |
