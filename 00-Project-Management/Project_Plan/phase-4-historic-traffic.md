# Phase 4 — Historic Traffic & Growth Modeling

## Goal
Download, stage, and analyze historic traffic count data to compute AADT growth rates, 2050 projections, V/C ratios, and congestion metrics for the RAPTOR **Mobility** category (0.20 weight).

## Status: Not Started
**Depends on**: Phase 1 (roadway base layer with current AADT)

---

## Absorbed From
- Old Phase 6 (Mobility) — traffic count datasets, V/C computation, LOS, congestion
- AADT modeling v2 (station-Actuals k-NN model) — from TODO

---

## Datasets

### 4.1 GDOT Traffic_Historical.zip (Statewide)
- **Source**: GDOT file server
- **URL**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/Traffic_Historical.zip`
- **Format**: ~591 MB zip
- **Coverage**: Statewide, years TBD (need to download and inspect)
- **Use**: Primary source for statewide historic AADT trend analysis
- **Place in**: `01-Raw-Data/mobility/`

### 4.2 GDOT 2010-2019 Published Traffic
- **Source**: GDOT file server
- **URL**: `https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/2010_thr_2019_Published_Traffic.zip`
- **Format**: ~516 MB zip
- **Coverage**: 2010-2019, statewide
- **Use**: Decade of published traffic counts for trend analysis
- **Place in**: `01-Raw-Data/mobility/`

### 4.3 GDOT Traffic Counts (TADA)
- **Source**: GDOT TADA application
- **URL**: `https://gdottrafficdata.drakewell.com/`
- **Format**: CSV (historical counts by station)
- **Place in**: `01-Raw-Data/mobility/`

### 4.4 HPMS Georgia Shapefiles (Yearly, 2011-2023)
- **Source**: FHWA HPMS
- **URL**: `https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles.cfm`
- **Format**: Shapefiles by year
- **Coverage**: 2011-2023, Federal-Aid roads
- **Use**: Yearly AADT snapshots for growth rate computation
- **Place in**: `01-Raw-Data/mobility/hpms/`

### 4.5 ARC Historical Traffic Counts (2008-2017)
- **Source**: Atlanta Regional Commission / GDOT
- **Format**: Shapefile
- **Coverage**: 2008-2017, Metro Atlanta
- **Use**: Historical trend supplement for metro area
- **Place in**: `01-Raw-Data/mobility/`

### 4.6 Georgia Traffic Monitoring Program Guide (2025)
- **Source**: GDOT
- **Format**: PDF (reference only)
- **Use**: Count station locations and methodology; WIM sites for truck factors

### 4.7 GEOCOUNTS
- **Source**: Third-party interface to GDOT count data
- **Use**: Cross-reference with TADA

---

## Processing

### AADT 2050 Projection
Texas uses `AADT_DESGN` (20-year design AADT) for exponential projection. Georgia lacks this field. Workaround:
1. Build historic AADT time series per segment from GDOT + HPMS yearly data
2. Compute compound annual growth rate (CAGR) per segment
3. Project to 2050: `AADT_2050 = AADT_current × (1 + CAGR)^(2050 - current_year)`
4. Cap unreasonable projections (growth rate outliers)

This includes the **AADT modeling v2** work (Direction C: station-Actuals target, k-NN station model).

### V/C Ratio (HCM formula)
```
Volume = K_FACTOR × D_FACTOR × AADT × 0.01 × 0.01
fhv = 1 / (1 + TRUCK_PCT × 0.01 × 0.5)
Lane_peak = LANES // 2 (minimum 1)
Capacity = 2200 × fhv × Lane_peak
VC_Ratio = Volume / Capacity
```

### Road Category
2L / 4U+ / 4D+ from lane count + median type.

### Top Congested Flag
Derive from computed V/C ratios — rank segments, flag top N or V/C > threshold.

---

## RAPTOR Metrics (Mobility — Phase 4 contributions)

| Metric | Type | Default Weight | Source |
|--------|------|----------------|--------|
| AADT_Current | min_max_standard | 0.00 | Phase 1 roadway inventory |
| AADT_2050 | min_max_standard | 0.00 | Historic trend projection |
| Volume_to_Capacity_Ratio | threshold | 0.00 | HCM formula |
| Top_Congested_Flag | boolean | 0.33 | Derived from V/C ranking |
| Roadway_Cross_Section | categorical | 0.00 | Lane count + median |
| Num_Lanes | min_max_standard | 0.00 | Phase 1 |
| LOS_Model | threshold | 0.33 | V/C thresholds |

**Note**: `Num_Railroad_Crossings` (0.34) comes from Phase 2. `LOTTR` and `TTTR` come from Phase 5 (NPMRDS, DSA-dependent).

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| GDOT Traffic_Historical.zip | GDOT | Yes | ⬜ Download & inspect |
| GDOT 2010-2019 Published Traffic | GDOT | Yes | ⬜ Download & inspect |
| GDOT Traffic Counts (TADA) | GDOT | Yes | ⬜ Download |
| HPMS Georgia (yearly 2011-2023) | FHWA | Yes | ⬜ Download |
| ARC Historical Counts (2008-2017) | ARC | Yes | ⬜ Download |

## Deliverables
- `02-Data-Staging/databases/mobility.db` (tables: `traffic_counts`, `hpms_yearly`)
- `05-RAPTOR-Integration/states/Georgia/categories/Mobility.py` (V/C, LOS, congestion metrics)
- AADT 2050 projection field added to staged roadway output

## Verification
- [ ] V/C > 1.0 on known congested segments (I-285, I-75/85 downtown connector)
- [ ] AADT growth rates are plausible (0-3% for most segments)
- [ ] 2050 projections don't produce unreasonable values
- [ ] Road category distribution is reasonable
- [ ] No row duplication after merges
