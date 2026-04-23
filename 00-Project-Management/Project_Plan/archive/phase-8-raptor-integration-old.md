# Phase 8 — RAPTOR Integration + Output

## Goal
Wire Georgia into the RAPTOR_Pipeline framework, produce scored output with Total Needs Score using the 6 RAPTOR categories:

| Category | RAPTOR Class | Weight | Data Readiness |
|----------|-------------|--------|----------------|
| Asset Preservation | `AssetPreservation` | 0.20 | Bridges: ready. Pavement: **blocked on GDOT data request** |
| Safety | `Safety` | 0.20 | **Blocked on GEARS DSA** (FARS interim for fatal-only) |
| Mobility | `Mobility` | 0.20 | Ready (V/C, railroad crossings). 2050 projection: depends on historic AADT |
| Connectivity | `Connectivity` | 0.15 | Ready (generators, SRP, NEVI) |
| Freight | `Freight` | 0.10 | **Major gap — no Transearch**. FAF5 as workaround |
| Socioeconomic | `SocioEconomic` | 0.15 | Ready (Census + LEHD + OPB projections) |

> **Freight strategy**: Unlike Texas which uses proprietary Transearch data (link-level commodity tonnage/value), Georgia will use FAF5 (state/metro-level flows) + ORNL truck network + truck AADT from the GDB. Freight category will launch with weight=0.10 but with coarser data than Texas. If Transearch is purchased later, the Freight class can be upgraded.

## Status: Not Started
**Depends on**: Phases 1-7 complete

---

## Tasks

### 8.1 Clone RAPTOR_Pipeline
```bash
cd D:\Jacobs
git clone https://github.com/People-Places-Solutions/RAPTOR_Pipeline.git
cd RAPTOR_Pipeline
git checkout -b feature/georgia-pipeline
py -3.13 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 8.2 Copy Georgia code
```bash
cp -r 05-RAPTOR-Integration/states/Georgia/ RAPTOR_Pipeline/states/Georgia/
```

### 8.3 Add Georgia CRS
In `configs/crs_config.py`:
```python
crs_config = {
    "texas": "EPSG:3081",
    "massachusetts": "EPSG:26986",
    "georgia": "EPSG:32617"
}
```

### 8.4 Wire `pipeline.py`
Extend `BaseStatePipeline` with 5 categories (Freight stub for when data arrives).

### 8.5 Implement `utils/build_schema.py`
`GeorgiaSchemaBuilder` — 6 categories matching Texas RAPTOR structure (Asset Preservation 0.20, Safety 0.20, Mobility 0.20, Connectivity 0.15, Freight 0.10, Socioeconomic 0.15). Weights sum to 1.0. Weights may be retuned for Georgia once data gaps are resolved.

### 8.6 Implement `utils/data_post_processing.py`
District names (7), county names (159), column filtering/ordering.

### 8.7 Set up data folder
Download from SharePoint to `RAPTOR_Pipeline/data/Georgia/`.

### 8.8 Test end-to-end
```bash
python main.py --state Georgia --year 2025
python main.py --state Georgia --year 2025 --district 7
```

### 8.9 Push when ready
```bash
git add states/Georgia/ configs/crs_config.py
git commit -m "Add Georgia state pipeline"
git push -u origin feature/georgia-pipeline
```

---

## Deliverables
- `output/Georgia/georgia_2025.sqlite`
- `configs/Georgia/scoring_schema.json` and `filters_schema.json`
- Feature branch pushed to org repo, PR opened

## Verification
- [ ] `python main.py --state Georgia --year 2025` completes
- [ ] `--district 7` returns Metro Atlanta only
- [ ] Scoring weights sum to 1.0 per category
- [ ] Output columns analogous to Texas
