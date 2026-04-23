# Phase 6 — Integration & Deployment

## Goal
Wire Georgia into the RAPTOR_Pipeline framework, upload data to SharePoint, produce scored output with Total Needs Score across all 6 RAPTOR categories.

## Status: Not Started
**Depends on**: Phases 1-5 (all data staged and RAPTOR category classes built)

---

## Absorbed From
- Old Phase 7 (SharePoint Data Organization)
- Old Phase 8 (RAPTOR Integration + Output)

---

## RAPTOR Scoring Summary

| Category | Weight | Source Phases | Status |
|----------|--------|-------------|--------|
| Roadways | — | Phase 1 | Done |
| Connectivity | 0.15 | Phase 1 + 1b + 2 | Ready when Phase 2 complete |
| Freight | 0.10 | Phase 1 + 2 | Ready when Phase 2 complete |
| Asset Preservation | 0.20 | Phase 2 (bridges) + Phase 5 (pavement) | Bridges ready; pavement blocked on DSA |
| Safety | 0.20 | Phase 5 | Blocked on DSA (FARS interim available) |
| Mobility | 0.20 | Phase 2 (railroad) + Phase 4 (V/C) + Phase 5 (LOTTR) | Partial without DSA |
| SocioEconomic | 0.15 | Phase 3 | Ready when Phase 3 complete |

---

## Tasks

### 6.1 SharePoint Data Upload
Upload finalized data from `01-Raw-Data/` and `02-Data-Staging/` to:
```
Raptor/DATA/Georgia/
├── Roadway-Inventory/
├── connectivity/
├── freight/
├── bridge/
├── pavement/ (when available)
├── safety/ (when available)
├── mobility/
├── demographics/
└── economy/
```

### 6.2 Clone RAPTOR_Pipeline
```bash
git clone https://github.com/People-Places-Solutions/RAPTOR_Pipeline.git
git checkout -b feature/georgia-pipeline
```

### 6.3 Copy Georgia Code
```bash
cp -r 05-RAPTOR-Integration/states/Georgia/ RAPTOR_Pipeline/states/Georgia/
```

### 6.4 Add Georgia CRS
In `configs/crs_config.py`:
```python
crs_config = {
    "texas": "EPSG:3081",
    "massachusetts": "EPSG:26986",
    "georgia": "EPSG:32617"
}
```

### 6.5 Wire `pipeline.py`
Extend `BaseStatePipeline` with all implemented categories.

### 6.6 Implement `utils/build_schema.py`
`GeorgiaSchemaBuilder` — 6 categories matching weights above. All weights sum to 1.0.

### 6.7 Implement `utils/data_post_processing.py`
District names (7), county names (159), column filtering/ordering.

### 6.8 Set Up Data Folder
Download from SharePoint to `RAPTOR_Pipeline/data/Georgia/`.

### 6.9 Test End-to-End
```bash
python main.py --state Georgia --year 2025
python main.py --state Georgia --year 2025 --district 7
```

### 6.10 Push When Ready
```bash
git push -u origin feature/georgia-pipeline
```

---

## Deliverables
- SharePoint `Georgia/` folder populated and accessible to team
- `output/Georgia/georgia_2025.sqlite`
- `configs/Georgia/scoring_schema.json` and `filters_schema.json`
- Feature branch pushed to org repo

## Verification
- [ ] `python main.py --state Georgia --year 2025` completes
- [ ] `--district 7` returns Metro Atlanta only
- [ ] Scoring weights sum to 1.0 per category
- [ ] Output columns analogous to Texas
- [ ] SharePoint data accessible to team
