# Phase 7 — SharePoint Data Organization

## Goal
Organize all Georgia datasets on the shared Raptor SharePoint site so the RAPTOR pipeline can read from `data/Georgia/`.

## Status: Not Started
**Depends on**: Data from Phases 1-6

---

## SharePoint Location

```
https://jacobsengineering.sharepoint.com/:f:/r/sites/ICDataInsightsandDigitalIntelligenceGroup/Shared Documents/General/Raptor/DATA/
```

## Folder Structure

```
Raptor/DATA/Georgia/
├── Roadway-Inventory/
│   └── Road_Inventory_Geodatabase.gdb/
├── pavement/
│   └── COPACES_Ratings_{year}.csv
├── bridge/
│   └── NBI_Georgia_{year}.csv
├── safety/
│   ├── {year}/
│   └── concatenated_files/
│       └── crash_data_{year}.csv
├── mobility/
│   ├── NTAD_Railroad_Grade_Crossings.csv
│   ├── GDOT_Traffic_Counts_{year}.csv
│   └── HPMS_Georgia.shp
├── connectivity/
│   ├── SRP_Priority_Routes.shp
│   ├── NEVI_Corridors.shp
│   └── generators/ (airports, seaports, universities, military, parks, rail, freight)
├── economy/
│   └── (FAF5 data when available from colleague)
└── demographics/
    ├── Census_Decennial_2020/
    ├── ACS_5Year/
    ├── LEHD_LODES/
    ├── Economic_Census/
    ├── OPB_Population_Projections.xlsx
    └── Opportunity_Zones/
```

## Tasks
1. Create `Georgia/` folder on SharePoint
2. Upload finalized data from `01-Raw-Data/` and `02-Data-Staging/`
3. Add `Georgia/README.md` documenting sources, dates, contacts
4. Update data paths in Georgia code for RAPTOR integration

## Deliverable
- SharePoint `Georgia/` folder populated and accessible to team
