# Phase 11 — Environmental (Post-RAPTOR)

## Goal
Add environmental datasets to the Georgia database. Potential future RAPTOR category.

## Status: Not Started (Post-RAPTOR)

---

## Datasets

### 11.1 National Wetlands Inventory (NWI)
- **Source**: U.S. Fish & Wildlife Service
- **URL**: `https://www.fws.gov/program/national-wetlands-inventory/data-download`
- **Format**: WMS/feature services; downloads
- **Key fields**: Wetlands & deepwater habitats statewide
- **Coverage**: U.S. (cumulative), biannual updates (May & October)

### 11.2 Critical Habitat (ECOS)
- **Source**: U.S. Fish & Wildlife Service (ECOS)
- **URL**: `https://ecos.fws.gov/ecp/services`
- **Format**: Feature & tile services
- **Key fields**: Proposed/final critical habitat layers (filter to Georgia)
- **Coverage**: U.S. (cumulative), continuous updates

### 11.3 Georgia EPD GIS Databases
- **Source**: Georgia EPD
- **URL**: `https://epd.georgia.gov/geographic-information-systems-gis-databases-and-documentation`
- **Format**: GIS databases; shapefiles
- **Key fields**: Wetlands, protected rivers/mountains, water supply watersheds, water quality sites
- **Coverage**: Georgia, varies by dataset
- **Use**: State-specific environmental constraints; supplements federal NWI/ECOS data

### 11.4 Georgia EPD Water Quality (GOMAS)
- **Source**: Georgia EPD
- **URL**: `https://gomaspublic.gaepd.org/`
- **Format**: Web database; query tool
- **Key fields**: Physical, chemical, biological water quality data statewide
- **Coverage**: Georgia, multi-year historical, continuous updates
- **Use**: Water quality context near transportation corridors; NEPA screening support

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| National Wetlands Inventory | USFWS | Yes | ⬜ Download |
| Critical Habitat (ECOS) | USFWS | Yes | ⬜ Download |
| GA EPD GIS Databases | GA EPD | Yes | ⬜ Download |
| GA EPD GOMAS Water Quality | GA EPD | Yes | ⬜ Explore |

## Deliverables
- `02-Data-Staging/databases/environmental.db`
- `02-Data-Staging/spatial/environmental.gpkg` (layers: `wetlands`, `critical_habitat`, `protected_areas`, `water_quality_sites`)

## Verification
- [ ] NWI wetlands cover all Georgia counties
- [ ] Critical habitat polygons are valid geometries
- [ ] GA EPD layers align with federal data
- [ ] Water quality sites geocode correctly
