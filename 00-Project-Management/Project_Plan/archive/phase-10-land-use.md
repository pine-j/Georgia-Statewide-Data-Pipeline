# Phase 10 — Land Use (Post-RAPTOR)

## Goal
Add land use and land cover data to the Georgia database. Potential future RAPTOR category.

## Status: Not Started (Post-RAPTOR)

---

## Datasets

### 10.1 National Land Cover Database (NLCD)
- **Source**: USGS (The National Map)
- **URL**: `https://www.mrlc.gov/data`
- **Format**: Download (raster)
- **Key fields**: Land cover classifications; annual since 1985 in latest release
- **Coverage**: U.S., 1985-2024
- **Use**: Primary land cover classification; urbanization change detection

### 10.2 Georgia Land Use Trends (GLUT)
- **Source**: UGA NARSAL
- **URL**: `https://narsal.uga.edu/glut/`
- **Format**: GIS raster
- **Key fields**: Statewide 18-class land cover; 1974-2008 time series from LANDSAT
- **Coverage**: Georgia, 1974-2008 (static/archived)
- **Use**: Historical land use change analysis pre-NLCD annual coverage; long-term urbanization trends

### 10.3 Fulton County Parcels & Zoning
- **Source**: Fulton County GIS
- **URL**: `https://gisdata.fultoncountyga.gov/`
- **Format**: ArcGIS Open Data (SHP, GeoJSON)
- **Key fields**: Parcels, zoning, land use for Fulton County (Atlanta area)
- **Use**: Parcel-level land use for Metro Atlanta corridors; prototype for county-level parcel integration

### 10.4 Georgia DCA Mapping and Analytics
- **Source**: Georgia Dept. of Community Affairs
- **URL**: `https://dca.georgia.gov/community-assistance/government-authority-reporting/mapping-and-analytics`
- **Format**: Web tools; PDF
- **Key fields**: Community planning data, comprehensive plans, land use maps, regional resources
- **Use**: Statewide planning context; comprehensive plan references for corridor analysis

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| NLCD Land Cover | USGS | Yes | ⬜ Download |
| GLUT Land Use (1974-2008) | UGA NARSAL | Yes | ⬜ Download |
| Fulton County Parcels | Fulton County GIS | Yes | ⬜ Download |
| GA DCA Planning Data | GA DCA | Yes | ⬜ Explore |

## Deliverables
- `02-Data-Staging/databases/land_use.db`
- `02-Data-Staging/spatial/land_use.gpkg` (layers: `land_cover`, `parcels_fulton`)

## Verification
- [ ] NLCD raster clips to Georgia boundary correctly
- [ ] GLUT raster aligns with NLCD for overlapping years
- [ ] Fulton County parcels load with valid geometries
- [ ] Land cover classes are consistent with NLCD legend
