# Phase 15 — Bicycle & Pedestrian

## Goal
Download, clean, and load bicycle and pedestrian infrastructure data into the Georgia database. Potential future RAPTOR category for active transportation scoring.

## Status: Not Started (Post-RAPTOR)
**Depends on**: Phase 1 (base layer for spatial joins)

---

## Datasets

### 15.1 GDOT Bicycle & Pedestrian Program
- **Source**: Georgia DOT (GDOT)
- **URL**: `https://www.dot.ga.gov/GDOT/pages/BikePed.aspx`
- **Format**: Reports; maps
- **Key fields**: Georgia bicycle routes and pedestrian program data
- **Use**: Statewide bike route network; GDOT program context
- **Place in**: `01-Raw-Data/bike_ped/`

### 15.2 ARC Bike Data
- **Source**: Atlanta Regional Commission (ARC)
- **URL**: `https://arc-garc.opendata.arcgis.com/search?tags=Bike`
- **Format**: Download (various)
- **Key fields**: Bike lanes and trail data for the Atlanta metro region
- **Place in**: `01-Raw-Data/bike_ped/arc/`

### 15.3 Atlanta Region Trail Plan Inventory
- **Source**: Atlanta Regional Commission
- **URL**: `https://opendata.atlantaregional.com/datasets/atlanta-region-trail-plan-inventory`
- **Format**: GIS feature layer
- **Key fields**: Existing and planned regionally-significant trail corridors
- **Place in**: `01-Raw-Data/bike_ped/arc/`

### 15.4 City of Atlanta Open Data — Bike/Ped Layers
- **Source**: City of Atlanta DPCD
- **URL**: `https://dpcd-coaplangis.opendata.arcgis.com/`
- **Format**: ArcGIS Open Data (SHP, GeoJSON, CSV)
- **Key fields**: Bike lanes, sidewalks, pedestrian infrastructure in City of Atlanta
- **Place in**: `01-Raw-Data/bike_ped/atlanta/`

### 15.5 Atlanta Bicycle Coalition Infrastructure Tracker
- **Source**: Atlanta Bicycle Coalition
- **URL**: `https://www.atlantabike.org/infra-tracker/`
- **Format**: Interactive web map
- **Key fields**: Ongoing bike lanes, sidewalks, trail improvements across Atlanta
- **Use**: Current status of active transportation projects; supplement official data

---

## ETL Pipeline

**`02-Data-Staging/scripts/15_bike_ped/`**:

1. `download.py` — Download ARC bike data, trail plan, City of Atlanta layers
2. `normalize.py` — Standardize facility types (bike lane, shared-use path, sidewalk, trail), CRS, merge sources
3. `create_db.py` — Load into `bike_ped.db` (tables: `bike_facilities`, `trails`, `sidewalks`)
4. `create_gpkg.py` — Write to `bike_ped.gpkg` layers: `bike_lanes`, `trails`, `sidewalks` (EPSG:32617)
5. `validate.py` — Facility counts, geometry validity, no duplicate segments across sources

---

## Key Metrics (weight=0 initially, available for future RAPTOR category)

| Metric | Type | Description |
|--------|------|-------------|
| Bike_Facility_Present | boolean | Whether a bike lane/path exists along or adjacent to road segment |
| Sidewalk_Present | boolean | Whether sidewalk exists along road segment |
| Trail_Proximity | min_max_inverted | Distance to nearest multi-use trail |
| Bike_Ped_Crash_Count | min_max_standard | Pedestrian/bicycle crashes on segment (cross-ref Phase 3) |

---

## Data Needed
| Dataset | Source | Public? | Status |
|---------|--------|---------|--------|
| GDOT Bike/Ped Program | GDOT | Yes | ⬜ Explore |
| ARC Bike Data | ARC | Yes | ⬜ Download |
| Atlanta Trail Plan Inventory | ARC | Yes | ⬜ Download |
| City of Atlanta Bike/Ped | Atlanta DPCD | Yes | ⬜ Download |
| Atlanta Bicycle Coalition Tracker | ABC | Yes | ⬜ Explore |

## Deliverables
- `02-Data-Staging/databases/bike_ped.db`
- `02-Data-Staging/spatial/bike_ped.gpkg` (layers: `bike_lanes`, `trails`, `sidewalks`)
- Updated `Georgia_Data_Inventory.csv`

## Verification
- [ ] Atlanta BeltLine trail appears in trail inventory
- [ ] Bike lane data covers major Atlanta corridors (Peachtree, 10th St, etc.)
- [ ] No duplicate facilities across ARC and City of Atlanta sources
- [ ] Facility types are consistently classified
- [ ] Geometries are valid and in EPSG:32617
