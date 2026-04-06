# Roadway Gap-Fill Exploratory Analysis

## Objective

Identify candidate road geometry datasets that can fill local-street gaps in the current Georgia staged roadway network without losing the GDOT-aligned attributes that RAPTOR and the web app already depend on.

This is an exploratory document. It does not change the production network yet.

## Why This Analysis Was Needed

The current staged roadway layer in `02-Data-Staging/spatial/base_network.gpkg` shows visible local-road gaps in some urban neighborhoods in the web app. Two representative checks were used to determine whether this was a rendering issue or a source-coverage issue.

### What We Verified Locally

1. The staged network is built from GDOT's `GA_2024_Routes` layer in [normalize.py](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py).
2. The staged network preserves the source routes rather than dropping them later in normalization.
3. The gaps seen in the web app are upstream source-coverage gaps, not frontend rendering loss.

### Evidence

- Raw GDOT route layer:
  - Source layer: `Road_Inventory_2024.gdb -> GA_2024_Routes`
  - CRS: `EPSG:4326`
  - Feature count: `206,994`
- Staged roadway outputs:
  - `roadway_inventory.db -> segments`: `622,255` segmented rows
  - Distinct `ROUTE_ID`: `206,994`
  - `current_aadt_covered = 1`: `185,748`
  - `SYSTEM_CODE` distribution in staged data:
    - `1`: `109,314`
    - `2`: `512,941`
- Columbus / Dinglewood spot check:
  - Raw `GA_2024_Routes` in the test bbox returned `6` route records.
  - The staged outputs preserved those same route IDs.
  - Result: the full neighborhood grid is not present in the upstream GDOT route layer for that area.

## Current Baseline Dataset

### A. Current staged GPKG / SQLite network

- Source: GDOT road inventory geodatabase plus GDOT traffic products
- Local files:
  - `01-Raw-Data/GA_RDWY_INV/Road_Inventory_2024.gdb`
  - `01-Raw-Data/GA_RDWY_INV/Traffic_2024_Geodatabase/TRAFFIC_Data_2024.gdb`
  - `02-Data-Staging/databases/roadway_inventory.db`
  - `02-Data-Staging/spatial/base_network.gpkg`
- Strengths:
  - Best alignment with existing RAPTOR logic and `ROUTE_ID` / `RCLINK` conventions
  - Strong GDOT-specific attributes after normalization
  - Stable basis for milepoint-based traffic joins
- Weaknesses:
  - Does not provide complete local-street coverage in all areas
  - Current staged network only carries `SYSTEM_CODE` 1 and 2 in practice, even though the live service exposes additional route types
- Scope note:
  - [Roadways.py](../../05-RAPTOR-Integration/states/Georgia/categories/Roadways.py) still filters RAPTOR scoring inputs to `SYSTEM_CODE = 1`.
  - That means a future gap-fill decision for the web app or exploratory network analysis does not automatically imply that the RAPTOR scoring network should expand to all local roads.

## Candidate Supplemental or Replacement Sources

### 1. GDOT live `Statewide Roads` service

- Source:
  - `https://egisp.dot.ga.gov/arcgis/rest/services/ARCWEBSVCMAP/MapServer/4?f=pjson`
- Why it matters:
  - This is the strongest official candidate because it stays in GDOT's route system and exposes GDOT-native keys and codes.
- Confirmed fields from the live layer metadata:
  - `RCLINK`
  - `ROAD_NAME`
  - `ROUTE_TYPE`
  - `ROUTE_NUMBER`
  - `OWNERSHIP`
  - `DIVIDED`
  - `ONE_WAY`
  - `PAVED`
  - `SURFACE_TYPE`
  - `NUMBER_TRAVEL_LANE`
  - `SPEED_LIMIT`
  - `FUNCTIONAL_CLASS`
  - `CITY_CODE`
  - `COUNTY_CODE`
  - `GDOT_DISTRICT`
  - `SEGMENT_LENGTH`
  - `TOPOLOGY_CODE`
  - `DATA_SOURCE`
  - `REVISION_DATE`
- Alignment with current GPKG:
  - Excellent. This is the only candidate reviewed here that stays directly in GDOT's own route coding model.
- Completeness expectation:
  - Potentially better than the downloaded snapshot because it is a live service and includes more route-type codes than the staged snapshot currently exposes.
  - Not yet proven to close the observed gaps. That requires an empirical AOI test against known missing streets.
- Licensing / usage considerations:
  - The ArcGIS service description includes GDOT-specific usage restrictions. Treat this as official state data, not unrestricted public-domain data.
- Recommended role:
  - First supplemental candidate to test.
  - Best option if we want to stay as close as possible to the existing GDOT geometry model.

### 2. U.S. Census TIGER/Line `All Roads` or `All Lines`

- Sources:
  - TIGER/Line geodatabases: `https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geodatabase-file.html`
  - 2024 technical documentation: `https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2024/TGRSHP2024_TechDoc.pdf`
- Why it matters:
  - TIGER is the clearest public-domain baseline for complete local-street coverage.
- Confirmed coverage notes from Census documentation:
  - The `All Roads` files include "primary roads, secondary roads, local neighborhood roads, rural roads, city streets, vehicular trails, ramps, service drives, walkways, stairways, alleys, and private roads."
  - The `All Roads` files can contain multiple overlapping road segments for one geometry because multiple named routes can coincide.
  - `All Lines` gives unique edge geometry and can be linked back through identifiers.
- Confirmed base attributes:
  - `LINEARID`
  - `FULLNAME`
  - `RTTYP`
  - `MTFCC`
  - county/state identifiers depending on product
- Alignment with current GPKG:
  - Moderate.
  - Good for geometry fill.
  - Not naturally aligned to GDOT `RCLINK` or milepoint logic.
- Completeness expectation:
  - High for public roads and local streets.
- Licensing / usage considerations:
  - Federal source, practical low-friction usage.
- Recommended role:
  - Best low-risk public-domain filler if we want broad road coverage without licensing complexity.
  - Better as a supplemental geometry source than as a full replacement for the GDOT analysis network.

### 3. OpenStreetMap / Geofabrik Georgia extract

- Sources:
  - OSM licensing: `https://www.openstreetmap.org/copyright`
  - Geofabrik schema guide: `https://download.geofabrik.de/osm-data-in-gis-formats-free.pdf`
  - Georgia extracts: `https://download.geofabrik.de/north-america/us/georgia.html`
- Why it matters:
  - OSM is the strongest completeness candidate for local streets and edge cases.
- Confirmed coverage / attributes from Geofabrik schema:
  - Road classes include `primary`, `secondary`, `tertiary`, `unclassified`, `residential`, `service`, `track`, `living_street`, `pedestrian`, `links`, and path classes.
  - Confirmed supplemental fields include `ref`, `oneway`, `maxspeed`, `layer`, `bridge`, and `tunnel`.
  - Common extract attributes also include `name` and class information.
- Alignment with current GPKG:
  - Moderate to low.
  - Strong geometry coverage, but conflation to GDOT `RCLINK` would be a geometry-and-name matching exercise rather than a direct key join.
- Completeness expectation:
  - Highest among candidates reviewed here.
  - Also best for neighborhood streets, service roads, and edge cases that official route inventories often miss.
- Licensing / usage considerations:
  - ODbL.
  - Requires attribution and introduces database-share obligations if we build and distribute a derived database from OSM.
- Recommended role:
  - Best completeness fallback if GDOT live services still miss local streets.
  - Strong supplemental geometry source.
  - Not the first choice if we want the lowest licensing friction.

### 4. USGS National Transportation Dataset (NTD)

- Sources:
  - USGS download overview: `https://www.usgs.gov/tools/download-data-maps-national-map`
  - USGS NTD catalog entry: `https://data.usgs.gov/datacatalog/data/USGS%3Aad3d631d-f51f-4b6a-91a3-e617d6a58b4e`
  - USGS source description: `https://www.usgs.gov/faqs/what-sources-were-used-create-boundaries-structures-and-transportation-layers-national-map`
- Why it matters:
  - NTD is broader than a roads-only product and can support future multimodal analysis.
- Confirmed source notes:
  - USGS says transportation data in The National Map includes roads, airports, railroads, trails, and more.
  - USGS says the transportation data is public.
  - USGS says roads are based on Census TIGER/Line, with some major-road updates by USGS and U.S. Forest Service road data over National Forests.
- Alignment with current GPKG:
  - Moderate.
  - Better than generic national data for broader transportation context, but still not native to GDOT route identifiers.
- Completeness expectation:
  - Likely stronger than raw TIGER for some major-road areas, with extra transportation modes useful beyond roads.
- Licensing / usage considerations:
  - Public domain per USGS catalog entry.
- Recommended role:
  - Good secondary candidate if broader transportation enrichment matters at the same time as road-gap fill.
  - Better for multimodal expansion than for direct GDOT route replacement.

### 5. Georgia GIS Clearinghouse / county road centerlines

- Source families already inventoried in this repo:
  - `https://georgiagisclearinghouse.com/`
  - `https://data.georgiaspatial.org/`
- Why it matters:
  - Potential source of county-maintained or locally curated centerlines.
- Alignment with current GPKG:
  - Variable.
  - Could be very useful in specific counties.
  - Poor fit for a single statewide baseline unless schema normalization is worth the extra effort.
- Completeness expectation:
  - Potentially very good in counties with strong local GIS publishing.
  - Inconsistent statewide.
- Licensing / usage considerations:
  - Varies by dataset and publisher.
- Recommended role:
  - County-by-county patching source, not first-choice statewide supplemental network.

## Comparison Summary

| Candidate | Completeness for local streets | Alignment to current GDOT GPKG | Additional attributes beyond geometry | Licensing / usage friction | Best role |
|---|---|---|---|---|---|
| GDOT `Statewide Roads` service | Unknown until tested, but promising | Best | Road name, route coding, ownership, one-way, paved, surface, lanes, speed, district, topology, revision date, data source | Medium | First supplemental candidate to test |
| TIGER/Line `All Roads` / `All Lines` | High | Moderate | Name, route type, feature class, identifiers | Low | Public-domain geometry filler |
| OSM / Geofabrik | Highest | Moderate to low | Name, ref, oneway, maxspeed, bridge, tunnel, local classes | Higher | Coverage-maximizing supplemental source |
| USGS NTD | High to moderate | Moderate | Roads plus rail, trails, airports, broader network context | Low | Multimodal supplemental source |
| GA GIS Clearinghouse / county centerlines | Variable | Variable | Often locally rich, but inconsistent | Variable | County-specific patch source |

## Recommended Evaluation Order

### Recommendation 1: Test GDOT live `Statewide Roads` first

Reason:
- Best schema and identifier alignment with the existing GDOT-derived network
- Official source
- Exposes `ROAD_NAME`, route-type codes, and provenance fields not present in the current staged web layer
- Lowest conflation risk if it actually fills some of the observed gaps

### Recommendation 2: If GDOT live still misses local streets, test OSM next

Reason:
- Best completeness
- Strongest chance of eliminating visible neighborhood gaps
- Rich enough attributes to help future transportation analysis, even if those attributes are not GDOT-native

### Recommendation 3: Keep TIGER/Line as the low-risk public-domain fallback

Reason:
- Simpler licensing
- Predictable national schema
- Good candidate if OSM licensing is a concern

## Recommended Merge Strategy

Do not replace the current GDOT staged network immediately.

Instead:

1. Keep the GDOT-derived GPKG as the authoritative analysis network.
2. Add a supplemental geometry layer for roads missing from the GDOT-derived network.
3. Track provenance at both geometry and attribute level.

### Provenance fields to add

- `geometry_source`
- `attribute_source`
- `source_priority`
- `source_record_id`
- `match_method`
- `match_confidence`
- `is_gap_fill_geometry`
- `is_gap_fill_attribute`

### Merge rules to preserve clarity

- If GDOT geometry exists, prefer GDOT geometry and GDOT attributes.
- If GDOT geometry does not exist, allow supplemental geometry with clearly marked source.
- Never silently overwrite GDOT attributes with non-GDOT values.
- For overlapping segments, keep both source lineage and the conflict-resolution rule.

## Recommended Next Experiment

Before making a network decision, run the same AOI test on at least three known gap areas using:

1. Current staged GPKG
2. GDOT live `Statewide Roads`
3. OSM Georgia extract
4. TIGER/Line `All Roads` or `All Lines`

Measure:

- road-count coverage
- total centerline length in the AOI
- named-road capture
- distance needed to snap supplemental roads to GDOT routes
- duplicate / overlap behavior
- attribute fill rates

## Bottom Line

- The current gaps are upstream of the staged GPKG.
- The most GDOT-aligned candidate is the live `Statewide Roads` service.
- The most complete candidate is OSM.
- The safest public-domain fallback is TIGER/Line.
- The most broadly useful transportation supplement is USGS NTD.

The best decision path is to test `GDOT live -> OSM -> TIGER` in that order, while preserving explicit source provenance in the merged network.
