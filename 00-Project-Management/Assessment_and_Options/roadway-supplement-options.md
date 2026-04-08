# Roadway Supplement Options for Filling Gaps in `base_network.gpkg`

## Purpose

The current Georgia roadway GeoPackage in `02-Data-Staging/spatial/base_network.gpkg` is strong for GDOT route inventory and traffic-linked analysis, but it leaves visible gaps in local street coverage in the web app. This note documents:

1. what the current GPKG actually contains,
2. why the gaps are happening,
3. what supplemental datasets are worth considering,
4. what extra attributes those datasets can contribute, and
5. a recommended decision path.

This is exploratory analysis only. No merge decision is being made in this document.

Current note: a `2026-04-04` full-load visual re-check lowered the urgency of supplementation for immediate planning use, but this source-completeness analysis is still relevant for follow-on QA.

## Executive Summary

- The current staged network is behaving as designed. It is not a frontend rendering problem.
- `base_network.gpkg` is built from GDOT `GA_2024_Routes` and preserves that source faithfully through normalization.
- In the current raw GDOT extract, `GA_2024_Routes` only exposes `SYSTEM_CODE` values `1` and `2`. The live GDOT LRS metadata interprets those as `1 = State Highway Routes` and `2 = Public Roads`. Even with public roads present, the staged network is still not giving us complete named local-street coverage in places where the basemap clearly shows additional public streets.
- In the Columbus / Dinglewood test area, the raw GDOT route layer only returns 6 route records and the staged GPKG only returns 14 segmented features. That is too sparse for the visible street grid.
- A supplemental statewide street source is worth evaluating if the web app needs complete local-road display, but it is not currently treated as a Phase 1 blocker.
- The most pragmatic path is:
  1. keep GDOT as the authoritative primary inventory layer,
  2. evaluate the GDOT live `Statewide Roads` service as the highest-alignment supplement,
  3. if an offline supplement is still needed, choose between TIGER first for governance or OSM first for maximum coverage.

## Current Baseline

### What builds the current GPKG

The staged roadway layer is built from the GDOT road inventory workflow:

- Raw source layer: `01-Raw-Data/Roadway-Inventory/Road_Inventory_2024.gdb`, layer `GA_2024_Routes`
- Normalization: `02-Data-Staging/scripts/01_roadway_inventory/normalize.py`
- SQLite output: `02-Data-Staging/databases/roadway_inventory.db`
- Spatial output: `02-Data-Staging/spatial/base_network.gpkg`, layer `roadway_segments`

Relevant implementation points:

- [`normalize.py`](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py) loads `GA_2024_Routes` as the base network and then segments it using the current traffic milepoint breaks retained in the simplified build.
- [`create_db.py`](../../02-Data-Staging/scripts/01_roadway_inventory/create_db.py) writes the normalized records into SQLite and GeoPackage.
- [`Roadways.py`](../../05-RAPTOR-Integration/states/Georgia/categories/Roadways.py) is separate from the web app and applies an additional RAPTOR-specific filter later, but that is not the source of the web app gap.

### What the current staged network contains

Observed from `roadway_inventory.db` and `base_network.gpkg`:

- Total staged segments: `244,904`
- Unique route IDs: `206,994`
- Current AADT coverage: `244,819` segments
- Total staged miles: `133,994.4`
- Miles with current AADT: `133,830.6`

Observed `SYSTEM_CODE` distribution in the staged database:

| SYSTEM_CODE | Segment Count | Miles | Notes |
|---|---:|---:|---|
| `1` | 18,499 | 24,976.5 | State Highway Route |
| `2` | 226,405 | 109,017.9 | Public roads as represented in this extract |

Important note:

- The current raw GDOT `GA_2024_Routes` extract also only exposed `SYSTEM_CODE` values `1` and `2` in local inspection.
- That means the current gap is not being introduced by segmentation; it is already present in the source layer we are using.

### What the current staged network does not contain well

The current GDOT-derived network is excellent for:

- route inventory identifiers,
- milepoint-based segmentation,
- district and county assignment,
- functional class and traffic-derived metrics,
- statewide consistency for GDOT analysis.

The current GDOT-derived network is weak for:

- complete local-street display,
- human-readable street names in the current workflow,
- neighborhood-scale cartographic completeness for the web map.

Local inspection of the raw GDB and staged database did not surface a street-name field that would immediately solve the UI road-label problem.

## Gap Verification

### Columbus / Dinglewood test case

This area was used because the web app visibly showed missing local roads while the basemap clearly displayed a denser local network.

Bounding area used in inspection:

- roughly West / East Dinglewood Drive and Wynnton Road in Columbus, Muscogee County

Findings:

- Raw GDOT `GA_2024_Routes` returned only 6 route records in the test area.
- Staged `base_network.gpkg` returned only 14 segmented roadway features in the same area.
- TIGER local roads for the same area returned named roads including:
  - `W Dinglewood Dr`
  - `E Dinglewood Dr`
  - `Wynnton Rd`
  - `Buena Vista Rd`
  - `16th Ave`
  - `12th St`

Interpretation:

- The current web app is not simply failing to draw already-loaded roads.
- The observed local-road gaps are consistent with source-network coverage limitations in the GDOT-based extract.

## Candidate Supplemental Datasets

### Option A: Keep GDOT inventory only

**Role**

- Primary inventory and traffic-analysis layer.

**Strengths**

- Best match to the existing pipeline.
- Official GDOT source.
- Already segmented and linked to traffic history.
- Clear district and county context.
- Good base for RAPTOR mobility and related scoring work.

**Weaknesses**

- Insufficient local-street completeness for a public-facing statewide map.
- Street-name experience is weak in the current extract.
- The observed source coverage does not match the level of neighborhood detail we want in the web app.

**Use if**

- the map only needs the GDOT analytical network, not a full public-road display.

### Option B: GDOT live `Statewide Roads` service

**Role**

- Highest-alignment supplement or replacement candidate within the GDOT ecosystem.

**Strengths**

- Same institutional source family as the current inventory.
- Richer road attributes are exposed in the live service metadata, including:
  - `ROAD_NAME`
  - `ROUTE_TYPE`
  - `OWNERSHIP`
  - `SURFACE_TYPE`
  - `NUMBER_TRAVEL_LANE`
  - `SPEED_LIMIT`
  - `FUNCTIONAL_CLASS`
  - `CITY_CODE`
- Most likely to preserve the clearest provenance story for a GDOT-first product.
- Best candidate if the problem is partly "downloaded inventory is thinner than the current live state service."

**Weaknesses**

- Service performance and exportability need to be tested.
- It introduces a live-service dependency unless we stage our own snapshot.
- Bulk extraction behavior was not validated in this memo.
- It is still a GDOT-maintained road network, so if the source logic still underrepresents certain streets, it may not fully solve the completeness problem.

**What it can add beyond geometry**

- better road names,
- ownership and surface metadata,
- lane and speed-limit fields,
- a stronger local-road UI and popup experience than the current staged extract.

**Best use**

- first candidate to test when the goal is best alignment with the current GDOT GPKG and minimum provenance ambiguity.

### Option C: U.S. Census TIGER/Line Roads

**Role**

- Official statewide backfill for missing roads.

**Strengths**

- Official U.S. government source.
- Available in GeoPackage, shapefile, and geodatabase formats.
- Statewide and consistent.
- Includes road naming structure and route-type coding.
- Can be linked to Census geography for downstream demographic analysis.
- Address range relationship files exist for many road features.
- Easy licensing posture compared with OSM.

**Weaknesses**

- Not transportation-operations grade.
- Not tied to GDOT route milepoints.
- Limited roadway engineering attributes compared with GDOT.
- Geometry may not align tightly enough for direct one-to-one conflation against GDOT segments without tolerance rules.

**What it can add beyond geometry**

- road names / basenames,
- route type codes,
- Census geographic identifiers,
- address-range relationship support,
- a cleaner bridge into Census demographics and address-based analysis.

**Best use**

- statewide public-road backfill where GDOT is sparse,
- name enrichment for missing local streets,
- a second-source geometry layer with cleaner government provenance.

### Option D: OpenStreetMap / Geofabrik Georgia extract

**Role**

- Highest-likelihood completeness backfill and optional richer local-street attribute source.

**Strengths**

- Usually the best neighborhood-scale completeness.
- Daily refresh cadence through Geofabrik extracts.
- Available in `.osm.pbf`, shapefile, and GeoPackage.
- Strong potential attribute depth:
  - street names,
  - refs,
  - `highway` classification,
  - one-way flags,
  - speed limits where tagged,
  - lane counts where tagged,
  - surface,
  - access restrictions,
  - bridge / tunnel tags,
  - service-road distinctions.

**Weaknesses**

- ODbL licensing obligations must be handled correctly.
- Tag completeness is uneven by place.
- Schema is less controlled than GDOT or TIGER.
- Conflation to GDOT route-milepoint segments is harder.
- Stronger governance is needed to keep provenance explicit.

**What it can add beyond geometry**

- richer street-level operational tags than TIGER,
- better local-road and service-road coverage,
- potentially better UX for local-road popups and filtering.

**Best use**

- coverage-first gap filling,
- richer local-road context,
- secondary backfill after TIGER if official-source supplementation still leaves holes.

### Option E: County / city authoritative centerlines

**Role**

- Targeted correction source for high-priority metros or counties.

**Strengths**

- Often the best local authority for naming, jurisdiction, address ranges, and maintenance context.
- Can outperform both TIGER and OSM in specific jurisdictions.

**Weaknesses**

- No turnkey statewide pipeline.
- High maintenance burden.
- Schema and licensing vary by jurisdiction.
- Statewide conflation would be expensive to govern.

**What it can add beyond geometry**

- jurisdiction-specific aliases,
- addressing detail,
- local maintenance ownership,
- sometimes speed limits or local functional class.

**Best use**

- targeted patches after statewide supplementation is in place,
- not as the first statewide supplement.

## Comparison Matrix

| Option | Coverage for local streets | Alignment to current GDOT GPKG | Additional attributes | Licensing / governance | Merge difficulty | Best role |
|---|---|---|---|---|---|---|
| GDOT only | Low to moderate in current extract | Exact | High for GDOT traffic / route inventory | Clean | None | Primary analytical source |
| GDOT live `Statewide Roads` | Unknown until tested, but highest GDOT alignment | High | High | Clean, but service-dependent | Moderate | First alignment-first test |
| TIGER/Line Roads | Moderate to high | Moderate | Moderate | Cleanest offline supplement | Moderate | First offline backfill |
| OSM / Geofabrik | High | Moderate to low | High, but variable | Requires ODbL handling | High | Coverage-first or attribute-rich backfill |
| County / city centerlines | High, but only where available | Variable | Moderate to high | Fragmented | Very high | Targeted patch source |

## Recommendation

### Recommended architecture

Do not replace the GDOT roadway inventory.

Instead, use a layered network strategy:

1. **Primary layer: GDOT staged roadway inventory**
   - authoritative for route inventory,
   - authoritative for traffic-linked attributes,
   - authoritative for RAPTOR-style analysis.

2. **First supplement to evaluate: GDOT live `Statewide Roads`**
   - highest alignment with the current GDOT model,
   - potentially richer official attributes than the downloaded GDB extract,
   - best fit if the team wants the strongest provenance continuity.

3. **First offline supplement: TIGER/Line Roads**
   - backfill missing public roads statewide,
   - add usable street names and basic road classes,
   - preserve a government-to-government provenance chain,
   - keep file-format compatibility with the current GeoPackage workflow.

4. **Second supplement if needed: OSM / Geofabrik**
   - apply only where TIGER still leaves visible gaps or where richer local-street attributes are worth the licensing and conflation overhead.

### Why GDOT live first

GDOT live is the best first candidate when alignment matters most because it balances:

- source continuity,
- richer road attributes than the current staged extract,
- the clearest provenance story,
- the lowest semantic drift from the existing GDOT-based workflow.

The tradeoff is operational, not conceptual: the service needs to be validated for bulk use, repeatability, and staging performance.

### Why TIGER is still the best first offline supplement

TIGER is the best offline supplement because it balances:

- statewide completeness,
- official provenance,
- permissive reuse,
- GeoPackage availability,
- simpler schema management,
- direct value for later demographic linkage.

It is less complete and less locally rich than OSM, but it is a lower-risk first merge target once we leave the GDOT ecosystem.

### Why not OSM first

OSM is likely the stronger coverage winner, but it creates harder governance questions on:

- licensing,
- provenance communication,
- schema normalization,
- reproducibility,
- conflation and attribution.

If the team wants a fast, defensible first supplement, TIGER is the better first test. If the team wants the maximum possible road display coverage regardless of governance overhead, OSM becomes more attractive.

## Provenance Rules for Any Merge

If we supplement the GDOT GPKG, provenance needs to be explicit at the feature level.

Minimum fields to add to the merged network:

- `geometry_source`
  - `gdot`
  - `tiger`
  - `osm`
  - `local_authority`
- `attribute_source`
  - `gdot`
  - `tiger`
  - `osm`
  - `derived`
- `source_confidence`
  - `primary`
  - `supplemental`
  - `fallback`
- `source_id`
  - GDOT route or `unique_id`
  - TIGER `LINEARID` or equivalent
  - OSM way / relation ID
- `name_source`
  - source of displayed street name

Suggested merge rule:

- prefer GDOT geometry and attributes wherever a defensible match exists,
- only inject external geometry when no GDOT feature is matched,
- never silently overwrite GDOT traffic-derived attributes with supplement values.

## What Additional Transportation Value the Supplements Can Add

### From TIGER

- street names for local-road UI display,
- better linkability to Census geographies,
- address-range related workflows,
- statewide completeness for road-based demographic joins.

### From OSM

- routing-style attributes useful for operations and mobility workflows,
- local-road hierarchy,
- service-road distinction,
- access restrictions,
- potential bridge / tunnel / surface / speed / lane enrichment.

### From county / city centerlines

- local ownership,
- local aliases,
- addressing depth,
- potentially the best local source for jurisdiction-specific transportation work.

## Decision Questions Before Implementation

1. Is the immediate need map completeness for public display, or analytical completeness for RAPTOR?
2. Is ODbL acceptable for a supplemental layer in the deployed product?
3. Do we want a clean two-source model (`GDOT + TIGER`) first, or do we want to optimize for maximum coverage immediately (`GDOT + OSM`)?
4. Do we want a single merged layer, or a stacked rendering model with source-specific symbology and popups?
5. How much conflation effort are we willing to fund before the first working release?

## Recommended Next Step

Run a contained proof-of-concept in one metro test area:

1. keep the current GDOT staged layer,
2. compare it against the GDOT live `Statewide Roads` service,
3. compare it against TIGER roads,
4. compare it against OSM only if the first two still leave obvious holes,
5. measure:
   - visible gap reduction,
   - geometry mismatch rate,
   - name enrichment rate,
   - proportion of supplement-only roads,
   - extra attribute value beyond geometry,
6. decide between two decision paths:
   - **alignment-first**: `GDOT live -> TIGER -> OSM`
   - **coverage-first**: `GDOT live -> OSM -> TIGER`

## Sources Reviewed

- GDOT road inventory overview: https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx
- GDOT live `Statewide Roads` service metadata: https://egisp.dot.ga.gov/arcgis/rest/services/ARCWEBSVCMAP/MapServer/4
- GDOT LRS layer metadata: https://rnh.qa.dot.ga.gov/hosting/rest/services/GPAS/LRSN_GDOT_GDOT/MapServer/exts/LRSServer/layers
- Census TIGER/Line shapefiles: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
- Census TIGER/Line GeoPackages: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geopackage-file.html
- Census TIGER technical documentation: https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2025/TGRSHP2025_TechDoc.pdf
- Census TIGERweb geography descriptions: https://tigerweb.geo.census.gov/tigerwebmain/TIGERweb_geography_details.html
- Geofabrik Georgia extract: https://download.geofabrik.de/north-america/us/georgia.html
- OpenStreetMap copyright and license: https://www.openstreetmap.org/copyright
