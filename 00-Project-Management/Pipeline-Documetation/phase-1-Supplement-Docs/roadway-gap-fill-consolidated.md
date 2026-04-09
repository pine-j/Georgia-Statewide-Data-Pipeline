# Roadway Gap-Fill and Supplement Strategy

## Status

This is the single consolidated note for roadway gap-fill and supplemental-road-source evaluation in Georgia.

It replaces the earlier split notes and comparison table with one maintained document.

This remains exploratory analysis. It does not change the staged roadway pipeline or web app data source by itself.

For general transportation planning purposes, we decided this level of roadway gap-fill detail is not needed in the active workflow. This file is being kept so the exploratory analysis remains available if future QA, map-improvement, or more detailed transportation planning work needs it.

Current note: a `2026-04-04` full-load visual re-check lowered the urgency of statewide supplementation for immediate planning use, but the source-completeness analysis still matters for follow-on QA and any future map-quality improvement work.

## Purpose

Document:

1. what the current staged Georgia roadway network contains,
2. why local-street gaps were investigated,
3. what supplemental datasets are worth considering,
4. what those supplements can add beyond geometry, and
5. the recommended evaluation and merge strategy.

## Executive Summary

- The current staged network is behaving as designed. The observed issue is not primarily a frontend rendering failure.
- `base_network.gpkg` is built from GDOT `GA_2024_Routes` and preserves that source faithfully through normalization and segmentation.
- In local inspection, the raw GDOT extract exposed only `SYSTEM_CODE` values `1` and `2`, and the staged network reflects that same limitation.
- In the Columbus / Dinglewood test area, the GDOT-derived network was too sparse to represent the visible neighborhood street grid.
- Later visual re-checks in Columbus, Atlanta, and Savannah lowered the urgency of statewide supplementation for initial planning use.
- If supplementation becomes necessary, the best default decision path is:
  1. keep GDOT as the authoritative primary analytical network,
  2. test GDOT live `Statewide Roads` first,
  3. use TIGER/Line as the first offline supplement,
  4. use OSM / Geofabrik where coverage or richer local-road attributes still justify the added governance overhead.

## Current Working Assumption

As of the `2026-04-04` web app visual re-check, the practical working assumption should be:

- proceed with the current GDOT-based statewide network for initial planning and prototype scoring,
- treat statewide gap-fill work as a validation and improvement track, not an immediate blocker,
- revisit TIGER, OSM, or additional GDOT supplementation only if later AOI checks or metric-based QA show planning-relevant omissions.

## Current Baseline

### What builds the current network

The staged roadway layer is built from the GDOT road inventory workflow:

- Raw source layer: `01-Raw-Data/Roadway-Inventory/Road_Inventory_2024.gdb`, layer `GA_2024_Routes`
- Normalization: [`normalize.py`](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py)
- SQLite output: `02-Data-Staging/databases/roadway_inventory.db`
- Spatial output: `02-Data-Staging/spatial/base_network.gpkg`, layer `roadway_segments`

Relevant implementation notes:

- [`normalize.py`](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py) loads `GA_2024_Routes` as the base network and segments it using the retained traffic milepoint breaks.
- [`create_db.py`](../../02-Data-Staging/scripts/01_roadway_inventory/create_db.py) writes the normalized records into SQLite and GeoPackage.
- [`Roadways.py`](../../05-RAPTOR-Integration/states/Georgia/categories/Roadways.py) applies a later RAPTOR-specific filter and is not the source of the web app gap investigation.

### What the current staged network contains

Observed from `roadway_inventory.db` and `base_network.gpkg`:

- Raw GDOT route count: `206,994`
- Total staged segments: `244,904`
- Unique route IDs: `206,994`
- Current AADT coverage: `244,819` segments
- Total staged miles: `133,994.4`
- Miles with current AADT: `133,830.6`

Observed `SYSTEM_CODE` distribution in the staged database:

| SYSTEM_CODE | Segment Count | Miles | Notes |
|---|---:|---:|---|
| `1` | 18,499 | 24,976.5 | State Highway Route |
| `2` | 226,405 | 109,017.9 | Public roads as represented in the current extract |

Important note:

- The raw GDOT `GA_2024_Routes` extract also only exposed `SYSTEM_CODE` values `1` and `2` in local inspection.
- That means the current gap is not being introduced by segmentation; it is already present in the source layer being staged.

### What the current staged network is good at

- route inventory identifiers,
- milepoint-based segmentation,
- district and county assignment,
- functional class and traffic-linked metrics,
- statewide consistency for GDOT-aligned analysis.

### What the current staged network is weaker at

- complete local-street display,
- human-readable street names in the current workflow,
- neighborhood-scale cartographic completeness in the web map.

Scope note:

- [`Roadways.py`](../../05-RAPTOR-Integration/states/Georgia/categories/Roadways.py) still filters RAPTOR scoring inputs to `SYSTEM_CODE = 1`.
- A future web-map gap-fill decision does not automatically imply that RAPTOR scoring should expand to all local roads.

## Why The Gap Investigation Happened

Visible local-road gaps were seen in some urban neighborhoods in the web app. The question was whether that was a rendering problem or an upstream source-coverage problem.

Local validation showed:

1. the staged network is built from GDOT `GA_2024_Routes`,
2. the staged network preserves those source routes rather than dropping them later in normalization, and
3. the observed local-road gap is upstream of the frontend.

## Gap Verification

### Columbus / Dinglewood test case

This area was used because the web app visibly showed missing local roads while the basemap displayed a denser local network.

Bounding area used in inspection:

- roughly West / East Dinglewood Drive and Wynnton Road in Columbus, Muscogee County

Findings:

- Raw GDOT `GA_2024_Routes` returned only `6` route records in the test area.
- Staged `base_network.gpkg` returned only `14` segmented roadway features in the same area.
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

### 2026-04-04 visual follow-up

After the initial exploratory write-up, the local web app at `http://127.0.0.1:5173/` was re-checked after allowing the staged roadway layer to finish loading completely (`244,904 segments loaded` in the simplified build).

Three sample areas were reviewed visually against the basemap:

- Columbus / Dinglewood
- Atlanta in-town sample near `xmin=-84.369, ymin=33.748, xmax=-84.353, ymax=33.764`
- Savannah sample near `xmin=-81.111, ymin=32.064, xmax=-81.095, ymax=32.080`

Observed result:

- no obvious planning-relevant roadway gaps were seen in those sample views,
- the rendered network appeared to cover both local street grids and larger connectors reasonably well,
- no clear evidence was seen of broken access to major roads, commercial or industrial areas, or major destinations.

Interpretation:

- the current GDOT-based staged network appears good enough for initial statewide planning use,
- if gaps remain, they appear more likely to be minor and localized than a statewide planning blocker,
- that lowers the urgency of introducing a supplemental statewide source before initial planning work.

## Official GDOT Services Also Checked

The following live GDOT services were inspected because they are the most likely to align cleanly with the current GDOT-based GPKG.

### A. `EOC_SUPPORT_LAYERS / GDOT Roads`

Observed metadata fields included:

- `ROUTE_ID`
- `ROUTE_KEY`
- `FUNCTION_TYPE`
- `COUNTY`
- `SYSTEM_CODE`
- `DIRECTION`

Observed test result:

- the Dinglewood AOI query returned `7` features,
- this included the same `6` GDOT route IDs already present in the raw snapshot plus one short `SYSTEM_CODE = 3` city-street feature.

Conclusion:

- slightly better than the current snapshot in the test area,
- not enough by itself to solve the observed neighborhood gap.

### B. `GDOT FunctionalClass / Local Road`

Observed metadata fields included:

- `route_id`
- `from_measure`
- `to_measure`
- `SYSTEM_CODE`
- `FUNCTIONAL_CLASS`

Observed test result:

- the Dinglewood AOI query returned `4` local-road features,
- the related `Ownership` layer returned `7` features in the same area.

Conclusion:

- useful for validation and enrichment,
- not clearly fuller than the current staged source in the tested AOI.

### C. `ARCWEBSVCMAP / Statewide Roads`

Observed metadata fields included:

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

Observed test result:

- the service was responsive for metadata,
- repeated timeouts prevented reliable AOI verification during the earlier exploratory pass.

Conclusion:

- this remains the most promising official GDOT-aligned supplement or replacement candidate,
- coverage still needs a dedicated extraction and performance test.

## Candidate Supplemental Datasets

### Consolidated comparison matrix

| Candidate | Source type | Coverage for local streets | Alignment to GDOT / `RCLINK` model | Additional attributes | Licensing or usage | Recommended role |
|---|---|---|---|---|---|---|
| GDOT `EOC_SUPPORT_LAYERS / GDOT Roads` | Official GDOT ArcGIS feature layer | Low improvement in tested AOI | Excellent | `ROUTE_ID`, `ROUTE_KEY`, `FUNCTION_TYPE`, `COUNTY`, `SYSTEM_CODE`, `DIRECTION` | GDOT-specific service terms | Validation source, not sufficient standalone gap filler in tested AOI |
| GDOT `FunctionalClass / Local Road` | Official GDOT / ITOS ArcGIS feature layer | Low improvement in tested AOI | Excellent | `route_id`, measures, `SYSTEM_CODE`, `FUNCTIONAL_CLASS` | GDOT / ITOS service terms | Validation and enrichment source, not proven as statewide gap filler |
| GDOT `ARCWEBSVCMAP / Statewide Roads` | Official GDOT ArcGIS feature layer | Unknown until extraction test | Excellent | `RCLINK`, `ROAD_NAME`, `ROUTE_TYPE`, `OWNERSHIP`, `DIVIDED`, `ONE_WAY`, `PAVED`, `SURFACE_TYPE`, `NUMBER_TRAVEL_LANE`, `SPEED_LIMIT`, `FUNCTIONAL_CLASS`, `COUNTY_CODE`, `GDOT_DISTRICT`, `DATA_SOURCE`, `REVISION_DATE` | GDOT-specific service terms | Most promising GDOT-aligned supplement still needing dedicated extraction test |
| TIGER/Line `All Roads` or `All Lines` | Federal Census road product | High | Moderate | `FULLNAME`, `RTTYP`, `MTFCC`, `LINEARID`, county and state identifiers | Low-friction federal source | Best first statewide public-domain geometry filler |
| OpenStreetMap / Geofabrik Georgia extract | Crowdsourced open road network | Highest | Moderate to low | `name`, `ref`, `oneway`, `maxspeed`, `layer`, `bridge`, `tunnel`, detailed local road classes | ODbL attribution and share-alike obligations | Coverage-maximizing supplemental source |
| USGS National Transportation Dataset | USGS national transportation product | High to moderate | Moderate | roads plus railroads, trails, airports, broader transportation context | Public domain | Multimodal supplemental source |
| Georgia GIS Clearinghouse `FrameWork / Transportation` | Statewide Clearinghouse ArcGIS service | Moderate to high | High field-level alignment with GDOT `Statewide Roads`, but weaker provenance continuity than the direct GDOT service | `RCLINK`, `ROAD_NAME`, `ROUTE_TYPE`, `OWNERSHIP`, `NUMBER_TRAVEL_LANE`, `SPEED_LIMIT`, `FUNCTIONAL_CLASS`, `DATA_SOURCE`, `REVISION_DATE`; same service also exposes `Bridges` and `Railroads` | Clearinghouse service terms; roadway layer is an older statewide extract | Useful reference or offline fallback, not the first-choice statewide supplement |
| County or city authoritative centerlines | Local government GIS portals | Variable | Variable | Often locally rich local attributes, but inconsistent statewide | Varies by publisher | County-by-county patch source |

### Option A: Keep GDOT inventory only

**Strengths**

- best match to the existing pipeline,
- official GDOT source,
- already segmented and linked to traffic history,
- clear district and county context,
- good base for RAPTOR mobility and related scoring work.

**Weaknesses**

- insufficient local-street completeness for a public-facing statewide map,
- street-name experience is weak in the current extract,
- observed source coverage does not match the level of neighborhood detail desired in some web-map use cases.

**Use if**

- the map only needs the GDOT analytical network, not a full public-road display.

### Option B: GDOT live `Statewide Roads`

**Strengths**

- same institutional source family as the current inventory,
- richer road attributes than the downloaded extract,
- strongest provenance continuity,
- lowest semantic drift from the current GDOT-based workflow.

**Weaknesses**

- service performance and exportability still need to be tested,
- introduces a live-service dependency unless a snapshot is staged,
- may still underrepresent some streets if the underlying source logic is similar.

**Best use**

- first supplement to test when alignment and provenance matter most.

### Option C: TIGER/Line Roads

**Strengths**

- official U.S. government source,
- statewide and consistent,
- available in common GIS formats,
- strong road naming structure and route-type coding,
- easier licensing posture than OSM,
- good bridge into Census geography and demographic linkage.

**Weaknesses**

- not transportation-operations grade,
- not tied to GDOT milepoints,
- limited roadway engineering attributes compared with GDOT,
- geometry may require tolerance-based conflation.

**Best use**

- first offline statewide backfill,
- name enrichment for missing local streets,
- government-to-government provenance chain.

### Option D: OpenStreetMap / Geofabrik

**Strengths**

- usually the best neighborhood-scale completeness,
- richer street-level operational tags than TIGER,
- good coverage for service roads and local edge cases.

**Weaknesses**

- ODbL obligations must be handled correctly,
- tag completeness is uneven by place,
- schema is less controlled than GDOT or TIGER,
- conflation to GDOT route-milepoint segments is harder.

**Best use**

- coverage-first gap filling,
- richer local-road context,
- secondary supplement after TIGER if official-source supplementation still leaves visible holes.

### Option E: USGS National Transportation Dataset

**Strengths**

- public domain,
- broader transportation context than roads alone,
- useful if multimodal enrichment matters at the same time.

**Weaknesses**

- not the first-choice road-gap filler if roads alone are the primary concern,
- still not native to GDOT route identifiers.

**Best use**

- multimodal expansion rather than direct GDOT road replacement.

### Option F: Georgia GIS Clearinghouse `FrameWork / Transportation`

**Strengths**

- live statewide transportation service with roads, bridges, and railroads in one place,
- `Common Roads and Streets` exposes a large statewide local-road layer with `390,632` features in a quick query,
- road schema is almost the same as GDOT live `Statewide Roads`, which makes field mapping relatively straightforward,
- only `11,400` features returned null `ROAD_NAME` values in a quick statewide count, so naming coverage appears materially better than the current staged GDOT inventory for UI use,
- feature-level `DATA_SOURCE` values preserve mixed provenance such as GDOT, TIGER, DOQQ, and some county contributors.

**Weaknesses**

- the layer description states this is an extract of the Georgia DLG-F Road Basemap from `December 2013`,
- the source description also states the county datasets were not edge-matched and duplicate county-boundary features were included,
- it is largely redundant with GDOT live `Statewide Roads` at the field level, so it adds less unique analytical value than going to GDOT directly,
- the service is queryable and paginated, but not presented as a richer bulk-export workflow,
- quick inspection surfaced noisy `REVISION_DATE` values, so staging would still need QA rather than trusting the service as-is.

**Best use**

- secondary reference source when a GDOT-like statewide road layer is needed outside the live GDOT environment,
- targeted road-name or local-road display enrichment,
- ancillary statewide `Bridges` and `Railroads` reference layers if multimodal context becomes useful.

### Option G: County or city authoritative centerlines

**Strengths**

- often the best local authority for naming, jurisdiction, address ranges, and maintenance context,
- can outperform both TIGER and OSM in specific jurisdictions.

**Weaknesses**

- no turnkey statewide pipeline,
- high maintenance burden,
- schema and licensing vary by jurisdiction,
- statewide conflation would be expensive to govern.

**Best use**

- targeted metro or county patches after a statewide supplement exists.

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
   - apply where TIGER still leaves visible gaps or where richer local-road attributes justify the added licensing and conflation overhead.

### Recommended evaluation order

Default alignment-first order:

1. `GDOT live Statewide Roads`
2. `TIGER/Line`
3. `OSM / Geofabrik`

Coverage-first variant:

1. `GDOT live Statewide Roads`
2. `OSM / Geofabrik`
3. `TIGER/Line`

Interpretation:

- the earlier exploratory note that placed OSM ahead of TIGER is still valid when maximum completeness is the dominant objective,
- the current default recommendation favors TIGER first once leaving the GDOT ecosystem because it is easier to govern, easier to explain, and lower-friction to stage.

### Where Georgia GIS Clearinghouse fits

Georgia GIS Clearinghouse is worth keeping on the bench, but not as the default next step.

The `FrameWork / Transportation` road layer is useful because it is statewide, queryable, and nearly field-compatible with GDOT live `Statewide Roads`. That makes it a reasonable fallback if the team needs a GDOT-like reference outside the direct GDOT service.

It should still sit behind GDOT live and TIGER in the default evaluation order because the road layer is explicitly described as a `December 2013` statewide extract compiled from county datasets that were not edge-matched and may include duplicate boundary features.

### Why GDOT live first

GDOT live is the best first candidate when alignment matters most because it balances:

- source continuity,
- richer road attributes than the current staged extract,
- the clearest provenance story,
- the lowest semantic drift from the existing workflow.

The tradeoff is operational, not conceptual: the service still needs to be validated for bulk use, repeatability, and staging performance.

### Why TIGER is the best first offline supplement

TIGER balances:

- statewide completeness,
- official provenance,
- permissive reuse,
- GeoPackage compatibility,
- simpler schema management,
- direct value for later demographic linkage.

It is less locally rich than OSM, but it is the lower-risk first merge target once work leaves the GDOT ecosystem.

### Why not OSM first by default

OSM is likely the coverage winner, but it creates harder governance questions around:

- licensing,
- provenance communication,
- schema normalization,
- reproducibility,
- conflation and attribution.

If the team wants a fast, defensible first supplement, TIGER is the better first test. If the team wants the maximum possible road display coverage regardless of governance overhead, OSM becomes more attractive.

## Provenance Rules for Any Merge

If the GDOT GPKG is supplemented, provenance needs to be explicit at the feature level.

Minimum fields to add to the merged network:

- `geometry_source`
- `attribute_source`
- `source_confidence`
- `source_id`
- `name_source`
- `match_method`
- `match_confidence`
- `is_gap_fill_geometry`
- `is_gap_fill_attribute`

Suggested coded values:

- `geometry_source`: `gdot`, `tiger`, `osm`, `local_authority`
- `attribute_source`: `gdot`, `tiger`, `osm`, `derived`
- `source_confidence`: `primary`, `supplemental`, `fallback`

Suggested merge rule:

- prefer GDOT geometry and attributes wherever a defensible match exists,
- only inject external geometry when no GDOT feature is matched,
- never silently overwrite GDOT traffic-derived attributes with supplement values.

## What Supplemental Sources Can Add Beyond Geometry

### From GDOT live `Statewide Roads`

- better road names,
- ownership and surface metadata,
- lane and speed-limit fields,
- a stronger local-road UI and popup experience than the current staged extract.

### From TIGER

- street names for local-road display,
- better linkability to Census geographies,
- address-range related workflows,
- statewide completeness for road-based demographic joins.

### From OSM

- routing-style attributes useful for operations and mobility workflows,
- local-road hierarchy,
- service-road distinction,
- access restrictions,
- potential bridge, tunnel, surface, speed, and lane enrichment.

### From Georgia GIS Clearinghouse

- GDOT-like road-name and roadway-display attributes in a statewide service,
- feature-level source provenance through `DATA_SOURCE`,
- statewide `Bridges` and `Railroads` layers that may help if the pipeline later adds multimodal or structure context.

### From county or city centerlines

- local ownership,
- local aliases,
- addressing depth,
- jurisdiction-specific transportation detail.

## Decision Questions Before Implementation

1. Is the immediate need map completeness for public display, or analytical completeness for RAPTOR?
2. Is ODbL acceptable for a supplemental layer in the deployed product?
3. Does the team want a clean two-source model first, or maximum coverage immediately?
4. Does the team want a single merged layer, or stacked rendering with source-specific symbology and popups?
5. How much conflation effort is acceptable before the first working release?

## Recommended Next Step

Run a contained proof-of-concept in one metro test area:

1. keep the current GDOT staged layer,
2. compare it against the GDOT live `Statewide Roads` service,
3. if GDOT live proves too slow or awkward to extract, compare it against Georgia GIS Clearinghouse `FrameWork / Transportation` as a GDOT-like fallback,
4. compare it against TIGER roads,
5. compare it against OSM only if the first three still leave obvious holes,
6. measure:
   - visible gap reduction,
   - geometry mismatch rate,
   - name enrichment rate,
   - proportion of supplement-only roads,
   - extra attribute value beyond geometry.

If later QA reopens the issue, repeat that comparison in at least three AOIs and score candidates on:

- gap reduction,
- geometry alignment with the GDOT network,
- usable roadway attributes beyond geometry,
- operational complexity for the local-first workflow.

## Source URLs Reviewed

| Source | URL |
|---|---|
| GDOT road inventory overview | https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx |
| GDOT `Statewide Roads` service metadata | https://egisp.dot.ga.gov/arcgis/rest/services/ARCWEBSVCMAP/MapServer/4 |
| GDOT LRS metadata | https://rnh.qa.dot.ga.gov/hosting/rest/services/GPAS/LRSN_GDOT_GDOT/MapServer/exts/LRSServer/layers |
| GDOT `EOC_SUPPORT_LAYERS / GDOT Roads` metadata | https://rnhp.dot.ga.gov/hosting/rest/services/EOC/EOC_SUPPORT_LAYERS/MapServer/5?f=pjson |
| GDOT `FunctionalClass / Local Road` metadata | https://maps.itos.uga.edu/arcgis/rest/services/GDOT/GDOT_FunctionalClass/MapServer/6?f=pjson |
| Georgia GIS Clearinghouse transportation service metadata | https://maps.itos.uga.edu/arcgis/rest/services/FrameWork/Transportation/MapServer |
| Georgia GIS Clearinghouse `Common Roads and Streets` layer metadata | https://maps.itos.uga.edu/arcgis/rest/services/FrameWork/Transportation/MapServer/13 |
| Georgia GIS Clearinghouse web services help | https://data.georgiaspatial.org/help/chouse_webservices.pdf |
| Census TIGER/Line shapefiles | https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html |
| Census TIGER/Line geodatabases | https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geodatabase-file.html |
| Census TIGER/Line GeoPackages | https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geopackage-file.html |
| Census TIGER 2025 technical documentation | https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2025/TGRSHP2025_TechDoc.pdf |
| Geofabrik Georgia extract | https://download.geofabrik.de/north-america/us/georgia.html |
| Geofabrik GIS schema guide | https://download.geofabrik.de/osm-data-in-gis-formats-free.pdf |
| OpenStreetMap copyright and license | https://www.openstreetmap.org/copyright |
| USGS download overview | https://www.usgs.gov/tools/download-data-maps-national-map |
| USGS NTD catalog entry | https://data.usgs.gov/datacatalog/data/USGS%3Aad3d631d-f51f-4b6a-91a3-e617d6a58b4e |
| USGS transportation source description | https://www.usgs.gov/faqs/what-sources-were-used-create-boundaries-structures-and-transportation-layers-national-map |
| Georgia GIS Clearinghouse | https://georgiagisclearinghouse.com/ |
| Georgia Spatial Data Infrastructure portal | https://data.georgiaspatial.org/ |
