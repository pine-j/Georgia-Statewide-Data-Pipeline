# Roadway Gap-Fill Exploratory Report

Date: 2026-04-03

## Purpose

Document the current Georgia roadway-gap problem, evaluate candidate supplemental datasets, and recommend an evidence-based testing order before any production merge.

## Scope

This is exploratory analysis only. It does not change the staged roadway pipeline or the web app data source.

## 2026-04-04 Follow-Up Note

After the initial exploratory write-up, the current local web app at `http://127.0.0.1:5173/` was re-checked in Playwright after allowing the staged roadway layer to finish loading completely (`622,255 segments loaded`).

Three sample areas were reviewed visually against the basemap using stabilized map views and delayed screenshots:

- Columbus / Dinglewood
- Atlanta in-town sample near `xmin=-84.369, ymin=33.748, xmax=-84.353, ymax=33.764`
- Savannah sample near `xmin=-81.111, ymin=32.064, xmax=-81.095, ymax=32.080`

Observed result from that check:

- no obvious planning-relevant roadway gaps were seen in any of the three samples
- the rendered network appeared to cover both local street grids and larger connectors reasonably well
- no clear evidence was seen in those views of broken access to major roads, commercial/industrial areas, or major destinations

Interpretation:

- the current GDOT-based staged network appears good enough for initial statewide planning use
- if gaps remain, they appear more likely to be minor and localized than a statewide planning blocker
- this does not invalidate the earlier source-completeness concerns, but it does lower the urgency of introducing a supplemental statewide gap-fill source before initial planning work

Playwright screenshots captured for this follow-up:

- `columbus-dinglewood-map.png`
- `atlanta-sample-map.png`
- `savannah-sample-map.png`

## Executive Summary

- The visible missing-road problem is upstream of the web app.
- The current staged network in `base_network.gpkg` is built from GDOT `GA_2024_Routes` and preserves that source faithfully.
- In the Columbus / Dinglewood test area, the current GDOT-derived network is too sparse to represent the visible neighborhood street grid.
- Two additional official GDOT live services were checked:
  - `EOC_SUPPORT_LAYERS / GDOT Roads`
  - `GDOT FunctionalClass / Local Road`
- Those live services did not materially close the Dinglewood gap in the test area.
- The best aligned candidate remains a GDOT live road service if one proves fuller than the current snapshot, but the strongest practical statewide gap-fill candidates are now:
  1. `TIGER/Line` as the first supplement to test
  2. `OpenStreetMap` as the completeness-maximizing fallback
  3. `USGS National Transportation Dataset` as a multimodal-enrichment alternative

## Local Findings

### 1. What the current GPKG is built from

The staged roadway network is built from:

- `01-Raw-Data/GA_RDWY_INV/Road_Inventory_2024.gdb`, layer `GA_2024_Routes`
- normalized by [normalize.py](../../02-Data-Staging/scripts/01_roadway_inventory/normalize.py)
- written to [create_db.py](../../02-Data-Staging/scripts/01_roadway_inventory/create_db.py) outputs:
  - `02-Data-Staging/databases/roadway_inventory.db`
  - `02-Data-Staging/spatial/base_network.gpkg`

Supporting evidence:

- `GA_2024_Routes` raw feature count: `206,994`
- staged `segments` row count: `622,255`
- staged distinct `ROUTE_ID` count: `206,994`

Interpretation:

- The staging pipeline is segmenting the raw GDOT route network.
- It is not dropping route IDs wholesale after ingest.

### 2. Why local-road gaps are happening

Representative Dinglewood / Columbus spot check:

- Raw `GA_2024_Routes` in the AOI returned `6` route records.
- Staged `base_network.gpkg` returned `14` segmented roadway features tied to those routes.
- That density is materially lower than the local street pattern visible in the basemap.

Interpretation:

- The local-road gap is a source-coverage problem in the GDOT-derived route network used by the pipeline.
- It is not primarily a frontend rendering problem.

### 3. Additional official GDOT services checked

The following live services were inspected because they are the most likely to align cleanly with the current GDOT-based GPKG.

#### A. `EOC_SUPPORT_LAYERS / GDOT Roads`

- Service metadata exposes:
  - `ROUTE_ID`
  - `ROUTE_KEY`
  - `FUNCTION_TYPE`
  - `COUNTY`
  - `SYSTEM_CODE`
  - `DIRECTION`
- Dinglewood AOI query returned `7` features:
  - the same 6 GDOT route IDs already present in the raw snapshot
  - plus one short `SYSTEM_CODE = 3` city-street feature

Conclusion:

- Better than the current snapshot by a small amount in the test area.
- Not enough by itself to solve the observed neighborhood gap.

#### B. `GDOT FunctionalClass / Local Road`

- Service metadata exposes:
  - `route_id`
  - `from_measure`
  - `to_measure`
  - `SYSTEM_CODE`
  - `FUNCTIONAL_CLASS`
- Dinglewood AOI query returned `4` local-road features.
- The related `Ownership` layer returned `7` features in the same area.

Conclusion:

- Useful for validation and enrichment.
- Did not show a clearly fuller street network than the current staged source in the test AOI.

#### C. `ARCWEBSVCMAP / Statewide Roads`

- Metadata is promising and includes:
  - `RCLINK`
  - `ROAD_NAME`
  - `ROUTE_TYPE`
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
  - `DATA_SOURCE`
  - `REVISION_DATE`
- The service was responsive for metadata but repeatedly timed out for even simple count queries during this analysis.

Conclusion:

- This remains a potentially valuable official candidate because of schema richness and GDOT alignment.
- Coverage could not be verified empirically in this pass because the live service was too slow for consistent AOI querying.

## Candidate Dataset Assessment

### 1. TIGER/Line Roads

Why it is attractive:

- official federal source
- statewide road coverage
- public-domain friendly usage posture
- clear street names and road-class fields
- easier governance than OSM

Likely contribution beyond geometry:

- `FULLNAME`
- `LINEARID`
- `MTFCC`
- route-type coding
- census geography linkage

Fit:

- best first supplement to test if we want a practical, low-friction statewide gap fill

### 2. OpenStreetMap / Geofabrik

Why it is attractive:

- highest expected local-street completeness
- strongest neighborhood and edge-case coverage
- richer street-level tags than TIGER

Likely contribution beyond geometry:

- name / ref
- oneway
- maxspeed
- lanes where tagged
- surface
- bridge / tunnel
- access and service-road distinctions

Fit:

- best completeness fallback if TIGER is still too sparse
- higher conflation and licensing overhead than TIGER

### 3. USGS National Transportation Dataset

Why it is attractive:

- public-domain
- broader transportation context than roads alone
- useful if we want multimodal enrichment at the same time

Fit:

- not the first gap-fill choice for road completeness alone
- strong option if the next step needs roads plus rail / trails / airports context

### 4. Local Georgia / county centerlines

Why they matter:

- can be very strong in specific counties
- can offer better naming and local maintenance context

Fit:

- not the first statewide supplement
- best reserved for targeted county-level patches after a statewide baseline supplement exists

## Recommendation

### Recommended evaluation order

1. `TIGER/Line`
2. `OpenStreetMap`
3. `USGS NTD`

Parallel note:

- Keep `ARCWEBSVCMAP / Statewide Roads` on the shortlist.
- It is still the best GDOT-aligned live candidate on schema, but it needs a dedicated extraction/performance test before it can be ranked above TIGER.

### Recommended merge strategy

Do not replace the GDOT staged network.

Instead:

1. Keep the current GDOT-derived network as the primary analytical source.
2. Add a supplemental geometry source only where GDOT coverage is missing.
3. Preserve explicit provenance for both geometry and attributes.

Minimum provenance fields:

- `geometry_source`
- `attribute_source`
- `source_record_id`
- `match_method`
- `match_confidence`
- `is_gap_fill_geometry`
- `is_gap_fill_attribute`

Merge rule:

- Prefer GDOT geometry and GDOT attributes where a defensible match exists.
- Use external geometry only when no GDOT feature exists.
- Never silently replace GDOT traffic-derived attributes with non-GDOT values.

## Deferred validation experiment

If later QA shows planning-relevant omissions, run a controlled AOI comparison in at least three known gap areas using:

1. current GDOT staged GPKG
2. TIGER/Line roads
3. OSM Georgia extract
4. optionally `ARCWEBSVCMAP / Statewide Roads` if extraction can be stabilized

Measure:

- visible gap reduction
- road-count coverage
- total centerline length
- named-road capture
- overlap / duplicate behavior
- geometry snap distance to GDOT features
- attribute fill rate by source

## Deferred Action Plan If Later QA Reopens The Issue

1. Keep the current production roadway pipeline unchanged unless later QA shows a planning-relevant omission pattern.
2. If needed, run a three-AOI bake-off using:
   - current GDOT staged GPKG
   - TIGER/Line roads
   - OSM Georgia extract
   - `ARCWEBSVCMAP / Statewide Roads` if extraction can be stabilized
3. Score each candidate on:
   - gap reduction
   - geometry alignment with the GDOT network
   - usable roadway attributes beyond geometry
   - operational complexity for a local-first workflow
4. If TIGER materially closes the gaps with acceptable alignment, use TIGER as the first statewide supplement candidate.
5. If TIGER is still too sparse, evaluate OSM as the maximum-coverage fallback.
6. If the GDOT `Statewide Roads` service becomes queryable at scale, rerun the comparison before making a final supplement decision.
7. Whatever supplement is chosen, preserve explicit provenance for geometry source, attribute source, and gap-fill status in the merged network.

## Current Working Note

As of the 2026-04-04 web app visual re-check, the practical working assumption should be:

- proceed with the current GDOT-based statewide network for initial planning and prototype scoring
- treat supplemental gap-fill work as a validation/improvement track, not an immediate blocker
- revisit TIGER / OSM / additional GDOT supplementation only if later AOI checks or metric-based QA show planning-relevant omissions

## Related Analysis Files

- [roadway-gap-fill-options.md](./roadway-gap-fill-options.md)
- [roadway-supplement-options.md](./roadway-supplement-options.md)
- [roadway-gap-fill-options.csv](./roadway-gap-fill-options.csv)
