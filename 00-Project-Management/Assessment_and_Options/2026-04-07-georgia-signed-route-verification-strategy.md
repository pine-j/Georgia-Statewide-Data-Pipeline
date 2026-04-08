# Georgia Signed-Route Verification Strategy

## Purpose

Raise Georgia `U.S. Route` versus `State Route` classification from medium confidence to high confidence wherever an official signed-route source can verify the result.

This is a follow-on verification design for the existing `ROUTE_ID` crosswalk. It is not a replacement for the GDOT `ROUTE_ID` parser.

## Current Implementation Status

An initial ETL scaffold is now implemented for the next staged rebuild:

- baseline signed-route verification fields are materialized from the existing `ROUTE_ID` crosswalk
- official GDOT `Interstates`, `US Highway`, and `State Routes` layers are now wired as live reference matchers
- matching currently uses derived `RCLINK` candidates and then prefers interval overlap when the official layer exposes milepoint fields
- if one official reference is unavailable, the ETL continues with the others instead of failing all signed-route verification
- if all official references are unavailable, the ETL falls back to baseline crosswalk-derived values and logs warnings instead of failing hard

## Short Answer on "SV"

If `SV` means the GDOT statewide viewer / ArcWeb service family, then yes: Georgia does have official layers that are suitable for signed-route cross-checking.

The most useful service family is:

- GDOT `ARCWEBSVCMAP`

Known relevant layers in that service family:

- `Interstates`
- `US Highway`
- `State Routes`
- `Statewide Roads`

These are better for signed-route verification than TIGER or OSM because they are official GDOT products and already separate Interstate, U.S. Highway, and state-route views.

## Why the Current Result Is Only Medium Confidence

The current ETL uses official GDOT route-ID logic, but Georgia `ROUTE_ID` values encode state route numbers rather than a direct shield family.

That means:

- `Interstate` can be identified with high confidence from GDOT's Interstate route-code appendix.
- `U.S. Route` versus `State Route` is still partly an interpretation of state-route codes and concurrency tables.

To move beyond that, the pipeline needs a second step that asks:

- is this segment present in an official GDOT signed U.S. Highway layer?
- is this segment present in an official GDOT signed Interstate layer?
- is this segment present only in the official state-route layer?

## Official-First Verification Stack

### Tier 1: GDOT ArcWeb / statewide-viewer layers

Use these first.

#### `US Highway`

Source:

- <https://rnhp.dot.ga.gov/hosting/rest/services/ARCWEBSVCMAP/MapServer/2>

Published layer fields shown in current metadata include:

- `RCLINK`
- `BEGINNING_MILEPOINT`
- `ENDING_MILEPOINT`
- `US_ROUTE_NUMBER`
- `STATE_ROUTE_NUMBER`
- `US_ROUTE_NUMBER_ABBREVIATED`
- `STATE_ROUTE_NUMBER_ABBREVIATED`

This is the strongest official verifier for `U.S. Route`.

#### `State Routes`

Source:

- <https://rnhp.dot.ga.gov/hosting/rest/services/ARCWEBSVCMAP/MapServer/3>

Published layer fields shown in current metadata include:

- `RCLINK`
- `BEGINNING_MILEPOINT`
- `ENDING_MILEPOINT`
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
- `ROUTE_DESCRIPTION`

This is the strongest official verifier for signed state routes.

#### `Statewide Roads`

Source:

- <https://egisp.dot.ga.gov/arcgis/rest/services/ARCWEBSVCMAP/MapServer/4?f=pjson>

Published layer fields shown in current metadata include:

- `RCLINK`
- `ROAD_NAME`
- `ROUTE_TYPE`
- `ROUTE_NUMBER`
- `ROUTE_SUFFIX`
- `CITY_CODE`
- `COUNTY_CODE`
- `GDOT_DISTRICT`

This is a useful fallback and context layer because it carries GDOT's route-type coding for the general network.

#### `Interstates`

Source:

- <https://egisp.dot.ga.gov/arcgis/rest/services/ARCWEBSVCMAP/MapServer/1>

This is the strongest official verifier for Interstate presence. If attribute extraction is stable, prefer exact key-based matching; otherwise use geometry + milepoint matching.

### Tier 2: Other official GDOT service backups

Use these as backup or schema-recovery layers if ArcWeb extraction is unstable.

- GDOT `EOC_SUPPORT_LAYERS / GDOT Roads`
- GDOT `EOC_SUPPORT_LAYERS / GDOT Interstates`
- GDOT `GDOT_ROUTE_NETWORK`
- GDOT `GDOT_STATE_ROUTE_NETWORK`
- GDOT `GPAS` layers such as `US Route` and `GDOT Interstates`

These are still official Georgia sources and should rank above TIGER or OSM if they provide the needed attribute detail.

### Tier 3: TIGER

Use TIGER for corroboration and conflict detection, not as Georgia source of truth.

Useful official Census signals:

- `RTTYP = I` for Interstate
- `RTTYP = U` for U.S.
- `RTTYP = S` for State recognized

Source:

- <https://www.census.gov/library/reference/code-lists/route-type-codes.html>

Use TIGER to:

- confirm broad route family
- detect systematic GDOT mismatches
- support QA dashboards

Do not let TIGER override a clean official GDOT match.

### Tier 4: OSM

Use OSM only as tertiary corroboration or conflict review.

Useful OSM signals:

- `ref=*`
- route relations
- relation `network=*` values such as `US:I`, `US:US`, and state-level route networks
- business / loop / spur relation membership

Sources:

- <https://wiki.openstreetmap.org/wiki/Interstate_Highway_relations>
- <https://www.openstreetmap.org/copyright>

OSM is helpful for edge cases and shield-style variants, but it is not the preferred official verifier and carries ODbL governance overhead.

## Recommended ETL Verification Design

### New staged reference datasets

Stage these as separate verification inputs:

- `gdot_interstates_reference`
- `gdot_us_highway_reference`
- `gdot_state_routes_reference`
- `gdot_statewide_roads_reference`
- `tiger_roads_reference` for QA only
- optional `osm_route_reference` for QA only

### New segment-level verification fields

Add these fields to the staged roadway outputs after the current `ROUTE_ID` crosswalk:

- `SIGNED_INTERSTATE_FLAG`
- `SIGNED_US_ROUTE_FLAG`
- `SIGNED_STATE_ROUTE_FLAG`
- `SIGNED_ROUTE_FAMILY_PRIMARY`
- `SIGNED_ROUTE_FAMILY_ALL`
- `SIGNED_ROUTE_VERIFY_SOURCE`
- `SIGNED_ROUTE_VERIFY_METHOD`
- `SIGNED_ROUTE_VERIFY_CONFIDENCE`
- `SIGNED_ROUTE_VERIFY_SCORE`
- `SIGNED_ROUTE_VERIFY_NOTES`

Recommended meanings:

- `SIGNED_*_FLAG`: boolean evidence that the segment matches that signed family
- `SIGNED_ROUTE_FAMILY_PRIMARY`: single prioritized family for reporting
- `SIGNED_ROUTE_FAMILY_ALL`: pipe- or JSON-list of all matched signed families so concurrency is preserved
- `SIGNED_ROUTE_VERIFY_SOURCE`: highest-priority source that drove the primary label
- `SIGNED_ROUTE_VERIFY_METHOD`: `exact_rclink`, `rclink_milepoint_overlap`, `geometry_overlap`, `tiger_support`, `osm_support`, etc.
- `SIGNED_ROUTE_VERIFY_CONFIDENCE`: `high`, `medium`, `low`
- `SIGNED_ROUTE_VERIFY_SCORE`: normalized numeric score, for example `0.0-1.0`

### Matching order

#### 1. Exact official key match

If the official GDOT split layer exposes `RCLINK` aligned with the staged segment:

- match on `RCLINK`
- then refine with milepoint overlap if the official layer is segmented

This is the preferred method.

#### 2. Official interval overlap match

If the layer has:

- `RCLINK`
- beginning milepoint
- ending milepoint

then match:

- same `RCLINK`
- overlapping interval against staged `FROM_MILEPOINT` / `TO_MILEPOINT`

This is the best general verifier for `US Highway` and `State Routes`.

#### 3. Official geometry overlap fallback

If exact keys are not stable:

- spatially overlay the staged segment against the official Interstate / U.S. / state-route geometry
- require a minimum overlap threshold by length, not just touching

Recommended starting threshold:

- at least `80%` overlap of the staged segment length against the candidate signed-route geometry

#### 4. Secondary corroboration

Only after the official pass:

- TIGER `RTTYP`
- OSM route relations / `ref`

Use these to adjust `VERIFY_NOTES`, not to replace a clean official result.

## Primary-label rule

Keep concurrency visible, but still derive one primary label for maps and rollups.

Recommended priority:

1. `Interstate`
2. `U.S. Route`
3. `State Route`
4. `Local/Other`

Examples:

- a segment matching both official `US Highway` and official `State Routes` becomes:
  - `SIGNED_ROUTE_FAMILY_ALL = ["U.S. Route", "State Route"]`
  - `SIGNED_ROUTE_FAMILY_PRIMARY = "U.S. Route"`
- a segment matching official `Interstates` and official `US Highway` becomes:
  - `PRIMARY = Interstate`

## Confidence rules

Recommended confidence ladder:

- `high`
  - official GDOT split-layer exact or interval match
- `medium`
  - current `ROUTE_ID` crosswalk only
  - or official geometry-only match without stable IDs
- `low`
  - TIGER-only or OSM-only support
  - or conflicting signals between official and non-official sources

## Manual exception handling

Keep a small explicit exception table for conflict resolution:

- `route_verification_exceptions.csv`

Recommended fields:

- `ROUTE_ID`
- `unique_id` or segment key
- `exception_type`
- `override_primary_family`
- `override_all_families`
- `reason`
- `source_note`
- `review_date`

Use this only for:

- known concurrency edge cases
- service schema anomalies
- persistent geometry mismatch exceptions

## Recommended implementation phases

### Phase A: official GDOT verification only

Implement:

- split-layer extraction
- `RCLINK` plus milepoint matching
- official verification fields

This is the highest-value step.

### Phase B: TIGER QA audit

Implement:

- TIGER statewide roads subset
- family comparison reports against GDOT primary labels

Use this to find systemic mismatches, not to overwrite GDOT.

### Phase C: OSM edge-case review

Implement only if needed for:

- business routes
- loops
- spurs
- shield-rich cartography or public-facing map QA

## Recommended closeout position

Do not replace the current Georgia `ROUTE_ID` crosswalk.

Instead:

1. keep it as the baseline classifier,
2. add an official signed-route verification pass using GDOT ArcWeb / statewide-viewer layers,
3. use TIGER and OSM only as secondary QA layers,
4. preserve both `PRIMARY` and `ALL` signed-route families so concurrency is not lost.
