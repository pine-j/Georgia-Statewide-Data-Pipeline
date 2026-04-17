# Refactor Evacuation Enrichment — Hybrid Attribute + Spatial Matching

## Git

Create a new branch `feature/hybrid-evac-matching` off the current `master` and do all work there. Do not merge into master.

## Files

- `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py` (main enrichment logic)
- `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py` (QC map generator)
- `02-Data-Staging/qc/evacuation_route_qc/index.html` (Leaflet QC map)

## Current State — DO NOT Regress

The existing spatial matching is ~85-90% accurate. The QC summary shows 1,772 evacuation segments flagged (257 Local/Other, 328 State Route, 500 U.S. Route, 252 Interstate, etc.) and 216 contraflow (all Interstate). The major corridors (I-75, I-16, SR/US routes in south Georgia) are well-matched. The goal is to improve precision by removing false positives — NOT to rewrite the matching from scratch. Treat this as iterative refinement on top of working code.

### Known False Positive Patterns to Fix (from visual QC)

1. **Local/Other road segments** flagged because they run parallel to an evac corridor within 30m (side streets, frontage roads near SR/US evac routes — especially visible around Brunswick and rural SR junctions)
2. **Interstate segments** flagged near SR/US evacuation routes where the buffer catches a highway overpass or adjacent Interstate ramp
3. **Segments extending past route termini** — where a flagged segment continues beyond the official evac route endpoint

## Architecture — Separate Matching Paths for Evac vs Contraflow

The evacuation routes and contraflow routes must use **two distinct matching algorithms**. Do NOT funnel them through the same function with flag toggles. The reasons:

- **Evacuation routes** (268 features) have `ROUTE_NAME` attributes (`SR 26`, `I 75 North`, `US 319`) that enable attribute pre-filtering. They match against all road families. False positives are the primary problem — cross-roads and parallel roads getting incorrectly flagged. The matching needs attribute + spatial hybrid logic.

- **Contraflow routes** (12 features) have NO usable route name attributes — only phonetic code names (`Mary`, `Lincoln`, `King`) and exit numbers in `DESCRIPTION`. They only match against Interstate segments (`interstate_only=True`). False positives are rare because Interstates are physically separated from other roads. These need spatial-only matching but can be tuned independently (different buffer, thresholds, etc.) without affecting evac matching.

Splitting them into separate functions means each algorithm can evolve independently — threshold changes for evac won't accidentally break contraflow, and vice versa.

## Phase 1 — Hybrid Matching for Evacuation Routes

Modify `evacuation_enrichment.py`:

1. **Parse evac `ROUTE_NAME` into `(route_type_gdot, base_route_number)` tuples.** Map: `SR` → `SR`, `I`/`Interstate` → `IS`, `US` → `US`, `CR` → `CR`. Handle dual designations like `SR 1/US 27` (two tuples). Strip directional suffixes like `I 75 North`. The parser `_parse_expected_family()` exists at line 149 — extend or create a sibling that returns type+number instead of just family.

2. **Pre-filter segments before spatial join.** For each evac feature with a parseable ROUTE_NAME, restrict spatial join candidates to segments where `ROUTE_TYPE_GDOT` and `BASE_ROUTE_NUMBER` match. This is the main mechanism to eliminate Local/Other false positives and cross-road catches.

3. **Fall back to spatial-only for unparseable names.** 36 of 268 features have null ROUTE_NAME. Names like `Liberty Expy`, `Ocean Hwy` won't parse. Keep existing spatial-only matching with current thresholds for these.

4. **Add `SEC_EVAC_MATCH_METHOD` column** with values `attribute+spatial` or `spatial_only`.

5. **Keep all existing thresholds** (buffer, overlap ratio tiers, alignment angle, corridor proximity post-filter) for the spatial portion. Do not change them unless the Playwright QC loop reveals a specific reason.

## Phase 1b — Separate Contraflow Matching Function

1. **Extract contraflow matching into its own dedicated function** — no longer sharing `_overlay_matches()` with evacuation routes.

2. **Keep it spatial-only** (code names like `Mary`, `Lincoln` provide no route attribute info).

3. **Keep `interstate_only=True` filter** — this is already the main constraint and works well.

4. **Contraflow thresholds can now be tuned independently** without risk of affecting evacuation matching.

## Phase 2 — Playwright Visual QC After Each Iteration

After modifying the enrichment logic and regenerating the QC map:

1. **Run `generate_qc_map.py`** to produce fresh GeoJSON + HTML.

2. **Open `02-Data-Staging/qc/evacuation_route_qc/index.html`** in Playwright.

3. **Regression check FIRST — verify these known-good areas still match before looking for improvements:**
   - I-75 corridor from Macon south — should be fully covered in red
   - I-16 from Macon to Savannah — continuous red coverage
   - SR corridors in SW Georgia — red should follow the dark blue lines

4. **Then check these known problem regions for improvement:**
   - **Savannah area** (~32.08, -81.09, zoom 11) — look for Local/Other false positives near SR 21, SR 25
   - **Brunswick area** (~31.15, -81.49, zoom 11) — local roads near US-17/SR-25 corridor
   - **SW Georgia** (~31.2, -84.2, zoom 10) — rural SR junctions where cross-roads get flagged
   - **Statewide overview** (~32.5, -83.5, zoom 7) — overall count should stay ~1,700-1,800 evac, ~216 contraflow

5. **Click flagged segments** and read popups to verify `HWY_NAME` matches nearby evac `ROUTE_NAME`.

6. **If regression detected** (good matches lost), revert the change that caused it before proceeding. If improvements confirmed, commit and continue.

## Phase 3 — Codex Review and Fix Loop

After Phase 1/1b code changes are complete and committed (before or after Playwright QC):

1. **Spawn a Codex sub-agent** to review the modified `evacuation_enrichment.py` for bugs, logic errors, edge cases, and regressions against the original behavior. Ask it to specifically check:
   - The ROUTE_NAME parser handles all 52 unique names correctly (including edge cases: `SR 1/US 27`, `I 16 Spur`, `SR 21 Business`, `SR 300 Connector`, `Liberty Expy`, `Ocean Hwy`, `CR 780`)
   - The pre-filter doesn't accidentally exclude valid matches (e.g., concurrent route designations where a US route segment carries an SR evac designation)
   - The spatial-only fallback path still works identically to the old code for the 36 null-name features
   - The new contraflow function produces the same results as the old shared function did

2. **Review the Codex findings yourself.** Triage which issues are real bugs vs. acceptable tradeoffs.

3. **Spawn another sub-agent to fix** any confirmed issues. Re-run the Playwright QC loop after fixes to verify no regressions.

## Constraints

- Don't change output column schema except adding `SEC_EVAC_MATCH_METHOD`.
- Python executable is at `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`.
- The QC map loads data from adjacent GeoJSON files via `fetch()` — serve it via a local HTTP server or use Playwright's `file://` protocol (the map uses relative fetch paths, so a local server is preferred).
- Commit progress on `feature/hybrid-evac-matching` as you go. Do not push — the user will handle that.
