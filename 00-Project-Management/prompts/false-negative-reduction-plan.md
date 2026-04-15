# False Negative Reduction Plan — 100% Corridor Coverage

## Goal

**Zero visible gaps on every evacuation corridor.** Every segment physically
on an evacuation corridor must be flagged — no false negatives. Use Playwright
MCP as the ground truth: iterate algorithm fixes and hard-codes until every
corridor passes visual QC with red covering the full extent of blue.

**Stopping criterion:** Playwright iterates through all 51 corridors in the
dropdown filter, and for every single one, red segments span the full blue
corridor extent with no gaps. Zero corridors fail.

## Why 100% Is Achievable

- We have 51 named corridors with a dropdown filter for isolated inspection
- Playwright MCP can visually verify each corridor in seconds
- For any gap, we can programmatically query the roadway segments in that
  area and identify exactly which `unique_id`s need to be flagged
- If algorithm logic can't catch a segment, we hard-code it by `unique_id`
- A human could do this manually — the agent + Playwright can do it faster

## Current State

| Metric | Count |
|---|---|
| Total evac flagged | 1,537 |
| hwy_name+spatial | 513 |
| concurrent+spatial | 831 |
| spatial_only | 193 |
| Corridors with visible gaps | ~15 |
| Contraflow | 170 (unchanged) |

## Diagnostic Findings

### Root Cause A: Attribute-Matched Segments Rejected by Overlap Threshold (33 segments)

Segments whose `HWY_NAME` matches the corridor designation but fail the
tiered overlap thresholds. Common pattern: the segment is longer than the
evac corridor section, so the overlap RATIO is low even though the absolute
overlap is substantial.

**Top near-misses (overlap >= 100m, clearly on the corridor):**

| Corridor | HWY_NAME | Seg Length | Overlap | Ratio | Why Rejected |
|---|---|---|---|---|---|
| US 319 | US-319 | 8,327m | 823m | 0.099 | ratio < 0.20 |
| SR 17 | SR-17 | 4,258m | 793m | 0.186 | ratio < 0.20 |
| SR 196 | SR-196 | 6,293m | 666m | 0.106 | ratio < 0.20 |
| I 75 South | I-75 | 4,444m | 640m | 0.144 | ratio < 0.20 |
| I 75 North | I-75 | 2,290m | 450m | 0.197 | ratio < 0.20 |
| SR 94 | SR-94 | 9,916m | 385m | 0.039 | ratio < 0.20 |
| I 75 | I-75 | 5,398m | 318m | 0.059 | ratio < 0.20 |
| SR 144 | SR-144 | 4,790m | 216m | 0.045 | ratio < 0.20 |
| SR 520 | SR-520 | 10,466m | 129m | 0.012 | overlap < 150m |
| US 319 | US-319 | 1,744m | 114m | 0.065 | overlap < 150m |
| SR 1/US 27 | US-27 BUS | 1,362m | 113m | 0.083 | overlap < 150m |
| SR 133 | SR-133 | 16,894m | 112m | 0.007 | overlap < 150m |

### Root Cause B: Attribute-Matched Segments Rejected by Proximity Filter (6 segments)

Mega-segments (>10km) where the corridor covers only a fraction of their
total length. The `inside_ratio < 0.10` filter rejects them.

| Corridor | HWY_NAME | Seg Length | Overlap | Inside Ratio |
|---|---|---|---|---|
| I 75 South | I-75 | 570,931m | 34,280m | 0.060 |
| US 82 | US-82 | 322,443m | 10,138m | 0.031 |
| US 341 | US-341 | 165,107m | 4,267m | 0.026 |
| SR 144 | SR-144 | 13,537m | 1,204m | 0.089 |
| US 341 | US-341 | 11,189m | 436m | 0.039 |
| I 75 | I-75 | 570,931m | 286m | 0.001 |

### Root Cause C: Geometry Offset (2 segments)

Two SR-135 segments have near-zero overlap despite HWY_NAME matching:
- SR-135 (1,764m): 0m overlap — route polyline is >30m from segment
- SR-135 (13,897m): 48m overlap — mega-segment barely clips buffer

These are digitization offsets. No threshold relaxation helps — hard-code.

### Root Cause D: Local/Other on Corridor Pavement (low priority)

On SR 76, two city street segments (CS-775, CS-776) sit entirely within the
corridor. Risk of re-introducing 122+ false positives is too high. **Skip
unless the iterative loop identifies them as the only remaining gap.**

## Algorithm — Two-Tier Acceptance + Hard-Codes + Iterative Verification

### Phase 1: Two-Tier Acceptance in `_per_corridor_evac_overlay()`

Add a fast-path for segments whose HWY_NAME matches the corridor patterns.
These segments carry the corridor's route designation — they ARE the
corridor. Only need minimal spatial confirmation.

**Implementation in the inner loop:**

```python
# After computing overlap_len, overlap_ratio, segment_length_m:

# Check if segment's HWY_NAME matches any corridor pattern
attribute_matched = (
    pos_idx in corridor_hwy_positions.get(route_name_str, set())
    or pos_idx in corridor_hpms_positions.get(route_name_str, set())
)

if attribute_matched:
    # TIER 1: Relaxed thresholds — they ARE the corridor
    accepted = _accept_overlap_attribute_matched(
        overlap_len, overlap_ratio, segment_length_m,
    )
    # Skip corridor proximity filter for attribute matches
    skip_proximity = True
else:
    # TIER 2: Standard thresholds for concurrent/unmatched segments
    accepted = _accept_overlap(
        overlap_len, overlap_ratio, segment_length_m,
        overlap_geom, corridor_line_geom, seg_geom, ROUTE_BUFFER_M,
    )
    skip_proximity = False
```

**New function:**

```python
# Attribute-boosted thresholds
ATTR_NORMAL_MIN_OVERLAP_M = 50.0   # was 150.0
ATTR_SHORT_MIN_RATIO = 0.30        # was 0.40

def _accept_overlap_attribute_matched(
    overlap_len: float,
    overlap_ratio: float,
    segment_length_m: float,
) -> bool:
    """Relaxed acceptance for segments whose HWY_NAME matches the corridor.

    No ratio minimum for normal/mega segments — attribute match confirms
    segment identity. No angular alignment check — attribute match
    already confirms correct road. No proximity filter applied after.
    """
    is_short = segment_length_m < SHORT_SEGMENT_MAX_M

    if is_short:
        return overlap_ratio >= ATTR_SHORT_MIN_RATIO
    else:
        return overlap_len >= ATTR_NORMAL_MIN_OVERLAP_M
```

**Rationale:**
- 50m minimum overlap prevents incidental clips
- No ratio minimum because the segment IS the corridor road
- No alignment check because attribute match confirms identity
- No proximity filter because a 500km I-75 segment with 34km overlap IS
  on the I-75 corridor regardless of what fraction 34km is of 500km

### Phase 2: Hard-Code Override Mechanism

For segments that no threshold can catch (geometry offsets, edge cases
discovered during the iterative loop):

```python
# Hard-coded segment overrides — discovered during Playwright QC iteration.
# Key: corridor ROUTE_NAME, Value: list of segment unique_ids
_HARDCODE_OVERRIDES: dict[str, list[str]] = {
    # Populated during Phase 4 iterative QC
}

# In _METHOD_PRECEDENCE, add:
"hardcode": 0  # lowest precedence — any real spatial match overrides
```

**At the start of each corridor loop:**

```python
if route_name_str in _HARDCODE_OVERRIDES:
    for uid in _HARDCODE_OVERRIDES[route_name_str]:
        mask = segments["unique_id"] == uid
        if mask.any():
            idx = segments.index[mask][0]
            if idx not in results:
                _merge_result(results, idx, route_name_str,
                              "hardcode", 0.0, 0.0)
                corridor_match_count += 1
```

### Phase 3: Implement and Run Initial QC

1. Add `_accept_overlap_attribute_matched()` to `_evac_corridor_match.py`
2. Modify `_per_corridor_evac_overlay()` to use two-tier acceptance
3. Add `_HARDCODE_OVERRIDES` dict (initially empty)
4. Run `generate_qc_map.py` — expect total ~1,560
5. Commit

### Phase 4: Iterative Playwright Verification Loop

This is the core of the plan. Repeat until all corridors pass:

```
WHILE any corridor has visible gaps:
    FOR each corridor in dropdown:
        1. Select corridor in dropdown filter
        2. Take screenshot
        3. Check: does red span the FULL extent of blue?
        
        IF gap found:
            4. Zoom into gap area
            5. Run JS to query roadway segments in gap bounds:
               
               // Get map bounds around the gap
               const b = map.getBounds();
               // Query the base network for segments in this area
               // (load from roadway GeoPackage via Python)
            
            6. Run Python to identify specific segments in the gap:
               
               python -c "
               import geopandas as gpd
               roads = gpd.read_file('base_network.gpkg',
                   layer='roadway_segments',
                   bbox=(minx, miny, maxx, maxy))
               # Filter to state-system segments
               # Print unique_id, HWY_NAME, ROUTE_FAMILY
               "
            
            7. Determine fix:
               a. If segment has HWY_NAME matching corridor AND overlap > 0:
                  → Algorithm should catch it with Tier 1. Debug why it didn't.
               b. If segment has HWY_NAME matching but overlap = 0 (geometry offset):
                  → Add to _HARDCODE_OVERRIDES
               c. If segment has DIFFERENT HWY_NAME (concurrent designation)
                  AND was rejected by Tier 2 thresholds:
                  → Consider lowering Tier 2 thresholds for this corridor
                  → Or add to _HARDCODE_OVERRIDES if edge case
               d. If segment is Local/Other sitting entirely on corridor:
                  → Add to _HARDCODE_OVERRIDES (safest — no systemic risk)
            
            8. Apply fix (algorithm tweak or hard-code)
            9. Re-run generate_qc_map.py
            10. Re-check THIS corridor
        
        IF no gap:
            Mark corridor as PASSED
    
    Commit after each batch of fixes
```

**Playwright JS for gap investigation:**

```javascript
// After selecting corridor and zooming to gap area:

// 1. Get bounds of the visible gap
const b = map.getBounds();
const sw = b.getSouthWest();
const ne = b.getNorthEast();
console.log(`Gap bounds: ${sw.lng},${sw.lat},${ne.lng},${ne.lat}`);

// 2. Count flagged segments in current view
let flaggedInView = 0;
evacFlaggedLayer.eachLayer(layer => {
  const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
  if (name.includes(CURRENT_CORRIDOR) && b.contains(layer.getBounds().getCenter())) {
    flaggedInView++;
  }
});
console.log(`Flagged segments in gap area: ${flaggedInView}`);

// 3. Check if official route passes through this area
let routeInView = false;
evacRoutesLayer.eachLayer(layer => {
  if (layer.feature.properties.ROUTE_NAME === CURRENT_CORRIDOR
      && b.intersects(layer.getBounds())) {
    routeInView = true;
  }
});
console.log(`Official route in gap: ${routeInView}`);
```

**Python for segment identification in gap area:**

```python
# After getting gap bounds from Playwright (in EPSG:4326)
import geopandas as gpd
from shapely.geometry import box

# Convert bounds from WGS84 to UTM
gap_box_4326 = box(min_lon, min_lat, max_lon, max_lat)
gap_gdf = gpd.GeoDataFrame(geometry=[gap_box_4326], crs="EPSG:4326").to_crs("EPSG:32617")
gap_box_utm = gap_gdf.geometry[0]

roads = gpd.read_file("base_network.gpkg", layer="roadway_segments",
    columns=["unique_id", "HWY_NAME", "ROUTE_FAMILY", "segment_length_m", "geometry"])
roads_in_gap = roads[roads.geometry.intersects(gap_box_utm)]

# Show state-system segments not already flagged
for _, seg in roads_in_gap.iterrows():
    print(f"{seg['unique_id']}  {seg['HWY_NAME']}  {seg['ROUTE_FAMILY']}  {seg['segment_length_m']:.0f}m")
```

### Phase 5: Final Validation

After the iterative loop completes with zero gaps:

1. **Full statewide screenshot** — red everywhere blue is
2. **Automated QC metrics** — total count, contraflow unchanged at 170
3. **Per-corridor JS count query** — every corridor has >= 1 match
4. **Codex review** of all algorithm changes and hard-codes
5. **Final commit** on `feature/hybrid-evac-matching`

## Acceptance Criteria

- [ ] Every named corridor in the dropdown shows red spanning full blue extent
- [ ] Zero corridors with visible gaps (verified by Playwright screenshot)
- [ ] Contraflow count unchanged at 170
- [ ] Zero Local/Other false positives (except hard-coded specific segments)
- [ ] Every hard-coded segment has a comment explaining why it's there
- [ ] `run_automated_qc()` passes

## Files to Modify

- `02-Data-Staging/scripts/01_roadway_inventory/_evac_corridor_match.py`
  - Add `_accept_overlap_attribute_matched()` function
  - Add attribute-boosted constants
  - Add `_HARDCODE_OVERRIDES` dict
  - Modify inner loop for two-tier acceptance
  - Add `"hardcode"` to `_METHOD_PRECEDENCE`
- No changes to `generate_qc_map.py` (corridor filter already works)
- No changes to `evacuation_enrichment.py` (calls the shared module)

## Constraints

- Do not modify contraflow matching logic
- Do not change Tier 2 (standard) thresholds — only add Tier 1 fast-path
- Hard-codes must have comments explaining the reason
- Python executable: `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`
- Commit on `feature/hybrid-evac-matching`. Do not push. No AI/Claude attribution.
