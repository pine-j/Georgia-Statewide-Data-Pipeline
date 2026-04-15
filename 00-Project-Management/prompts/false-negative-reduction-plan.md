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

## Sub-Agent Architecture

**Codex CLI handles code. Claude Code handles visual QC. They alternate.**

```
┌─────────────────────────────────────────────────┐
│ Claude Code (main) — Playwright MCP access      │
│                                                 │
│  1. Spawn Codex → implement two-tier acceptance │
│  2. Run generate_qc_map.py                      │
│  3. Playwright: iterate all 51 corridors        │
│     → produce gap report (corridor + bounds)    │
│  4. Spawn Codex → investigate gaps via Python,  │
│     identify unique_ids, apply hard-codes/fixes │
│  5. Run generate_qc_map.py                      │
│  6. Playwright: re-check failed corridors       │
│  7. GOTO 4 until zero gaps                      │
│  8. Final commit                                │
└─────────────────────────────────────────────────┘
```

### Why This Split

- **Codex CLI** has Bash access → can run Python scripts against the
  245K-segment GeoPackage, edit `_evac_corridor_match.py`, run
  `generate_qc_map.py`. It does the heavy lifting.
- **Claude Code main** has Playwright MCP → can take screenshots, run JS
  queries on the QC map, select corridors in the dropdown. It's the
  verifier and orchestrator.
- Each Codex invocation starts fresh → no context bloat from screenshots
  and JS query results accumulating.

### Codex Invocation Pattern

Each Codex sub-agent receives:
1. This plan document (for context)
2. A specific task with inputs (e.g., "investigate these gap areas")
3. File paths and Python executable path
4. Expected outputs (e.g., "return a list of unique_ids to hard-code")

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

## Implementation Phases

### Phase 1: Two-Tier Acceptance (Codex sub-agent)

**Task for Codex:** Modify `_evac_corridor_match.py` to add two-tier
acceptance logic. See "Algorithm Details" section below for exact code.

**Inputs:** Path to `_evac_corridor_match.py`, this plan.
**Outputs:** Modified file with two-tier acceptance + hard-code mechanism.

### Phase 2: Initial QC (Claude Code main)

1. Run `generate_qc_map.py`
2. Serve QC map on port 8091
3. Playwright: iterate all 51 corridors, take screenshots
4. Produce gap report: list of `(corridor_name, gap_bounds_wgs84)`

### Phase 3: Gap Investigation (Codex sub-agent)

**Task for Codex:** For each gap in the report, run Python against the
base_network.gpkg to identify missing segments.

**Inputs:** Gap report from Phase 2, path to base_network.gpkg.
**Outputs:** For each gap:
- List of `unique_id`s that should be flagged
- Diagnosis: why was each segment missed (overlap too low? geometry offset?
  Local/Other family?)
- Recommended fix: algorithm tweak or hard-code

### Phase 4: Apply Fixes (Codex sub-agent)

**Task for Codex:** Apply the fixes from Phase 3:
- Add unique_ids to `_HARDCODE_OVERRIDES` with comments
- Adjust algorithm if a systemic fix was identified
- Run `generate_qc_map.py` to regenerate

**Inputs:** Phase 3 outputs, file paths.
**Outputs:** Modified files, QC run output.

### Phase 5: Re-Verify (Claude Code main)

Playwright re-checks only the corridors that failed in Phase 2.
- If all pass → Phase 6
- If any fail → back to Phase 3 with new gap report

### Phase 6: Final Validation + Commit (Claude Code main)

1. Full Playwright sweep of all 51 corridors (final confirmation)
2. Automated QC metrics check
3. Codex review of all changes
4. Commit on `feature/hybrid-evac-matching`

## Algorithm Details

### New Constants

```python
# Attribute-boosted thresholds (Tier 1)
ATTR_NORMAL_MIN_OVERLAP_M = 50.0   # was 150.0 for Tier 2
ATTR_SHORT_MIN_RATIO = 0.30        # was 0.40 for Tier 2
```

### New Function: `_accept_overlap_attribute_matched()`

```python
def _accept_overlap_attribute_matched(
    overlap_len: float,
    overlap_ratio: float,
    segment_length_m: float,
) -> bool:
    """Relaxed acceptance for segments whose HWY_NAME matches the corridor.

    No ratio minimum for normal/mega — attribute match confirms identity.
    No angular alignment check. No proximity filter applied after.
    """
    is_short = segment_length_m < SHORT_SEGMENT_MAX_M
    if is_short:
        return overlap_ratio >= ATTR_SHORT_MIN_RATIO
    else:
        return overlap_len >= ATTR_NORMAL_MIN_OVERLAP_M
```

### Modified Inner Loop (in `_per_corridor_evac_overlay`)

```python
# After computing overlap_len, overlap_ratio:

attribute_matched = (
    pos_idx in corridor_hwy_positions.get(route_name_str, set())
    or pos_idx in corridor_hpms_positions.get(route_name_str, set())
)

if attribute_matched:
    accepted = _accept_overlap_attribute_matched(
        overlap_len, overlap_ratio, segment_length_m,
    )
    # Skip proximity filter for attribute matches
else:
    accepted = _accept_overlap(
        overlap_len, overlap_ratio, segment_length_m,
        overlap_geom, corridor_line_geom, seg_geom, ROUTE_BUFFER_M,
    )
    if accepted:
        inside_ratio = overlap_len / float(seg_geom.length) if seg_geom.length > 0 else 0.0
        if inside_ratio < MIN_INSIDE_CORRIDOR_RATIO:
            accepted = False

# Label match method (after acceptance, not before)
if accepted:
    if attribute_matched:
        method = "hpms+spatial" if pos_idx in corridor_hpms_positions.get(route_name_str, set()) else "hwy_name+spatial"
    else:
        method = "concurrent+spatial"
    ...
```

### Hard-Code Override Dict

```python
_HARDCODE_OVERRIDES: dict[str, list[str]] = {
    # Format: "ROUTE_NAME": ["unique_id_1", "unique_id_2"]
    # Each entry has a comment explaining why it's hard-coded.
    #
    # Populated during iterative Playwright QC (Phase 3-5).
}

_METHOD_PRECEDENCE = {
    "hwy_name+spatial": 4,
    "hpms+spatial": 3,
    "concurrent+spatial": 2,
    "spatial_only": 1,
    "hardcode": 0,
}
```

## Constraints

- Do not modify contraflow matching logic
- Do not change Tier 2 (standard) thresholds — only add Tier 1 fast-path
- Hard-codes must have comments explaining the reason
- Python executable: `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`
- Commit on `feature/hybrid-evac-matching`. Do not push. No AI/Claude attribution.

## Files to Modify

- `02-Data-Staging/scripts/01_roadway_inventory/_evac_corridor_match.py`
  - Add `_accept_overlap_attribute_matched()` function
  - Add attribute-boosted constants
  - Add `_HARDCODE_OVERRIDES` dict
  - Modify inner loop for two-tier acceptance
  - Add `"hardcode"` to `_METHOD_PRECEDENCE`
- No changes to `generate_qc_map.py` (corridor filter already works)
- No changes to `evacuation_enrichment.py` (calls the shared module)

## Key Data References

- Roadway segments: `02-Data-Staging/spatial/base_network.gpkg` (layer `roadway_segments`, 245,863 rows)
- Evac routes: `02-Data-Staging/spatial/ga_evac_routes.geojson` (268 features, 52 named + 36 null)
- QC map output: `02-Data-Staging/qc/evacuation_route_qc/`
- QC map URL: `http://localhost:8091/index.html`
- Python: `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`
