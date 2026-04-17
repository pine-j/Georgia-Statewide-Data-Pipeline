# Spatial-First Per-Corridor Evacuation Route Matching Plan

## Goal

Achieve near-100% accuracy for evacuation route segment identification using
a **spatial-first** approach: buffer each corridor, find ALL roadway segments
inside the buffer, then filter out false positives. This guarantees **zero
false negatives** by construction — every segment physically on the corridor
is in the candidate pool.

**Business rule:** This is **physical-alignment matching** — flag every
roadway segment that lies on the physical pavement of an evacuation corridor.
Designation labels are secondary to spatial truth.

## Why This Replaces the Previous Approach

The first attempt (commit `73cfcf7`) used attribute-first matching: find
segments by HWY_NAME, then add concurrent fallback. Problems:

1. **54% of matches came from the concurrent fallback** — the attribute-first
   approach was too narrow for Georgia's concurrent route designations
2. **Visual gaps** in corridor coverage — the fallback missed segments at
   corridor edges
3. **Overcomplicated** — three separate passes, complex merge logic, hard to
   debug

The new approach is simpler: **buffer → spatial candidates → filter false
positives → label match method**. One pass per corridor, no fallback needed.

## Current State (baseline)

| Metric | Count |
|---|---|
| Total evac flagged (previous code) | 1,682 |
| Interstate | 265 |
| State Route | 238 |
| U.S. Route | 1,057 |
| Local/Other | 122 (mostly false positives) |
| Contraflow | 170 |

The first attempt produced 1,535 — correctly removed Local/Other false
positives but introduced false negatives on concurrent-designation corridors.

**Contraflow matching is complete and accurate — do NOT modify contraflow
logic.** All changes target evacuation route matching only.

## Starting Point

Branch: `feature/hybrid-evac-matching`
Last commit: `73cfcf7` — "Replace bulk spatial join with per-corridor evacuation route matching"

Files already in place from the first attempt:
- `02-Data-Staging/scripts/01_roadway_inventory/_evac_corridor_match.py` — **rewrite this**
- `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py` — already calls `_per_corridor_evac_overlay`, keep the interface
- `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py` — already imports from shared module, update for per-corridor filtering UI

**Do NOT touch:**
- `_contraflow_overlay_standalone()` in `evacuation_enrichment.py`
- Any contraflow logic in `generate_qc_map.py`

## Algorithm — Spatial-First Per-Corridor

```python
results = {}  # seg_idx -> {names, overlap_m, overlap_ratio, match_method}

# ONE-TIME SETUP
hwy_index = build_hwy_prefix_index(segments)  # for labeling, not filtering
seg_sindex = segments.sindex

# === FOR EACH CORRIDOR ===
for route_name, corridor_features in evac_routes.groupby("ROUTE_NAME"):
    # 1. Build corridor buffer (buffer individual features, then union)
    corridor_buffer = unary_union(corridor_features.geometry.buffer(ROUTE_BUFFER_M))
    corridor_line = collect_lines(corridor_features.geometry)  # for alignment

    # 2. Spatial query: ALL segments intersecting the buffer
    candidate_positions = sindex.query(corridor_buffer, predicate="intersects")

    # 3. Filter false positives
    for pos_idx in candidate_positions:
        # 3a. Exclude Local/Other (unless corridor is CR)
        if is_local_other(segment) and not corridor_is_cr:
            continue

        # 3b. Compute overlap with corridor buffer
        overlap_geom = seg_geom.intersection(corridor_buffer)
        overlap_len = overlap_geom.length
        overlap_ratio = overlap_len / segment_length

        # 3c. Tiered acceptance thresholds (unchanged from current code)
        if not accepted_by_thresholds(overlap_len, overlap_ratio, segment_length):
            continue

        # 3d. Angular alignment check (skip for short segments)
        if not aligned(overlap_geom, corridor_line, segment):
            continue

        # 3e. Corridor proximity: segment must have >= 10% inside buffer
        inside_ratio = overlap_len / seg_geom.length
        if inside_ratio < 0.10:
            continue

        # 4. Label match method (for diagnostics only — not used for filtering)
        if hwy_name_matches_corridor(segment, route_name):
            method = "hwy_name+spatial"
        else:
            method = "concurrent+spatial"

        merge_result(results, idx, route_name, method, overlap_len, overlap_ratio)

# === NULL-NAME FEATURES ===
# Same spatial-first approach, but difference against named corridor union
# to avoid double-counting.  method = "spatial_only"
```

### Key Design Differences from First Attempt

| Aspect | First Attempt | This Plan |
|---|---|---|
| Candidate pool | HWY_NAME match + concurrent fallback | ALL segments in buffer |
| False negatives | Possible (fallback misses edges) | Zero by construction |
| False positive filtering | Attribute pre-filter | Post-filter (Local/Other, alignment, thresholds) |
| Match method | Determines inclusion | Label only (diagnostics) |
| Passes | 3 (primary + fallback + null) | 1 per corridor + null |
| Concurrent routes | Separate pass 2 | Caught automatically |

### Why Zero False Negatives

Every roadway segment within 30m of the evacuation route polyline is in the
candidate pool. The only removals are:

1. **Local/Other** — county roads and city streets (CR/CS) running parallel
   to evacuation corridors. These are the false positives from the old code
   (122 in baseline). Exception: CR corridors (CR 780, CR 785) keep their
   CR segments.
2. **Overlap too small** — segment barely clips the buffer edge
3. **Wrong angle** — segment crosses the corridor at >30 degrees
4. **Corridor proximity** — segment extends far beyond the corridor (>90%
   of its length is outside the buffer)

A correctly aligned state-system segment on the corridor pavement will always
pass all four filters.

## Implementation

### Phase 1: Rewrite `_per_corridor_evac_overlay()` in `_evac_corridor_match.py`

Replace the three-pass algorithm with the spatial-first single-pass approach.

**Function signature stays the same:**
```python
def _per_corridor_evac_overlay(
    segments: gpd.GeoDataFrame,
    evac_routes: gpd.GeoDataFrame,
    name_field: str,
) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
```

**Keep from the existing module (do not rewrite):**
- All constants (`ROUTE_BUFFER_M`, thresholds, etc.)
- `_SUFFIX_ABBREV`, `_MANUAL_NAME_MAP` (Liberty Expy → US-19/US-82, Ocean Hwy → US-17)
- `_build_hwy_patterns()`, `_build_hwy_prefix_index()`, `_normalize_hpms()`
- `_line_azimuth()`, `_alignment_angle_deg()`
- `_accept_overlap()`
- `_merge_result()`
- `run_automated_qc()`

**Rewrite:** Only the body of `_per_corridor_evac_overlay()`.

**Performance requirements (from first attempt learnings):**
- Buffer individual features then `unary_union` — do NOT use `geometry.union_all().buffer()` (causes 400s/corridor hang)
- Collect corridor line geometry as `MultiLineString(parts)` — do NOT union lines
- Use `segments.sindex.query(buffer, predicate="intersects")` — do NOT use `sjoin`
- Cache column indices for `segment_length_m`, `ROUTE_FAMILY`, `HWY_NAME` with `columns.get_loc()` — avoid repeated string lookups in the inner loop
- Access geometry via `segments.geometry.values[pos_idx]` (numpy array) — faster than `.iat[pos_idx]`

**Corridor iteration:**

For named corridors (ROUTE_NAME not null), group by ROUTE_NAME. For each:
1. Buffer + union polygon
2. Collect line parts for alignment
3. Spatial index query → all candidate positions
4. For each candidate:
   - Check ROUTE_FAMILY: skip Local/Other unless corridor is CR
   - Compute overlap, apply acceptance thresholds
   - Apply alignment check
   - Apply corridor proximity filter
   - Label: check if segment's HWY_NAME matches any of the corridor's
     `_build_hwy_patterns()` or `_MANUAL_NAME_MAP` patterns
   - `_merge_result()` into results dict

For null-name features (36 features with null ROUTE_NAME):
- Difference each against union of all named corridor buffers
- If residual > 100m, buffer residual and match nearby state-system segments
- method = "spatial_only"

### Phase 2: Update QC Map with Per-Corridor Filtering

Update `generate_qc_map.py` to add **per-corridor filtering** to the HTML.

**New UI elements in the QC map:**
1. **Corridor dropdown** — a `<select>` element listing all corridor names
   (sorted alphabetically) plus an "All corridors" option
2. **Filter behavior** — selecting a corridor:
   - Shows only flagged segments where `SEC_EVAC_ROUTE_NAME` contains the
     selected corridor name
   - Zooms the map to the selected corridor's bounds
   - Updates the summary panel to show that corridor's stats
   - Hides the context layer to reduce visual noise
   - Keeps the official evac route layer visible for comparison
3. **"All corridors" option** — resets to full statewide view

**Implementation approach:**

In `html_template()`, add a corridor filter control below the existing
summary panel:

```javascript
// Build corridor filter dropdown
const corridorControl = L.control({ position: 'topleft' });
corridorControl.onAdd = function() {
  const div = L.DomUtil.create('div', 'info-box');
  const select = L.DomUtil.create('select', '', div);
  select.id = 'corridor-filter';
  select.innerHTML = '<option value="">All corridors</option>';

  // Get unique corridor names from the data
  const corridorNames = new Set();
  evacFlaggedLayer.eachLayer(layer => {
    const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME;
    if (name) {
      name.split('; ').forEach(n => corridorNames.add(n));
    }
  });
  [...corridorNames].sort().forEach(name => {
    select.innerHTML += `<option value="${name}">${name}</option>`;
  });

  select.onchange = function() {
    filterByCorridor(this.value);
  };
  L.DomEvent.disableClickPropagation(div);
  return div;
};
corridorControl.addTo(map);

function filterByCorridor(corridorName) {
  evacFlaggedLayer.eachLayer(layer => {
    const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
    const visible = !corridorName || name.includes(corridorName);
    if (visible) {
      layer.setStyle({ opacity: 0.7, weight: 3 });
    } else {
      layer.setStyle({ opacity: 0, weight: 0 });
    }
  });

  if (corridorName) {
    // Zoom to corridor bounds
    const bounds = L.latLngBounds();
    evacFlaggedLayer.eachLayer(layer => {
      const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
      if (name.includes(corridorName)) {
        bounds.extend(layer.getBounds());
      }
    });
    if (bounds.isValid()) {
      map.fitBounds(bounds.pad(0.1));
    }
    // Update title to show corridor name
    document.querySelector('.title-banner').textContent =
      `GDOT Evacuation Route QC — ${corridorName}`;
  } else {
    // Reset to full view
    const bounds = evacRoutesLayer.getBounds();
    if (bounds.isValid()) map.fitBounds(bounds.pad(0.08));
    document.querySelector('.title-banner').textContent =
      'GDOT Evacuation Route QC Map';
  }
}
```

Also add `SEC_EVAC_ROUTE_NAME` to the exported GeoJSON columns so the filter
can access it. (This was already done in the first attempt — verify it's
still there.)

### Phase 3: Run and Validate

1. **Run `generate_qc_map.py`** with the new matching engine
2. **Check automated diagnostics:**
   - Zero corridors with zero matches (the spatial-first approach should
     eliminate this entirely)
   - Total count should be in the range 1,550–1,700 (higher than first
     attempt because no false negatives, but lower than baseline because
     Local/Other false positives removed)
   - Contraflow count unchanged at 170
3. **Serve QC map on port 8091** and do Playwright visual QC

### Phase 4: Playwright Visual QC

Start an HTTP server and use Playwright MCP tools to validate the map.

```
Server: python -m http.server 8091 --directory <QC output dir>
URL:    http://localhost:8091/index.html
```

**Playwright MCP tools to use:**
- `mcp__plugin_playwright_playwright__browser_navigate` — open the QC map
- `mcp__plugin_playwright_playwright__browser_wait_for` — wait for tiles/data
- `mcp__plugin_playwright_playwright__browser_evaluate` — set map view to
  specific coordinates, run JS to query layer data
- `mcp__plugin_playwright_playwright__browser_take_screenshot` — capture each
  check area

**Regression checks (must all pass):**
1. **Statewide overview** — `map.setView([32.5, -83.5], 7)` — red follows blue everywhere
2. **I-75 Macon south** — `map.setView([32.0, -83.7], 9)` — full coverage
3. **I-16 Macon→Savannah** — `map.setView([32.4, -82.2], 8)` — continuous
4. **SW Georgia** — `map.setView([31.2, -84.2], 10)` — red follows blue

**Improvement checks:**
5. **Savannah** — `map.setView([32.08, -81.09], 11)` — no Local/Other false positives
6. **Brunswick** — `map.setView([31.15, -81.49], 11)` — clean US-17/SR-25
7. **Valdosta** — `map.setView([30.85, -83.28], 12)` — click to verify popups

**Per-corridor spot checks (use the new dropdown filter):**
8. Use `browser_evaluate` to select corridors in the dropdown:
   ```javascript
   document.getElementById('corridor-filter').value = 'SR 26';
   document.getElementById('corridor-filter').onchange();
   ```
9. Take screenshots of at least: SR 26, I 75 North, US 82, SR 520
10. For each, verify: red segments cover the full corridor extent with no gaps

**Per-corridor JS query:**
```javascript
let corridorCounts = {};
map.eachLayer(layer => {
  if (layer.feature?.properties?.match_method) {
    const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || 'unknown';
    corridorCounts[name] = (corridorCounts[name] || 0) + 1;
  }
});
console.log(JSON.stringify(corridorCounts, null, 2));
```

### Phase 5: Commit and Codex Review

1. **Commit** on `feature/hybrid-evac-matching`. No AI/Claude attribution.
2. **Spawn Codex sub-agent** with these checks:
   - Is the candidate pool truly all spatial hits (no attribute pre-filter)?
   - Is Local/Other exclusion correct (skip unless corridor is CR)?
   - Are acceptance thresholds identical to current code?
   - Is match_method a label only (not used for filtering)?
   - Does the null-name feature pass correctly difference against named
     corridor union?
   - Does the QC map filter work correctly for multi-corridor segments?
3. **Fix any bugs**, re-run QC, repeat until clean.

## Acceptance Logic (do not change — copy verbatim)

```python
# Tiered overlap thresholds
if is_short_segment:  # segment_length_m < 400
    accepted = overlap_ratio >= 0.40
elif is_mega_segment:  # segment_length_m > 10,000
    accepted = (overlap_len >= 200 and overlap_ratio >= 0.50)
    if not accepted and overlap_len >= 150:  # fallback
        accepted = True
else:  # normal segment
    accepted = (overlap_len >= 150 and overlap_ratio >= 0.20)

# Angular alignment (skip for short segments)
if accepted and not is_short_segment:
    angle = alignment_angle(segment, route_section)
    if angle is not None and angle > 30:
        accepted = False

# Corridor proximity post-filter
inside_ratio = overlap_len / seg_geom.length
if inside_ratio < 0.10:
    accepted = False
```

## Spatial Constants (do not change)

```python
ROUTE_BUFFER_M = 30.0
SHORT_SEGMENT_MAX_M = 400.0
SHORT_SEGMENT_MIN_RATIO = 0.40
NORMAL_MIN_OVERLAP_M = 150.0
NORMAL_MIN_RATIO = 0.20
MEGA_SEGMENT_LENGTH_M = 10_000.0
MEGA_MIN_OVERLAP_M = 200.0
MEGA_MIN_RATIO = 0.50
MAX_ALIGNMENT_ANGLE_DEG = 30.0
MIN_INSIDE_CORRIDOR_RATIO = 0.10
```

## Data Reference

### Evac Route Names (52 unique + 36 null)

```
CR 780, CR 785, I 16 Spur, I 185 North, I 75, I 75 North, I 75 South,
Liberty Expy, Ocean Hwy, SR 1 Business, SR 1/US 27, SR 11, SR 111,
SR 133, SR 135, SR 144, SR 15, SR 17, SR 196, SR 21, SR 21 Business,
SR 241, SR 25, SR 26, SR 27, SR 3, SR 30, SR 300, SR 300 Connector,
SR 302, SR 31, SR 32, SR 333, SR 35, SR 376, SR 4, SR 520, SR 57,
SR 62, SR 7, SR 76, SR 89, SR 91, SR 93, SR 94, SR 97, US 19, US 23,
US 319, US 341, US 82, US 84
+ 36 features with null ROUTE_NAME
```

### 14 SR Corridors with 100% US Concurrent Designation

These corridors have zero segments with SR HWY_NAME. The spatial-first
approach catches them automatically (no concurrent fallback needed):

```
SR-4, SR-7, SR-11, SR-15, SR-17, SR-21, SR-25, SR-26,
SR-27, SR-30, SR-31, SR-32, SR-76, SR-97
```

### Manual Name Map

```python
_MANUAL_NAME_MAP = {
    "Liberty Expy": {"hwy_patterns": ["US-19", "US-82"], "hpms_contains": "Liberty"},
    "Ocean Hwy": {"hwy_patterns": ["US-17"], "hpms_contains": "Ocean Hwy"},
}
```

### Key Columns

| Column | Use | Notes |
|---|---|---|
| `HWY_NAME` | **Match method labeling** | `SR-26`, `US-82`, `I-75`. Zero nulls. |
| `ROUTE_FAMILY` | **False positive filter** | Skip `Local/Other` unless corridor is CR |
| `segment_length_m` | Overlap ratio calc | Fall back to geometry length |
| `HPMS_ROUTE_NAME` | Manual name matching | Contains "Liberty", "Ocean Hwy" |

### Performance Learnings from First Attempt

| Operation | Fast | Slow (avoid) |
|---|---|---|
| Buffer corridor | `unary_union(features.buffer(30))` | `features.union_all().buffer(30)` — 400s! |
| Line geometry | `MultiLineString(parts)` — O(n) | `union_all()` on LineStrings — O(n^2) |
| Spatial query | `sindex.query(buffer)` | `gpd.sjoin()` on full dataset |
| Column access | `iat[pos_idx]` + cached col index | `at[idx, "col_name"]` per iteration |
| Geometry access | `geom_arr = segments.geometry.values` | `.iat[pos_idx]` per iteration |

## Constraints

- Do not change output column schema (`SEC_EVAC`, `SEC_EVAC_ROUTE_NAME`,
  `SEC_EVAC_MATCH_METHOD`, `SEC_EVAC_OVERLAP_M`, `SEC_EVAC_OVERLAP_RATIO`, etc.)
- Match method values: `hwy_name+spatial`, `hpms+spatial`, `concurrent+spatial`, `spatial_only`
- Do not modify contraflow matching logic
- Python executable: `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`
- QC map served via local HTTP server on port 8091
- Commit on `feature/hybrid-evac-matching`. Do not push. No AI/Claude attribution.
