# Per-Corridor Evacuation Route Matching Plan

## Goal

Achieve near-100% accuracy for evacuation route segment identification by
replacing the current bulk spatial join with **per-corridor matching** that
processes each evacuation route independently — the same architecture that
already achieves high accuracy for contraflow.

**Business rule:** This is **physical-alignment matching** — the goal is to
flag every roadway segment that lies on the physical pavement of an evacuation
corridor. If a segment carries a concurrent designation different from the
corridor name (e.g., `HWY_NAME=US-82` on the SR 520 corridor), it should
still be flagged. Designation labels are secondary to spatial truth.

## Current State (baseline to not regress from)

| Metric | Count |
|---|---|
| Total evac flagged | 1,682 |
| Interstate | 265 |
| State Route | 238 |
| U.S. Route | 1,057 |
| Local/Other | 122 |
| Contraflow | 170 |

Contraflow matching is complete and accurate — **do not modify contraflow
logic**. All changes target evacuation route matching only.

## Files to Modify

- `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py` (main enrichment)
- `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py` (QC map generator)

Consider extracting shared corridor-matching helpers into a common module
(e.g., `02-Data-Staging/scripts/01_roadway_inventory/_evac_corridor_match.py`)
to avoid duplicating per-corridor logic across both files. This is optional
but strongly recommended — drift between the two copies has already caused
bugs in previous iterations.

## Root Cause Analysis — Why the Current Approach Falls Short

### Problem 1: Bulk Spatial Join Conflates Corridors

The current approach does `sjoin(all_segments, all_268_evac_features_buffered)`.
A segment at the junction of SR-26 and SR-25 can match either corridor. This
creates false positives at every intersection where evac routes cross.

**Fix:** Process each ROUTE_NAME as its own corridor. A segment only matches
a corridor if it carries that corridor's route designation AND overlaps
spatially.

### Problem 2: BASE_ROUTE_NUMBER Is Broken for US Routes

Critical data finding from this session:

```
HWY_NAME=US-82,  BASE_ROUTE_NUMBER=45   (not 82!)
HWY_NAME=US-441, BASE_ROUTE_NUMBER=29   (not 441!)
HWY_NAME=US-319, BASE_ROUTE_NUMBER=319  (happens to match, but not reliable)
HWY_NAME=US-27,  BASE_ROUTE_NUMBER=1    (state route number, not US number)
```

GDOT's `BASE_ROUTE_NUMBER` for US routes uses an internal numbering system
that does NOT correspond to the signed US route number. The current attribute
matching (`ROUTE_TYPE_GDOT='US' AND BASE_ROUTE_NUMBER=X`) silently fails for
most US routes. SR routes are fine (0 mismatches confirmed).

**Fix:** Use `HWY_NAME` as the primary attribute matching field. It contains
the signed route name in a consistent format: `SR-26`, `US-82`, `I-75`,
`US-23 BUS`, `SR-300 CONN`, `I-16 SPUR`. Zero nulls in the 245,863-segment
dataset.

### Problem 3: Concurrent Route Designations

Many SR evacuation corridors run along roads that also carry US designations.
Each segment has a single `HWY_NAME` value — concurrency is tracked in
`SIGNED_ROUTE_FAMILY_ALL` but there is no multi-valued route-number field.

Confirmed concurrent evac SR corridors (segments where `SIGNED_ROUTE_FAMILY_ALL`
includes both "U.S. Route" and "State Route"):

```
SR-4, SR-7, SR-11, SR-15, SR-17, SR-21, SR-25, SR-26, SR-27,
SR-30, SR-31, SR-32, SR-76, SR-97 — all 14 have 100% of their
segments carrying concurrent US designations
```

Example: `SR-520` has 19 segments, `US-82` has 208 segments — they share
physical alignment but have different `HWY_NAME` values in separate segment
records.

**Fix:** Two-pass matching per corridor:
1. **Primary:** HWY_NAME prefix match (high precision, catches designated segments)
2. **Concurrent fallback:** State-system segments within the corridor buffer
   that were NOT matched by primary on ANY corridor — these are concurrent
   designation segments that would otherwise be missed. Flagged as
   `concurrent+spatial`.

### Problem 4: Unparseable Names Have No Attribute Anchor

36 features have null ROUTE_NAME. Two more have local names:
- `Liberty Expy` = US-280 segments (confirmed via `HPMS_ROUTE_NAME` containing "Liberty")
- `Ocean Hwy` = US-17 segments (confirmed via `HPMS_ROUTE_NAME` containing "Ocean Hwy")

**Fix:** Manual lookup for the two named exceptions. For null features, use
geometry differencing: subtract the union of all named corridors, then run
spatial-only matching on the residual geometry (if any exceeds a minimum
length threshold). This prevents both silent drops AND double-counting.

## Architecture — Per-Corridor Matching Engine

### Performance Strategy

The dataset has **245,863 segments** (not ~80K as initially estimated). Running
52+ spatial joins against the full dataset would be slow. Key optimizations:

1. **Pre-build a HWY_NAME lookup index** — group segment indices by
   `HWY_NAME` prefix (e.g., `"SR-26"` → set of segment indices). Build this
   once; reuse per corridor. This replaces repeated `.str.startswith()` calls.

2. **Build `sindex` once** — construct the spatial index on the full segment
   GeoDataFrame once at the start. Per-corridor spatial queries use
   `sindex.query(corridor_buffer)` instead of full `sjoin`.

3. **Pre-buffer corridor geometries** — union and buffer each corridor's
   features once, not per iteration.

4. **Null-feature spatial queries** — use the null feature's buffered geometry
   with `sindex.query()`, NOT the bounding box (bounding boxes of curving
   lines in urban areas include many unrelated roads).

### Core Algorithm

```python
# === ONE-TIME SETUP ===
hwy_index = build_hwy_name_prefix_index(segments)   # {"SR-26": [idx,...], ...}
seg_sindex = segments.sindex                          # spatial index
named_corridor_union = None                           # for null-feature differencing

results = {}  # seg_idx -> {names, overlap_m, overlap_ratio, match_method}

# === PASS 1: NAMED CORRIDORS ===
for route_name, corridor_features in evac_routes.groupby("ROUTE_NAME"):
    corridor_geom = corridor_features.geometry.union_all()
    corridor_buffer = corridor_geom.buffer(ROUTE_BUFFER_M)

    if route_name in MANUAL_NAME_MAP:
        candidates = manual_lookup(segments, MANUAL_NAME_MAP[route_name])
        method = "hpms+spatial"
    elif parseable(route_name):
        patterns = build_hwy_patterns(route_name)
        candidate_idxs = union(hwy_index.get(p, set()) for p in patterns)
        candidates = segments.iloc[list(candidate_idxs)]
        method = "hwy_name+spatial"
    else:
        continue  # handled in pass 3

    # Spatial filter via pre-built index
    spatial_hits = seg_sindex.query(corridor_buffer, predicate="intersects")
    matched_idxs = set(spatial_hits) & set(candidates.index)

    for idx in matched_idxs:
        # Apply overlap/alignment/corridor-proximity filters
        # (exact same thresholds as current code)
        if accepted:
            merge_result(results, idx, route_name, method, overlap_m, overlap_ratio)

    # Accumulate named corridor union for null-feature differencing
    named_corridor_union = union(named_corridor_union, corridor_buffer)

# === PASS 2: CONCURRENT ROUTE FALLBACK ===
# Find state-system segments inside ANY corridor buffer that were NOT
# matched by Pass 1.  These are concurrent-designation segments.
all_corridor_buffer = union of all named corridor buffers
all_spatial_hits = seg_sindex.query(all_corridor_buffer, predicate="intersects")
unmatched_state_system = [
    idx for idx in all_spatial_hits
    if idx not in results
    and segments.loc[idx, "ROUTE_FAMILY"] != "Local/Other"
]
for idx in unmatched_state_system:
    # Apply same overlap/alignment filters against nearest corridor
    if accepted:
        merge_result(results, idx, corridor_name, "concurrent+spatial", ...)

# === PASS 3: NULL-NAME FEATURES ===
null_features = evac_routes[evac_routes["ROUTE_NAME"].isna()]
for _, feature in null_features.iterrows():
    residual = feature.geometry.difference(named_corridor_union)
    if residual.is_empty or residual.length < 100:  # skip if <100m residual
        continue
    residual_buffer = residual.buffer(ROUTE_BUFFER_M)
    spatial_hits = seg_sindex.query(residual_buffer, predicate="intersects")
    for idx in spatial_hits:
        if idx not in results:  # don't overwrite named corridor matches
            # Apply overlap/alignment filters
            if accepted:
                merge_result(results, idx, None, "spatial_only", ...)
```

### Merge Rules (for segments matching multiple corridors)

When a segment matches more than one corridor (e.g., at a junction):

1. **Deduplicate by `unique_id`** — each segment appears once in final output
2. **Union corridor names** — `SEC_EVAC_ROUTE_NAME = "SR 26; SR 25"`
3. **Keep max overlap metrics** — strongest `overlap_m` and `overlap_ratio`
4. **Precedence for `SEC_EVAC_MATCH_METHOD`:**
   `hwy_name+spatial` > `hpms+spatial` > `concurrent+spatial` > `spatial_only`

### Acceptance Logic (exact copy from current code — do not change)

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

# Corridor proximity post-filter (applied after all corridors processed)
inside_ratio = segment_length_inside_corridor / segment_total_length
if inside_ratio < 0.10:
    accepted = False
```

**Important:** The corridor-proximity post-filter must compute
`inside_corridor` against the specific matched corridor's buffer, NOT
the statewide evac union. This is a behavior change from the current code
that improves precision — a segment that clips two corridors briefly should
be evaluated against each corridor independently.

## Implementation Phases

### Phase 1: HWY_NAME Pattern Builder and Index

Create `_build_hwy_patterns(route_name: str) -> list[str]` that converts evac
ROUTE_NAME values into HWY_NAME prefix patterns.

**Mapping rules** (derived from actual GDOT HWY_NAME formats):

| Evac ROUTE_NAME | HWY_NAME Pattern(s) |
|---|---|
| `SR 26` | `SR-26` |
| `US 82` | `US-82` |
| `I 75` | `I-75` |
| `I 75 North` | `I-75` (strip directional) |
| `I 16 Spur` | `I-16 SPUR` |
| `SR 21 Business` | `SR-21 BUS` |
| `SR 300 Connector` | `SR-300 CONN` |
| `SR 1/US 27` | `SR-1` OR `US-27` (dual) |
| `SR 1 Business` | `SR-1 BUS` |
| `CR 780` | `CR-780` |
| `CR 785` | `CR-785` |
| `Liberty Expy` | Manual: `US-280` where HPMS contains "Liberty" |
| `Ocean Hwy` | Manual: `US-17` where HPMS contains "Ocean Hwy" |

**Suffix abbreviation map** (from GDOT HWY_NAME conventions):
```python
_SUFFIX_ABBREV = {
    "spur": "SPUR",
    "business": "BUS",
    "connector": "CONN",
    "bypass": "BYP",
    "loop": "LOOP",
    "alternate": "ALT",
}
```

**Manual name map** for unparseable evac names:
```python
_MANUAL_NAME_MAP = {
    "Liberty Expy": {"hwy_patterns": ["US-280"], "hpms_contains": "Liberty"},
    "Ocean Hwy": {"hwy_patterns": ["US-17"], "hpms_contains": "Ocean Hwy"},
}
```

**HWY_NAME prefix index** — built once before the corridor loop:
```python
def _build_hwy_prefix_index(segments: gpd.GeoDataFrame) -> dict[str, list[int]]:
    """Map HWY_NAME prefixes to segment indices for O(1) corridor lookups."""
    index = {}
    for idx, name in segments["HWY_NAME"].items():
        if pd.isna(name):
            continue
        # Index by full HWY_NAME and by base prefix (e.g., "SR-26" from "SR-26 CONN")
        index.setdefault(name, []).append(idx)
        base = name.split()[0] if " " in name else name
        if base != name:
            index.setdefault(base, []).append(idx)
    return index
```

**HPMS_ROUTE_NAME normalization** — uppercase, strip semicolons and extra
whitespace before substring matching:
```python
def _normalize_hpms(val: str) -> str:
    return re.sub(r"[;\s]+", " ", str(val)).strip().upper()
```

### Phase 2: Per-Corridor Matching Function

Replace `_hybrid_evac_overlay()` with `_per_corridor_evac_overlay()`.

Implement the three-pass algorithm described above (named corridors →
concurrent fallback → null features).

**Key design decisions:**

1. **Group evac features by ROUTE_NAME** — features sharing the same name
   form a single corridor and are unioned into one geometry before buffering.

2. **HWY_NAME matching** — for each corridor, use the pre-built prefix index
   to get candidate segment indices in O(1). No repeated string operations.

3. **Spatial index queries** — use `segments.sindex.query(corridor_buffer)`
   to get spatially intersecting segment indices. Intersect with HWY_NAME
   candidates to get the final candidate set.

4. **Overlap and alignment filters** — keep all existing thresholds unchanged
   (see Acceptance Logic section above).

5. **Concurrent route fallback** — after all named corridors are processed,
   sweep for state-system segments inside any corridor buffer that weren't
   matched. These get `concurrent+spatial` match method.

6. **Null-name features** — difference against named corridor union, match
   residual geometry (if >100m) against all segments via spatial index.

7. **Match method tracking:**
   - `hwy_name+spatial` — segment's HWY_NAME matches the corridor designation
     AND spatial overlap confirmed
   - `hpms+spatial` — matched via HPMS_ROUTE_NAME (for manual-map names)
   - `concurrent+spatial` — state-system segment on corridor with different
     HWY_NAME (concurrent designation)
   - `spatial_only` — null-name feature, no attribute anchor

8. **SEC_EVAC_ROUTE_NAME** — set to the corridor's ROUTE_NAME for each matched
   segment (more precise than current approach which can assign wrong names).
   Multi-corridor segments get semicolon-joined names.

### Phase 3: Update generate_qc_map.py

If a shared module was created, import from it. Otherwise mirror the same
per-corridor logic in the QC map generator.

The QC map should show:
- Per-corridor match counts in the summary panel
- Match method breakdown in the summary panel
- `match_method` and corridor name in segment popups

### Phase 4: Automated Corridor-Level QC Metrics

Before visual QC, generate **automated diagnostics** that catch regressions
a visual spot-check would miss:

```python
corridor_report = {
    "corridors_with_zero_matches": [...],     # ALERT: should be empty
    "per_corridor_counts": {"SR 26": 45, ...},
    "match_method_breakdown": {"hwy_name+spatial": N, ...},
    "multi_corridor_segments": N,              # segments matching 2+ corridors
    "null_feature_matches": N,
    "concurrent_fallback_matches": N,
}
```

**Automated checks (fail = block commit):**
- Zero corridors with zero matches (every named corridor should match something)
- Total count within 10% of baseline (1,682 ± 168)
- Contraflow count unchanged at 170

**Warning checks (log but don't block):**
- Any corridor with <3 matched segments (suspiciously low)
- `concurrent+spatial` matches >10% of total (might indicate HWY_NAME
  matching is too restrictive)
- `spatial_only` matches >5% of total (might indicate too many null features
  falling through)

### Phase 5: Playwright Visual QC

After automated checks pass, verify visually in Playwright.

**Regression checks (must pass before any other checks):**
1. I-75 Macon south (~32.0, -83.7, zoom 9) — full red coverage
2. I-16 Macon→Savannah (~32.4, -82.2, zoom 8) — continuous coverage
3. SW Georgia SR corridors (~31.2, -84.2, zoom 10) — red follows blue
4. Statewide overview (~32.5, -83.5, zoom 7) — total count ~1,600-1,800

**Improvement checks:**
5. Savannah (~32.08, -81.09, zoom 11) — no Local/Other false positives
6. Brunswick (~31.15, -81.49, zoom 11) — clean US-17/SR-25 corridor
7. Valdosta (~30.85, -83.28, zoom 12) — click segments, verify HWY_NAME
   matches corridor

**Per-corridor spot checks** (use JS to query layer data):
```javascript
// Count segments per corridor name
let corridorCounts = {};
map.eachLayer(layer => {
  if (layer.feature?.properties?.match_method) {
    const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || 'unknown';
    corridorCounts[name] = (corridorCounts[name] || 0) + 1;
  }
});
console.log(JSON.stringify(corridorCounts, null, 2));
```

### Phase 6: Codex Review Loop

After implementation is complete:

1. **Spawn Codex sub-agent** to review changes. Give it these specific checks:
   - Does `_build_hwy_patterns()` produce correct patterns for ALL 52 unique
     ROUTE_NAME values? Test each one explicitly.
   - Does the per-corridor loop handle dual designations (`SR 1/US 27`)
     correctly — matching segments for BOTH patterns?
   - Do the null-name features get differenced correctly? Does the 100m
     residual threshold make sense?
   - Is the suffix abbreviation correct? (`Business` → `BUS`, `Connector` →
     `CONN`, `Spur` → `SPUR`)
   - Does the concurrent fallback correctly identify unmatched state-system
     segments? Does it avoid re-flagging segments already matched by primary?
   - Does the merge logic correctly handle multi-corridor segments?
   - Are existing thresholds preserved exactly (copy the acceptance logic
     verbatim)?
   - Is `SEC_EVAC_MATCH_METHOD` populated for every matched segment?
   - Is the corridor-proximity post-filter computed per-corridor, not against
     the statewide union?
   - Does the HWY_NAME prefix index handle suffix variants correctly (e.g.,
     `SR-300 CONN` indexed under both `SR-300 CONN` and `SR-300`)?

2. **Review findings** — triage into definite bugs vs. acceptable tradeoffs.

3. **Spawn fix sub-agent** for confirmed bugs with specific instructions.

4. **Re-run automated QC metrics** + Playwright visual QC after fixes.

5. **Repeat** until Codex finds no definite bugs.

## Sub-Agent Delegation Strategy

### Agent 1: Implementation (General-Purpose)

Prompt should include:
- This full plan document
- The current state of both files (they can read them)
- The data findings: HWY_NAME formats, BASE_ROUTE_NUMBER issues, suffix
  abbreviations, manual name mappings
- Explicit instruction: modify only evacuation matching, do NOT touch
  contraflow logic
- Python executable path: `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`
- Commit on `feature/hybrid-evac-matching` branch as they go

### Agent 2: Codex Review (codex:codex-rescue)

Prompt should include:
- What changed and why
- The specific review checklist from Phase 6
- Path to the modified file(s)

### Agent 3: Fix Agent (General-Purpose)

Prompt should include:
- The specific bugs found by Codex
- The expected behavior
- The file path and relevant line numbers

### Agent 4: QC Verification (General-Purpose with Playwright)

Prompt should include:
- The automated QC metrics to check first
- The regression check coordinates and expected results
- The per-corridor spot check JS snippets
- Instructions to start a local HTTP server on port 8091
- Instructions to take screenshots of each check area

## Data Reference

### Evac Route Names (all 52 unique + 36 null)

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

### Evac Feature Counts per ROUTE_NAME

```
SR 26: 18, I 75 North: 18, SR 30: 16, SR 520: 16, SR 3: 12,
SR 32: 10, SR 111: 8, SR 333: 8, SR 144: 6, SR 196: 6, SR 300: 6,
US 319: 6, SR 89: 6, SR 133: 4, SR 21: 4, SR 31: 4, SR 35: 4,
SR 376: 4, SR 4: 4, SR 57: 4, SR 76: 4, SR 94: 4, US 23: 4,
and 29 more with 2 features each.
+ 36 features with null ROUTE_NAME
```

### Concurrent Route Crosswalk (SR corridors with US concurrency)

These 14 SR evac corridors have 100% of their segments also carrying a US
designation in `SIGNED_ROUTE_FAMILY_ALL`. The concurrent fallback (Pass 2)
must catch US-designated segments on these corridors:

```
SR-4, SR-7, SR-11, SR-15, SR-17, SR-21, SR-25, SR-26,
SR-27, SR-30, SR-31, SR-32, SR-76, SR-97
```

Note: SR-520 has only 19 segments while US-82 has 208 on the same alignment.
Both are separate evac corridors, so both will match their own HWY_NAME
segments. The concurrent fallback handles any gap.

### GDOT HWY_NAME Suffix Formats (confirmed from data)

| Suffix in Evac Name | HWY_NAME Format | ROUTE_TYPE_GDOT |
|---|---|---|
| Business | `{type}-{N} BUS` | BU |
| Connector | `{type}-{N} CONN` | CN |
| Spur | `{type}-{N} SPUR` | SP |
| Bypass | `{type}-{N} BYP` | BY |
| Loop | `{type}-{N} LOOP` | LP |
| Alternate | `{type}-{N} ALT` | AL |

### Key Column Reference

| Column | Use | Notes |
|---|---|---|
| `HWY_NAME` | **Primary attribute match** | Signed route name: `SR-26`, `US-82`, `I-75`. Zero nulls in 245,863 segments. |
| `ROUTE_TYPE_GDOT` | Route type code | `I`, `US`, `SR`, `CR`, `CS`, `SP`, `BU`, `CN`, etc. |
| `BASE_ROUTE_NUMBER` | Internal GDOT number | Matches signed number for SR only; **broken for US and I** |
| `ROUTE_FAMILY` | Road classification | `Interstate`, `U.S. Route`, `State Route`, `Local/Other` |
| `HPMS_ROUTE_NAME` | Alternate road names | Contains local names like `Liberty;`, `Ocean Hwy;`. Normalize: uppercase, strip `;` and whitespace. |
| `SIGNED_ROUTE_FAMILY_ALL` | All signed families | JSON-like list: `["U.S. Route", "State Route"]`. Use for concurrency detection. |
| `segment_length_m` | Segment length in meters | Used for overlap ratio calculation |

### Spatial Constants (do not change)

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

## Constraints

- Do not change output column schema except `SEC_EVAC_MATCH_METHOD` values
  change to: `hwy_name+spatial`, `hpms+spatial`, `concurrent+spatial`,
  `spatial_only`.
- Do not modify contraflow matching logic at all.
- Python executable: `/c/Users/adith/AppData/Local/Programs/Python/Python313/python.exe`
- QC map uses relative fetch paths — serve via local HTTP server on port 8091.
- Commit progress on `feature/hybrid-evac-matching` as you go. Do not push.
- Do not add AI/Claude attribution to commits, PRs, or code.
