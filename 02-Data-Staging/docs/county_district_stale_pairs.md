# 41 Stale (COUNTY_CODE, DISTRICT) Pairs in the County Dropdown

Diagnostic report for `get_staged_filter_options()` in
`04-Webapp/backend/app/services/staged_roadways.py` returning **200 (COUNTY_CODE,
DISTRICT) pairs** to the UI dropdown when Georgia has **159 counties**. The
41-row surplus surfaced during Step 6 webapp rollout (see TODO dated 2026-04-18).

Produced 2026-04-19 from the staged SQLite database
(`02-Data-Staging/databases/roadway_inventory.db`) and the authoritative
`county_boundaries` layer in `02-Data-Staging/spatial/base_network.gpkg`.

---

## 1. Finding

The SQL that feeds the dropdown is:

```sql
SELECT COUNTY_CODE, DISTRICT
FROM segments
WHERE COUNTY_CODE IS NOT NULL AND DISTRICT IS NOT NULL
GROUP BY COUNTY_CODE, DISTRICT
ORDER BY DISTRICT, COUNTY_CODE
```

This returns **200 rows** today.

- **159** distinct `COUNTY_CODE` values — one per Georgia county, matching
  `02-Data-Staging/config/county_codes.json`. No invalid or NULL county codes
  leak into the result set.
- **35** of those 159 counties appear with more than one `DISTRICT`. Each
  county has exactly one majority `DISTRICT` (thousands of segments) plus one
  to three minority `DISTRICT` values (1 to 153 segments each). The 35
  multi-district counties contribute **41 extra rows**, explaining the
  surplus (159 + 41 = 200).
- **5** segments have NULL `COUNTY_CODE` or NULL `DISTRICT`. They are
  excluded by the `WHERE` clause, so they are not part of the 41 surplus.

The 41 extras are all one classification: **valid `COUNTY_CODE`, stale
`DISTRICT` attribute from the upstream Road Inventory GDB**. The
`DISTRICT` column on the source road inventory is assigning a handful of
segments per county to the DISTRICT of a neighboring county, against
GDOT's own authoritative county→district mapping (`GDOT_DISTRICT` on the
`county_boundaries` layer).

---

## 2. Classification — all 41 rows

Classification legend:

- **valid COUNTY_CODE, stale DISTRICT from source GDB** — the `COUNTY_CODE`
  is canonical (one of the 159 Georgia FIPS codes), and the majority DISTRICT
  for that county in `segments` matches the canonical `GDOT_DISTRICT` on the
  `county_boundaries` layer. The minority DISTRICT on these rows disagrees
  with that canonical assignment.

No rows fell into any other classification bucket. There are **no** bad
`COUNTY_CODE` values, **no** null-leak rows, and **no** coastal/edge/
multi-county segments reaching the query. The entire 41-row surplus is
driven by the single pattern below.

| # | COUNTY_CODE | County | Canonical DISTRICT (county_boundaries) | Stale DISTRICT | n_segments | Classification |
|---:|---:|---|---:|---:|---:|---|
| 1 | 009 | Baldwin | 2 | 3 | 2 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 2 | 043 | Candler | 5 | 2 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 3 | 045 | Carroll | 6 | 3 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 4 | 057 | Cherokee | 6 | 7 | 7 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 5 | 057 | Cherokee | 6 | 1 | 2 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 6 | 059 | Clarke | 1 | 2 | 2 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 7 | 063 | Clayton | 7 | 3 | 11 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 8 | 067 | Cobb | 7 | 6 | 26 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 9 | 077 | Coweta | 3 | 7 | 3 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 10 | 081 | Crisp | 4 | 3 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 11 | 085 | Dawson | 1 | 6 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 12 | 089 | DeKalb | 7 | 1 | 24 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 13 | 091 | Dodge | 2 | 3 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 14 | 093 | Dooly | 3 | 4 | 5 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 15 | 097 | Douglas | 7 | 6 | 19 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 16 | 101 | Echols | 4 | 5 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 17 | 113 | Fayette | 3 | 7 | 4 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 18 | 117 | Forsyth | 1 | 7 | 35 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 19 | 117 | Forsyth | 1 | 6 | 4 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 20 | 121 | Fulton | 7 | 1 | 151 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 21 | 121 | Fulton | 7 | 3 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 22 | 121 | Fulton | 7 | 6 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 23 | 135 | Gwinnett | 1 | 7 | 26 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 24 | 151 | Henry | 3 | 7 | 6 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 25 | 209 | Montgomery | 5 | 2 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 26 | 217 | Newton | 2 | 7 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 27 | 221 | Oglethorpe | 2 | 1 | 3 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 28 | 223 | Paulding | 6 | 7 | 10 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 29 | 235 | Pulaski | 3 | 2 | 3 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 30 | 241 | Rabun | 1 | 2 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 31 | 247 | Rockdale | 7 | 3 | 3 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 32 | 247 | Rockdale | 7 | 2 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 33 | 251 | Screven | 5 | 2 | 9 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 34 | 261 | Sumter | 3 | 4 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 35 | 273 | Terrell | 4 | 3 | 6 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 36 | 289 | Twiggs | 3 | 2 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 37 | 291 | Union | 1 | 3 | 2 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 38 | 307 | Webster | 3 | 4 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 39 | 315 | Wilcox | 4 | 2 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 40 | 315 | Wilcox | 4 | 3 | 1 | valid COUNTY_CODE, stale DISTRICT from source GDB |
| 41 | 319 | Wilkinson | 2 | 3 | 2 | valid COUNTY_CODE, stale DISTRICT from source GDB |

Total n_segments across the 41 stale rows: **382** (out of 263,947 staged
segments, 0.14%).

---

## 3. Root cause

The `DISTRICT` attribute on `segments` is carried forward from the source
GDOT Road Inventory geodatabase
(`01-Raw-Data/.../Road_Inventory_2024.gdb` → `GA_2024_Routes` + related
layers). That column is authored at the segment level by GDOT staff. For
382 segments — concentrated in the metro-Atlanta cluster (Cobb, DeKalb,
Fulton, Forsyth, Gwinnett, Douglas, Cherokee, Clayton) and a long
thin-tail of 1-to-2-segment mis-stamps in smaller counties — the
`DISTRICT` value on the source row does not match the `GDOT_DISTRICT`
polygon of the county the segment geometrically falls in.

A separate spatial check (representative-point-in-polygon of each
minority-district segment against `county_boundaries`) found that **345
of the 382** lie geometrically inside the stamped `COUNTY_CODE` — i.e.
the source GDB has them in the right county but with the wrong
DISTRICT. The other **37 lie geometrically in a neighboring county** —
both `COUNTY_CODE` and `DISTRICT` on those rows are the neighbor's, but
only the DISTRICT discrepancy surfaces in the dropdown because a
`GROUP BY` on both still buckets them under the stamped county code.

Neither `normalize.py` (which only spatially backfills `COUNTY_CODE`
and `DISTRICT` when at least one is NULL —
`backfill_county_district_from_geometry`) nor the SQLite loader
(`create_db.py`) re-stamp these attributes against the county polygons
for segments that already have a value. So the mis-stamps propagate
from the GDB straight through to the staged SQLite.

---

## 4. Why this is user-visible

The dropdown in the webapp is built from the `(COUNTY_CODE, DISTRICT)`
result set and passed into `CountyOption(county_fips, district, ...)`.
Because the same COUNTY_CODE can appear with a canonical + one or more
stale DISTRICTs, the dropdown renders multiple entries for the same
county:

- e.g. Fulton appears as (Fulton, D7), (Fulton, D1), (Fulton, D3),
  (Fulton, D6) — 4 dropdown entries for one county.
- Clicking any of the non-canonical ones filters the map to just the
  minority segments (151, 1, or 1 segment in Fulton's case), which
  looks like a broken filter to the user.

---

## 5. Recommended fix — query-side `DISTINCT` on canonical county list

Fix the symptom at the query layer rather than re-stamping the 382
segments in staging. Two reasons:

1. The staged `DISTRICT` column is still correct for the 263,565
   segments that are not in the stale set. Re-stamping would add a
   GPKG-aware spatial join step to `normalize.py` for a 0.14% attribute
   fix, which is a lot of pipeline complexity for a dropdown bug.
2. The authoritative county→district mapping is already present on the
   `county_boundaries` layer (`GDOT_DISTRICT`), which the webapp already
   reads. We do not need a new config file or a new config-side mapping.

### 5a. Alternative considered: staging-side cleanup

A staging-side fix would mean adding a pass inside `normalize.py` after
`backfill_county_district_from_geometry` that overrides `DISTRICT` on
every segment to the canonical `GDOT_DISTRICT` for that segment's
COUNTY_CODE. That is arguably more correct ("`segments.DISTRICT` should
always agree with `county_boundaries.GDOT_DISTRICT`"), but it silently
rewrites 382 source values. That deserves an explicit user decision
before being adopted — it is a data-quality opinion, not a bug-fix.
Not recommended without approval.

### 5b. Recommended: query-side fix

Rewrite the `county_rows` query in
`04-Webapp/backend/app/services/staged_roadways.py`
(`get_staged_filter_options`, lines 1094-1104) so each COUNTY_CODE
emits exactly one row, with the DISTRICT taken from the canonical
`county_boundaries` layer rather than the per-segment `DISTRICT`
column. Two implementation sketches:

**Option A — pure SQL, majority-district per county:**

```sql
WITH district_counts AS (
    SELECT COUNTY_CODE, DISTRICT, COUNT(*) AS n
    FROM segments
    WHERE COUNTY_CODE IS NOT NULL AND DISTRICT IS NOT NULL
    GROUP BY COUNTY_CODE, DISTRICT
)
SELECT COUNTY_CODE, DISTRICT
FROM district_counts dc
WHERE n = (
    SELECT MAX(n) FROM district_counts dc2
    WHERE dc2.COUNTY_CODE = dc.COUNTY_CODE
)
ORDER BY DISTRICT, COUNTY_CODE;
```

Produces exactly 159 rows. Relies on "majority segment DISTRICT per
county == canonical DISTRICT", which holds for all 35 affected
counties today (verified as part of this diagnostic).

**Option B — read the canonical mapping from GPKG once, join in Python:**

Load the `county_boundaries` layer on first call (like the existing
`_load_county_maps()` lru_cache), build `{COUNTYFP → GDOT_DISTRICT}`,
then replace the DISTRICT in the SQL result with the canonical value.
Drops the "what if minority ever beats majority" foot-gun in Option A
but adds a GPKG read to the filter-options endpoint.

Option A is the smaller, lower-risk fix. Option B is the correct one
if we ever expect a stale-district mis-stamp to grow past 50% for some
county (unlikely but not structurally impossible).

### 5c. Second-order fix (not required for the dropdown)

The 37 segments whose geometry lies in a neighboring county (both
COUNTY_CODE and DISTRICT from the wrong county) are a second, smaller
data-quality issue — they will display under the wrong county filter,
wrong district filter, and wrong county in the segment popup. They are
not fixed by either option above. A targeted re-stamp of those 37
rows in `backfill_county_district_from_geometry` (relax its NULL-only
gate to also cover "segment representative-point not inside stamped
COUNTY_CODE polygon") would fix them, but this is a separate
investigation and should get its own TODO entry after the dropdown
bug is addressed. That investigation was done on 2026-04-19; see
Section 6 — the conclusion is that the 37 are cross-boundary
segments whose stamped attribution is already correct by
majority-of-length, and the NULL-only gate should not be relaxed.

---

## 6. Follow-up: the 37 mislocated-geometry segments

Section 5c flagged a second-order question: 37 of the 382 stale rows
have a representative point that lies outside the county stamped on
the segment. On its face that suggests both `COUNTY_CODE` and
`DISTRICT` were mis-attributed and should be rewritten from geometry.
A classification pass on those 37 segments on 2026-04-19 shows the
opposite — the stamped attribution is already the right one for every
case, and a naive geometry-based overwrite would make the data worse.

### 6a. Method

For each of the 382 minority-district segments:

1. Reproject to EPSG:26917 (UTM 17N) so all distances are in meters.
2. Take the segment's `representative_point` (Shapely's guaranteed-
   interior point for a line).
3. Spatially join that point against `county_boundaries` to find which
   county the point falls in (`geom_cc`).
4. Flag as "mislocated" if `geom_cc` differs from the stamped
   `COUNTY_CODE` — this yields the 37 segments from 5c.

Then, for each of the 37, measure:

- `n_counties_touched`: distinct county polygons the full line
  intersects.
- `frac_in_stamped_county`: fraction of the line's length that lies
  inside the stamped county polygon.
- `frac_in_true_county`: fraction of the line's length that lies
  inside the rep-point's county polygon.
- `dist_to_stamped_m`: distance from the rep-point to the stamped
  county's boundary (in meters).

### 6b. Findings

**Every one of the 37 segments crosses a county boundary.**
`n_counties_touched == 2` for all 37. There is no case (b) (segment
fully inside a wrong county) and no case (c) (near-boundary
geocoding noise in a genuinely wrong county). The breakdown is:

| Category | Count |
|---|---:|
| (a) crosses boundary — attribution is a majority-by-length call | 37 |
| (b) fully inside wrong county — clear error | 0 |
| (c) near-boundary geocoding noise | 0 |
| **Total** | **37** |

Supporting distributions on the 37 rows:

| Metric | min | median | max |
|---|---:|---:|---:|
| `frac_in_stamped_county` | 0.500 | 0.977 | 0.999 |
| `frac_in_true_county` | 0.001 | 0.023 | 0.500 |
| `dist_to_stamped_m` | 0.00 | 0.02 | 0.08 |
| `n_counties_touched` | 2 | 2 | 2 |

Two things fall out of those numbers:

1. **No segment has less than 50% of its length in the stamped
   county.** The minimum `frac_in_stamped_county` is 0.500. By any
   reasonable "majority wins" rule, the stamped county is already the
   best choice.
2. **Every rep-point is within 8 cm of the stamped county's
   boundary.** `dist_to_stamped_m` maxes out at 0.075 m. The 37 rows
   are not geocoding errors in the true sense; they are boundary-
   straddling segments where Shapely's `representative_point()`
   happens to pick an interior point that falls on the minority side
   of the boundary. A representative point is a deterministic
   geometric construction, not evidence about attribution.

The 37-segment "mislocation" finding in 5c was therefore an artifact
of the rep-point-in-polygon check, not a data-quality defect.

### 6c. Sample segments

A representative spread across the 37 rows (county columns are
FIPS codes; `frac_stamped` = fraction of line length in stamped
county):

| `unique_id` | stamped | geom rep-pt | frac_stamped | frac_true |
|---|---|---|---:|---:|
| `1009200008200INC_0.0000_0.0002` | 009 / D3 | 169 / D3 | 0.839 | 0.161 |
| `1067200909500INC_0.1774_0.1808` | 067 / D6 | 057 / D6 | 0.979 | 0.021 |
| `1085200005900INC_2.1029_2.1030` | 085 / D6 | 227 / D6 | 0.500 | 0.500 |
| `1117200371700INC_3.8082_3.8100` | 117 / D7 | 121 / D7 | 0.977 | 0.023 |
| `1135200395700INC_0.0006_0.0354` | 089 / D1 | 135 / D1 | 0.999 | 0.001 |
| `1247200030100INC_0.0009_0.0010` | 247 / D3 | 151 / D3 | 0.510 | 0.490 |
| `1261200013800INC_0.4488_0.4489` | 261 / D4 | 177 / D4 | 0.786 | 0.214 |

All 22 distinct (stamped, geom) county pairs appear with the
classification pass; the highest-frequency pairs are Fulton/Forsyth
(7 + 5 rows in either direction), DeKalb/Gwinnett (4 + 2), and
Paulding/Cobb (2) — matching the metro-Atlanta concentration that
dominated the primary 382-segment bucket.

### 6d. Recommendation — do NOT relax `backfill_county_district_from_geometry`

The Section 5c hypothesis was that the 37 segments had a wrong
`COUNTY_CODE` that could be safely overwritten if the geometry
disagreed. The data does not support that hypothesis:

- **No segment has a wrong `COUNTY_CODE` in the majority-by-length
  sense.** Every stamped county is the county that contains ≥50% of
  the segment's length. Overwriting those stamps based on the
  rep-point's polygon would flip 37 correct attributions to the
  minority-county side.
- **The side-finding is already resolved for the user-visible
  dropdown.** Task 1's Option A fix collapses the county filter to
  one row per county at the majority DISTRICT. Because all 37 of
  these segments sit in counties whose canonical majority is already
  the stamped county, they are no longer surfaced as phantom dropdown
  entries.

A residual cosmetic issue remains: these 37 segments will still
display under their minority-county filter in fine-grained UI like
segment popups and district-boundary overlays, because `segments.
COUNTY_CODE` is unchanged. That is a cross-boundary segment
rendering question (which county label do we show?), not a
staging-enrichment question, and it should be addressed — if at all
— at the UI layer, not by rewriting 37 GDOT source attributions.

The NULL-only gate on `backfill_county_district_from_geometry` is
**correct as written**. Do not relax it.

### 6e. Follow-up

Cross-boundary segments are a known UI corner case. If the project
lead wants a deterministic "which county owns this segment" rule for
display purposes, the right place to author it is the webapp layer,
using the same majority-by-length logic Task 1 applies to the
dropdown. A TODO has been added to `D:/TODO.md` capturing the UI
decision that needs a human call before any display-layer rewrite.

---

## 7. Reproduction

From the repo root, with the staged DB and GPKG present in the main
worktree:

```python
import sqlite3, json, pathlib, collections
import geopandas as gpd, pandas as pd

DB = '02-Data-Staging/databases/roadway_inventory.db'
GPKG = '02-Data-Staging/spatial/base_network.gpkg'
CFG = pathlib.Path('02-Data-Staging/config/county_codes.json')

counties = gpd.read_file(GPKG, layer='county_boundaries')
canon = {int(r['COUNTYFP']): int(r['GDOT_DISTRICT'])
         for _, r in counties.iterrows()
         if pd.notna(r['COUNTYFP']) and pd.notna(r['GDOT_DISTRICT'])}
cc_name = {int(k): v for k, v in json.loads(CFG.read_text()).items()}

con = sqlite3.connect(DB)
rows = con.execute(
    'SELECT COUNTY_CODE, DISTRICT, COUNT(*) n FROM segments '
    'WHERE COUNTY_CODE IS NOT NULL AND DISTRICT IS NOT NULL '
    'GROUP BY COUNTY_CODE, DISTRICT'
).fetchall()

by_cc = collections.defaultdict(list)
for cc, d, n in rows:
    by_cc[int(cc)].append((int(d), n))

print('pairs:', len(rows))
print('multi-district counties:', sum(1 for p in by_cc.values() if len(p) > 1))
print('extras:', sum(len(p) - 1 for p in by_cc.values() if len(p) > 1))
```
