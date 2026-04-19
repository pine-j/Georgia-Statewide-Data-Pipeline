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
bug is addressed.

---

## 6. Reproduction

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
