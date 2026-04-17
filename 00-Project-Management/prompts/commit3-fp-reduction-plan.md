# Commit 3 — Manual QC Review: False Positive Reduction Plan

**Date:** 2026-04-16  
**Branch:** feature/hybrid-evac-matching  
**QC Map:** http://localhost:8091/  
**Baseline at start:** 1,460 matched segments (hwy_name+spatial=1083, concurrent+spatial=153, spatial_only=180, gap_fill=36, hardcode=8)

---

## Methodology

Playwright-driven QC walk of all 5 target corridors. For each corridor:

1. Selected corridor in the Filter by Corridor dropdown
2. Fetched `network_evac_flagged.geojson` (1,460 features) programmatically
3. Extracted all `concurrent+spatial` and `gap_fill` segments (highest FP risk — no HWY_NAME attribute confirmation)
4. Computed segment midpoint coordinates; zoomed Leaflet map to each suspect
5. Screenshotted at zoom 14–17 to determine whether a blue line (official GDOT evac route) exists under the red segment
6. Classified: **red-with-blue = legitimate**, **red-without-blue = FP**

---

## 1. Per-Corridor Findings

| Corridor | Total Segs | Methods | Confirmed FPs | UIDs | Notes |
|---|---|---|---|---|---|
| I 75 North | 117 | hwy_name+spatial=110, concurrent+spatial=7 | **4** | see below | All FPs are tiny interchange segments (26–268m) with no blue underneath |
| I 75 South | 16 | hwy_name+spatial=16 | **0** | — | All I-75 segments, perfect red+blue alignment throughout |
| SR 3 | 126 | hwy_name+spatial=88, hardcode=5, concurrent+spatial=13, gap_fill=20 | **1** | see below | US-80 crosses SR 3 (US-19) at right angle in Taylor Co.; all gap_fill confirmed legitimate |
| SR 26 | 176 | hwy_name+spatial=154, hardcode=1, concurrent+spatial=20, gap_fill=1 | **1** | see below | US-280 runs parallel to SR 26 near Savannah without sharing the designated route |
| Liberty Expy | 14 | hwy_name+spatial=12, concurrent+spatial=2 | **0** | — | US-133 (in concurrent map) and US-300 both confirmed; blue visible throughout |

**Total confirmed FPs: 6**

---

## 2. Detailed FP Analysis

### I 75 North — 4 FPs

All four are segments whose HWY_NAME is NOT I-75 but whose geometry briefly enters the 30m corridor buffer at an interchange or where a bypass diverges. Blue line is absent under each.

| UID | HWY_NAME | Overlap | Ratio | County | Why FP |
|---|---|---|---|---|---|
| `1000100003100INC_8.8674_8.9000` | SR-31 | 26 m | 49.8% | Lowndes | Cross-street through I-75 interchange at Exit 16 near Valdosta; bridge deck/approach enters 30m buffer for just 26m |
| `1000100010700INC_0.0000_0.0425` | SR-107 | 57 m | 83.1% | Turner | Exit ramp road at I-75 Exit 82 (Ashburn); start of SR-107 route is entirely within interchange ramp geometry |
| `10001000247COINC_3.0473_3.1040` | SR-247 CONN | 72 m | 75.2% | Peach | Cross-road at I-75 interchange near Warner Robins / Perry area; briefly enters buffer, no blue underneath |
| `1000100040800INC_0.0000_0.4006` | I-475 | 268 m | 41.1% | Bibb | I-475 Macon bypass diverges from I-75 at the north Macon split; first 268m is inside I-75 buffer before veering away; GDOT evac route does NOT use I-475 |

### SR 3 — 1 FP

| UID | HWY_NAME | Overlap | Ratio | County | Why FP |
|---|---|---|---|---|---|
| `1000100002200INC_56.1013_58.1779` | US-80 | 702 m | 21.6% | Taylor | US-80 runs E-W, SR 3 (US-19) runs N-S; they cross in Taylor County. 702m of US-80 is within the 30m N-S corridor buffer at/near the crossing. Screenshotted at z15: clear E-W red branch with NO blue underneath. US-80 is not in `_CONCURRENT_DESIGNATION_MAP["SR 3"]`. |

**SR 3 gap_fill segments (20) — all LEGITIMATE.** Segments in Lee Co. (CS-528, CS-529, CS-530, CR-531, CS-536), Schley Co. (CR-149, CR-150, CR-152, CR-153, CR-154, CR-156, CS-552, CS-556), Sumter Co. (CR-448, CR-452), Taylor Co. (CR-365) all have 79–100% overlap ratios and fill genuine state-system gaps.

### SR 26 — 1 FP

| UID | HWY_NAME | Overlap | Ratio | County | Why FP |
|---|---|---|---|---|---|
| `1000100003000INC_203.5772_214.7710` | US-280 | 11,153 m | 59.1% | Effingham | US-280 runs as a separate parallel road to SR 26 (US-25) in Effingham County west of Savannah. At z12 and z14, two distinct parallel lines visible; lower line (SR 26) has blue, upper line (US-280) is red-only. US-280 is not in `_CONCURRENT_DESIGNATION_MAP["SR 26"]` — it IS in SR 30's concurrent map, confirming it's a different corridor. |

**SR 26 concurrent segments reviewed and confirmed legitimate:**
- US-441 / US-319 in Laurens Co. (Dublin): 100% overlap, blue visible throughout; concurrent through downtown Dublin
- SR-56 in Emanuel Co. (Swainsboro): 33.8% ratio, brief shared section through downtown; no diverging branch visible
- US-119 in Bulloch Co.: 76.7% ratio, 228m — concurrent through Statesboro area

### I 75 South — CLEAN

All 16 segments are `hwy_name+spatial` with HWY_NAME = "I-75". Screenshot confirms perfect red+blue alignment from Lowndes County to the Cook County approach.

### Liberty Expy — CLEAN

| Segment | Status | Reason |
|---|---|---|
| US-133 (Dougherty, 5.4km, 56.5%) | Legitimate | Explicitly in `_CONCURRENT_DESIGNATION_MAP["Liberty Expy"]`; blue visible through Albany |
| US-300 (Thomas, 62km, 53.4%) | Legitimate | US-300 / US-19 run concurrent through Thomasville; also serves SR 3 and SR 300 corridors; red+blue aligned throughout |

---

## 3. Ready-to-Paste `_HARDCODE_EXCLUSIONS`

```python
# Hard-code exclusions: segments force-excluded regardless of thresholds.
# Use "" key to exclude globally across all corridors.
# Populated from QC map review 2026-04-16 (Playwright walk of I 75 North,
# I 75 South, SR 3, SR 26, Liberty Expy).
_HARDCODE_EXCLUSIONS: dict[str, list[str]] = {
    "I 75 North": [
        # SR-31 (Lowndes) — 26m overlap at I-75 Exit 16 interchange crossing near
        # Valdosta; bridge approach enters 30m buffer but SR-31 is NOT concurrent
        # with I-75 here. No blue line under segment. concurrent+spatial FP.
        "1000100003100INC_8.8674_8.9000",
        # SR-107 (Turner) — 57m overlap, start of SR-107 exit ramp at I-75 Exit 82
        # (Ashburn). Route begins at the interchange; not part of I-75 evac corridor.
        "1000100010700INC_0.0000_0.0425",
        # SR-247 CONN (Peach) — 72m overlap at I-75 interchange near Warner Robins /
        # Perry. Cross-road briefly enters 30m buffer; no blue line present.
        "10001000247COINC_3.0473_3.1040",
        # I-475 (Bibb) — 268m at Macon split where I-475 bypass diverges from I-75.
        # GDOT I 75 North evac route uses I-75 mainline only; I-475 has no blue line.
        "1000100040800INC_0.0000_0.4006",
    ],
    "SR 3": [
        # US-80 (Taylor) — 702m, 21.6% ratio. US-80 runs E-W and crosses the N-S
        # SR 3 / US-19 corridor in Taylor County. Not concurrent; no blue line on
        # the E-W branch. Passes Tier 2 thresholds because the crossing section
        # extends ~702m inside the 30m corridor buffer, but is off-route.
        "1000100002200INC_56.1013_58.1779",
    ],
    "SR 26": [
        # US-280 (Effingham) — 11km, 59.1% ratio. US-280 runs as a distinct
        # parallel road to SR 26 (US-25) west of Savannah in Effingham County.
        # Two separate road alignments visible at z14; only the lower SR 26 line
        # has blue underneath. US-280 is concurrent with SR 30, not SR 26.
        "1000100003000INC_203.5772_214.7710",
    ],
}
```

---

## 4. False Negatives Found

**None.** Blue line coverage was continuous throughout all 5 corridors. No gaps observed where the official GDOT route had no red coverage. No additions to `_HARDCODE_OVERRIDES` required from this review pass.

---

## 5. Updated QC Baseline

| Metric | Before Commit 3 | After Commit 3 |
|---|---|---|
| Total matched segments | 1,460 | **1,454** |
| I 75 North | 117 | 113 (−4) |
| SR 3 | 126 | 125 (−1) |
| SR 26 | 176 | 175 (−1) |
| I 75 South | 16 | 16 (no change) |
| Liberty Expy | 14 | 14 (no change) |
| hwy_name+spatial | 1,083 | 1,083 |
| concurrent+spatial | 153 | 147 (−6) |

---

## 6. Verification Steps

After applying `_HARDCODE_EXCLUSIONS` to `_evac_corridor_match.py`:

1. **Re-run QC map generator:**
   ```
   C:/Users/adith/AppData/Local/Programs/Python/Python313/python.exe \
     d:/Jacobs/Georgia-Statewide-Data-Pipeline/02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py
   ```

2. **Confirm QC PASSED** — check final log line for `QC PASSED` (no errors, no zero-match corridors).

3. **Confirm new total is 1,454** — QC Summary panel should show `Total evacuation flagged: 1454`.

4. **Confirm no new zero-match corridors** — `corridors_with_zero_matches` should remain empty.

5. **Spot-check in QC map** — reload http://localhost:8091/, filter to I 75 North, zoom to:
   - Turner Co. (31.712, -83.636) — SR-107 segment should be GONE
   - Bibb Co. (32.757, -83.709) — I-475 start should be GONE
   - Filter to SR 26, zoom to Effingham Co. (32.177, -81.393) — US-280 parallel line should be GONE

---

## Hard Constraints Verified

- ✅ **Zero false negatives:** No gaps in blue coverage found; no `_HARDCODE_OVERRIDES` additions needed
- ✅ **No hardcode exclusions:** All 6 excluded segments have `match_method = "concurrent+spatial"` (none are `"hardcode"`)
- ✅ **Gap_fill segments checked:** All 20 SR 3 gap_fill segments have 79–100% overlap ratios; none excluded
- ✅ **Coverage preserved:** Each excluded segment's stretch is covered by the I-75 or SR 3/US-19 mainline segment

---

## Segments Reviewed But Kept

| UID | HWY | Corridor | Reason kept |
|---|---|---|---|
| `1000100000700INC_28.8449_35.8506` | US-41 | I 75 North | 10.7km concurrent run through Lowndes/Echols; blue visible |
| `1000100000700INC_35.8506_36.7938` | US-41 | I 75 North | 605m extension of US-41 concurrent section; blue visible |
| `1000100054000INC_79.5959_96.8183` | US-80 | I 75 North | 24.3km concurrent through Macon; blue visible throughout |
| `1000100030000INC_0.0000_71.4236` | US-300 | SR 3 / Liberty Expy / SR 300 / US 19 | Legitimate concurrent through Thomasville; serves 4 corridors |
| `1000100013300INC_73.3134_79.1759` | US-133 | SR 3 / Liberty Expy | Explicitly in `_CONCURRENT_DESIGNATION_MAP["Liberty Expy"]` |
| `1000100003000INC_18.8303_19.1285` | SR-30 | SR 3 | 100% overlap in Sumter Co.; concurrent |
| All 20 SR 3 gap_fill segs | CS/CR | SR 3 | 79–100% overlap; fill genuine state-system gaps in Lee, Schley, Sumter, Taylor Co. |
| `1000100002900INC_47.1517_50.2678` (×6) | US-441 | SR 26 | Concurrent through Dublin; 35–100% ratios; blue visible |
| `1000100003100INC_147.9481_150.4596` (×7) | US-319 | SR 26 | Concurrent through Dublin; 70–100% ratios; blue visible |
| `1000100005600INC_73.4856_75.3771` (×3) | SR-56 | SR 26 | Concurrent section through Swainsboro; no diverging branch visible |
| `1000100011900INC_51.3475_51.5339` | US-119 | SR 26 | 76.7% overlap in Bulloch/Statesboro; concurrent |
