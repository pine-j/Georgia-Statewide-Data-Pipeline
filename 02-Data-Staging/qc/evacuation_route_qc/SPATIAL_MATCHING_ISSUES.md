# Evacuation Route Spatial Matching — Known Issues & Investigation Plan

## Context

We are building a Georgia statewide roadway data pipeline that enriches ~245,000 roadway segments with hurricane evacuation route flags (`SEC_EVAC`, `SEC_EVAC_CONTRAFLOW`). The enrichment script spatially overlays GDOT's official evacuation route polylines against our staged roadway network and flags segments that overlap.

A QC web map was built to visually verify the results. The QC map revealed that **both the evacuation and contraflow matching are producing false positives** — segments are being flagged that are geographically far from the official routes.

## Data Sources

### Official GDOT Layers (what we're matching against)
- **Evacuation Routes** (Layer 7): 268 polyline features from `rnhp.dot.ga.gov/hosting/rest/services/EOC/EOC_RESPONSE_LAYERS/MapServer/7`. Each is a LineString with a `ROUTE_NAME` field (e.g., "SR 17", "I 75 North"). These are signed highway corridors directing traffic inland during hurricanes.
- **Contraflow Routes** (Layer 8): 12 polyline features from the same service, layer 8. Named with phonetic codes (Adam through Mary), each "connecting to" a specific I-16 exit. These represent the I-16 contraflow operations plan.

### Staged Roadway Network (what we're flagging)
- ~245,000 segments in `02-Data-Staging/spatial/base_network.gpkg` layer `roadway_segments`
- CRS: EPSG:32617 (UTM Zone 17N)
- Segments have `unique_id`, `HWY_NAME`, `ROUTE_FAMILY`, `AADT`, geometry, etc.

### Downloaded GeoJSON (cached locally)
- `02-Data-Staging/spatial/ga_evac_routes.geojson` (268 features, ~57 MB)
- `02-Data-Staging/spatial/ga_contraflow_routes.geojson` (12 features, ~42 KB)

## Current Matching Approach

File: `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py`

1. Load evacuation/contraflow polylines, reproject to EPSG:32617
2. Buffer the polylines by 30m to create corridor polygons (`ROUTE_BUFFER_M = 30.0`)
3. `gpd.sjoin(segments, buffered_corridors, predicate="intersects")` to find candidates
4. For each candidate, measure `segment.intersection(buffered_corridor).length`
5. Keep matches where overlap >= 200m (`MIN_OVERLAP_M = 200.0`)
6. Flag those segments as `SEC_EVAC = True`

The 30m buffer is necessary because the evacuation routes and roadway segments are digitized from different sources — without it, polyline-to-polyline intersection returns zero-length point contacts, not measurable overlap.

## Issue 1: Contraflow Routes Are Closed Loops, Not I-16 Segments

### What we expected
12 short segments of I-16 between exit pairs (Exit 12 through Exit 167A), covering the ~150-mile Macon-to-Savannah contraflow corridor.

### What the data actually contains
12 **closed-loop routes** where start point = end point. Each loop is 35-50 km long and traces a path that goes out from I-16 along local/US roads and returns to the same exit point. These are the contraflow **approach/feeder routes**, not the I-16 mainline itself.

Evidence:
```
George: start=(32.3745, -82.0727) end=(32.3745, -82.0727)  # same point!
  Length: 47.0 km, 45 vertices
Frank:  start=(32.4033, -82.3116) end=(32.4033, -82.3116)   # same point!
  Length: 72.6 km, 99 vertices
```

### Impact
228 segments flagged as contraflow, including 22 US Routes, 7 Local/Other, and 1 State Route spread across counties far from I-16. The closed-loop geometry passes through these areas, so the spatial match is technically correct but semantically misleading — users expect "contraflow" to mean I-16 only.

## Issue 2: Oversized Roadway Segments Cause False Positives

### What we found
Some staged roadway segments are absurdly long — a single US-1 segment spans lat 30.777 to 33.475 (~300 km, nearly the full height of Georgia). When a 300 km segment intersects a 30m-wide corridor, it produces hundreds of meters of "overlap" even though only a tiny fraction of the segment is near the evacuation route.

Evidence:
```
US-1 in Emanuel County:
  Segment bounds: lon -82.464 to -81.979, lat 30.777 to 33.475
  Reported overlap: 608m
  Actual distance to nearest contraflow route: 0-2m (it physically crosses the loop)
  But the segment is 300 km long — the crossing is a tiny fraction
```

### Impact
These mega-segments inflate match counts for both evacuation and contraflow. A segment that's 99% non-evacuation but clips a corridor for 300m gets fully flagged.

## Issue 3: Evacuation Route Matching (Likely Similar Problems)

The evacuation routes (268 features) showed 1,345 flagged segments. While less obviously wrong than contraflow, the same two problems likely apply:
- Some evacuation route geometries may have unexpected shapes or extents
- Oversized roadway segments crossing evacuation corridors get fully flagged even when only a tiny fraction overlaps

The earlier analysis showed:
- 105 Local/Other segments flagged (should these be?)
- 832 US Route segments flagged (plausible but unverified visually)

## QC Web Map

A Leaflet-based QC map is available for visual inspection:

**Location**: `02-Data-Staging/qc/evacuation_route_qc/index.html`
**Serve with**: `npx http-server -p 8090 -c-1 --cors` from that directory
**URL**: `http://localhost:8090`

### Layers (togglable via layer control, top-right):
| Layer | Color | Description |
|-------|-------|-------------|
| Road Network (context) | Gray, thin | 5,000 sampled non-flagged segments for geographic reference |
| GDOT Evacuation Routes (official) | Blue, weight 4 | The raw 268 GDOT evacuation polylines |
| GDOT Contraflow Routes (official) | Purple, weight 5 | The raw 12 GDOT contraflow loop polylines |
| Flagged Segments (evacuation) | Red, weight 3 | 1,345 roadway segments flagged SEC_EVAC |
| Flagged Segments (contraflow) | Orange, weight 4 | 228 roadway segments flagged SEC_EVAC_CONTRAFLOW |

Click any feature for a popup with attributes (HWY_NAME, ROUTE_FAMILY, overlap_m, etc.).

## Key Files

| File | Purpose |
|------|---------|
| `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py` | Production enrichment script (wired into normalize.py) |
| `02-Data-Staging/qc/evacuation_route_qc/generate_qc_map.py` | QC map data generator |
| `02-Data-Staging/qc/evacuation_route_qc/index.html` | QC map viewer |
| `02-Data-Staging/spatial/base_network.gpkg` | Staged roadway network (layer: roadway_segments) |
| `02-Data-Staging/spatial/ga_evac_routes.geojson` | Cached GDOT evacuation routes |
| `02-Data-Staging/spatial/ga_contraflow_routes.geojson` | Cached GDOT contraflow routes |
| `00-Project-Management/Pipeline-Documentation/data_dictionary.csv` | Data dictionary (SEC_EVAC entries at bottom) |
| `00-Project-Management/Pipeline-Documentation/phase-1-roadway-data-pipeline.md` | Pipeline documentation |

## Questions to Answer

1. **Should we use overlap ratio instead of absolute overlap?** A 300 km segment with 300m overlap has 0.1% overlap — should that be flagged? A ratio-based threshold (e.g., >= 50% of segment length within corridor) might be more meaningful.

2. **Should we filter contraflow to Interstate-only?** Since the contraflow loops include feeder roads, should `SEC_EVAC_CONTRAFLOW` only flag Interstate segments that fall within the corridor?

3. **Should we cap segment length?** Segments longer than X km are likely unsegmented base routes that will be split in a future pipeline run. Should we skip them or handle them differently?

4. **Are the evacuation route geometries trustworthy?** Do the 268 evacuation polylines actually follow the roads they're supposed to represent, or do they have similar geometric surprises?

5. **Is the 30m buffer too wide or too narrow?** The buffer converts polylines to corridors for measurable intersection. 30m was chosen as a reasonable positional offset between independently digitized datasets. Should it be tighter?

## Investigation Steps

1. Visually inspect the QC map at `http://localhost:8090` — toggle layers to compare official routes vs flagged segments
2. Zoom into specific false-positive areas to understand why segments are being flagged
3. Analyze the distribution of segment lengths in the flagged results
4. Check if evacuation routes (not just contraflow) have similar closed-loop or unexpected geometry issues
5. Prototype alternative matching strategies (ratio-based, length-capped, route-family-filtered)
6. Regenerate QC map with the improved approach and verify visually
