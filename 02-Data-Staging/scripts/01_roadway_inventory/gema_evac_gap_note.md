# Gap 4: GEMA Evacuation Routes — Data Availability Assessment

**Date**: 2026-04-23
**Finding**: No broader GEMA evacuation route GIS data is publicly available.

## Investigation

GEMA/HS operates an ArcGIS Hub (hub-gema-soc.opendata.arcgis.com) but it
contains only disaster declarations, situational dashboards, and hurricane
evacuation zone lookups — no downloadable evacuation route polyline layers.

Their Mapping and GIS page describes internal GIS capabilities but publishes
no route datasets. The hurricane evacuation page links to an interactive
zone finder and defers route information to "Georgia Navigator" (GDOT's ITS
system, not a downloadable GIS source).

For nuclear EPZ routes (Vogtle, Hatch), evacuation routes exist only as text
directions in annual emergency brochures from Southern Nuclear — no GIS
layers are publicly available.

## Resolution

`SEC_EVAC` (GDOT EOC hurricane evacuation routes, layers 7-8, 268 + 12
features) is used as the best available public proxy for "GEMA Evacuation
Routes" in SRP derivation. The nuclear EPZ gap is independently addressed
by the IS_NUCLEAR_EPZ_ROUTE field (10-mile buffer approach).

Obtaining additional GEMA route data would require a direct data-sharing
request to GEMA/HS.
