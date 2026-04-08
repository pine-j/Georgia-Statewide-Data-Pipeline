# Assessment And Options

## Roadway Gap Fill

- [2026-04-03 Roadway Gap-Fill Exploratory Report](./2026-04-03-roadway-gap-fill-exploratory-report.md)
  - Includes a `2026-04-04` follow-up note from Playwright visual inspection indicating no obvious planning-relevant roadway gaps in the sampled Columbus, Atlanta, and Savannah views after full layer load.
- [2026-04-07 Georgia Route-Family Classification Strategy](./2026-04-07-georgia-route-family-classification-strategy.md)
- [2026-04-07 Georgia Signed-Route Verification Strategy](./2026-04-07-georgia-signed-route-verification-strategy.md)
- [Roadway Gap-Fill Options](./roadway-gap-fill-options.md)
- [Roadway Supplement Options](./roadway-supplement-options.md)
- [Roadway Gap-Fill Options CSV](./roadway-gap-fill-options.csv)

## Notes

- These files document exploratory analysis only.
- They are intended to preserve dataset screening, local verification, and merge-design decisions before production changes are made.

## Utilities

- [gdot_statewide_roads_probe.py](../../02-Data-Staging/scripts/01_roadway_inventory/gdot_statewide_roads_probe.py)
  - Exploratory probe and tiled extractor for the GDOT live `ARCWEBSVCMAP / Statewide Roads` layer.
  - Saves metadata, probe responses, checkpoints, and tile outputs under `01-Raw-Data/GDOT_Statewide_Roads_Live/`.
  - Not production-ready merge input: tiled outputs can duplicate features across tile boundaries.
- [analyze_aoi_roadway_coverage.py](../../02-Data-Staging/scripts/01_roadway_inventory/analyze_aoi_roadway_coverage.py)
  - Clips the staged `base_network.gpkg` to one or more AOIs and writes review artifacts under `.tmp/roadway_gap_fill/aoi_roadway_reviews/`.
- [aoi_samples.example.csv](../../02-Data-Staging/scripts/01_roadway_inventory/aoi_samples.example.csv)
  - Starter AOIs for the Columbus, Atlanta, and Savannah exploratory checks.
