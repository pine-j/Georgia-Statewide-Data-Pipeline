#!/usr/bin/env python3
"""Generate the Georgia state outline GeoJSON bundled with the frontend.

The outline is a single dissolved, simplified polygon derived from the
cached GDOT county layer. The frontend imports it as a static asset and
renders it as a reference frame when the "Statewide" boundary overlay
is toggled on — there is no backend call for this layer.

Inputs:
    01-Raw-Data/Boundaries/cache/counties.fgb  (produced by download_boundaries.py)

Output:
    04-Webapp/frontend/src/assets/georgia-outline.json

Re-run whenever the counties cache is refreshed. The output file is tracked
in git since it ships with the frontend bundle.

Usage:
    python 01-Raw-Data/Boundaries/scripts/generate_frontend_outline.py
    python 01-Raw-Data/Boundaries/scripts/generate_frontend_outline.py --tolerance 0.005
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import geopandas as gpd

logger = logging.getLogger("generate_frontend_outline")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
COUNTIES_CACHE = REPO_ROOT / "01-Raw-Data" / "Boundaries" / "cache" / "counties.fgb"
OUTPUT_PATH = (
    REPO_ROOT / "04-Webapp" / "frontend" / "src" / "assets" / "georgia-outline.json"
)

# ~0.003 degrees ≈ 300 m — keeps the outline recognizable while holding the
# file to ~30 KB so the frontend bundle cost is negligible.
DEFAULT_SIMPLIFY_TOLERANCE_DEG = 0.003


def generate(tolerance_deg: float) -> None:
    if not COUNTIES_CACHE.exists():
        raise SystemExit(
            f"Counties cache not found at {COUNTIES_CACHE}. Run "
            "download_boundaries.py first."
        )

    gdf = gpd.read_file(COUNTIES_CACHE).to_crs(4326)
    outline = gdf.dissolve()
    outline["geometry"] = outline.geometry.simplify(
        tolerance_deg, preserve_topology=True
    )
    outline = outline[["geometry"]]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()
    outline.to_file(OUTPUT_PATH, driver="GeoJSON")

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    logger.info(
        "wrote %s (%.1f KB, simplify=%.4f deg, %d county inputs)",
        OUTPUT_PATH,
        size_kb,
        tolerance_deg,
        len(gdf),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_SIMPLIFY_TOLERANCE_DEG,
        help=(
            "Douglas-Peucker simplification tolerance in degrees. Larger = "
            "smaller file, coarser outline. Default: %(default)s"
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    generate(args.tolerance)
    return 0


if __name__ == "__main__":
    sys.exit(main())
