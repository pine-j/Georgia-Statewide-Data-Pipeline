"""Validate the connectivity GeoPackage and database.

Checks:
    - Layer counts and feature counts
    - Geometry validity per layer
    - CRS verification (EPSG:32617)
    - Known-feature spot checks (Hartsfield-Jackson, Port of Savannah, etc.)
"""

import logging
import sys
from pathlib import Path

import geopandas as gpd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
GPKG_PATH = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "connectivity.gpkg"
CLEAN_DIR = PROJECT_ROOT / "02-Data-Staging" / "tables" / "connectivity"

EXPECTED_LAYERS = [
    "priority_routes",
    "nevi_corridors",
    "alt_fuel_stations",
    "airports",
    "seaports",
    "universities",
    "military_bases",
    "national_parks",
    "rail_facilities",
    "freight_generators",
]

# Spot-check features: (layer, column_substring_to_search, expected_substring)
SPOT_CHECKS = [
    ("airports", None, "Hartsfield"),
    ("airports", None, "Savannah"),
    ("seaports", None, "Savannah"),
    ("national_parks", None, "Okefenokee"),
    ("universities", None, "Georgia"),
    ("military_bases", None, "Fort"),
]


def _check_layer(layer_name: str, errors: list[str]) -> None:
    """Validate a single GeoPackage layer."""
    src = CLEAN_DIR / f"{layer_name}.geojson"
    if not src.exists():
        log.warning("  [SKIP] %s - staged source file not found.", layer_name)
        return

    gdf = gpd.read_file(src)
    count = len(gdf)
    log.info("  %-25s  features=%d", layer_name, count)

    if count == 0:
        errors.append(f"{layer_name}: zero features")

    # CRS check
    if gdf.crs is None:
        errors.append(f"{layer_name}: CRS is None")
    elif gdf.crs.to_epsg() != 32617:
        errors.append(f"{layer_name}: CRS is {gdf.crs} (expected EPSG:32617)")

    # Geometry validity
    if "geometry" in gdf.columns and not gdf.geometry.is_empty.all():
        invalid = (~gdf.geometry.is_valid).sum()
        if invalid > 0:
            errors.append(f"{layer_name}: {invalid} invalid geometries")
            log.warning("    %d invalid geometries", invalid)


def _spot_check(errors: list[str]) -> None:
    """Run spot checks for known features."""
    log.info("Spot checks ...")
    for layer, _col, substring in SPOT_CHECKS:
        src = CLEAN_DIR / f"{layer}.geojson"
        if not src.exists():
            log.warning("  [SKIP] %s – not found.", layer)
            continue

        gdf = gpd.read_file(src)
        # Search all string columns for the substring
        found = False
        for col in gdf.select_dtypes(include="object").columns:
            if gdf[col].astype(str).str.contains(substring, case=False, na=False).any():
                found = True
                break

        status = "PASS" if found else "FAIL"
        log.info("  [%s] '%s' in %s", status, substring, layer)
        if not found:
            errors.append(f"Spot check failed: '{substring}' not found in {layer}")


def _check_gpkg(errors: list[str]) -> None:
    """Validate the GeoPackage itself, if it exists."""
    if not GPKG_PATH.exists():
        log.warning("GeoPackage not found at %s – skipping GPKG layer checks.", GPKG_PATH)
        return

    import fiona
    layers = fiona.listlayers(str(GPKG_PATH))
    log.info("GeoPackage layers present: %s", layers)

    missing = set(EXPECTED_LAYERS) - set(layers)
    if missing:
        errors.append(f"GPKG missing layers: {missing}")

    for lyr in layers:
        gdf = gpd.read_file(str(GPKG_PATH), layer=lyr)
        if gdf.crs is None or gdf.crs.to_epsg() != 32617:
            errors.append(f"GPKG layer '{lyr}': CRS is {gdf.crs} (expected EPSG:32617)")


def main() -> int:
    """Run all validation checks."""
    errors: list[str] = []

    log.info("=== Connectivity Validation ===")
    log.info("Checking staged connectivity sources ...")
    for layer in EXPECTED_LAYERS:
        _check_layer(layer, errors)

    _spot_check(errors)
    _check_gpkg(errors)

    if errors:
        log.error("Validation completed with %d error(s):", len(errors))
        for e in errors:
            log.error("  - %s", e)
        return 1

    log.info("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
