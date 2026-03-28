"""Create a GeoPackage from cleaned connectivity datasets.

Reads cleaned GeoJSON from 02-Data-Staging/cleaned/connectivity/ and writes
all layers into 02-Data-Staging/spatial/connectivity.gpkg in EPSG:32617.
"""

import logging
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
CLEAN_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned" / "connectivity"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
GPKG_PATH = SPATIAL_DIR / "connectivity.gpkg"
TARGET_CRS = "EPSG:32617"

# Layer mapping: cleaned file stem -> GeoPackage layer name
LAYER_MAP = {
    "priority_routes": "priority_routes",
    "nevi_corridors": "nevi_corridors",
    "alt_fuel_stations": "alt_fuel_stations",
    "airports": "airports",
    "seaports": "seaports",
    "universities": "universities",
    "military_bases": "military_bases",
    "national_parks": "national_parks",
    "rail_facilities": "rail_facilities",
    "freight_generators": "freight_generators",
}


def main() -> None:
    """Build connectivity.gpkg with one layer per dataset."""
    SPATIAL_DIR.mkdir(parents=True, exist_ok=True)

    if GPKG_PATH.exists():
        GPKG_PATH.unlink()
        log.info("Removed existing %s", GPKG_PATH.name)

    log.info("Creating %s ...", GPKG_PATH)

    for stem, layer_name in LAYER_MAP.items():
        src = CLEAN_DIR / f"{stem}.geojson"
        if not src.exists():
            log.warning("  %s not found – skipping layer '%s'.", src.name, layer_name)
            continue

        gdf = gpd.read_file(src)
        if gdf.crs is None or gdf.crs.to_epsg() != 32617:
            gdf = gdf.to_crs(TARGET_CRS)

        gdf.to_file(str(GPKG_PATH), layer=layer_name, driver="GPKG")
        log.info(
            "  Layer %-25s  %d features  geom=%s",
            layer_name,
            len(gdf),
            gdf.geom_type.unique().tolist(),
        )

    log.info("GeoPackage complete: %s", GPKG_PATH)


if __name__ == "__main__":
    main()
