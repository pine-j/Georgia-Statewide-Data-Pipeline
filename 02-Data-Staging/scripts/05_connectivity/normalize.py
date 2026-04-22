"""Normalize downloaded connectivity datasets for Georgia.

Reads raw GeoJSON files from 01-Raw-Data/connectivity/, reprojects to
EPSG:32617 (UTM 17N), standardizes column names, and writes normalized outputs
to 02-Data-Staging/tables/connectivity/.
"""

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "connectivity"
CLEAN_DIR = PROJECT_ROOT / "02-Data-Staging" / "tables" / "connectivity"
TARGET_CRS = "EPSG:32617"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_geojson(path: Path) -> gpd.GeoDataFrame:
    """Load a GeoJSON file and reproject to the target CRS."""
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    gdf = gdf.to_crs(TARGET_CRS)
    return gdf


def _standardize_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Lowercase and strip whitespace from column names."""
    gdf.columns = [c.strip().lower().replace(" ", "_") for c in gdf.columns]
    return gdf


def _save_clean(gdf: gpd.GeoDataFrame, name: str) -> None:
    """Write a cleaned GeoDataFrame to the staging directory."""
    out_path = CLEAN_DIR / f"{name}.geojson"
    gdf.to_file(out_path, driver="GeoJSON")
    log.info("  Saved %d features -> %s", len(gdf), out_path.name)


# ---------------------------------------------------------------------------
# Per-dataset normalization
# ---------------------------------------------------------------------------

def normalize_srp_priority_routes() -> None:
    """Merge and clean SRP layers 13-16."""
    log.info("Normalizing SRP Priority Routes ...")
    srp_dir = RAW_DIR / "srp_priority_routes"
    if not srp_dir.exists():
        log.warning("  SRP directory not found – skipping.")
        return

    frames = []
    priority_map = {
        "srp_critical": "Critical",
        "srp_high": "High",
        "srp_medium": "Medium",
        "srp_low": "Low",
    }
    for stem, label in priority_map.items():
        path = srp_dir / f"{stem}.geojson"
        if not path.exists():
            log.warning("  Missing %s – skipping.", path.name)
            continue
        gdf = _load_geojson(path)
        gdf = _standardize_columns(gdf)
        gdf["priority_level"] = label
        frames.append(gdf)

    if not frames:
        return

    merged = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=TARGET_CRS)
    _save_clean(merged, "priority_routes")


def normalize_nevi_corridors() -> None:
    """Clean NEVI corridor data."""
    log.info("Normalizing NEVI Corridors ...")
    path = RAW_DIR / "nevi_corridors.geojson"
    if not path.exists():
        log.warning("  File not found – skipping.")
        return
    gdf = _load_geojson(path)
    gdf = _standardize_columns(gdf)
    _save_clean(gdf, "nevi_corridors")


def normalize_alt_fuel_stations() -> None:
    """Clean AFDC alternative fueling station data."""
    log.info("Normalizing Alt-Fuel Stations ...")
    path = RAW_DIR / "alt_fuel_stations.geojson"
    if not path.exists():
        log.warning("  File not found – skipping.")
        return
    gdf = _load_geojson(path)
    gdf = _standardize_columns(gdf)

    # Keep a useful subset of columns
    keep_cols = [
        c for c in gdf.columns
        if c in (
            "station_name", "street_address", "city", "state", "zip",
            "fuel_type_code", "status_code", "ev_network", "ev_level2_evse_num",
            "ev_dc_fast_num", "geometry",
        )
    ]
    if keep_cols:
        gdf = gdf[keep_cols]

    _save_clean(gdf, "alt_fuel_stations")


def normalize_generators() -> None:
    """Clean each traffic generator dataset."""
    log.info("Normalizing Traffic Generators ...")
    gen_dir = RAW_DIR / "generators"
    if not gen_dir.exists():
        log.warning("  Generators directory not found – skipping.")
        return

    for path in sorted(gen_dir.glob("*.geojson")):
        log.info("  %s ...", path.stem)
        gdf = _load_geojson(path)
        gdf = _standardize_columns(gdf)

        # Filter to Georgia for datasets that may include other states
        for col in ("state", "state_post", "state_terr"):
            if col in gdf.columns:
                gdf = gdf[
                    gdf[col].str.upper().isin(["GA", "GEORGIA"])
                ].copy()
                break

        _save_clean(gdf, path.stem)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run all normalization steps."""
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    normalize_srp_priority_routes()
    normalize_nevi_corridors()
    normalize_alt_fuel_stations()
    normalize_generators()

    log.info("All connectivity normalization complete.")


if __name__ == "__main__":
    main()
