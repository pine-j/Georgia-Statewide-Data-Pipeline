"""Catalog all layers and columns in the Georgia Roadway Inventory GDB.

Loads the geodatabase with geopandas/pyogrio, enumerates every layer
and its columns with data types, and writes the result to
02-Data-Staging/config/gdb_column_inventory.json.
"""

import json
import logging
from pathlib import Path

import geopandas as gpd
import pyogrio

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "GA_RDWY_INV"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"


def find_gdb(raw_dir: Path) -> Path:
    """Locate the .gdb directory under the raw data folder."""
    gdb_dirs = list(raw_dir.rglob("*.gdb"))
    if not gdb_dirs:
        raise FileNotFoundError(
            f"No .gdb directory found under {raw_dir}. Run download.py first."
        )
    return gdb_dirs[0]


def catalog_layers(gdb_path: Path) -> dict:
    """Read all layers and return a dict of layer -> column info."""
    layers = pyogrio.list_layers(gdb_path)
    logger.info("Found %d layers in %s", len(layers), gdb_path.name)

    inventory = {}

    for layer_name, geometry_type in layers:
        logger.info("  Cataloging layer: %s (%s)", layer_name, geometry_type)

        try:
            gdf = gpd.read_file(
                gdb_path,
                layer=layer_name,
                engine="pyogrio",
                rows=10,  # Only need schema, not full data
            )
        except Exception as exc:
            logger.warning("    Could not read layer %s: %s", layer_name, exc)
            inventory[layer_name] = {
                "geometry_type": geometry_type,
                "error": str(exc),
            }
            continue

        columns = {}
        for col in gdf.columns:
            dtype_str = str(gdf[col].dtype)
            columns[col] = {
                "dtype": dtype_str,
                "sample_values": [
                    str(v) for v in gdf[col].dropna().head(3).tolist()
                ],
            }

        inventory[layer_name] = {
            "geometry_type": geometry_type,
            "num_columns": len(gdf.columns),
            "columns": columns,
        }

    return inventory


def main() -> None:
    """Run column cataloging workflow."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    gdb_path = find_gdb(RAW_DIR)
    logger.info("Using GDB: %s", gdb_path)

    inventory = catalog_layers(gdb_path)

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    output_path = CONFIG_DIR / "gdb_column_inventory.json"
    output_path.write_text(json.dumps(inventory, indent=2, default=str))

    total_cols = sum(
        layer.get("num_columns", 0) for layer in inventory.values()
    )
    logger.info(
        "Cataloged %d layers, %d total columns -> %s",
        len(inventory),
        total_cols,
        output_path,
    )


if __name__ == "__main__":
    main()
