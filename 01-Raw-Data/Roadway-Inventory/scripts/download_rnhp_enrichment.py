"""Download RNHP enrichment layers for Georgia roadway ETL.

Outputs are written under:
`01-Raw-Data/Roadway-Inventory/GDOT_GPAS/rnhp_enrichment/`
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
STAGING_SCRIPT_DIR = (
    PROJECT_ROOT / "02-Data-Staging" / "scripts" / "01_roadway_inventory"
)
if str(STAGING_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(STAGING_SCRIPT_DIR))

from rnhp_enrichment import LAYER_CONFIG, fetch_enrichment_layer

LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    for layer_key in LAYER_CONFIG:
        gdf = fetch_enrichment_layer(layer_key, refresh=True)
        LOGGER.info(
            "Downloaded enrichment layer %s: %d features",
            layer_key,
            len(gdf),
        )


if __name__ == "__main__":
    main()
