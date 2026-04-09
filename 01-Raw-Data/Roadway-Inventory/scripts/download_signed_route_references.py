"""Download official GDOT signed-route verification reference layers.

This stages the signed-route verification inputs used by `route_verification.py`:

- `Interstates`
- `US Highway`
- `State Routes`

Outputs are written under:
`01-Raw-Data/Roadway-Inventory/GDOT_GPAS/signed_route_references/`
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

from route_verification import REFERENCE_CONFIG, fetch_reference_layer

LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    for reference_key in REFERENCE_CONFIG:
        gdf = fetch_reference_layer(reference_key, refresh=True)
        LOGGER.info(
            "Downloaded signed-route reference %s: %d features",
            reference_key,
            len(gdf),
        )


if __name__ == "__main__":
    main()
