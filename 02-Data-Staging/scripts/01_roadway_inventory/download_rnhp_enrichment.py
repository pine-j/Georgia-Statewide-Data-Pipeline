"""Download RNHP enrichment layers for Georgia roadway ETL.

Outputs are written under:
`01-Raw-Data/GA_RDWY_INV/GDOT_GPAS/rnhp_enrichment/`
"""

from __future__ import annotations

import logging

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
