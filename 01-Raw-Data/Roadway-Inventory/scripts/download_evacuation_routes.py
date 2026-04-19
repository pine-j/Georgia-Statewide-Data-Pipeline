"""Download GDOT hurricane evacuation + contraflow route layers.

Staged to `01-Raw-Data/Roadway-Inventory/GDOT_EOC/`:

- `ga_evac_routes.geojson`     - Layer 7 (~268 polylines)
- `ga_contraflow_routes.geojson` - Layer 8 (~12 polylines)

Consumed by `02-Data-Staging/scripts/01_roadway_inventory/evacuation_enrichment.py`.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_GDOT_EOC_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "GDOT_EOC"

EOC_SERVICE_URL = (
    "https://rnhp.dot.ga.gov/hosting/rest/services/EOC/EOC_RESPONSE_LAYERS/MapServer"
)

LAYERS = [
    {
        "layer_id": 7,
        "name": "ga_evac_routes",
        "description": "GDOT Hurricane Evacuation Routes",
        "output": RAW_GDOT_EOC_DIR / "ga_evac_routes.geojson",
    },
    {
        "layer_id": 8,
        "name": "ga_contraflow_routes",
        "description": "GDOT Hurricane Evacuation Routes - Contraflow",
        "output": RAW_GDOT_EOC_DIR / "ga_contraflow_routes.geojson",
    },
]

USER_AGENT = "Georgia-Statewide-Data-Pipeline EOC downloader"
TIMEOUT = 120
MAX_RETRIES = 4
RETRY_BASE_DELAY_SECONDS = 2.0


def _download_with_retry(url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=TIMEOUT) as response:
                return response.read()
        except (URLError, TimeoutError, ConnectionResetError) as exc:
            last_error = exc
            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning("transport error on attempt %d/%d: %s (sleep %.1fs)", attempt, MAX_RETRIES, exc, delay)
            time.sleep(delay)
    raise RuntimeError(f"All {MAX_RETRIES} retries exhausted. Last error: {last_error}")


def download_layer(layer_id: int, output: Path) -> int:
    query_url = (
        f"{EOC_SERVICE_URL}/{layer_id}/query?where=1%3D1&outFields=*"
        f"&f=geojson&returnGeometry=true"
    )
    LOGGER.info("Downloading evacuation layer %d: %s", layer_id, query_url)
    payload = _download_with_retry(query_url)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(payload)
    feature_count = len(json.loads(payload).get("features", []))
    LOGGER.info("Wrote %d features to %s", feature_count, output)
    return feature_count


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    results: list[dict] = []
    for layer in LAYERS:
        feature_count = download_layer(layer["layer_id"], layer["output"])
        results.append(
            {
                "layer_id": layer["layer_id"],
                "name": layer["name"],
                "description": layer["description"],
                "output": str(layer["output"].relative_to(PROJECT_ROOT)),
                "feature_count": feature_count,
                "file_bytes": layer["output"].stat().st_size,
            }
        )

    metadata_path = RAW_GDOT_EOC_DIR / "download_metadata.json"
    metadata = {
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_service": EOC_SERVICE_URL,
        "layers": results,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    LOGGER.info("Wrote download metadata to %s", metadata_path)


if __name__ == "__main__":
    sys.exit(main() or 0)
