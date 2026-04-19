"""Download FHWA HPMS 2024 Georgia tabular submission.

Fetches the HPMS_FULL_GA_2024 FeatureServer hosted by USDOT and writes the
attributes-only snapshot that `hpms_enrichment.py` expects:

    01-Raw-Data/Roadway-Inventory/FHWA_HPMS/2024/hpms_ga_2024_tabular.json

File format is ESRI JSON:  ``{"features": [{"attributes": {...}}, ...]}``.
Geometry is intentionally omitted — the enrichment joins by ROUTE_ID +
milepoint interval, not spatially, so a geometry-less pull keeps the file
manageable (~a few hundred MB instead of >1 GB).
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_BASE_DELAY_SECONDS = 2.0

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TARGET_PATH = (
    PROJECT_ROOT
    / "01-Raw-Data"
    / "Roadway-Inventory"
    / "FHWA_HPMS"
    / "2024"
    / "hpms_ga_2024_tabular.json"
)
METADATA_PATH = TARGET_PATH.parent / "download_metadata.json"

SERVICE_URL = (
    "https://geo.dot.gov/server/rest/services/Hosted/HPMS_FULL_GA_2024/FeatureServer/0"
)
BATCH_SIZE = 2000
USER_AGENT = "Georgia-Statewide-Data-Pipeline HPMS downloader"
TIMEOUT = 180


def _build_request(query_url: str, params: dict) -> Request:
    query = urlencode(params, doseq=True)
    full_url = f"{query_url}?{query}"
    if len(full_url) > 2000:
        return Request(
            query_url,
            data=query.encode("utf-8"),
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
    return Request(full_url, headers={"User-Agent": USER_AGENT})


def _get_json(query_url: str, params: dict) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            request = _build_request(query_url, params)
            with urlopen(request, timeout=TIMEOUT) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, ConnectionResetError) as exc:
            last_error = exc
            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning("transport error on attempt %d/%d: %s (sleep %.1fs)", attempt, MAX_RETRIES, exc, delay)
            time.sleep(delay)
            continue

        error = payload.get("error")
        if error:
            code = error.get("code") if isinstance(error, dict) else None
            # 4xx = client error, don't retry. 5xx or unspecified = server issue, retry.
            if code and 400 <= int(code) < 500:
                raise RuntimeError(error)
            last_error = RuntimeError(error)
            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning("server error on attempt %d/%d: %s (sleep %.1fs)", attempt, MAX_RETRIES, error, delay)
            time.sleep(delay)
            continue

        return payload

    raise RuntimeError(f"All {MAX_RETRIES} retries exhausted. Last error: {last_error}")


def fetch_object_ids() -> list[int]:
    query_url = f"{SERVICE_URL}/query"
    payload = _get_json(
        query_url,
        {"f": "json", "where": "1=1", "returnIdsOnly": "true"},
    )
    object_ids = payload.get("objectIds") or []
    return sorted(int(object_id) for object_id in object_ids)


def fetch_features(object_ids: list[int]) -> list[dict]:
    query_url = f"{SERVICE_URL}/query"
    features: list[dict] = []
    total_batches = (len(object_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_index, start in enumerate(range(0, len(object_ids), BATCH_SIZE), start=1):
        batch = object_ids[start : start + BATCH_SIZE]
        payload = _get_json(
            query_url,
            {
                "f": "json",
                "where": "1=1",
                "objectIds": ",".join(str(object_id) for object_id in batch),
                "outFields": "*",
                "returnGeometry": "false",
            },
        )
        batch_features = payload.get("features") or []
        features.extend(batch_features)
        LOGGER.info(
            "batch %d/%d: %d features (cumulative %d)",
            batch_index,
            total_batches,
            len(batch_features),
            len(features),
        )
    return features


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Fetching HPMS object ids from %s", SERVICE_URL)
    object_ids = fetch_object_ids()
    LOGGER.info("Found %d HPMS records", len(object_ids))

    features = fetch_features(object_ids)
    if len(features) != len(object_ids):
        LOGGER.warning(
            "Feature count %d does not match object id count %d",
            len(features),
            len(object_ids),
        )

    payload = {"features": features}
    TARGET_PATH.write_text(json.dumps(payload), encoding="utf-8")
    LOGGER.info("Wrote %d features to %s", len(features), TARGET_PATH)

    metadata = {
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_url": SERVICE_URL,
        "feature_count": len(features),
        "file_bytes": TARGET_PATH.stat().st_size,
        "file_path": str(TARGET_PATH.relative_to(PROJECT_ROOT)),
        "note": "Attributes-only (no geometry) — hpms_enrichment.py joins by ROUTE_ID + milepoint.",
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    LOGGER.info("Wrote download metadata to %s", METADATA_PATH)


if __name__ == "__main__":
    sys.exit(main() or 0)
