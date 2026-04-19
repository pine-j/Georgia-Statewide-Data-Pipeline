"""Download FHWA HPMS Georgia tabular submission for a given year.

Fetches the specified FeatureServer hosted by USDOT and writes the
attributes-only snapshot that the enrichment script expects:

    01-Raw-Data/Roadway-Inventory/FHWA_HPMS/<year>/hpms_ga_<year>_tabular.json

File format is ESRI JSON:  ``{"features": [{"attributes": {...}}, ...]}``.
Geometry is intentionally omitted — the enrichment joins by ROUTE_ID +
milepoint interval, not spatially, so a geometry-less pull keeps the file
manageable.

Default service name (when --service-name is omitted) is HPMS_FULL_GA_<year>.
"""

from __future__ import annotations

import argparse
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
BASE_URL = "https://geo.dot.gov/server/rest/services/Hosted/{service_name}/FeatureServer/0"
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
            if code and 400 <= int(code) < 500:
                raise RuntimeError(error)
            last_error = RuntimeError(error)
            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning("server error on attempt %d/%d: %s (sleep %.1fs)", attempt, MAX_RETRIES, error, delay)
            time.sleep(delay)
            continue

        return payload

    raise RuntimeError(f"All {MAX_RETRIES} retries exhausted. Last error: {last_error}")


def fetch_object_ids(service_url: str) -> list[int]:
    query_url = f"{service_url}/query"
    payload = _get_json(
        query_url,
        {"f": "json", "where": "1=1", "returnIdsOnly": "true"},
    )
    object_ids = payload.get("objectIds") or []
    return sorted(int(object_id) for object_id in object_ids)


def fetch_features(service_url: str, object_ids: list[int]) -> list[dict]:
    query_url = f"{service_url}/query"
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
    parser = argparse.ArgumentParser(description="Download FHWA HPMS Georgia tabular data for a given year.")
    parser.add_argument("--year", type=int, required=True, help="HPMS year to download (e.g. 2022).")
    parser.add_argument(
        "--service-name",
        default=None,
        help="USDOT FeatureServer service name. Defaults to HPMS_FULL_GA_<year>.",
    )
    args = parser.parse_args()

    year = args.year
    service_name = args.service_name or f"HPMS_FULL_GA_{year}"
    service_url = BASE_URL.format(service_name=service_name)

    target_path = (
        PROJECT_ROOT
        / "01-Raw-Data"
        / "Roadway-Inventory"
        / "FHWA_HPMS"
        / str(year)
        / f"hpms_ga_{year}_tabular.json"
    )
    metadata_path = target_path.parent / "download_metadata.json"

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Fetching HPMS object ids from %s", service_url)
    object_ids = fetch_object_ids(service_url)
    LOGGER.info("Found %d HPMS records", len(object_ids))

    features = fetch_features(service_url, object_ids)
    if len(features) != len(object_ids):
        LOGGER.warning(
            "Feature count %d does not match object id count %d",
            len(features),
            len(object_ids),
        )

    payload = {"features": features}
    target_path.write_text(json.dumps(payload), encoding="utf-8")
    LOGGER.info("Wrote %d features to %s", len(features), target_path)

    metadata = {
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_url": service_url,
        "feature_count": len(features),
        "file_bytes": target_path.stat().st_size,
        "file_path": str(target_path.relative_to(PROJECT_ROOT)),
        "note": "Attributes-only (no geometry) — enrichment script joins by ROUTE_ID + milepoint.",
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    LOGGER.info("Wrote download metadata to %s", metadata_path)


if __name__ == "__main__":
    sys.exit(main() or 0)
