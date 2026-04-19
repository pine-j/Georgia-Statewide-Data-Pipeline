"""Download FHWA HPMS Georgia tabular submission for a given year.

Fetches the HPMS_FULL_GA_<year> FeatureServer hosted by USDOT and writes the
attributes-only snapshot that `hpms_enrichment.py` expects:

    01-Raw-Data/Roadway-Inventory/FHWA_HPMS/<year>/hpms_ga_<year>_tabular.json

File format is ESRI JSON:  ``{"features": [{"attributes": {...}}, ...]}``.
Geometry is intentionally omitted — the enrichment joins by ROUTE_ID +
milepoint interval, not spatially, so a geometry-less pull keeps the file
manageable (~a few hundred MB instead of >1 GB).

Historic years (2020-2023) use older FHWA field-naming vintages. Pass
``--rename-map-json`` pointing at a JSON ``{"source_field": "target_field"}``
map; renames are applied to every feature's attribute dict before the output
is written, so the emitted file matches the 2024 schema.
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


def fetch_object_ids(service_url: str, where: str) -> list[int]:
    query_url = f"{service_url}/query"
    payload = _get_json(
        query_url,
        {"f": "json", "where": where, "returnIdsOnly": "true"},
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


def apply_rename_map(features: list[dict], rename_map: dict[str, str]) -> None:
    if not rename_map:
        return
    for feature in features:
        attrs = feature.get("attributes")
        if not attrs:
            continue
        for src, dst in rename_map.items():
            if src in attrs:
                attrs[dst] = attrs.pop(src)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FHWA HPMS Georgia tabular submission for a given year.")
    parser.add_argument("--year", type=int, required=True, help="HPMS submission year (e.g. 2024).")
    parser.add_argument(
        "--service-name",
        default=None,
        help="FeatureServer service name. Defaults to HPMS_FULL_GA_<year>. Override for anomalies like HPMS_FULL_GA3_2023.",
    )
    parser.add_argument("--layer", type=int, default=0, help="FeatureServer layer id (default 0).")
    parser.add_argument(
        "--where",
        default="1=1",
        help="Server-side where-clause passed to the object-id query. Default 1=1 (no filter).",
    )
    parser.add_argument(
        "--rename-map-json",
        default=None,
        help="Path to a JSON file mapping source_field -> target_field. Applied to every feature's attributes before output is written.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args(argv)

    year = args.year
    service_name = args.service_name or f"HPMS_FULL_GA_{year}"
    service_url = f"https://geo.dot.gov/server/rest/services/Hosted/{service_name}/FeatureServer/{args.layer}"
    target_path = (
        PROJECT_ROOT
        / "01-Raw-Data"
        / "Roadway-Inventory"
        / "FHWA_HPMS"
        / str(year)
        / f"hpms_ga_{year}_tabular.json"
    )
    metadata_path = target_path.parent / "download_metadata.json"

    rename_map: dict[str, str] = {}
    if args.rename_map_json:
        rename_map_path = Path(args.rename_map_json)
        rename_map = json.loads(rename_map_path.read_text(encoding="utf-8"))
        LOGGER.info("Loaded %d rename entries from %s", len(rename_map), rename_map_path)

    target_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Fetching HPMS object ids from %s (where=%s)", service_url, args.where)
    object_ids = fetch_object_ids(service_url, args.where)
    LOGGER.info("Found %d HPMS records", len(object_ids))

    features = fetch_features(service_url, object_ids)
    if len(features) != len(object_ids):
        LOGGER.warning(
            "Feature count %d does not match object id count %d",
            len(features),
            len(object_ids),
        )

    if rename_map:
        apply_rename_map(features, rename_map)
        LOGGER.info("Applied %d field renames to %d features", len(rename_map), len(features))

    payload = {"features": features}
    target_path.write_text(json.dumps(payload), encoding="utf-8")
    LOGGER.info("Wrote %d features to %s", len(features), target_path)

    metadata = {
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_url": service_url,
        "service_name": service_name,
        "layer_id": args.layer,
        "where_clause": args.where,
        "rename_map_applied": rename_map or None,
        "feature_count": len(features),
        "file_bytes": target_path.stat().st_size,
        "file_path": str(target_path.relative_to(PROJECT_ROOT)),
        "track": "rest",
        "note": "Attributes-only (no geometry) — hpms_enrichment.py joins by ROUTE_ID + milepoint.",
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    LOGGER.info("Wrote download metadata to %s", metadata_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
