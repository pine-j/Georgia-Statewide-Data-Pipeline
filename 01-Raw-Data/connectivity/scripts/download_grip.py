"""Download GRIP corridor data from GDOT GIS services.

GRIP (Governor's Road Improvement Program) established 19 economic
development corridors + 3 truck access routes (3,323 miles total).

Attempts to download from the GDOT FunctionalClass MapServer first.
If no GRIP layer is available, falls back to a curated route lookup.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = PROJECT_ROOT / "01-Raw-Data" / "connectivity" / "grip_corridors"

GDOT_FUNCTIONAL_CLASS_URL = (
    "https://maps.itos.uga.edu/arcgis/rest/services/GDOT/"
    "GDOT_FunctionalClass/MapServer"
)

GDOT_ROUTE_NETWORK_URL = (
    "https://rnhp.dot.ga.gov/arcgis/rest/services"
)


def _request_json(url: str, timeout: int = 30) -> dict:
    """Fetch JSON from an ArcGIS REST endpoint."""
    resp = requests.get(url, params={"f": "json"}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _paginated_query(
    url: str,
    params: dict,
    max_record_count: int = 2000,
    pause: float = 0.5,
) -> dict:
    """Query an ArcGIS REST endpoint with pagination."""
    all_features = []
    offset = 0
    params = dict(params)

    while True:
        params["resultOffset"] = offset
        params["resultRecordCount"] = max_record_count
        log.info("  GET offset=%d", offset)

        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        if len(features) < max_record_count:
            break

        offset += max_record_count
        time.sleep(pause)

    return {"type": "FeatureCollection", "features": all_features}


def _discover_grip_layer_on_service(base_url: str) -> tuple[str, int] | None:
    """Search a MapServer or ArcGIS REST service catalog for a GRIP layer."""
    data = _request_json(base_url)
    for layer in data.get("layers", []):
        name = layer.get("name", "").upper()
        if "GRIP" in name:
            log.info("Found GRIP layer: %s (id=%d)", layer["name"], layer["id"])
            return base_url, layer["id"]

    for service in data.get("services", []):
        service_name = service.get("name")
        service_type = service.get("type")
        if not service_name or not service_type:
            continue

        service_url = f"{base_url.rstrip('/')}/{service_name}/{service_type}"
        try:
            nested = _request_json(service_url)
        except Exception as exc:
            log.debug("Could not inspect %s: %s", service_url, exc)
            continue

        for layer in nested.get("layers", []):
            name = layer.get("name", "").upper()
            if "GRIP" in name:
                log.info(
                    "Found GRIP layer: %s (id=%d) in %s",
                    layer["name"],
                    layer["id"],
                    service_url,
                )
                return service_url, layer["id"]

    return None


def discover_grip_layer() -> tuple[str, int] | None:
    """Check GDOT services for a GRIP-related layer."""
    try:
        for base_url in (GDOT_ROUTE_NETWORK_URL, GDOT_FUNCTIONAL_CLASS_URL):
            discovered = _discover_grip_layer_on_service(base_url)
            if discovered:
                return discovered
    except Exception as exc:
        log.warning("Could not query GRIP services: %s", exc)

    return None


def download_grip_from_mapserver(base_url: str, layer_id: int) -> dict | None:
    """Download GRIP features from a discovered MapServer layer."""
    query_url = f"{base_url}/{layer_id}/query"
    params = {"where": "1=1", "outFields": "*", "f": "geojson"}
    data = _paginated_query(query_url, params)
    if data["features"]:
        log.info("Downloaded %d GRIP features from layer %d", len(data["features"]), layer_id)
        return data
    return None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    metadata = {
        "dataset": "GRIP Corridors",
        "run_utc": datetime.now(timezone.utc).isoformat(),
    }

    layer_info = discover_grip_layer()
    if layer_info:
        base_url, layer_id = layer_info
        data = download_grip_from_mapserver(base_url, layer_id)
        if data:
            out_path = OUT_DIR / "grip_corridors.geojson"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            log.info("Saved GRIP corridors to %s", out_path)
            metadata["source"] = f"{base_url}/{layer_id}"
            metadata["features"] = len(data["features"])
            metadata["method"] = "mapserver_download"
        else:
            log.warning("GRIP layer found but returned no features")
            metadata["method"] = "mapserver_empty"
    else:
        log.info("No GRIP layer found on GDOT MapServer — route lookup required")
        metadata["method"] = "route_lookup_needed"

    meta_path = OUT_DIR / "download_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    log.info("Metadata written to %s", meta_path)


if __name__ == "__main__":
    main()
