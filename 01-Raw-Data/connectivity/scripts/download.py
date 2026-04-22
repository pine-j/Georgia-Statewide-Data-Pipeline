"""Download connectivity datasets for the Georgia RAPTOR pipeline.

Downloads:
    - SRP Priority Routes from GDOT MapServer (layers 13-16)
    - NEVI corridor data from GDOT ArcGIS Hub
    - Traffic generator shapefiles (airports, seaports, universities,
      military bases, national parks, intermodal rail, freight generators)
    - AFDC Alternative Fueling Station data

All outputs are saved to 01-Raw-Data/connectivity/.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

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
GENERATORS_DIR = RAW_DIR / "generators"

# ---------------------------------------------------------------------------
# Source endpoints
# ---------------------------------------------------------------------------

# GDOT SRP Priority Routes – FunctionalClass MapServer layers 13-16
SRP_BASE_URL = (
    "https://maps.itos.uga.edu/arcgis/rest/services/GDOT/"
    "GDOT_FunctionalClass/MapServer"
)
SRP_LAYERS = {
    "srp_critical": 13,
    "srp_high": 14,
    "srp_medium": 15,
    "srp_low": 16,
}

# NEVI corridors – GDOT ArcGIS Hub FeatureServer
NEVI_URL = (
    "https://services1.arcgis.com/2iUE8l8JKrP2tygQ/arcgis/rest/services/"
    "NEVI_Corridors/FeatureServer/0/query"
)

# AFDC Alternative Fueling Stations (Georgia, all fuel types)
AFDC_URL = (
    "https://developer.nrel.gov/api/alt-fuel-stations/v1.json"
)
AFDC_API_KEY = "DEMO_KEY"  # Replace with a real key for production use

# Traffic generator sources
GENERATOR_SOURCES: dict[str, dict[str, Any]] = {
    "airports": {
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Airports/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'GA'",
            "outFields": "*",
            "f": "geojson",
        },
    },
    "seaports": {
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Principal_Ports/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE_POST = 'GA'",
            "outFields": "*",
            "f": "geojson",
        },
    },
    "universities": {
        "url": (
            "https://services1.arcgis.com/Ua5sjt3LWTPigjyD/arcgis/rest/services/"
            "NCES_Postsecondary_School_Locations_Current/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'Georgia'",
            "outFields": "*",
            "f": "geojson",
        },
    },
    "military_bases": {
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Military_Installations/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE_TERR = 'Georgia'",
            "outFields": "*",
            "f": "geojson",
        },
    },
    "national_parks": {
        "url": (
            "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/"
            "NPS_Land_Resources_Division_Boundary_and_Tract_Data_Service/"
            "FeatureServer/2/query"
        ),
        "params": {
            "where": "STATE = 'GA'",
            "outFields": "*",
            "f": "geojson",
        },
    },
    "rail_facilities": {
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Intermodal_Freight_Facilities_Rail_TOFC_COFC/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'GA'",
            "outFields": "*",
            "f": "geojson",
        },
    },
    "freight_generators": {
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Freight_Analysis_Framework/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'GA' OR STATE = 'Georgia'",
            "outFields": "*",
            "f": "geojson",
        },
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dirs() -> None:
    """Create output directories if they do not exist."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    GENERATORS_DIR.mkdir(parents=True, exist_ok=True)


def _paginated_geojson_query(
    url: str,
    params: dict[str, Any],
    max_record_count: int = 2000,
    pause: float = 0.5,
) -> dict[str, Any]:
    """Query an ArcGIS REST endpoint with pagination and return merged GeoJSON.

    Parameters
    ----------
    url:
        The query endpoint URL.
    params:
        Base query parameters (must include ``f=geojson``).
    max_record_count:
        Number of records to request per page.
    pause:
        Seconds to wait between requests to avoid rate-limiting.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with all features.
    """
    all_features: list[dict] = []
    offset = 0
    params = dict(params)  # don't mutate caller's dict

    while True:
        params["resultOffset"] = offset
        params["resultRecordCount"] = max_record_count
        log.info("  GET %s  offset=%d", url.split("/")[-2], offset)

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

    return {
        "type": "FeatureCollection",
        "features": all_features,
    }


def _save_geojson(data: dict, path: Path) -> None:
    """Write a GeoJSON dict to *path*."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)
    log.info("  Saved %d features -> %s", len(data.get("features", [])), path.name)


# ---------------------------------------------------------------------------
# Download functions
# ---------------------------------------------------------------------------

def download_srp_priority_routes(metadata: dict) -> None:
    """Download SRP priority route layers 13-16 from GDOT MapServer."""
    log.info("Downloading SRP Priority Routes ...")
    out_dir = RAW_DIR / "srp_priority_routes"
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, layer_id in SRP_LAYERS.items():
        url = f"{SRP_BASE_URL}/{layer_id}/query"
        params = {"where": "1=1", "outFields": "*", "f": "geojson"}
        data = _paginated_geojson_query(url, params)
        out_path = out_dir / f"{name}.geojson"
        _save_geojson(data, out_path)
        metadata["srp_priority_routes"][name] = {
            "url": f"{SRP_BASE_URL}/{layer_id}",
            "features": len(data["features"]),
            "downloaded_utc": datetime.now(timezone.utc).isoformat(),
        }


def download_nevi_corridors(metadata: dict) -> None:
    """Download NEVI corridor data from GDOT ArcGIS Hub."""
    log.info("Downloading NEVI Corridors ...")
    params = {"where": "1=1", "outFields": "*", "f": "geojson"}
    data = _paginated_geojson_query(NEVI_URL, params)
    out_path = RAW_DIR / "nevi_corridors.geojson"
    _save_geojson(data, out_path)
    metadata["nevi_corridors"] = {
        "url": NEVI_URL.rsplit("/query", 1)[0],
        "features": len(data["features"]),
        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
    }


def download_afdc_stations(metadata: dict) -> None:
    """Download AFDC Alternative Fueling Station data for Georgia."""
    log.info("Downloading AFDC Alternative Fueling Stations ...")
    params = {
        "api_key": AFDC_API_KEY,
        "state": "GA",
        "status": "E",
        "access": "public",
        "limit": "all",
    }
    resp = requests.get(AFDC_URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    # Convert to GeoJSON FeatureCollection
    features = []
    for station in data.get("fuel_stations", []):
        if station.get("longitude") and station.get("latitude"):
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [station["longitude"], station["latitude"]],
                },
                "properties": {
                    k: v for k, v in station.items()
                    if k not in ("longitude", "latitude")
                },
            })

    geojson = {"type": "FeatureCollection", "features": features}
    out_path = RAW_DIR / "alt_fuel_stations.geojson"
    _save_geojson(geojson, out_path)
    metadata["alt_fuel_stations"] = {
        "url": AFDC_URL,
        "features": len(features),
        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
    }


def download_traffic_generators(metadata: dict) -> None:
    """Download all traffic generator datasets."""
    log.info("Downloading Traffic Generators ...")
    metadata["generators"] = {}

    for name, cfg in GENERATOR_SOURCES.items():
        log.info("  %s ...", name)
        try:
            data = _paginated_geojson_query(cfg["url"], dict(cfg["params"]))
            out_path = GENERATORS_DIR / f"{name}.geojson"
            _save_geojson(data, out_path)
            metadata["generators"][name] = {
                "url": cfg["url"].rsplit("/query", 1)[0],
                "features": len(data["features"]),
                "downloaded_utc": datetime.now(timezone.utc).isoformat(),
            }
        except Exception:
            log.exception("Failed to download %s", name)
            metadata["generators"][name] = {"error": "download failed"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run all downloads and write metadata."""
    _ensure_dirs()

    metadata: dict[str, Any] = {
        "pipeline": "Georgia RAPTOR – Connectivity",
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "srp_priority_routes": {},
    }

    download_srp_priority_routes(metadata)
    download_nevi_corridors(metadata)
    download_afdc_stations(metadata)
    download_traffic_generators(metadata)

    meta_path = RAW_DIR / "download_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2, ensure_ascii=False)
    log.info("Metadata written to %s", meta_path)
    log.info("All connectivity downloads complete.")


if __name__ == "__main__":
    main()
