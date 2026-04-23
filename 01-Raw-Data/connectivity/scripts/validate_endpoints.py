"""Validate Phase 2 Connectivity download endpoints before full execution.

Hits each ArcGIS REST / API endpoint with a minimal query (count-only or
single-record) to confirm the service is live and returns Georgia features.
Produces a JSON report at 01-Raw-Data/connectivity/endpoint_validation.json.

Does NOT download the SRP priority route layers (13-16) — those are covered
by the Phase 1b SRP derivation pipeline.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
REPORT_PATH = PROJECT_ROOT / "01-Raw-Data" / "connectivity" / "endpoint_validation.json"

TIMEOUT = 30

ENDPOINTS = [
    {
        "name": "NEVI / Alternative Fuel Corridors",
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Alternative_Fuel_Corridors/FeatureServer/0/query"
        ),
        "params": {"where": "STATE = 'GA'", "returnCountOnly": "true", "f": "json"},
        "type": "arcgis",
    },
    {
        "name": "Airports",
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Aviation_Facilities/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE_CODE = 'GA'",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "Seaports",
        "url": (
            "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/"
            "Principal_Ports/FeatureServer/0/query"
        ),
        "params": {
            "where": "PORTNAME LIKE '%Savannah%' OR PORTNAME LIKE '%Brunswick%'",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "Universities",
        "url": (
            "https://services1.arcgis.com/Ua5sjt3LWTPigjyD/arcgis/rest/services/"
            "Postsecondary_School_Locations_Current/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'GA'",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "Military Bases",
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "Military_Installation/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE_TERR = 'Georgia'",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "National Parks",
        "url": (
            "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/"
            "NPS_Land_Resources_Division_Boundary_and_Tract_Data_Service/"
            "FeatureServer/2/query"
        ),
        "params": {
            "where": "STATE = 'GA'",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "Rail Facilities",
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Intermodal_Freight_Facilities_Rail_TOFC_COFC/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'GA'",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "Freight Generators (FAF5 Network Nodes)",
        "url": (
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
            "NTAD_Freight_Analysis_Framework_Network_Nodes/FeatureServer/0/query"
        ),
        "params": {
            "where": "STATE = 'GA' AND Facility_Type IS NOT NULL AND Facility_Type <> ''",
            "returnCountOnly": "true",
            "f": "json",
        },
        "type": "arcgis",
    },
    {
        "name": "AFDC Alt Fuel Stations",
        "url": "https://developer.nrel.gov/api/alt-fuel-stations/v1.json",
        "params": {
            "api_key": "DEMO_KEY",
            "state": "GA",
            "status": "E",
            "access": "public",
            "limit": "1",
        },
        "type": "afdc",
    },
]


def _check_arcgis(endpoint: dict) -> dict:
    """Query an ArcGIS REST endpoint with returnCountOnly."""
    resp = requests.get(endpoint["url"], params=endpoint["params"], timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        return {
            "status": "error",
            "http_code": resp.status_code,
            "error": data["error"].get("message", str(data["error"])),
        }

    count = data.get("count")
    return {
        "status": "ok" if count and count > 0 else "warning_zero_features",
        "http_code": resp.status_code,
        "feature_count": count,
    }


def _check_afdc(endpoint: dict) -> dict:
    """Query the AFDC API with limit=1 to verify connectivity."""
    resp = requests.get(endpoint["url"], params=endpoint["params"], timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    total = data.get("total_results", 0)
    stations = data.get("fuel_stations", [])

    if endpoint["params"].get("api_key") == "DEMO_KEY":
        api_key_note = "using DEMO_KEY — rate-limited, replace for production"
    else:
        api_key_note = "custom key"

    return {
        "status": "ok" if total > 0 else "warning_zero_results",
        "http_code": resp.status_code,
        "total_results": total,
        "sample_returned": len(stations),
        "api_key_note": api_key_note,
    }


def main() -> None:
    results = {
        "validation_run_utc": datetime.now(timezone.utc).isoformat(),
        "endpoints": {},
    }

    passed = 0
    failed = 0

    for ep in ENDPOINTS:
        name = ep["name"]
        log.info("Checking %s ...", name)

        try:
            if ep["type"] == "arcgis":
                result = _check_arcgis(ep)
            elif ep["type"] == "afdc":
                result = _check_afdc(ep)
            else:
                result = {"status": "unknown_type"}
        except requests.exceptions.ConnectionError as exc:
            result = {"status": "connection_error", "error": str(exc)}
        except requests.exceptions.Timeout:
            result = {"status": "timeout", "timeout_seconds": TIMEOUT}
        except requests.exceptions.HTTPError as exc:
            result = {"status": "http_error", "http_code": exc.response.status_code}
        except Exception as exc:
            result = {"status": "unexpected_error", "error": str(exc)}

        result["url"] = ep["url"]
        results["endpoints"][name] = result

        if result["status"] == "ok":
            log.info("  OK — %s", {k: v for k, v in result.items() if k != "url"})
            passed += 1
        else:
            log.warning("  ISSUE — %s", result)
            failed += 1

    results["summary"] = {
        "total": len(ENDPOINTS),
        "passed": passed,
        "failed_or_warning": failed,
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)

    log.info("Report written to %s", REPORT_PATH)
    log.info("Results: %d passed, %d issues out of %d endpoints", passed, failed, len(ENDPOINTS))

    if failed > 0:
        log.warning("Some endpoints had issues — review the report before running download.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
