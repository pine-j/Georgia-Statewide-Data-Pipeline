"""Download Economic Census data from the Census Bureau API.

Fetches county-level economic indicators from the Economic Census (latest
available vintage is 2017; 2022 may be available once released).  Includes
number of establishments, annual payroll, and number of employees by NAICS
sector for the specified state.

Requires ``CENSUS_API_KEY`` environment variable.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
RAW_DIR = REPO_ROOT / "01-Raw-Data" / "demographics" / "economic_census"

CENSUS_BASE = "https://api.census.gov/data"
EC_VINTAGE = "2017"
EC_DATASET = "ecnbasic"

# Key variables from the Economic Census
EC_VARIABLES = [
    "NAICS2017",
    "NAICS2017_LABEL",
    "ESTAB",       # Number of establishments
    "PAYANN",      # Annual payroll ($1,000)
    "EMP",         # Number of employees
    "GEO_ID",
    "NAME",
]


def _api_key() -> str:
    key = os.environ.get("CENSUS_API_KEY", "")
    if not key:
        raise EnvironmentError("Set CENSUS_API_KEY environment variable.")
    return key


def download_economic_census(state_fips: str = "13") -> Path:
    """Download Economic Census county-level data for *state_fips*."""
    api_key = _api_key()

    url = f"{CENSUS_BASE}/{EC_VINTAGE}/{EC_DATASET}"
    params = {
        "get": ",".join(EC_VARIABLES),
        "for": "county:*",
        "in": f"state:{state_fips}",
        "NAICS2017": "00",  # All sectors
        "key": api_key,
    }

    logger.info("GET %s", url)
    resp = requests.get(url, params=params, timeout=300)
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])

    outpath = RAW_DIR / f"economic_census_{EC_VINTAGE}_{state_fips}.csv"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outpath, index=False)
    logger.info("Saved %d rows to %s", len(df), outpath)

    # Also try sector-level breakdown (2-digit NAICS)
    params_sector = {
        "get": ",".join(EC_VARIABLES),
        "for": "county:*",
        "in": f"state:{state_fips}",
        "key": api_key,
    }
    try:
        resp_sector = requests.get(url, params=params_sector, timeout=300)
        resp_sector.raise_for_status()
        data_sector = resp_sector.json()
        df_sector = pd.DataFrame(data_sector[1:], columns=data_sector[0])
        sector_path = RAW_DIR / f"economic_census_{EC_VINTAGE}_{state_fips}_by_sector.csv"
        df_sector.to_csv(sector_path, index=False)
        logger.info("Saved %d sector rows to %s", len(df_sector), sector_path)
    except Exception:
        logger.warning("Could not download sector-level breakdown; skipping.")

    return outpath


def write_metadata(state_fips: str = "13") -> Path:
    """Write metadata for the economic census download."""
    meta = {
        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
        "state_fips": state_fips,
        "vintage": EC_VINTAGE,
        "dataset": EC_DATASET,
        "endpoint": f"{CENSUS_BASE}/{EC_VINTAGE}/{EC_DATASET}",
        "variables": EC_VARIABLES,
        "geography": "county",
    }
    outpath = RAW_DIR / "download_metadata.json"
    outpath.write_text(json.dumps(meta, indent=2))
    return outpath


def main(state_fips: str = "13") -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    download_economic_census(state_fips)
    write_metadata(state_fips)
    logger.info("Economic Census download complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download Economic Census data")
    parser.add_argument("--state-fips", default="13")
    args = parser.parse_args()
    main(state_fips=args.state_fips)
