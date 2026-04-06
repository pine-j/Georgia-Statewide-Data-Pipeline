"""Download Census data (Decennial 2020 and ACS 5-Year) via the Census Bureau API.

Parameterized by state FIPS code so it works for any state. Default is
Georgia (FIPS 13).  Outputs land in the corresponding subfolders under
``01-Raw-Data/demographics/``.

Environment variable ``CENSUS_API_KEY`` is required.  Register for a free
key at https://api.census.gov/data/key_signup.html
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths (relative to repo root)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[4]
RAW_DIR = REPO_ROOT / "01-Raw-Data" / "demographics"
DECENNIAL_DIR = RAW_DIR / "census_decennial"
ACS_DIR = RAW_DIR / "acs_5year"

# ---------------------------------------------------------------------------
# Census API configuration
# ---------------------------------------------------------------------------
CENSUS_BASE = "https://api.census.gov/data"

# Decennial 2020 PL redistricting variables at **block** level
DECENNIAL_VINTAGE = "2020"
DECENNIAL_DATASET = "dec/pl"
DECENNIAL_VARIABLES = [
    "P1_001N",   # Total population
    "P1_003N",   # Population of one race – White alone
    "P1_004N",   # Black or African American alone
    "H1_001N",   # Total housing units
    "H1_002N",   # Occupied housing units
]

# ACS 5-Year (latest available vintage) at **block group** level
ACS_VINTAGE = "2022"
ACS_DATASET = "acs/acs5"
ACS_VARIABLES = [
    "B01003_001E",  # Total population
    "B19013_001E",  # Median household income
    "B15003_022E",  # Bachelor's degree
    "B15003_023E",  # Master's degree
    "B15003_025E",  # Doctorate degree
    "B15003_001E",  # Total education universe (25+)
    "B17001_002E",  # Income below poverty level
    "B17001_001E",  # Total poverty universe
    "B08301_001E",  # Total commuters
    "B08301_010E",  # Public transportation
    "B08301_019E",  # Walked
    "B08301_021E",  # Worked from home
    "B23025_004E",  # Employed (civilian labor force)
    "B23025_005E",  # Unemployed (civilian labor force)
]


def _api_key() -> str:
    """Return the Census API key or raise."""
    key = os.environ.get("CENSUS_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "Set the CENSUS_API_KEY environment variable. "
            "Register at https://api.census.gov/data/key_signup.html"
        )
    return key


def _fetch_census(
    vintage: str,
    dataset: str,
    variables: list[str],
    geo_for: str,
    geo_in: str,
    api_key: str,
) -> pd.DataFrame:
    """Generic Census API fetch returning a DataFrame."""
    url = f"{CENSUS_BASE}/{vintage}/{dataset}"
    params: dict[str, str] = {
        "get": ",".join(variables),
        "for": geo_for,
        "in": geo_in,
        "key": api_key,
    }
    logger.info("GET %s  params=%s", url, {k: v for k, v in params.items() if k != "key"})
    resp = requests.get(url, params=params, timeout=300)
    resp.raise_for_status()
    data: list[list[str]] = resp.json()
    return pd.DataFrame(data[1:], columns=data[0])


# ---------------------------------------------------------------------------
# Decennial 2020 – blocks
# ---------------------------------------------------------------------------

def download_decennial(state_fips: str = "13") -> Path:
    """Download Decennial 2020 PL data for every block in *state_fips*.

    The Census API requires county-level iteration for block geography, so
    we first pull the list of counties and then loop.
    """
    api_key = _api_key()

    # Get county list
    county_df = _fetch_census(
        DECENNIAL_VINTAGE,
        DECENNIAL_DATASET,
        ["NAME"],
        "county:*",
        f"state:{state_fips}",
        api_key,
    )
    county_codes = sorted(county_df["county"].unique())
    logger.info("Found %d counties in state %s", len(county_codes), state_fips)

    frames: list[pd.DataFrame] = []
    for county in county_codes:
        df = _fetch_census(
            DECENNIAL_VINTAGE,
            DECENNIAL_DATASET,
            DECENNIAL_VARIABLES,
            "block:*",
            f"state:{state_fips}&in=county:{county}",
            api_key,
        )
        frames.append(df)
        logger.debug("  county %s: %d blocks", county, len(df))

    result = pd.concat(frames, ignore_index=True)

    # Build full GEOID (state + county + tract + block)
    result["GEOID"] = (
        result["state"] + result["county"] + result["tract"] + result["block"]
    )

    outpath = DECENNIAL_DIR / f"decennial_2020_blocks_{state_fips}.csv"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(outpath, index=False)
    logger.info("Saved %d blocks to %s", len(result), outpath)
    return outpath


# ---------------------------------------------------------------------------
# ACS 5-Year – block groups
# ---------------------------------------------------------------------------

def download_acs(state_fips: str = "13") -> Path:
    """Download ACS 5-Year estimates at block-group level for *state_fips*."""
    api_key = _api_key()

    df = _fetch_census(
        ACS_VINTAGE,
        ACS_DATASET,
        ACS_VARIABLES,
        "block group:*",
        f"state:{state_fips}",
        api_key,
    )

    # Build GEOID (state + county + tract + block group)
    df["GEOID"] = df["state"] + df["county"] + df["tract"] + df["block group"]

    outpath = ACS_DIR / f"acs5_{ACS_VINTAGE}_blockgroups_{state_fips}.csv"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outpath, index=False)
    logger.info("Saved %d block groups to %s", len(df), outpath)
    return outpath


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def write_metadata(state_fips: str = "13") -> Path:
    """Write download_metadata.json alongside the raw data."""
    meta: dict[str, Any] = {
        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
        "state_fips": state_fips,
        "decennial": {
            "vintage": DECENNIAL_VINTAGE,
            "dataset": DECENNIAL_DATASET,
            "endpoint": f"{CENSUS_BASE}/{DECENNIAL_VINTAGE}/{DECENNIAL_DATASET}",
            "variables": DECENNIAL_VARIABLES,
            "geography": "block",
        },
        "acs_5year": {
            "vintage": ACS_VINTAGE,
            "dataset": ACS_DATASET,
            "endpoint": f"{CENSUS_BASE}/{ACS_VINTAGE}/{ACS_DATASET}",
            "variables": ACS_VARIABLES,
            "geography": "block group",
        },
    }
    outpath = RAW_DIR / "download_metadata.json"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_text(json.dumps(meta, indent=2))
    logger.info("Wrote metadata to %s", outpath)
    return outpath


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(state_fips: str = "13") -> None:
    """Run all Census downloads for the given state."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logger.info("Downloading Census data for state FIPS %s", state_fips)

    download_decennial(state_fips)
    download_acs(state_fips)
    write_metadata(state_fips)

    logger.info("Census download complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download Census data via API")
    parser.add_argument(
        "--state-fips",
        default="13",
        help="State FIPS code (default: 13 for Georgia)",
    )
    args = parser.parse_args()
    main(state_fips=args.state_fips)
