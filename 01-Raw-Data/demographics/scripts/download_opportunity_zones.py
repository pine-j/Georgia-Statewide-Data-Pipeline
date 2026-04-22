"""Download Opportunity Zone shapefiles from the CDFI Fund and LEHD/LODES
employment data from the Census Bureau, plus TIGER/Line boundary shapefiles.

This script consolidates three related downloads:
1. Qualified Opportunity Zones (from CDFI Fund / Treasury)
2. LEHD LODES WAC and RAC files (gzipped CSVs from Census)
3. TIGER/Line shapefiles for block groups and tracts (from Census)
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = REPO_ROOT / "01-Raw-Data" / "demographics"
OZ_DIR = RAW_DIR / "opportunity_zones"
LODES_DIR = RAW_DIR / "lehd_lodes"
TIGER_DIR = RAW_DIR / "tiger_shapefiles"


# ---------------------------------------------------------------------------
# Opportunity Zones
# ---------------------------------------------------------------------------

# The CDFI Fund publishes the designated QOZ list as a CSV and shapefiles.
# The shapefile is derived from TIGER tract boundaries filtered to QOZ tracts.
CDFI_QOZ_URL = (
    "https://www.cdfifund.gov/system/files/documents/"
    "designated-qozs.12.14.18.xlsx"
)

TIGER_TRACT_URL_TEMPLATE = (
    "https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/"
    "tl_{year}_{state_fips}_tract.zip"
)


def download_opportunity_zones(state_fips: str = "13") -> Path:
    """Download the national QOZ tract list and filter to *state_fips*.

    Also downloads TIGER tract boundaries for the state so the QOZ tracts
    can be spatially joined later.
    """
    OZ_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading QOZ tract list from CDFI Fund")
    resp = requests.get(CDFI_QOZ_URL, timeout=120)
    resp.raise_for_status()
    out_path = OZ_DIR / "designated_qozs.xlsx"
    out_path.write_bytes(resp.content)
    logger.info("Saved QOZ list to %s", out_path)

    return out_path


# ---------------------------------------------------------------------------
# LEHD LODES (WAC and RAC)
# ---------------------------------------------------------------------------

LODES_BASE = "https://lehd.ces.census.gov/data/lodes/LODES8"

# WAC = Workplace Area Characteristics, RAC = Residence Area Characteristics
LODES_FILE_TEMPLATES = {
    "wac": "{state_abbr}_wac_S000_JT00_{year}.csv.gz",
    "rac": "{state_abbr}_rac_S000_JT00_{year}.csv.gz",
}


def download_lodes(
    state_abbr: str = "ga",
    year: int = 2021,
) -> list[Path]:
    """Download LEHD LODES WAC and RAC files for the given state and year.

    Files are gzipped CSVs organized by state abbreviation (lowercase).
    """
    LODES_DIR.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    for kind, template in LODES_FILE_TEMPLATES.items():
        filename = template.format(state_abbr=state_abbr, year=year)
        url = f"{LODES_BASE}/{state_abbr}/{kind}/{filename}"
        logger.info("Downloading LODES %s: %s", kind.upper(), url)

        try:
            resp = requests.get(url, timeout=300)
            resp.raise_for_status()
        except requests.HTTPError:
            # Try previous year if latest is not yet available
            alt_year = year - 1
            alt_filename = template.format(state_abbr=state_abbr, year=alt_year)
            alt_url = f"{LODES_BASE}/{state_abbr}/{kind}/{alt_filename}"
            logger.warning("Year %d not available, trying %d", year, alt_year)
            resp = requests.get(alt_url, timeout=300)
            resp.raise_for_status()
            filename = alt_filename

        gz_path = LODES_DIR / filename
        gz_path.write_bytes(resp.content)

        # Also decompress for convenience
        csv_path = LODES_DIR / filename.replace(".gz", "")
        with gzip.open(io.BytesIO(resp.content), "rt") as f:
            csv_path.write_text(f.read())

        logger.info("Saved LODES %s to %s", kind.upper(), csv_path)
        downloaded.append(csv_path)

    return downloaded


# ---------------------------------------------------------------------------
# TIGER/Line Shapefiles
# ---------------------------------------------------------------------------

TIGER_YEAR = "2023"

TIGER_URLS = {
    "block_groups": (
        f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/BG/"
        "tl_{year}_{state_fips}_bg.zip"
    ),
    "tracts": (
        f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/TRACT/"
        "tl_{year}_{state_fips}_tract.zip"
    ),
}


def download_tiger(state_fips: str = "13") -> list[Path]:
    """Download TIGER/Line block group and tract shapefiles for *state_fips*."""
    TIGER_DIR.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    for layer, url_template in TIGER_URLS.items():
        url = url_template.format(year=TIGER_YEAR, state_fips=state_fips)
        logger.info("Downloading TIGER %s: %s", layer, url)

        resp = requests.get(url, timeout=300)
        resp.raise_for_status()

        # Extract the zip
        layer_dir = TIGER_DIR / layer
        layer_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(layer_dir)

        logger.info("Extracted TIGER %s to %s", layer, layer_dir)
        downloaded.append(layer_dir)

    return downloaded


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def write_metadata(state_fips: str = "13", state_abbr: str = "ga") -> Path:
    """Write combined download metadata."""
    meta = {
        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
        "state_fips": state_fips,
        "state_abbr": state_abbr,
        "opportunity_zones": {
            "source": "CDFI Fund",
            "url": CDFI_QOZ_URL,
        },
        "lehd_lodes": {
            "source": "Census Bureau LEHD",
            "base_url": LODES_BASE,
            "version": "LODES8",
        },
        "tiger_shapefiles": {
            "source": "Census Bureau TIGER/Line",
            "year": TIGER_YEAR,
            "layers": list(TIGER_URLS.keys()),
        },
    }
    outpath = RAW_DIR / "download_supplemental_metadata.json"
    outpath.write_text(json.dumps(meta, indent=2))
    return outpath


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(state_fips: str = "13", state_abbr: str = "ga") -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    download_opportunity_zones(state_fips)
    download_lodes(state_abbr)
    download_tiger(state_fips)
    write_metadata(state_fips, state_abbr)

    logger.info("All supplemental downloads complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Opportunity Zones, LODES, and TIGER shapefiles"
    )
    parser.add_argument("--state-fips", default="13")
    parser.add_argument("--state-abbr", default="ga")
    args = parser.parse_args()
    main(state_fips=args.state_fips, state_abbr=args.state_abbr)
