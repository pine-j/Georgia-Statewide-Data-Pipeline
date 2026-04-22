"""Download OPB (Governor's Office of Planning and Budget) population projections.

The OPB publishes county-level population projections for Georgia at:

    https://opb.georgia.gov/census-data/population-projections

The default downloads the 2025-2060 county residential population projection.
Override via the ``--url`` flag or ``OPB_PROJECTIONS_URL`` environment variable.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = REPO_ROOT / "01-Raw-Data" / "demographics" / "opb_projections"

# Default URL — may need periodic updates when OPB publishes new vintages
DEFAULT_URL = (
    "https://opb.georgia.gov/document/census-data/"
    "county-residential-population-2025-projection/download"
)


def download_opb(url: str | None = None) -> Path:
    """Download the OPB population projections Excel file.

    Parameters
    ----------
    url : str, optional
        Direct URL to the Excel workbook. Falls back to ``OPB_PROJECTIONS_URL``
        env var, then the built-in default.
    """
    url = url or os.environ.get("OPB_PROJECTIONS_URL", DEFAULT_URL)

    logger.info("Downloading OPB projections from %s", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    filename = url.split("/")[-1].replace("%20", "_")
    if not filename.endswith((".xlsx", ".xls")):
        filename = "opb_population_projections.xlsx"

    outpath = RAW_DIR / filename
    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_bytes(resp.content)
    logger.info("Saved OPB projections to %s (%d bytes)", outpath, len(resp.content))
    return outpath


def write_metadata(url: str | None = None) -> Path:
    """Write metadata for the OPB download."""
    url = url or os.environ.get("OPB_PROJECTIONS_URL", DEFAULT_URL)
    meta = {
        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Georgia Governor's Office of Planning and Budget",
        "url": url,
        "description": "County-level residential population projections for Georgia (2025-2060)",
    }
    outpath = RAW_DIR / "download_metadata.json"
    outpath.write_text(json.dumps(meta, indent=2))
    return outpath


def main(url: str | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    download_opb(url)
    write_metadata(url)
    logger.info("OPB download complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download OPB population projections")
    parser.add_argument("--url", default=None, help="Override download URL")
    args = parser.parse_args()
    main(url=args.url)
