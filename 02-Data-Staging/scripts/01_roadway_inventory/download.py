"""Download Georgia GDOT Roadway Inventory geodatabase and data dictionary.

Downloads the Road_Inventory_Geodatabase.zip from GDOT's Open Data portal,
extracts it to 01-Raw-Data/GA_RDWY_INV/, and records download metadata.
"""

import json
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

# GDOT Open Data portal URLs
ROADWAY_GDB_URL = (
    "https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/"
    "Road_Inventory_Geodatabase.zip"
)
DATA_DICT_URL = (
    "https://www.dot.ga.gov/DriveSmart/Data/Documents/"
    "Road_Inventory_Data_Dictionary.pdf"
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "GA_RDWY_INV"


def download_file(url: str, dest: Path, description: str = "Downloading") -> int:
    """Download a file with progress bar. Returns file size in bytes."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 8192

    with (
        open(dest, "wb") as f,
        tqdm(total=total_size, unit="B", unit_scale=True, desc=description) as pbar,
    ):
        for chunk in response.iter_content(chunk_size=block_size):
            f.write(chunk)
            pbar.update(len(chunk))

    return dest.stat().st_size


def extract_gdb(zip_path: Path, extract_to: Path) -> Path:
    """Extract GDB from zip archive. Returns path to the .gdb directory."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)

    # Find the .gdb folder inside the extracted contents
    gdb_dirs = list(extract_to.rglob("*.gdb"))
    if not gdb_dirs:
        raise FileNotFoundError("No .gdb directory found in the zip archive")

    logger.info("Extracted GDB to %s", gdb_dirs[0])
    return gdb_dirs[0]


def write_metadata(
    gdb_path: Path,
    zip_size: int,
    pdf_size: int,
) -> Path:
    """Write download metadata JSON."""
    metadata = {
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_urls": {
            "roadway_gdb": ROADWAY_GDB_URL,
            "data_dictionary": DATA_DICT_URL,
        },
        "files": {
            "gdb_zip_bytes": zip_size,
            "data_dictionary_pdf_bytes": pdf_size,
            "gdb_path": str(gdb_path.relative_to(PROJECT_ROOT)),
        },
    }
    meta_path = RAW_DIR / "download_metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2))
    logger.info("Metadata written to %s", meta_path)
    return meta_path


def main() -> None:
    """Run the full download workflow."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Download GDB zip
    zip_dest = RAW_DIR / "Road_Inventory_Geodatabase.zip"
    logger.info("Downloading roadway inventory GDB...")
    zip_size = download_file(ROADWAY_GDB_URL, zip_dest, "Roadway GDB")

    # Extract GDB
    logger.info("Extracting geodatabase...")
    gdb_path = extract_gdb(zip_dest, RAW_DIR)

    # Download data dictionary PDF
    pdf_dest = RAW_DIR / "DataDictionary.pdf"
    logger.info("Downloading data dictionary PDF...")
    try:
        pdf_size = download_file(DATA_DICT_URL, pdf_dest, "Data Dictionary")
    except requests.RequestException:
        logger.warning("Could not download data dictionary PDF, continuing...")
        pdf_size = 0

    # Write metadata
    write_metadata(gdb_path, zip_size, pdf_size)

    logger.info("Download complete. GDB at: %s", gdb_path)


if __name__ == "__main__":
    main()
