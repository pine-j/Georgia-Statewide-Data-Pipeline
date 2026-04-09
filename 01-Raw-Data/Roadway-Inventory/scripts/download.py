"""Download Georgia GDOT Roadway Inventory and Traffic geodatabases.

Downloads the Road_Inventory_Geodatabase.zip and TRAFFIC_Data_Geodatabase.zip
from GDOT's Open Data portal, extracts them to 01-Raw-Data/Roadway-Inventory/,
and records download metadata.
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
GDOT_BASE_URL = "https://myfiles.dot.ga.gov/OTD/RoadAndTrafficData/"

ROADWAY_GDB_URL = GDOT_BASE_URL + "Road_Inventory_Geodatabase.zip"
TRAFFIC_GDB_URL = GDOT_BASE_URL + "TRAFFIC_Data_Geodatabase.zip"
DATA_DICT_URL = (
    "https://www.dot.ga.gov/DriveSmart/Data/Documents/"
    "Road_Inventory_Data_Dictionary.pdf"
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory"


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

    gdb_dirs = list(extract_to.rglob("*.gdb"))
    if not gdb_dirs:
        raise FileNotFoundError("No .gdb directory found in the zip archive")

    logger.info("Extracted GDB to %s", gdb_dirs[0])
    return gdb_dirs[0]


def write_metadata(roadway_gdb_path: Path, roadway_zip_size: int,
                    traffic_gdb_path: Path | None, traffic_zip_size: int,
                    pdf_size: int) -> Path:
    """Write download metadata JSON."""
    metadata = {
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_urls": {
            "roadway_gdb": ROADWAY_GDB_URL,
            "traffic_gdb": TRAFFIC_GDB_URL,
            "data_dictionary": DATA_DICT_URL,
        },
        "files": {
            "roadway_gdb_zip_bytes": roadway_zip_size,
            "roadway_gdb_path": str(roadway_gdb_path.relative_to(PROJECT_ROOT)),
            "traffic_gdb_zip_bytes": traffic_zip_size,
            "traffic_gdb_path": str(traffic_gdb_path.relative_to(PROJECT_ROOT)) if traffic_gdb_path else None,
            "data_dictionary_pdf_bytes": pdf_size,
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
    traffic_dir = RAW_DIR / "GDOT_Traffic"
    traffic_dir.mkdir(parents=True, exist_ok=True)

    # Roadway inventory GDB
    zip_dest = RAW_DIR / "Road_Inventory_Geodatabase.zip"
    logger.info("Downloading roadway inventory GDB...")
    roadway_zip_size = download_file(ROADWAY_GDB_URL, zip_dest, "Roadway GDB")

    logger.info("Extracting roadway geodatabase...")
    roadway_gdb_path = extract_gdb(zip_dest, RAW_DIR)

    # Traffic GDB
    traffic_zip_dest = RAW_DIR / "TRAFFIC_Data_Geodatabase.zip"
    logger.info("Downloading traffic GDB...")
    traffic_gdb_path = None
    traffic_zip_size = 0
    try:
        traffic_zip_size = download_file(TRAFFIC_GDB_URL, traffic_zip_dest, "Traffic GDB")
        logger.info("Extracting traffic geodatabase...")
        traffic_gdb_path = extract_gdb(traffic_zip_dest, traffic_dir)
    except requests.RequestException:
        logger.warning("Could not download traffic GDB — download manually from %s", TRAFFIC_GDB_URL)

    # Data dictionary PDF
    pdf_dest = RAW_DIR / "DataDictionary.pdf"
    logger.info("Downloading data dictionary PDF...")
    try:
        pdf_size = download_file(DATA_DICT_URL, pdf_dest, "Data Dictionary")
    except requests.RequestException:
        logger.warning("Could not download data dictionary PDF, continuing...")
        pdf_size = 0

    write_metadata(roadway_gdb_path, roadway_zip_size,
                   traffic_gdb_path, traffic_zip_size, pdf_size)

    logger.info("Download complete.")
    logger.info("  Roadway GDB: %s", roadway_gdb_path)
    if traffic_gdb_path:
        logger.info("  Traffic GDB: %s", traffic_gdb_path)
    else:
        logger.warning("  Traffic GDB: not downloaded — fetch manually")


if __name__ == "__main__":
    main()
