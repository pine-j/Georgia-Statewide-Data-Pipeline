"""Download FAF5 highway assignment data (network links + flow tables).

Downloads from FHWA:
  - FAF5 Model Highway Network GDB (487k links, ~214 MB)
  - 2022 Highway Assignment Results (6 CSVs, ~530 MB)
  - 2050 Baseline Highway Assignment Results (11 CSVs, ~713 MB)

Extracts ZIPs and writes a download manifest.

Output structure:
  01-Raw-Data/freight/faf5/
    network/           <- FAF5_Model_Highway_Network GDB
    assignment_2022/   <- 6 CSVs (DOM/IMP/EXP x Total/SU/CU)
    assignment_2050/   <- 11 CSVs (same + hi/lo growth scenarios)
    download_metadata.json
"""

from __future__ import annotations

import json
import logging
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = PROJECT_ROOT / "01-Raw-Data" / "freight" / "faf5"

FHWA_BASE = "https://ops.fhwa.dot.gov/freight/freight_analysis/faf/faf_highway_assignment_results"

DOWNLOADS = [
    {
        "name": "FAF5 Model Highway Network",
        "url": f"{FHWA_BASE}/FAF5_Model_Highway_Network.zip",
        "extract_to": "network",
    },
    {
        "name": "FAF5 2022 Highway Assignment Results",
        "url": f"{FHWA_BASE}/FAF5_2022_HighwayAssignmentResults_04_07_2022.zip",
        "extract_to": "assignment_2022",
    },
    {
        "name": "FAF5 2050 Baseline Highway Assignment Results",
        "url": f"{FHWA_BASE}/FAF5_2050_HighwayAssignmentResults_09_17_2022.zip",
        "extract_to": "assignment_2050",
    },
]

USER_AGENT = "Georgia-Statewide-Data-Pipeline FAF5 downloader"
TIMEOUT = 300
MAX_RETRIES = 3
CHUNK_SIZE = 1024 * 1024


def _download_file(url: str, dest: Path) -> int:
    """Download a file with retries and progress logging. Returns bytes written."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=TIMEOUT) as resp:
                content_length = resp.headers.get("Content-Length")
                total = int(content_length) if content_length else None
                total_mb = f"{total / 1024 / 1024:.0f} MB" if total else "unknown size"
                log.info("  Downloading %s (%s)...", dest.name, total_mb)

                written = 0
                with open(dest, "wb") as f:
                    while True:
                        chunk = resp.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)
                        written += len(chunk)
                        if total and written % (10 * CHUNK_SIZE) == 0:
                            pct = written / total * 100
                            log.info("    %.0f%% (%d MB)", pct, written / 1024 / 1024)

                log.info("  Done: %d MB", written / 1024 / 1024)
                return written

        except (URLError, TimeoutError, OSError) as exc:
            log.warning("  Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
            else:
                raise

    return 0


def _extract_zip(zip_path: Path, extract_to: Path) -> list[str]:
    """Extract ZIP and return list of extracted file names."""
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)
        names = zf.namelist()
    log.info("  Extracted %d files to %s", len(names), extract_to.name)
    return names


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    metadata = {
        "download_utc": datetime.now(timezone.utc).isoformat(),
        "source": "FHWA Freight Analysis Framework v5 Highway Assignment Results",
        "source_url": FHWA_BASE,
        "files": {},
    }

    for dl in DOWNLOADS:
        name = dl["name"]
        url = dl["url"]
        extract_dir = OUTPUT_DIR / dl["extract_to"]
        zip_name = url.rsplit("/", 1)[-1]
        zip_path = OUTPUT_DIR / zip_name

        log.info("\n=== %s ===", name)

        if extract_dir.exists() and any(extract_dir.iterdir()):
            log.info("  Already extracted at %s, skipping download.", extract_dir)
            metadata["files"][name] = {
                "status": "skipped_already_exists",
                "extract_dir": str(extract_dir),
            }
            continue

        try:
            bytes_written = _download_file(url, zip_path)
            extracted = _extract_zip(zip_path, extract_dir)
            zip_path.unlink()

            metadata["files"][name] = {
                "status": "ok",
                "url": url,
                "bytes": bytes_written,
                "extracted_files": len(extracted),
                "extract_dir": str(extract_dir),
            }
        except Exception as exc:
            log.error("  FAILED: %s", exc)
            metadata["files"][name] = {
                "status": "failed",
                "url": url,
                "error": str(exc),
            }

    manifest_path = OUTPUT_DIR / "download_metadata.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    log.info("\nManifest written to %s", manifest_path)

    ok = sum(1 for v in metadata["files"].values() if v["status"] in ("ok", "skipped_already_exists"))
    failed = sum(1 for v in metadata["files"].values() if v["status"] == "failed")
    log.info("Results: %d ok, %d failed out of %d downloads", ok, failed, len(DOWNLOADS))

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
