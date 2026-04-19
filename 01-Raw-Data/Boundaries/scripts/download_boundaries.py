#!/usr/bin/env python3
"""Download the administrative boundary layers the roadway inventory
pipeline depends on, into a local cache under 01-Raw-Data/Boundaries/cache/.

After running this script once, normalize.py reads the boundaries from
disk instead of hitting the live URLs. Re-run any time you want to
refresh (sources republish on quarterly-to-yearly cadences).

Usage:
    python 01-Raw-Data/Boundaries/scripts/download_boundaries.py
    python 01-Raw-Data/Boundaries/scripts/download_boundaries.py --force
    python 01-Raw-Data/Boundaries/scripts/download_boundaries.py --only counties,mpos
    python 01-Raw-Data/Boundaries/scripts/download_boundaries.py --list

Sources:
    counties              GDOT_Boundaries MapServer Layer 1 (159 GA counties)
    districts             GDOT_Boundaries MapServer Layer 3 (7 GDOT districts)
    mpos                  FHWA/BTS MPO FeatureServer Layer 30 (national; GA
                          spatial clip happens in normalize.py, not here)
    regional_commissions  Georgia DCA Regional Commissions FeatureServer (12 RCs)
    cities                ARC OpenData Cities Georgia (~542 polygons)
    state_house           Census TIGER/Line SLDL (lower state chamber, post-2020)
    state_senate          Census TIGER/Line SLDU (upper state chamber, post-2020)
    congressional         Census TIGER/Line CD119 (119th Congress)

GDOT Layers 2/4/5/6 are intentionally NOT cached. They are stale:
    2  Area Offices (2014, 31 polygons, short of the current 38)
    4  State House (pre-2020 redistricting)
    5  State Senate (pre-2020 redistricting)
    6  Congressional (2011 'proposed')
Area Office polygons are DERIVED in normalize.py from the County layer
dissolved by config/area_office_codes.json, so no cached layer exists
for them.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import shutil
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.request import Request, urlopen

import geopandas as gpd

logger = logging.getLogger("download_boundaries")

# Layout on disk:
#   01-Raw-Data/Boundaries/
#     scripts/          <-- this file (tracked in git)
#     cache/            <-- downloaded artifacts (git-ignored)
#       counties.fgb
#       districts.fgb
#       mpos.fgb
#       regional_commissions.fgb
#       cities.fgb
#       state_house.zip        + state_house/ (extracted .shp)
#       state_senate.zip       + state_senate/
#       congressional.zip      + congressional/
#       manifest.json          <-- per-layer metadata (date, url, bytes, checksum)
SCRIPT_DIR = Path(__file__).resolve().parent
BOUNDARIES_DIR = SCRIPT_DIR.parent
CACHE_DIR = BOUNDARIES_DIR / "cache"
MANIFEST_PATH = CACHE_DIR / "manifest.json"

# URL constants are duplicated here by design: this script is intentionally
# self-contained so it can run without importing normalize.py (which has a
# heavy transitive import graph). Any source change must be mirrored here
# AND in 02-Data-Staging/scripts/01_roadway_inventory/normalize.py.
GDOT_BOUNDARIES_SERVICE = (
    "https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Boundaries/MapServer"
)
COUNTY_BOUNDARIES_URL = (
    f"{GDOT_BOUNDARIES_SERVICE}/1/query?where=1%3D1&outFields=*&f=geojson"
)
DISTRICT_BOUNDARIES_URL = (
    f"{GDOT_BOUNDARIES_SERVICE}/3/query?where=1%3D1&outFields=*&f=geojson"
)
MPO_BOUNDARIES_URL = (
    "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
    "Metropolitan_Planning_Organizations/FeatureServer/30/query"
    "?where=1%3D1&outFields=MPO_ID,MPO_NAME,STATE&f=geojson"
)
REGIONAL_COMMISSION_BOUNDARIES_URL = (
    "https://services2.arcgis.com/Gqyymy5JISeLzyNM/arcgis/rest/services/"
    "RegionalCommissions/FeatureServer/0/query?where=1%3D1&outFields=*&f=geojson"
)
GEORGIA_CITIES_URL = (
    "https://services1.arcgis.com/Ug5xGQbHsD8zuZzM/arcgis/rest/services/"
    "Georgia_Cities_view/FeatureServer/0/query?where=1%3D1&outFields=*&f=geojson"
)
TIGER_YEAR = 2024
GEORGIA_STATE_FIPS = "13"
STATE_HOUSE_BOUNDARIES_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/SLDL/"
    f"tl_{TIGER_YEAR}_{GEORGIA_STATE_FIPS}_sldl.zip"
)
STATE_SENATE_BOUNDARIES_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/SLDU/"
    f"tl_{TIGER_YEAR}_{GEORGIA_STATE_FIPS}_sldu.zip"
)
CONGRESSIONAL_BOUNDARIES_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/CD/"
    f"tl_{TIGER_YEAR}_{GEORGIA_STATE_FIPS}_cd119.zip"
)


@dataclass(frozen=True)
class BoundarySource:
    """One cacheable boundary layer."""

    name: str
    url: str
    kind: str  # "geojson" or "tiger_zip"
    cache_filename: str  # relative to CACHE_DIR
    expected_min_features: int
    description: str


SOURCES: tuple[BoundarySource, ...] = (
    BoundarySource(
        name="counties",
        url=COUNTY_BOUNDARIES_URL,
        kind="geojson",
        cache_filename="counties.fgb",
        expected_min_features=159,
        description="GDOT_Boundaries MapServer Layer 1 (159 GA counties)",
    ),
    BoundarySource(
        name="districts",
        url=DISTRICT_BOUNDARIES_URL,
        kind="geojson",
        cache_filename="districts.fgb",
        expected_min_features=7,
        description="GDOT_Boundaries MapServer Layer 3 (7 GDOT districts)",
    ),
    BoundarySource(
        name="mpos",
        url=MPO_BOUNDARIES_URL,
        kind="geojson",
        cache_filename="mpos.fgb",
        expected_min_features=400,
        description="FHWA/BTS MPO FeatureServer Layer 30 (national; GA clipped downstream)",
    ),
    BoundarySource(
        name="regional_commissions",
        url=REGIONAL_COMMISSION_BOUNDARIES_URL,
        kind="geojson",
        cache_filename="regional_commissions.fgb",
        expected_min_features=12,
        description="Georgia DCA Regional Commissions FeatureServer (12 RCs)",
    ),
    BoundarySource(
        name="cities",
        url=GEORGIA_CITIES_URL,
        kind="geojson",
        cache_filename="cities.fgb",
        expected_min_features=500,
        description="ARC OpenData Cities Georgia (~542 polygons)",
    ),
    BoundarySource(
        name="state_house",
        url=STATE_HOUSE_BOUNDARIES_URL,
        kind="tiger_zip",
        cache_filename="state_house.zip",
        expected_min_features=180,
        description="Census TIGER/Line SLDL (state lower chamber, post-2020)",
    ),
    BoundarySource(
        name="state_senate",
        url=STATE_SENATE_BOUNDARIES_URL,
        kind="tiger_zip",
        cache_filename="state_senate.zip",
        expected_min_features=56,
        description="Census TIGER/Line SLDU (state upper chamber, post-2020)",
    ),
    BoundarySource(
        name="congressional",
        url=CONGRESSIONAL_BOUNDARIES_URL,
        kind="tiger_zip",
        cache_filename="congressional.zip",
        expected_min_features=14,
        description="Census TIGER/Line CD119 (119th Congress)",
    ),
)


def _http_get(url: str, chunk_size: int = 65536) -> tuple[bytes, float]:
    """GET `url` with a custom User-Agent; return (payload, elapsed_seconds)."""
    request = Request(
        url,
        headers={"User-Agent": "ga-raptor-boundary-downloader/1.0"},
    )
    started = time.monotonic()
    with urlopen(request, timeout=300) as response:
        buffer = io.BytesIO()
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            buffer.write(chunk)
    elapsed = time.monotonic() - started
    return buffer.getvalue(), elapsed


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _download_geojson(source: BoundarySource, cache_path: Path) -> dict:
    """GeoJSON download path: fetch the URL, parse through geopandas, and
    write to a compact FlatGeobuf cache.

    Storing the reprojected+cleaned FGB (instead of the raw GeoJSON text)
    trades a slightly larger first-run cost for consistently fast
    subsequent reads - which is the hot path once normalize.py prefers
    local cache.
    """
    logger.info("  fetching %s", source.url)
    payload, elapsed = _http_get(source.url)
    logger.info("    %d bytes in %.1fs", len(payload), elapsed)

    # Write raw GeoJSON to temp file, then read with geopandas + write FGB.
    with tempfile.NamedTemporaryFile(
        prefix=f"{source.name}_", suffix=".geojson", delete=False
    ) as tmp:
        tmp.write(payload)
        tmp_path = Path(tmp.name)
    try:
        gdf = gpd.read_file(tmp_path, engine="pyogrio")
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    if cache_path.exists():
        cache_path.unlink()
    gdf.to_file(cache_path, driver="FlatGeobuf", engine="pyogrio")

    cached_bytes = cache_path.read_bytes()
    return {
        "feature_count": int(len(gdf)),
        "bytes": len(cached_bytes),
        "sha256": _sha256(cached_bytes),
        "source_bytes": len(payload),
        "source_sha256": _sha256(payload),
        "fetched_seconds": round(elapsed, 2),
        "crs": str(gdf.crs) if gdf.crs is not None else None,
    }


def _download_tiger_zip(source: BoundarySource, cache_path: Path) -> dict:
    """TIGER download path: store the raw zip (exact replica of what normalize.py
    would receive from the live URL) AND extract its .shp companions to a sibling
    directory so tools that prefer unpacked shapefiles also work.
    """
    logger.info("  fetching %s", source.url)
    payload, elapsed = _http_get(source.url)
    logger.info("    %d bytes in %.1fs", len(payload), elapsed)

    if cache_path.exists():
        cache_path.unlink()
    cache_path.write_bytes(payload)

    extract_dir = cache_path.with_suffix("")
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        archive.extractall(extract_dir)

    shp_matches = sorted(extract_dir.rglob("*.shp"))
    if not shp_matches:
        raise RuntimeError(
            f"TIGER archive at {source.url} contained no .shp file"
        )
    # Verify the shapefile is readable + feature count looks right.
    gdf = gpd.read_file(shp_matches[0], engine="pyogrio")

    return {
        "feature_count": int(len(gdf)),
        "bytes": len(payload),
        "sha256": _sha256(payload),
        "extracted_shp": shp_matches[0].name,
        "fetched_seconds": round(elapsed, 2),
        "crs": str(gdf.crs) if gdf.crs is not None else None,
    }


_DOWNLOAD_DISPATCH: dict[str, Callable[[BoundarySource, Path], dict]] = {
    "geojson": _download_geojson,
    "tiger_zip": _download_tiger_zip,
}


def _load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        return {"sources": {}}
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("could not read existing manifest (%s); starting fresh", exc)
        return {"sources": {}}


def _save_manifest(manifest: dict) -> None:
    manifest["last_updated_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _download_one(source: BoundarySource, force: bool) -> tuple[bool, dict | str]:
    """Download one source; return (succeeded, metadata_or_reason)."""
    cache_path = CACHE_DIR / source.cache_filename
    if cache_path.exists() and not force:
        return True, f"cached (skipped; use --force to re-download)"

    downloader = _DOWNLOAD_DISPATCH[source.kind]
    try:
        metadata = downloader(source, cache_path)
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"

    if metadata["feature_count"] < source.expected_min_features:
        return False, (
            f"feature count {metadata['feature_count']} below expected minimum "
            f"{source.expected_min_features}"
        )
    return True, metadata


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download admin boundary layers to 01-Raw-Data/Boundaries/cache/",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download layers that are already cached",
    )
    parser.add_argument(
        "--only",
        default=None,
        help="comma-separated list of source names to download (default: all)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print the registered sources and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.list:
        for source in SOURCES:
            print(f"{source.name:<22} {source.kind:<11} {source.description}")
        return 0

    if args.only:
        requested = {name.strip() for name in args.only.split(",") if name.strip()}
        unknown = requested - {source.name for source in SOURCES}
        if unknown:
            logger.error("unknown source names: %s", ", ".join(sorted(unknown)))
            return 2
        targets = [source for source in SOURCES if source.name in requested]
    else:
        targets = list(SOURCES)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest()
    failures: list[tuple[str, str]] = []

    for source in targets:
        logger.info("[%s] %s", source.name, source.description)
        succeeded, result = _download_one(source, args.force)
        if not succeeded:
            failures.append((source.name, str(result)))
            logger.error("  FAILED: %s", result)
            continue
        if isinstance(result, dict):
            manifest["sources"][source.name] = {
                "url": source.url,
                "kind": source.kind,
                "cache_filename": source.cache_filename,
                **result,
                "downloaded_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            logger.info(
                "  OK: %d features, %.1f KB cached",
                result["feature_count"],
                result["bytes"] / 1024.0,
            )
        else:
            logger.info("  %s", result)

    _save_manifest(manifest)

    if failures:
        logger.error("completed with %d failure(s):", len(failures))
        for name, reason in failures:
            logger.error("  %s: %s", name, reason)
        return 1

    logger.info("all %d source(s) cached in %s", len(targets), CACHE_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
