"""Compare derived SRP classification against official GDOT SRP layers.

Downloads official SRP layers 13-16 from the GDOT FunctionalClass
MapServer (if not already present), performs a spatial join against
the staged roadway segments, and produces a confusion matrix and
agreement statistics.

Output: 02-Data-Staging/reports/srp_derivation_validation.json
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import LineString, MultiLineString

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_SRP_DIR = PROJECT_ROOT / "01-Raw-Data" / "connectivity" / "srp_priority_routes"
REPORTS_DIR = PROJECT_ROOT / "02-Data-Staging" / "reports"

SRP_BASE_URL = (
    "https://maps.itos.uga.edu/arcgis/rest/services/GDOT/"
    "GDOT_FunctionalClass/MapServer"
)
SRP_LAYERS = {
    "Critical": 13,
    "High": 14,
    "Medium": 15,
    "Low": 16,
}

TIER_ORDER = ["Critical", "High", "Medium", "Low"]

BUFFER_M = 50.0
MIN_OVERLAP_RATIO = 0.30


def _paginated_geojson_query(
    url: str,
    params: dict,
    max_record_count: int = 2000,
    pause: float = 0.5,
) -> dict:
    """Query an ArcGIS REST endpoint with pagination."""
    all_features = []
    offset = 0
    params = dict(params)

    while True:
        params["resultOffset"] = offset
        params["resultRecordCount"] = max_record_count
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

    return {"type": "FeatureCollection", "features": all_features}


def download_official_srp() -> dict[str, gpd.GeoDataFrame]:
    """Download official SRP layers from GDOT MapServer."""
    RAW_SRP_DIR.mkdir(parents=True, exist_ok=True)
    layers = {}

    for tier, layer_id in SRP_LAYERS.items():
        cache_path = RAW_SRP_DIR / f"srp_{tier.lower()}.geojson"

        if cache_path.exists():
            LOGGER.info("Loading cached %s SRP layer from %s", tier, cache_path)
            gdf = gpd.read_file(cache_path, engine="pyogrio")
            layers[tier] = gdf
            continue

        LOGGER.info("Downloading %s SRP layer (layer %d) ...", tier, layer_id)
        url = f"{SRP_BASE_URL}/{layer_id}/query"
        params = {"where": "1=1", "outFields": "*", "f": "geojson"}

        try:
            data = _paginated_geojson_query(url, params)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            LOGGER.info("  Saved %d features -> %s", len(data["features"]), cache_path)
            gdf = gpd.read_file(cache_path, engine="pyogrio")
            layers[tier] = gdf
        except Exception as exc:
            LOGGER.warning("Failed to download %s SRP layer: %s", tier, exc)

    return layers


def _assign_official_tier(
    segments: gpd.GeoDataFrame,
    official_layers: dict[str, gpd.GeoDataFrame],
) -> pd.Series:
    """Assign official SRP tier to each segment via spatial overlay.

    Higher-priority tiers take precedence when a segment matches
    multiple layers.
    """
    result = pd.Series("Unclassified", index=segments.index, dtype="object")

    seg_crs = segments.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot match official SRP layers")
        return result

    for tier in reversed(TIER_ORDER):
        if tier not in official_layers:
            continue

        layer = official_layers[tier]
        if layer.empty:
            continue

        if layer.crs != seg_crs:
            layer = layer.to_crs(seg_crs)

        layer_union = layer.geometry.buffer(BUFFER_M).union_all()

        hits = segments[segments.geometry.intersects(layer_union)]
        for idx in hits.index:
            seg_geom = segments.at[idx, "geometry"]
            seg_len = float(seg_geom.length) if seg_geom and not seg_geom.is_empty else 0.0
            if seg_len <= 0:
                continue
            overlap = seg_geom.intersection(layer_union)
            overlap_ratio = float(overlap.length) / seg_len if seg_len > 0 else 0.0
            if overlap_ratio >= MIN_OVERLAP_RATIO:
                result.at[idx] = tier

        LOGGER.info("Official %s tier: %d segments matched", tier, int((result == tier).sum()))

    return result


def validate_srp(segments: gpd.GeoDataFrame) -> dict:
    """Run full SRP validation: download official, compare, produce report."""
    if "SRP_DERIVED" not in segments.columns:
        LOGGER.warning("No SRP_DERIVED column — run srp_derivation.py first")
        return {}

    LOGGER.info("Downloading official SRP layers ...")
    official_layers = download_official_srp()

    if not official_layers:
        LOGGER.warning("No official SRP layers available for validation")
        return {}

    LOGGER.info("Assigning official SRP tiers to segments ...")
    segments = segments.copy()
    segments["SRP_OFFICIAL"] = _assign_official_tier(segments, official_layers)

    classified_mask = segments["SRP_OFFICIAL"] != "Unclassified"
    classified = segments[classified_mask]

    confusion: dict[str, dict[str, int]] = {
        t: {t2: 0 for t2 in TIER_ORDER + ["Unclassified"]} for t in TIER_ORDER
    }
    for _, row in segments.iterrows():
        derived = row["SRP_DERIVED"]
        official = row["SRP_OFFICIAL"]
        if derived in confusion and official in confusion[derived]:
            confusion[derived][official] += 1

    exact_match = int((classified["SRP_DERIVED"] == classified["SRP_OFFICIAL"]).sum())
    total_classified = len(classified)
    agreement_pct = (100 * exact_match / total_classified) if total_classified > 0 else 0

    tier_summary = {
        "derived": segments["SRP_DERIVED"].value_counts().to_dict(),
        "official": segments["SRP_OFFICIAL"].value_counts().to_dict(),
    }

    within_one = 0
    for _, row in classified.iterrows():
        d_idx = TIER_ORDER.index(row["SRP_DERIVED"]) if row["SRP_DERIVED"] in TIER_ORDER else -1
        o_idx = TIER_ORDER.index(row["SRP_OFFICIAL"]) if row["SRP_OFFICIAL"] in TIER_ORDER else -1
        if d_idx >= 0 and o_idx >= 0 and abs(d_idx - o_idx) <= 1:
            within_one += 1
    within_one_pct = (100 * within_one / total_classified) if total_classified > 0 else 0

    promotion_count = 0
    demotion_count = 0
    for _, row in classified.iterrows():
        d_idx = TIER_ORDER.index(row["SRP_DERIVED"]) if row["SRP_DERIVED"] in TIER_ORDER else -1
        o_idx = TIER_ORDER.index(row["SRP_OFFICIAL"]) if row["SRP_OFFICIAL"] in TIER_ORDER else -1
        if d_idx >= 0 and o_idx >= 0:
            if d_idx < o_idx:
                promotion_count += 1
            elif d_idx > o_idx:
                demotion_count += 1

    report = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "total_segments": int(len(segments)),
        "official_classified": total_classified,
        "official_unclassified": int(len(segments) - total_classified),
        "exact_agreement": exact_match,
        "exact_agreement_pct": round(agreement_pct, 2),
        "within_one_tier": within_one,
        "within_one_tier_pct": round(within_one_pct, 2),
        "promotions_derived_higher": promotion_count,
        "demotions_derived_lower": demotion_count,
        "confusion_matrix": confusion,
        "tier_counts": tier_summary,
    }

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "srp_derivation_validation.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    LOGGER.info("Wrote SRP validation report to %s", output_path)
    LOGGER.info(
        "SRP validation: %d segments classified, %.1f%% exact agreement, "
        "%.1f%% within one tier, %d promotions, %d demotions",
        total_classified, agreement_pct, within_one_pct,
        promotion_count, demotion_count,
    )

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

    import sqlite3

    db_path = PROJECT_ROOT / "02-Data-Staging" / "databases" / "roadway_inventory.db"
    gpkg_path = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"

    if gpkg_path.exists():
        LOGGER.info("Loading segments from %s ...", gpkg_path)
        segments = gpd.read_file(gpkg_path, layer="roadway_segments", engine="pyogrio")
        LOGGER.info("Loaded %d segments", len(segments))
        report = validate_srp(segments)
        if report:
            print(json.dumps(report, indent=2))
    else:
        LOGGER.error("GPKG not found at %s — run the pipeline first", gpkg_path)
