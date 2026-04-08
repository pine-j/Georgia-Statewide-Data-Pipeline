"""Enrich Georgia roadway segments with FHWA HPMS 2024 data.

HPMS uses the same GDOT ROUTE_ID + milepoint system, enabling direct
interval-overlap matching without spatial joins.

Current enrichment:
- AADT gap-fill for segments missing GDOT official/analytical AADT
- Pavement condition: IRI, PSR, rutting, cracking_percent
- Safety geometry: access_control, terrain_type
- Route signing: routesigning, routenumber, routename

Downloaded data lives at:
  01-Raw-Data/GA_RDWY_INV/HPMS_2024/hpms_ga_2024_tabular.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import geopandas as gpd

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
HPMS_PATH = PROJECT_ROOT / "01-Raw-Data" / "GA_RDWY_INV" / "HPMS_2024" / "hpms_ga_2024_tabular.json"

MILEPOINT_TOLERANCE = 1e-4

HPMS_AADT_FIELDS = ["aadt", "aadt_single_unit", "aadt_combination", "k_factor", "dir_factor", "future_aadt"]
HPMS_PAVEMENT_FIELDS = ["iri", "psr", "rutting", "cracking_percent", "surface_type"]
HPMS_SAFETY_FIELDS = ["access_control", "terrain_type", "speed_limit"]
HPMS_ROUTE_FIELDS = ["routesigning", "routenumber", "routename"]


def load_hpms_data() -> pd.DataFrame:
    """Load the HPMS tabular snapshot."""

    if not HPMS_PATH.exists():
        LOGGER.warning("HPMS data not found at %s", HPMS_PATH)
        return pd.DataFrame()

    LOGGER.info("Loading HPMS data from %s", HPMS_PATH)
    with open(HPMS_PATH, encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame([feat["attributes"] for feat in raw["features"]])
    LOGGER.info("Loaded %d HPMS records with %d columns", len(df), len(df.columns))
    return df


def _build_hpms_lookup(hpms: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Build a ROUTE_ID -> list of HPMS records lookup."""

    lookup: dict[str, list[dict[str, Any]]] = {}
    if hpms.empty:
        return lookup

    valid = hpms[hpms["route_id"].notna()].copy()
    valid["route_id"] = valid["route_id"].astype(str).str.strip()
    valid = valid.sort_values(by=["route_id", "begin_point"])

    for route_id, group in valid.groupby("route_id", sort=False):
        lookup[str(route_id)] = group.to_dict("records")

    LOGGER.info("HPMS lookup: %d route keys", len(lookup))
    return lookup


def _find_best_hpms_match(
    route_id: str,
    from_mp: float | None,
    to_mp: float | None,
    lookup: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    """Find the HPMS record with the best milepoint overlap."""

    candidates = lookup.get(route_id, [])
    if not candidates:
        return None

    if from_mp is None or to_mp is None:
        return candidates[0] if candidates else None

    best = None
    best_overlap = -1.0

    for record in candidates:
        ref_from = record.get("begin_point")
        ref_to = record.get("end_point")
        if ref_from is None or ref_to is None:
            continue

        overlap = min(float(to_mp), float(ref_to)) - max(float(from_mp), float(ref_from))
        if overlap > best_overlap:
            best_overlap = overlap
            best = record

    return best if best_overlap > -MILEPOINT_TOLERANCE else None


def apply_hpms_enrichment(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Enrich roadway segments with HPMS 2024 data.

    Fills AADT gaps and adds pavement/safety attributes where available.
    Never overwrites existing official or analytical AADT values.
    """

    hpms = load_hpms_data()
    if hpms.empty:
        return gdf

    enriched = gdf.copy()
    lookup = _build_hpms_lookup(hpms)

    # Initialize HPMS-sourced columns if not present
    for col in ["HPMS_IRI", "HPMS_PSR", "HPMS_RUTTING", "HPMS_CRACKING_PCT",
                "HPMS_ACCESS_CONTROL", "HPMS_TERRAIN_TYPE",
                "HPMS_ROUTE_SIGNING", "HPMS_ROUTE_NUMBER", "HPMS_ROUTE_NAME"]:
        if col not in enriched.columns:
            enriched[col] = None

    aadt_fill_count = 0
    pavement_fill_count = 0
    total_matched = 0

    for idx in enriched.index:
        row = enriched.loc[idx]
        route_id = str(row.get("ROUTE_ID", ""))
        if not route_id or route_id == "nan":
            continue

        match = _find_best_hpms_match(
            route_id,
            row.get("FROM_MILEPOINT"),
            row.get("TO_MILEPOINT"),
            lookup,
        )
        if match is None:
            continue

        total_matched += 1

        # AADT gap-fill: only fill if not already covered
        if not bool(row.get("current_aadt_covered", False)):
            hpms_aadt = match.get("aadt")
            if hpms_aadt is not None and not pd.isna(hpms_aadt):
                enriched.at[idx, "AADT_2024"] = int(hpms_aadt)
                enriched.at[idx, "AADT"] = int(hpms_aadt)
                enriched.at[idx, "AADT_YEAR"] = 2024
                enriched.at[idx, "AADT_2024_SOURCE"] = "hpms_2024"
                enriched.at[idx, "AADT_2024_CONFIDENCE"] = "medium"
                enriched.at[idx, "AADT_2024_FILL_METHOD"] = "hpms_route_id_milepoint_match"
                enriched.at[idx, "current_aadt_covered"] = True
                aadt_fill_count += 1

        # Pavement condition — always fill if HPMS has data and we don't
        iri = match.get("iri")
        if iri is not None and not pd.isna(iri):
            enriched.at[idx, "HPMS_IRI"] = float(iri)
            pavement_fill_count += 1

        psr = match.get("psr")
        if psr is not None and not pd.isna(psr):
            enriched.at[idx, "HPMS_PSR"] = float(psr)

        rutting = match.get("rutting")
        if rutting is not None and not pd.isna(rutting):
            enriched.at[idx, "HPMS_RUTTING"] = float(rutting)

        cracking = match.get("cracking_percent")
        if cracking is not None and not pd.isna(cracking):
            enriched.at[idx, "HPMS_CRACKING_PCT"] = float(cracking)

        # Safety geometry
        ac = match.get("access_control")
        if ac is not None and not pd.isna(ac):
            enriched.at[idx, "HPMS_ACCESS_CONTROL"] = int(ac)

        terrain = match.get("terrain_type")
        if terrain is not None and not pd.isna(terrain):
            enriched.at[idx, "HPMS_TERRAIN_TYPE"] = int(terrain)

        # Route signing
        signing = match.get("routesigning")
        if signing is not None and not pd.isna(signing):
            enriched.at[idx, "HPMS_ROUTE_SIGNING"] = int(signing)

        routenum = match.get("routenumber")
        if routenum is not None and not pd.isna(routenum):
            enriched.at[idx, "HPMS_ROUTE_NUMBER"] = int(routenum)

        routename = match.get("routename")
        if routename is not None and not pd.isna(routename):
            enriched.at[idx, "HPMS_ROUTE_NAME"] = str(routename).strip()

    # Sync canonical AADT after fills
    enriched["AADT"] = enriched["AADT_2024"]
    enriched["current_aadt_covered"] = enriched["AADT_2024"].notna()

    LOGGER.info(
        "HPMS enrichment: %d segments matched, %d AADT gaps filled, %d with pavement data",
        total_matched,
        aadt_fill_count,
        pavement_fill_count,
    )

    return enriched


def write_hpms_enrichment_summary(gdf: pd.DataFrame) -> None:
    """Write HPMS enrichment coverage summary."""

    summary = {
        "segment_count": int(len(gdf)),
        "hpms_aadt_filled": int((gdf.get("AADT_2024_SOURCE", pd.Series()) == "hpms_2024").sum()),
        "hpms_iri_coverage": int(gdf["HPMS_IRI"].notna().sum()) if "HPMS_IRI" in gdf.columns else 0,
        "hpms_psr_coverage": int(gdf["HPMS_PSR"].notna().sum()) if "HPMS_PSR" in gdf.columns else 0,
        "hpms_rutting_coverage": int(gdf["HPMS_RUTTING"].notna().sum()) if "HPMS_RUTTING" in gdf.columns else 0,
        "hpms_cracking_coverage": int(gdf["HPMS_CRACKING_PCT"].notna().sum()) if "HPMS_CRACKING_PCT" in gdf.columns else 0,
        "hpms_access_control_coverage": int(gdf["HPMS_ACCESS_CONTROL"].notna().sum()) if "HPMS_ACCESS_CONTROL" in gdf.columns else 0,
        "hpms_terrain_coverage": int(gdf["HPMS_TERRAIN_TYPE"].notna().sum()) if "HPMS_TERRAIN_TYPE" in gdf.columns else 0,
        "hpms_route_signing_coverage": int(gdf["HPMS_ROUTE_SIGNING"].notna().sum()) if "HPMS_ROUTE_SIGNING" in gdf.columns else 0,
        "final_aadt_coverage": int(gdf["AADT_2024"].notna().sum()) if "AADT_2024" in gdf.columns else 0,
        "final_aadt_source_counts": {
            str(k): int(v)
            for k, v in gdf["AADT_2024_SOURCE"].value_counts(dropna=False).items()
        } if "AADT_2024_SOURCE" in gdf.columns else {},
    }

    output_path = PROJECT_ROOT / "02-Data-Staging" / "config" / "hpms_enrichment_summary.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("Wrote HPMS enrichment summary to %s", output_path)
