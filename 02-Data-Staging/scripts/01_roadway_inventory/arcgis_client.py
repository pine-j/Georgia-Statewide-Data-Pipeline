"""Generic ArcGIS feature-service fetch helpers for Phase 1 roadway ETL."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import geopandas as gpd
import pandas as pd

DEFAULT_QUERY_BATCH_SIZE = 500
DEFAULT_USER_AGENT = "Georgia-Statewide-Data-Pipeline ArcGIS client"


def _strip_extra_dims(coords: Any) -> Any:
    """Strip M/Z dimensions beyond XY from GeoJSON coordinates."""
    if not coords:
        return coords
    if isinstance(coords[0], (int, float)):
        return coords[:2]
    return [_strip_extra_dims(child) for child in coords]


def _esri_feature_to_geojson(feature: dict[str, Any], geometry_type: str | None) -> dict[str, Any]:
    """Convert an ESRI JSON feature to a GeoJSON Feature dict."""
    attrs = feature.get("attributes") or {}
    esri_geom = feature.get("geometry")
    geojson_geom: dict[str, Any] | None = None
    if esri_geom and geometry_type:
        if geometry_type == "esriGeometryPolyline":
            paths = esri_geom.get("paths") or []
            if len(paths) == 1:
                geojson_geom = {"type": "LineString", "coordinates": paths[0]}
            elif len(paths) > 1:
                geojson_geom = {"type": "MultiLineString", "coordinates": paths}
        elif geometry_type == "esriGeometryPolygon":
            rings = esri_geom.get("rings") or []
            if rings:
                geojson_geom = {"type": "Polygon", "coordinates": rings}
        elif geometry_type == "esriGeometryPoint":
            x = esri_geom.get("x")
            y = esri_geom.get("y")
            if x is not None and y is not None:
                geojson_geom = {"type": "Point", "coordinates": [x, y]}
        elif geometry_type == "esriGeometryMultipoint":
            points = esri_geom.get("points") or []
            if points:
                geojson_geom = {"type": "MultiPoint", "coordinates": points}
    return {"type": "Feature", "properties": attrs, "geometry": geojson_geom}


def _feature_collection_to_gdf(payload: dict[str, Any]) -> gpd.GeoDataFrame:
    features = payload.get("features", [])
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Detect ESRI JSON (has 'attributes') vs GeoJSON (has 'properties') and
    # convert ESRI → GeoJSON shape. GDOT's LRS layers (e.g. GPAS/MapServer/5)
    # fail server-side GeoJSON serialization for M-aware polylines, so this
    # client fetches f=json and normalizes here.
    if "attributes" in features[0]:
        geometry_type = payload.get("geometryType")
        features = [_esri_feature_to_geojson(feature, geometry_type) for feature in features]
    for feature in features:
        geometry = feature.get("geometry")
        if geometry and "coordinates" in geometry:
            geometry["coordinates"] = _strip_extra_dims(geometry["coordinates"])
    return gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")


def _get_json(
    service_url: str,
    params: dict[str, Any],
    timeout: int,
    user_agent: str,
) -> dict[str, Any]:
    query = urlencode(params, doseq=True)
    full_url = f"{service_url}?{query}"
    if len(full_url) > 2000:
        request = Request(
            service_url,
            data=query.encode("utf-8"),
            headers={
                "User-Agent": user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
    else:
        request = Request(full_url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload


def fetch_arcgis_object_ids(
    service_url: str,
    *,
    timeout: int = 120,
    user_agent: str = DEFAULT_USER_AGENT,
) -> list[int]:
    query_url = f"{service_url.rstrip('/')}/query"
    payload = _get_json(
        query_url,
        {
            "f": "json",
            "where": "1=1",
            "returnIdsOnly": "true",
        },
        timeout=timeout,
        user_agent=user_agent,
    )
    object_ids = payload.get("objectIds") or []
    return sorted(int(object_id) for object_id in object_ids)


def fetch_arcgis_features(
    service_url: str,
    object_ids: list[int],
    *,
    batch_size: int = DEFAULT_QUERY_BATCH_SIZE,
    timeout: int = 180,
    user_agent: str = DEFAULT_USER_AGENT,
) -> gpd.GeoDataFrame:
    query_url = f"{service_url.rstrip('/')}/query"
    frames: list[gpd.GeoDataFrame] = []

    for start in range(0, len(object_ids), batch_size):
        batch = object_ids[start : start + batch_size]
        payload = _get_json(
            query_url,
            {
                "f": "json",
                "where": "1=1",
                "objectIds": ",".join(str(object_id) for object_id in batch),
                "outFields": "*",
                "returnGeometry": "true",
                "returnM": "false",
                "returnZ": "false",
                "outSR": 4326,
            },
            timeout=timeout,
            user_agent=user_agent,
        )
        gdf = _feature_collection_to_gdf(payload)
        if not gdf.empty:
            frames.append(gdf)

    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    merged = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=frames[0].crs)


# Backward-compatible aliases retained only in this module so static grep can
# confirm the old private symbol names are no longer imported cross-module.
_fetch_arcgis_object_ids = fetch_arcgis_object_ids
_fetch_arcgis_features = fetch_arcgis_features
