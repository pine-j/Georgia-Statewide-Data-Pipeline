from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, box

# Add the enrichment scripts directory to sys.path so we can import the
# shared corridor-matching module.
_ENRICHMENT_DIR = str(Path(__file__).resolve().parents[2] / "scripts" / "01_roadway_inventory")
if _ENRICHMENT_DIR not in sys.path:
    sys.path.insert(0, _ENRICHMENT_DIR)
from _evac_corridor_match import (
    per_corridor_evac_overlay,
    run_automated_qc,
    ROUTE_BUFFER_M as _CORRIDOR_BUFFER_M,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = Path(__file__).resolve().parent
ROADWAY_PATH = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"
ROADWAY_LAYER = "roadway_segments"
RAW_GDOT_EOC_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory" / "GDOT_EOC"
EVAC_PATH = RAW_GDOT_EOC_DIR / "ga_evac_routes.geojson"
CONTRAFLOW_PATH = RAW_GDOT_EOC_DIR / "ga_contraflow_routes.geojson"

ROADWAY_CRS = "EPSG:32617"
WEB_CRS = "EPSG:4326"
# Tiered overlap thresholds — length-adaptive to avoid filtering out short
# segments that sit entirely within an evacuation corridor.
#   Short  (<400 m):   ratio >= 40 % only  (no absolute minimum)
#   Normal (400 m–10 km): overlap >= 150 m AND ratio >= 20 %
#   Mega   (>10 km):   overlap >= 200 m AND ratio >= 50 %
#                       OR clipped-to-corridor portion meets normal thresholds
SHORT_SEGMENT_MAX_M = 400.0
SHORT_SEGMENT_MIN_RATIO = 0.40
NORMAL_MIN_OVERLAP_M = 150.0
NORMAL_MIN_RATIO = 0.20
MEGA_SEGMENT_LENGTH_M = 10_000.0
MEGA_MIN_OVERLAP_M = 200.0
MEGA_MIN_RATIO = 0.50
MATCH_BUFFER_M = 30.0
MAX_ALIGNMENT_ANGLE_DEG = 30.0
# Contraflow-specific spatial parameters — tighter than general evacuation
# routes because contraflow applies only to specific Interstate lanes and
# both geometries (GDOT Interstates + contraflow polylines) are high-quality.
CONTRAFLOW_BUFFER_M = 15.0
CONTRAFLOW_MIN_OVERLAP_M = 200.0
CONTRAFLOW_MIN_RATIO = 0.30
CONTRAFLOW_SHORT_MIN_RATIO = 0.60
CONTRAFLOW_MAX_ALIGNMENT_ANGLE_DEG = 20.0
CONTRAFLOW_MIN_INSIDE_CORRIDOR_RATIO = 0.25
CONTEXT_SAMPLE_SIZE = 5000
RANDOM_STATE = 42
ROADWAY_COLUMNS = [
    "unique_id",
    "HWY_NAME",
    "ROUTE_FAMILY",
    "ROUTE_TYPE_GDOT",
    "BASE_ROUTE_NUMBER",
    "AADT",
    "DISTRICT_NAME",
    "COUNTY_NAME",
    "segment_length_m",
    "geometry",
]
CONTEXT_COLUMNS = ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "geometry"]


def _line_azimuth(geom):
    """Azimuth (radians) from first to last coordinate of a line geometry."""
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, MultiLineString):
        geom = max(geom.geoms, key=lambda g: g.length)
    if not isinstance(geom, LineString):
        return None
    coords = list(geom.coords)
    if len(coords) < 2:
        return None
    dx = coords[-1][0] - coords[0][0]
    dy = coords[-1][1] - coords[0][1]
    if abs(dx) < 1e-10 and abs(dy) < 1e-10:
        return None
    return math.atan2(dx, dy)


def _alignment_angle_deg(az1, az2):
    """Minimum angle (degrees) between two azimuths, direction-independent."""
    if az1 is None or az2 is None:
        return None
    diff = abs(az1 - az2) % math.pi
    return math.degrees(min(diff, math.pi - diff))


def load_routes(path: Path, target_crs: str) -> gpd.GeoDataFrame:
    routes = gpd.read_file(path)
    if routes.crs is None:
        raise ValueError(f"{path} is missing a CRS.")
    return routes.to_crs(target_crs)


def load_roadway_subset(bbox: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    roads = gpd.read_file(
        ROADWAY_PATH,
        layer=ROADWAY_LAYER,
        columns=ROADWAY_COLUMNS,
        bbox=bbox,
    )
    if roads.crs is None:
        roads = roads.set_crs(ROADWAY_CRS)
    elif roads.crs.to_string() != ROADWAY_CRS:
        raise ValueError(f"Unexpected roadway CRS: {roads.crs}")
    return roads


def summarize_route_families(frame: gpd.GeoDataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    counts = frame["ROUTE_FAMILY"].fillna("Unknown").value_counts().sort_index()
    return {str(key): int(value) for key, value in counts.items()}


_STATE_SYSTEM_FAMILIES = frozenset({"interstate", "us route", "u.s. route", "state route"})

_SUFFIX_TYPE_MAP = {
    "spur": "SP",
    "business": "BU",
    "connector": "CN",
    "bypass": "BY",
    "loop": "LP",
    "alternate": "AL",
}

_ROUTE_DESIGNATION_RE = re.compile(
    r"(?P<type>I|Interstate|US|SR|CR)\s*-?\s*"
    r"(?P<number>\d+)"
    r"(?:\s+(?P<suffix>North|South|East|West|Spur|Business|Connector|Bypass|Loop|Alternate))?",
    re.IGNORECASE,
)


def _parse_route_designations(
    route_name: str,
) -> list[tuple[str, int, str | None]]:
    """Parse evacuation ROUTE_NAME into (route_type, number, suffix) tuples."""
    if not isinstance(route_name, str):
        return []
    normalized = re.sub(r"\s+", " ", route_name).strip()
    if not normalized:
        return []
    results: list[tuple[str, int, str | None]] = []
    for part in normalized.split("/"):
        m = _ROUTE_DESIGNATION_RE.search(part.strip())
        if m is None:
            continue
        raw_type = m.group("type").upper()
        if raw_type in ("I", "INTERSTATE"):
            route_type = "I"
        elif raw_type == "US":
            route_type = "US"
        elif raw_type == "SR":
            route_type = "SR"
        elif raw_type == "CR":
            route_type = "CR"
        else:
            continue
        number = int(m.group("number"))
        suffix_raw = m.group("suffix")
        suffix: str | None = None
        if suffix_raw and suffix_raw.lower() in _SUFFIX_TYPE_MAP:
            suffix = suffix_raw.capitalize()
        results.append((route_type, number, suffix))
    return results


_LOCAL_ROUTE_TYPES = frozenset({"CR", "CS"})


def _attribute_prefilter_mask(
    segments: gpd.GeoDataFrame,
    designations: list[tuple[str, int, str | None]],
) -> pd.Series:
    """Build boolean mask excluding Local/Other (CR/CS) segments.

    CR/CS segments are re-included when a parsed designation explicitly
    targets them (e.g., CR 780 is a legitimate evac route).
    """
    if "ROUTE_TYPE_GDOT" not in segments.columns:
        return pd.Series(True, index=segments.index)

    local_designations = [
        (rt, num, sfx) for rt, num, sfx in designations if rt in _LOCAL_ROUTE_TYPES
    ]
    mask = ~segments["ROUTE_TYPE_GDOT"].isin(_LOCAL_ROUTE_TYPES)
    if local_designations and "BASE_ROUTE_NUMBER" in segments.columns:
        for route_type, number, _suffix in local_designations:
            mask |= (
                (segments["ROUTE_TYPE_GDOT"] == route_type)
                & (segments["BASE_ROUTE_NUMBER"] == number)
            )
    return mask


def _specific_designation_mask(
    segments: gpd.GeoDataFrame,
    designations: list[tuple[str, int, str | None]],
) -> pd.Series:
    """Build boolean mask for segments matching a specific designation."""
    mask = pd.Series(False, index=segments.index)
    has_hwy = "HWY_NAME" in segments.columns
    has_rt = "ROUTE_TYPE_GDOT" in segments.columns
    has_brn = "BASE_ROUTE_NUMBER" in segments.columns
    for route_type, number, suffix in designations:
        if route_type == "I":
            if has_hwy:
                if suffix:
                    suffix_upper = suffix.upper()
                    prefix = f"I-{number}"
                    mask |= (
                        segments["HWY_NAME"].str.startswith(prefix, na=False)
                        & segments["HWY_NAME"].str.upper().str.contains(
                            suffix_upper, na=False
                        )
                    )
                else:
                    prefix = f"I-{number}"
                    mask |= segments["HWY_NAME"].str.startswith(prefix, na=False)
        else:
            if has_rt and has_brn:
                type_set = {route_type}
                if suffix:
                    gdot_suffix = _SUFFIX_TYPE_MAP.get(suffix.lower())
                    if gdot_suffix:
                        type_set.add(gdot_suffix)
                mask |= (
                    segments["ROUTE_TYPE_GDOT"].isin(type_set)
                    & (segments["BASE_ROUTE_NUMBER"] == number)
                )
    return mask


def _parse_expected_family(route_name: str) -> str | None:
    """Infer roadway family from an evacuation route name."""
    if not isinstance(route_name, str):
        return None
    normalized = re.sub(r"\s+", " ", route_name).strip().lower()
    if not normalized:
        return None
    if normalized.startswith(("i ", "i-", "interstate")):
        return "Interstate"
    if normalized.startswith(("us ", "us-")):
        return "US Route"
    if normalized.startswith(("sr ", "sr-", "state route")):
        return "State Route"
    return None


def flag_matches(
    roads: gpd.GeoDataFrame,
    routes: gpd.GeoDataFrame,
    keep_columns: list[str],
    *,
    route_name_field: str | None = None,
    enforce_route_family: bool = False,
    interstate_only: bool = False,
    attribute_prefilter: bool = False,
    buffer_m: float | None = None,
    min_overlap_m: float | None = None,
    min_ratio: float | None = None,
    short_min_ratio: float | None = None,
    max_alignment_deg: float | None = None,
) -> gpd.GeoDataFrame:
    # Resolve per-call threshold overrides (fall back to module-level defaults)
    eff_buffer_m = buffer_m if buffer_m is not None else MATCH_BUFFER_M
    eff_min_overlap_m = min_overlap_m if min_overlap_m is not None else NORMAL_MIN_OVERLAP_M
    eff_min_ratio = min_ratio if min_ratio is not None else NORMAL_MIN_RATIO
    eff_short_min_ratio = short_min_ratio if short_min_ratio is not None else SHORT_SEGMENT_MIN_RATIO
    eff_max_alignment_deg = max_alignment_deg if max_alignment_deg is not None else MAX_ALIGNMENT_ANGLE_DEG

    eligible = roads
    if interstate_only and "ROUTE_FAMILY" in roads.columns:
        eligible = roads.loc[roads["ROUTE_FAMILY"] == "Interstate"]
    elif attribute_prefilter and route_name_field and route_name_field in routes.columns:
        all_designations: list[tuple[str, int, str | None]] = []
        for name in routes[route_name_field].dropna():
            all_designations.extend(_parse_route_designations(str(name)))
        if all_designations:
            attr_mask = _attribute_prefilter_mask(roads, all_designations)
            eligible = roads.loc[attr_mask]
            print(
                f"Attribute prefilter: {attr_mask.sum()} of {len(roads)} "
                f"segments eligible"
            )
    if eligible.empty:
        empty = roads.iloc[0:0][keep_columns + ["geometry"]].copy()
        empty["overlap_m"] = pd.Series(dtype="float64")
        empty["overlap_ratio"] = pd.Series(dtype="float64")
        return empty

    routes_indexed = routes[["geometry"]].copy()
    if route_name_field and route_name_field in routes.columns:
        routes_indexed[route_name_field] = routes[route_name_field].values
    routes_indexed = routes_indexed.reset_index(drop=True).rename_axis("route_idx").reset_index()
    original_route_geoms = routes_indexed.set_index("route_idx").geometry
    buffered = routes_indexed.copy()
    buffered["geometry"] = buffered.geometry.buffer(eff_buffer_m)
    candidates = gpd.sjoin(
        eligible,
        buffered[["route_idx", "geometry"] + ([route_name_field] if route_name_field and route_name_field in buffered.columns else [])],
        how="inner",
        predicate="intersects",
    )

    if candidates.empty:
        empty = roads.iloc[0:0][keep_columns + ["geometry"]].copy()
        empty["overlap_m"] = pd.Series(dtype="float64")
        empty["overlap_ratio"] = pd.Series(dtype="float64")
        return empty

    buffered_geoms = buffered.set_index("route_idx").geometry
    accepted_rows = []
    for seg_idx, group in candidates.groupby(candidates.index):
        seg_geom = eligible.loc[seg_idx, "geometry"]
        seg_length = (
            float(eligible.loc[seg_idx, "segment_length_m"])
            if "segment_length_m" in eligible.columns and pd.notna(eligible.loc[seg_idx, "segment_length_m"])
            else float(seg_geom.length)
        )
        if seg_length <= 0:
            seg_length = float(seg_geom.length)

        seg_family = None
        if "ROUTE_FAMILY" in eligible.columns:
            raw = eligible.loc[seg_idx, "ROUTE_FAMILY"]
            seg_family = str(raw).strip() if pd.notna(raw) else None

        best_overlap_m = 0.0
        best_overlap_ratio = 0.0
        for _, row in group.iterrows():
            if enforce_route_family and route_name_field and route_name_field in row.index:
                rn = row[route_name_field]
                if pd.notna(rn):
                    expected = _parse_expected_family(str(rn))
                    if expected is not None:
                        seg_fam_lower = seg_family.lower() if seg_family else ""
                        if seg_fam_lower not in _STATE_SYSTEM_FAMILIES:
                            continue

            corridor = buffered_geoms.loc[row["route_idx"]]
            try:
                overlap_geom = seg_geom.intersection(corridor)
                overlap_len = float(overlap_geom.length)
            except Exception:
                overlap_geom = None
                overlap_len = 0.0

            overlap_ratio = overlap_len / seg_length if seg_length > 0 else 0.0
            is_short = seg_length < SHORT_SEGMENT_MAX_M
            is_mega = seg_length > MEGA_SEGMENT_LENGTH_M
            if is_short:
                accepted = overlap_ratio >= eff_short_min_ratio
            elif is_mega:
                accepted = (
                    overlap_len >= MEGA_MIN_OVERLAP_M
                    and overlap_ratio >= MEGA_MIN_RATIO
                )
                # Fallback: accept mega-segments whose clipped-to-corridor
                # portion meets the normal absolute threshold.
                if not accepted and overlap_len >= eff_min_overlap_m:
                    accepted = True
            else:
                accepted = (
                    overlap_len >= eff_min_overlap_m
                    and overlap_ratio >= eff_min_ratio
                )

            if accepted and not is_short and overlap_geom is not None:
                route_geom = original_route_geoms.loc[row["route_idx"]]
                try:
                    route_section = route_geom.intersection(
                        seg_geom.buffer(eff_buffer_m)
                    )
                except Exception:
                    route_section = None
                seg_az = _line_azimuth(overlap_geom)
                route_az = _line_azimuth(route_section)
                angle = _alignment_angle_deg(seg_az, route_az)
                if angle is not None and angle > eff_max_alignment_deg:
                    accepted = False

            if not accepted:
                continue

            best_overlap_m = max(best_overlap_m, overlap_len)
            best_overlap_ratio = max(best_overlap_ratio, overlap_ratio)

        if best_overlap_m > 0:
            uid = eligible.loc[seg_idx, "unique_id"]
            accepted_rows.append({
                "unique_id": uid,
                "overlap_m": best_overlap_m,
                "overlap_ratio": best_overlap_ratio,
            })

    if not accepted_rows:
        empty = roads.iloc[0:0][keep_columns + ["geometry"]].copy()
        empty["overlap_m"] = pd.Series(dtype="float64")
        empty["overlap_ratio"] = pd.Series(dtype="float64")
        return empty

    accepted_df = pd.DataFrame(accepted_rows)
    flagged = eligible.merge(accepted_df, on="unique_id", how="inner")
    out_cols = [c for c in keep_columns if c in flagged.columns] + ["overlap_m", "overlap_ratio", "geometry"]
    return flagged[out_cols].sort_values("unique_id").reset_index(drop=True)


def build_context_layer(
    roads: gpd.GeoDataFrame,
    flagged_ids: set[str],
    bbox: tuple[float, float, float, float],
) -> gpd.GeoDataFrame:
    bbox_polygon = box(*bbox)
    candidates = roads.loc[
        ~roads["unique_id"].isin(flagged_ids) & roads.geometry.intersects(bbox_polygon),
        CONTEXT_COLUMNS,
    ].copy()
    if len(candidates) > CONTEXT_SAMPLE_SIZE:
        candidates = candidates.sample(n=CONTEXT_SAMPLE_SIZE, random_state=RANDOM_STATE)
    return candidates.sort_values("unique_id").reset_index(drop=True)


def export_geojson(frame: gpd.GeoDataFrame, path: Path) -> None:
    export_frame = frame.to_crs(WEB_CRS)
    export_frame.to_file(path, driver="GeoJSON")


def html_template(summary: dict[str, object]) -> str:
    summary_json = json.dumps(summary, indent=2)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GDOT Evacuation Route QC Map</title>
  <link
    rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
    crossorigin=""
  >
  <style>
    html, body, #map {{
      height: 100%;
      margin: 0;
      font-family: Arial, sans-serif;
    }}
    .title-banner {{
      position: absolute;
      top: 12px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 1000;
      background: rgba(255, 255, 255, 0.95);
      padding: 10px 14px;
      border-radius: 6px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
      font-size: 18px;
      font-weight: 700;
    }}
    .info-box {{
      background: rgba(255, 255, 255, 0.95);
      padding: 12px 14px;
      border-radius: 6px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
      line-height: 1.4;
      min-width: 240px;
    }}
    .info-box h3 {{
      margin: 0 0 8px;
      font-size: 15px;
    }}
    .info-box .metric {{
      margin-bottom: 6px;
      font-size: 13px;
    }}
    .info-box .breakdown-title {{
      margin: 10px 0 4px;
      font-size: 13px;
      font-weight: 700;
    }}
    .info-box ul {{
      margin: 0;
      padding-left: 18px;
      font-size: 12px;
    }}
    .leaflet-popup-content {{
      min-width: 220px;
    }}
  </style>
</head>
<body>
  <div class="title-banner">GDOT Evacuation Route QC Map</div>
  <div id="map"></div>
  <script>
    const QC_SUMMARY = {summary_json};
  </script>
  <script
    src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
    crossorigin=""
  ></script>
  <script>
    const map = L.map('map');
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const infoControl = L.control({{ position: 'topright' }});
    infoControl.onAdd = function() {{
      const div = L.DomUtil.create('div', 'info-box');
      const evacBreakdown = Object.entries(QC_SUMMARY.evac_route_family_breakdown)
        .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
        .join('');
      const contraBreakdown = Object.entries(QC_SUMMARY.contraflow_route_family_breakdown)
        .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
        .join('');
      const matchMethodBreakdown = QC_SUMMARY.evac_match_method_breakdown
        ? Object.entries(QC_SUMMARY.evac_match_method_breakdown)
            .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
            .join('')
        : '';
      const corridorCounts = QC_SUMMARY.per_corridor_counts
        ? Object.entries(QC_SUMMARY.per_corridor_counts)
            .sort((a, b) => b[1] - a[1])
            .map(([name, count]) => `<li>${{name}}: ${{count}}</li>`)
            .join('')
        : '';
      div.innerHTML = `
        <h3>QC Summary</h3>
        <div class="metric"><strong>Total evacuation flagged:</strong> ${{QC_SUMMARY.total_evac_flagged}}</div>
        <div class="metric"><strong>Total contraflow flagged:</strong> ${{QC_SUMMARY.total_contraflow_flagged}}</div>
        ${{QC_SUMMARY.evac_routes_null_name_excluded ? `<div class="metric" style="color:#888"><em>Unnamed evac route features excluded from blue layer: ${{QC_SUMMARY.evac_routes_null_name_excluded}}</em></div>` : ''}}
        <div class="breakdown-title">Evacuation by ROUTE_FAMILY</div>
        <ul>${{evacBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Contraflow by ROUTE_FAMILY</div>
        <ul>${{contraBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Evac Match Method</div>
        <ul>${{matchMethodBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Per-Corridor Counts</div>
        <ul style="max-height:200px;overflow-y:auto">${{corridorCounts || '<li>None</li>'}}</ul>
      `;
      return div;
    }};
    infoControl.addTo(map);

    function formatValue(value) {{
      if (value === null || value === undefined || value === '') {{
        return 'N/A';
      }}
      return value;
    }}

    function makePopup(properties, fields) {{
      return fields
        .map(([label, field]) => `<div><strong>${{label}}:</strong> ${{formatValue(properties[field])}}</div>`)
        .join('');
    }}

    function styleFactory(style) {{
      return function() {{
        return style;
      }};
    }}

    Promise.all([
      fetch('network_context.geojson').then((r) => r.json()),
      fetch('evac_routes_official.geojson').then((r) => r.json()),
      fetch('contraflow_routes_official.geojson').then((r) => r.json()),
      fetch('network_evac_flagged.geojson').then((r) => r.json()),
      fetch('network_contraflow_flagged.geojson').then((r) => r.json())
    ]).then(([contextData, evacRoutesData, contraRoutesData, evacFlaggedData, contraFlaggedData]) => {{
      const contextLayer = L.geoJSON(contextData, {{
        style: styleFactory({{ color: '#999999', weight: 1, opacity: 0.4 }})
      }});

      const evacRoutesLayer = L.geoJSON(evacRoutesData, {{
        style: styleFactory({{ color: '#2196F3', weight: 3, opacity: 0.6 }}),
        onEachFeature: (feature, layer) => {{
          layer.bindPopup(makePopup(feature.properties, [['ROUTE_NAME', 'ROUTE_NAME']]));
        }}
      }});

      const contraRoutesLayer = L.geoJSON(contraRoutesData, {{
        style: styleFactory({{ color: '#9C27B0', weight: 5, opacity: 0.8 }}),
        onEachFeature: (feature, layer) => {{
          layer.bindPopup(makePopup(feature.properties, [['TITLE', 'TITLE'], ['DESCRIPTION', 'DESCRIPTION']]));
        }}
      }});

      const evacFlaggedLayer = L.geoJSON(evacFlaggedData, {{
        style: styleFactory({{ color: '#d32f2f', weight: 5, opacity: 0.9 }}),
        onEachFeature: (feature, layer) => {{
          const p = feature.properties;
          const ratio = p.overlap_ratio != null ? (p.overlap_ratio * 100).toFixed(1) + '%' : 'N/A';
          const overlapM = p.overlap_m != null ? Math.round(p.overlap_m) + ' m' : 'N/A';
          const portion = p.match_portion != null ? (p.match_portion * 100).toFixed(1) + '%' : 'N/A';
          layer.bindPopup(
            makePopup(p, [
              ['HWY_NAME', 'HWY_NAME'],
              ['ROUTE_FAMILY', 'ROUTE_FAMILY'],
              ['Evac Route', 'SEC_EVAC_ROUTE_NAME'],
              ['AADT', 'AADT'],
              ['COUNTY_NAME', 'COUNTY_NAME'],
              ['DISTRICT_NAME', 'DISTRICT_NAME'],
              ['Match Method', 'match_method']
            ]) + `<div><strong>Overlap:</strong> ${{overlapM}} (${{ratio}})</div><div><strong>Match portion (clipped/seg):</strong> ${{portion}}</div>`
          );
        }}
      }});

      const contraFlaggedLayer = L.geoJSON(contraFlaggedData, {{
        style: styleFactory({{ color: '#f57c00', weight: 4, opacity: 0.8 }}),
        onEachFeature: (feature, layer) => {{
          const p = feature.properties;
          const ratio = p.overlap_ratio != null ? (p.overlap_ratio * 100).toFixed(1) + '%' : 'N/A';
          const overlapM = p.overlap_m != null ? Math.round(p.overlap_m) + ' m' : 'N/A';
          layer.bindPopup(
            makePopup(p, [
              ['HWY_NAME', 'HWY_NAME'],
              ['ROUTE_FAMILY', 'ROUTE_FAMILY']
            ]) + `<div><strong>Overlap:</strong> ${{overlapM}} (${{ratio}})</div>`
          );
        }}
      }});

      const overlays = {{
        'Road Network (context)': contextLayer,
        'GDOT Evacuation Routes (official)': evacRoutesLayer,
        'GDOT Contraflow Routes (official)': contraRoutesLayer,
        'Flagged Segments (evacuation)': evacFlaggedLayer,
        'Flagged Segments (contraflow)': contraFlaggedLayer
      }};

      evacFlaggedLayer.addTo(map);

      L.control.layers(null, overlays, {{ collapsed: false }}).addTo(map);
      const bounds = evacFlaggedLayer.getBounds();
      if (bounds.isValid()) {{
        map.fitBounds(bounds.pad(0.08));
      }} else {{
        map.setView([32.5, -83.5], 7);
      }}

      // --- Per-corridor dropdown filter ---
      const corridorControl = L.control({{ position: 'topleft' }});
      corridorControl.onAdd = function() {{
        const div = L.DomUtil.create('div', 'info-box');
        div.style.marginTop = '60px';
        const label = L.DomUtil.create('div', '', div);
        label.innerHTML = '<strong style="font-size:13px">Filter by Corridor</strong>';
        const select = L.DomUtil.create('select', '', div);
        select.id = 'corridor-filter';
        select.style.cssText = 'width:100%;margin-top:4px;padding:3px;font-size:13px';
        select.innerHTML = '<option value="">All corridors</option>';

        const corridorNames = new Set();
        evacFlaggedLayer.eachLayer(layer => {{
          const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME;
          if (name) {{
            name.split('; ').forEach(n => corridorNames.add(n));
          }}
        }});
        [...corridorNames].sort().forEach(name => {{
          select.innerHTML += `<option value="${{name}}">${{name}}</option>`;
        }});

        select.onchange = function() {{
          filterByCorridor(this.value);
        }};
        L.DomEvent.disableClickPropagation(div);
        return div;
      }};
      corridorControl.addTo(map);

      window.filterByCorridor = function(corridorName) {{
        // Filter flagged segments (red)
        evacFlaggedLayer.eachLayer(layer => {{
          const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
          const visible = !corridorName || name.includes(corridorName);
          if (visible) {{
            layer.setStyle({{ opacity: 0.9, weight: 5 }});
          }} else {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }}
        }});

        // Filter official evac routes (blue)
        evacRoutesLayer.eachLayer(layer => {{
          const rn = layer.feature.properties.ROUTE_NAME || '';
          const visible = !corridorName || rn === corridorName;
          if (visible) {{
            layer.setStyle({{ opacity: 0.6, weight: 3 }});
          }} else {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }}
        }});

        // Filter contraflow routes (purple) — hide when filtering
        contraRoutesLayer.eachLayer(layer => {{
          if (corridorName) {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }} else {{
            layer.setStyle({{ opacity: 0.8, weight: 5 }});
          }}
        }});

        // Filter contraflow flagged segments (orange) — hide when filtering
        contraFlaggedLayer.eachLayer(layer => {{
          if (corridorName) {{
            layer.setStyle({{ opacity: 0, weight: 0 }});
          }} else {{
            layer.setStyle({{ opacity: 0.8, weight: 4 }});
          }}
        }});

        if (corridorName) {{
          const cbounds = L.latLngBounds();
          evacFlaggedLayer.eachLayer(layer => {{
            const name = layer.feature.properties.SEC_EVAC_ROUTE_NAME || '';
            if (name.includes(corridorName)) {{
              cbounds.extend(layer.getBounds());
            }}
          }});
          // Also include the official route bounds
          evacRoutesLayer.eachLayer(layer => {{
            const rn = layer.feature.properties.ROUTE_NAME || '';
            if (rn === corridorName) {{
              cbounds.extend(layer.getBounds());
            }}
          }});
          if (cbounds.isValid()) {{
            map.fitBounds(cbounds.pad(0.1));
          }}
          document.querySelector('.title-banner').textContent =
            `GDOT Evacuation Route QC — ${{corridorName}}`;
          contextLayer.remove();
        }} else {{
          const allBounds = evacRoutesLayer.getBounds();
          if (allBounds.isValid()) map.fitBounds(allBounds.pad(0.08));
          document.querySelector('.title-banner').textContent =
            'GDOT Evacuation Route QC Map';
          contextLayer.addTo(map);
        }}
      }};
    }}).catch((error) => {{
      console.error(error);
      alert(`Failed to load one or more QC layers: ${{error.message}}`);
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    evac_routes = load_routes(EVAC_PATH, ROADWAY_CRS)
    contraflow_routes = load_routes(CONTRAFLOW_PATH, ROADWAY_CRS)

    evac_bounds = evac_routes.total_bounds
    contraflow_bounds = contraflow_routes.total_bounds
    roadway_bounds = (
        min(evac_bounds[0], contraflow_bounds[0]),
        min(evac_bounds[1], contraflow_bounds[1]),
        max(evac_bounds[2], contraflow_bounds[2]),
        max(evac_bounds[3], contraflow_bounds[3]),
    )
    roads = load_roadway_subset(roadway_bounds)

    evac_keep_cols = ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "AADT", "DISTRICT_NAME", "COUNTY_NAME"]

    # --- Per-corridor evacuation matching via shared module ---
    evac_matches, evac_diagnostics = per_corridor_evac_overlay(
        roads, evac_routes, name_field="ROUTE_NAME",
    )

    # Convert results dict to GeoDataFrame for export. Use the clipped-to-
    # corridor geometry so partial-follow overshoots render only the in-
    # corridor portion — the original full-segment geometry is only used
    # as a fallback when the matcher didn't produce a clip.
    if evac_matches:
        rows = []
        for idx, match in evac_matches.items():
            row = {col: roads.at[idx, col] for col in evac_keep_cols if col in roads.columns}
            row["geometry"] = match.get("clipped_geom") or roads.at[idx, "geometry"]
            row["overlap_m"] = match["overlap_m"]
            row["overlap_ratio"] = match["overlap_ratio"]
            row["match_method"] = match["match_method"]
            row["match_portion"] = match.get("match_portion", 1.0)
            row["SEC_EVAC_ROUTE_NAME"] = "; ".join(sorted(set(match["names"]))) if match["names"] else None
            rows.append(row)
        evac_flagged = gpd.GeoDataFrame(rows, crs=roads.crs)
        evac_flagged = evac_flagged.sort_values("unique_id").reset_index(drop=True)
    else:
        evac_flagged = gpd.GeoDataFrame(
            columns=evac_keep_cols + ["overlap_m", "overlap_ratio", "match_method", "match_portion", "SEC_EVAC_ROUTE_NAME", "geometry"]
        )

    # Print per-corridor diagnostics
    print(f"\n=== Per-Corridor Diagnostics ===")
    print(f"Total matched: {evac_diagnostics.get('total_matched', 0)}")
    print(f"Match methods: {json.dumps(evac_diagnostics.get('match_method_breakdown', {}), indent=2)}")
    print(f"Multi-corridor segments: {evac_diagnostics.get('multi_corridor_segments', 0)}")
    print(f"Concurrent fallback: {evac_diagnostics.get('concurrent_fallback_matches', 0)}")
    print(f"Null-feature matches: {evac_diagnostics.get('null_feature_matches', 0)}")
    zero_corridors = evac_diagnostics.get("corridors_with_zero_matches", [])
    if zero_corridors:
        print(f"ALERT — corridors with zero matches: {', '.join(zero_corridors)}")
    per_corridor = evac_diagnostics.get("per_corridor_counts", {})
    low = {k: v for k, v in per_corridor.items() if 0 < v < 3}
    if low:
        print(f"WARNING — corridors with <3 matches: {low}")

    contraflow_flagged = flag_matches(
        roads,
        contraflow_routes,
        ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "AADT", "DISTRICT_NAME", "COUNTY_NAME"],
        route_name_field="TITLE",
        interstate_only=True,
        buffer_m=CONTRAFLOW_BUFFER_M,
        min_overlap_m=CONTRAFLOW_MIN_OVERLAP_M,
        min_ratio=CONTRAFLOW_MIN_RATIO,
        short_min_ratio=CONTRAFLOW_SHORT_MIN_RATIO,
        max_alignment_deg=CONTRAFLOW_MAX_ALIGNMENT_ANGLE_DEG,
    )

    # Corridor proximity post-filter for contraflow
    if not contraflow_flagged.empty:
        contra_corridor = contraflow_routes.geometry.buffer(CONTRAFLOW_BUFFER_M).union_all()
        inside_lens = contraflow_flagged.geometry.intersection(contra_corridor).length
        seg_lens = contraflow_flagged.geometry.length
        inside_ratios = inside_lens / seg_lens.replace(0, 1)
        before = len(contraflow_flagged)
        contraflow_flagged = contraflow_flagged[inside_ratios >= CONTRAFLOW_MIN_INSIDE_CORRIDOR_RATIO].reset_index(drop=True)
        removed = before - len(contraflow_flagged)
        if removed > 0:
            print(f"Contraflow corridor proximity filter removed {removed} segments (kept {len(contraflow_flagged)})")

    flagged_ids = set(evac_flagged["unique_id"]).union(set(contraflow_flagged["unique_id"]))
    context = build_context_layer(roads, flagged_ids, tuple(evac_bounds))

    evac_routes_export = evac_routes.drop(columns=["_designations"], errors="ignore")
    # Label null-name features so they are visible on the QC map
    null_name_mask = evac_routes_export["ROUTE_NAME"].isna() | (evac_routes_export["ROUTE_NAME"] == "")
    null_name_count = int(null_name_mask.sum())
    if null_name_count:
        evac_routes_export.loc[null_name_mask, "ROUTE_NAME"] = "(unnamed)"
        print(f"Including {null_name_count} null-name evac route features on QC map blue layer as '(unnamed)'")
    export_geojson(evac_routes_export, OUTPUT_DIR / "evac_routes_official.geojson")
    export_geojson(contraflow_routes, OUTPUT_DIR / "contraflow_routes_official.geojson")
    export_geojson(evac_flagged, OUTPUT_DIR / "network_evac_flagged.geojson")
    export_geojson(contraflow_flagged, OUTPUT_DIR / "network_contraflow_flagged.geojson")
    export_geojson(context, OUTPUT_DIR / "network_context.geojson")

    match_method_breakdown = {}
    if "match_method" in evac_flagged.columns:
        counts = evac_flagged["match_method"].fillna("unknown").value_counts()
        match_method_breakdown = {str(k): int(v) for k, v in counts.items()}

    summary = {
        "total_evac_flagged": int(len(evac_flagged)),
        "total_contraflow_flagged": int(len(contraflow_flagged)),
        "evac_routes_null_name_excluded": null_name_count,
        "evac_route_family_breakdown": summarize_route_families(evac_flagged),
        "contraflow_route_family_breakdown": summarize_route_families(contraflow_flagged),
        "evac_match_method_breakdown": match_method_breakdown,
        "per_corridor_counts": evac_diagnostics.get("per_corridor_counts", {}),
    }
    (OUTPUT_DIR / "index.html").write_text(html_template(summary), encoding="utf-8")

    # Run automated QC checks
    passed, qc_errors, qc_warnings = run_automated_qc(
        evac_diagnostics, int(len(contraflow_flagged)),
    )
    for msg in qc_errors:
        print(msg)
    for msg in qc_warnings:
        print(msg)
    if passed:
        print("Automated QC checks PASSED")
    else:
        print("Automated QC checks FAILED — review errors above")

    report = {
        "evac_routes_official": int(len(evac_routes)),
        "contraflow_routes_official": int(len(contraflow_routes)),
        "network_evac_flagged": int(len(evac_flagged)),
        "network_contraflow_flagged": int(len(contraflow_flagged)),
        "network_context": int(len(context)),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
