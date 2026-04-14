from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
from shapely.geometry import box


OUTPUT_DIR = Path(r"D:\Jacobs\Georgia-Statewide-Data-Pipeline\02-Data-Staging\qc\evacuation_route_qc")
ROADWAY_PATH = Path(r"D:\Jacobs\Georgia-Statewide-Data-Pipeline\02-Data-Staging\spatial\base_network.gpkg")
ROADWAY_LAYER = "roadway_segments"
EVAC_PATH = Path(r"D:\Jacobs\Georgia-Statewide-Data-Pipeline\02-Data-Staging\spatial\ga_evac_routes.geojson")
CONTRAFLOW_PATH = Path(r"D:\Jacobs\Georgia-Statewide-Data-Pipeline\02-Data-Staging\spatial\ga_contraflow_routes.geojson")

ROADWAY_CRS = "EPSG:32617"
WEB_CRS = "EPSG:4326"
OVERLAP_THRESHOLD_M = 200.0
MATCH_BUFFER_M = 30.0
CONTEXT_SAMPLE_SIZE = 5000
RANDOM_STATE = 42
ROADWAY_COLUMNS = [
    "unique_id",
    "HWY_NAME",
    "ROUTE_FAMILY",
    "AADT",
    "DISTRICT_LABEL",
    "COUNTY_NAME",
    "geometry",
]
CONTEXT_COLUMNS = ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "geometry"]


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


def flag_matches(
    roads: gpd.GeoDataFrame,
    routes: gpd.GeoDataFrame,
    keep_columns: list[str],
) -> gpd.GeoDataFrame:
    routes_indexed = routes[["geometry"]].copy()
    routes_indexed = routes_indexed.reset_index(drop=True).rename_axis("route_idx").reset_index()
    # Buffer routes into corridor polygons for the sjoin — polyline-to-polyline
    # intersection returns zero-length point contacts, not measurable overlap.
    buffered = routes_indexed.copy()
    buffered["geometry"] = buffered.geometry.buffer(MATCH_BUFFER_M)
    candidates = gpd.sjoin(
        roads,
        buffered[["route_idx", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    if candidates.empty:
        return roads.iloc[0:0][keep_columns + ["geometry"]].assign(overlap_m=[])

    # Measure overlap against the BUFFERED corridor (not the raw polyline)
    buffered_geoms = buffered.set_index("route_idx").geometry
    candidates["overlap_m"] = candidates.apply(
        lambda row: float(row.geometry.intersection(buffered_geoms.loc[row["route_idx"]]).length),
        axis=1,
    )
    candidates = candidates[candidates["overlap_m"] >= OVERLAP_THRESHOLD_M].copy()

    if candidates.empty:
        return roads.iloc[0:0][keep_columns + ["geometry"]].assign(overlap_m=[])

    aggregated = (
        candidates.groupby("unique_id", as_index=False)
        .agg({"overlap_m": "sum"})
        .rename(columns={"overlap_m": "overlap_m"})
    )
    flagged = roads.merge(aggregated, on="unique_id", how="inner")
    return flagged[keep_columns + ["overlap_m", "geometry"]].sort_values("unique_id").reset_index(drop=True)


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
      div.innerHTML = `
        <h3>QC Summary</h3>
        <div class="metric"><strong>Total evacuation flagged:</strong> ${{QC_SUMMARY.total_evac_flagged}}</div>
        <div class="metric"><strong>Total contraflow flagged:</strong> ${{QC_SUMMARY.total_contraflow_flagged}}</div>
        <div class="breakdown-title">Evacuation by ROUTE_FAMILY</div>
        <ul>${{evacBreakdown || '<li>None</li>'}}</ul>
        <div class="breakdown-title">Contraflow by ROUTE_FAMILY</div>
        <ul>${{contraBreakdown || '<li>None</li>'}}</ul>
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
        style: styleFactory({{ color: '#2196F3', weight: 4, opacity: 0.8 }}),
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
        style: styleFactory({{ color: '#d32f2f', weight: 3, opacity: 0.7 }}),
        onEachFeature: (feature, layer) => {{
          layer.bindPopup(makePopup(feature.properties, [
            ['HWY_NAME', 'HWY_NAME'],
            ['ROUTE_FAMILY', 'ROUTE_FAMILY'],
            ['AADT', 'AADT'],
            ['overlap_m', 'overlap_m'],
            ['COUNTY_NAME', 'COUNTY_NAME'],
            ['DISTRICT_LABEL', 'DISTRICT_LABEL']
          ]));
        }}
      }});

      const contraFlaggedLayer = L.geoJSON(contraFlaggedData, {{
        style: styleFactory({{ color: '#f57c00', weight: 4, opacity: 0.8 }}),
        onEachFeature: (feature, layer) => {{
          layer.bindPopup(makePopup(feature.properties, [
            ['HWY_NAME', 'HWY_NAME'],
            ['ROUTE_FAMILY', 'ROUTE_FAMILY'],
            ['overlap_m', 'overlap_m']
          ]));
        }}
      }});

      const overlays = {{
        'Road Network (context)': contextLayer,
        'GDOT Evacuation Routes (official)': evacRoutesLayer,
        'GDOT Contraflow Routes (official)': contraRoutesLayer,
        'Flagged Segments (evacuation)': evacFlaggedLayer,
        'Flagged Segments (contraflow)': contraFlaggedLayer
      }};

      contextLayer.addTo(map);
      evacRoutesLayer.addTo(map);
      contraRoutesLayer.addTo(map);
      evacFlaggedLayer.addTo(map);
      contraFlaggedLayer.addTo(map);

      L.control.layers(null, overlays, {{ collapsed: false }}).addTo(map);
      const bounds = evacRoutesLayer.getBounds();
      if (bounds.isValid()) {{
        map.fitBounds(bounds.pad(0.08));
      }} else {{
        map.setView([32.5, -83.5], 7);
      }}
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

    evac_flagged = flag_matches(
        roads,
        evac_routes,
        ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "AADT", "DISTRICT_LABEL", "COUNTY_NAME"],
    )
    contraflow_flagged = flag_matches(
        roads,
        contraflow_routes,
        ["unique_id", "HWY_NAME", "ROUTE_FAMILY", "AADT", "DISTRICT_LABEL", "COUNTY_NAME"],
    )

    flagged_ids = set(evac_flagged["unique_id"]).union(set(contraflow_flagged["unique_id"]))
    context = build_context_layer(roads, flagged_ids, tuple(evac_bounds))

    export_geojson(evac_routes, OUTPUT_DIR / "evac_routes_official.geojson")
    export_geojson(contraflow_routes, OUTPUT_DIR / "contraflow_routes_official.geojson")
    export_geojson(evac_flagged, OUTPUT_DIR / "network_evac_flagged.geojson")
    export_geojson(contraflow_flagged, OUTPUT_DIR / "network_contraflow_flagged.geojson")
    export_geojson(context, OUTPUT_DIR / "network_context.geojson")

    summary = {
        "total_evac_flagged": int(len(evac_flagged)),
        "total_contraflow_flagged": int(len(contraflow_flagged)),
        "evac_route_family_breakdown": summarize_route_families(evac_flagged),
        "contraflow_route_family_breakdown": summarize_route_families(contraflow_flagged),
    }
    (OUTPUT_DIR / "index.html").write_text(html_template(summary), encoding="utf-8")

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
