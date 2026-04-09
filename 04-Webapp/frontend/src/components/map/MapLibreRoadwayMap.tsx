import { useEffect, useEffectEvent, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import { getRoadwayDetail } from "../../services/api";
import { GeoJsonFeatureCollection, RoadwayDetail, RoadwayFeatureCollection } from "../../types/api";

const DISTRICT_SOURCE_ID = "district-boundaries";
const DISTRICT_FILL_LAYER_ID = "district-boundaries-fill";
const DISTRICT_LINE_LAYER_ID = "district-boundaries-line";
const COUNTY_SOURCE_ID = "county-boundaries";
const COUNTY_LINE_LAYER_ID = "county-boundaries-line";
const SOURCE_ID = "roadways";
const CASING_LAYER_ID = "roadways-casing";
const LINE_LAYER_ID = "roadways-line";
const HIT_LAYER_ID = "roadways-hit";

interface MapLibreRoadwayMapProps {
  roadwayChunks: RoadwayFeatureCollection[];
  countyBoundaries?: GeoJsonFeatureCollection;
  districtBoundaries?: GeoJsonFeatureCollection;
  loadToken: number;
  bounds?: [number, number, number, number] | null;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatPopupValue(value: string | number | boolean | null): string {
  if (value === null) {
    return "N/A";
  }

  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }

  return String(value);
}

function displayDistrictLabel(label: string): string {
  const separatorIndex = label.indexOf(" - ");
  return separatorIndex >= 0 ? label.slice(separatorIndex + 3) : label;
}

function formatCountyMeta(county: string, countyAll?: string | null): string {
  if (!countyAll) {
    return `${county} County`;
  }

  const normalizedCountyAll = countyAll.trim();
  if (!normalizedCountyAll) {
    return `${county} County`;
  }

  if (normalizedCountyAll.toLowerCase() === county.trim().toLowerCase()) {
    return `${county} County`;
  }

  return `Counties: ${normalizedCountyAll}`;
}

function buildLoadingPopupHtml(summary: {
  roadName: string;
  county: string;
  districtLabel: string;
}): string {
  return `
    <div class="roadway-popup">
      <div class="roadway-popup__header">
        <div class="roadway-popup__title">${escapeHtml(summary.roadName)}</div>
        <div class="roadway-popup__meta">
          ${escapeHtml(summary.county)} County | ${escapeHtml(displayDistrictLabel(summary.districtLabel))}
        </div>
      </div>
      <div class="roadway-popup__status">Loading segment details...</div>
    </div>
  `.trim();
}

function buildErrorPopupHtml(summary: {
  roadName: string;
  county: string;
  districtLabel: string;
}): string {
  return `
    <div class="roadway-popup">
      <div class="roadway-popup__header">
        <div class="roadway-popup__title">${escapeHtml(summary.roadName)}</div>
        <div class="roadway-popup__meta">
          ${escapeHtml(summary.county)} County | ${escapeHtml(displayDistrictLabel(summary.districtLabel))}
        </div>
      </div>
      <div class="roadway-popup__status roadway-popup__status--error">
        Segment details could not be loaded.
      </div>
    </div>
  `.trim();
}

function buildRoadwayPopupHtml(detail: RoadwayDetail): string {
  const countyAll =
    typeof detail.attributes.county_all === "string" ? detail.attributes.county_all : null;
  const rows = Object.entries(detail.attributes)
    .map(
      ([key, value]) => `
        <div class="roadway-popup__row">
          <div class="roadway-popup__key">${escapeHtml(key)}</div>
          <div class="roadway-popup__value">${escapeHtml(formatPopupValue(value))}</div>
        </div>
      `,
    )
    .join("");

  return `
    <div class="roadway-popup">
      <div class="roadway-popup__header">
        <div class="roadway-popup__title">${escapeHtml(detail.road_name)}</div>
        <div class="roadway-popup__meta">
          ${escapeHtml(formatCountyMeta(detail.county, countyAll))} | ${escapeHtml(displayDistrictLabel(detail.district_label))}
        </div>
      </div>
      <div class="roadway-popup__body">
        ${rows}
      </div>
    </div>
  `.trim();
}

function buildEmptyCollection(): RoadwayFeatureCollection {
  return {
    type: "FeatureCollection",
    features: [],
  };
}

function combineRoadwayChunks(chunks: RoadwayFeatureCollection[]): RoadwayFeatureCollection {
  if (chunks.length === 0) {
    return buildEmptyCollection();
  }

  return {
    type: "FeatureCollection",
    features: chunks.flatMap((chunk) => chunk.features),
  };
}

function getRoadwaySource(map: maplibregl.Map): maplibregl.GeoJSONSource | null {
  const source = map.getSource(SOURCE_ID);
  if (!source || typeof (source as maplibregl.GeoJSONSource).setData !== "function") {
    return null;
  }

  return source as maplibregl.GeoJSONSource;
}

function getGeoJsonSource(
  map: maplibregl.Map,
  sourceId: string,
): maplibregl.GeoJSONSource | null {
  const source = map.getSource(sourceId);
  if (!source || typeof (source as maplibregl.GeoJSONSource).setData !== "function") {
    return null;
  }

  return source as maplibregl.GeoJSONSource;
}

export function MapLibreRoadwayMap({
  roadwayChunks,
  countyBoundaries,
  districtBoundaries,
  loadToken,
  bounds,
}: MapLibreRoadwayMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const roadwayChunksRef = useRef<RoadwayFeatureCollection[]>(roadwayChunks);
  const countyBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(countyBoundaries);
  const districtBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(districtBoundaries);
  const boundsRef = useRef(bounds);
  const loadTokenRef = useRef(loadToken);
  const renderedLoadTokenRef = useRef(loadToken);
  const detailCacheRef = useRef<Map<string, RoadwayDetail>>(new Map());
  const detailRequestTokenRef = useRef(0);

  roadwayChunksRef.current = roadwayChunks;
  countyBoundariesRef.current = countyBoundaries;
  districtBoundariesRef.current = districtBoundaries;
  boundsRef.current = bounds;
  loadTokenRef.current = loadToken;

  const syncBounds = useEffectEvent(() => {
    const map = mapRef.current;
    if (!map || !boundsRef.current) {
      return;
    }

    map.fitBounds(boundsRef.current, {
      padding: 56,
      duration: 0,
    });
  });

  const openRoadwayPopup = useEffectEvent(
    async (
      event: maplibregl.MapLayerMouseEvent,
      properties: {
        uniqueId: string;
        roadName: string;
        county: string;
        districtLabel: string;
      },
    ) => {
      const map = mapRef.current;
      const popup = popupRef.current;

      if (!map || !popup) {
        return;
      }

      const summary = {
        roadName: properties.roadName,
        county: properties.county,
        districtLabel: properties.districtLabel,
      };

      popup
        .setLngLat(event.lngLat)
        .setHTML(buildLoadingPopupHtml(summary))
        .addTo(map);

      const cached = detailCacheRef.current.get(properties.uniqueId);
      if (cached) {
        popup.setHTML(buildRoadwayPopupHtml(cached));
        return;
      }

      detailRequestTokenRef.current += 1;
      const requestToken = detailRequestTokenRef.current;

      try {
        const detail = await getRoadwayDetail(properties.uniqueId);
        detailCacheRef.current.set(properties.uniqueId, detail);

        if (requestToken !== detailRequestTokenRef.current) {
          return;
        }

        popup.setHTML(buildRoadwayPopupHtml(detail));
      } catch {
        if (requestToken !== detailRequestTokenRef.current) {
          return;
        }

        popup.setHTML(buildErrorPopupHtml(summary));
      }
    },
  );

  const ensureRoadwayLayers = useEffectEvent(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) {
      return;
    }

    const districtData = districtBoundariesRef.current ?? buildEmptyCollection();
    const districtSource = getGeoJsonSource(map, DISTRICT_SOURCE_ID);
    if (districtSource) {
      districtSource.setData(districtData);
    } else {
      map.addSource(DISTRICT_SOURCE_ID, {
        type: "geojson",
        data: districtData,
      });
    }

    if (!map.getLayer(DISTRICT_FILL_LAYER_ID)) {
      map.addLayer({
        id: DISTRICT_FILL_LAYER_ID,
        type: "fill",
        source: DISTRICT_SOURCE_ID,
        paint: {
          "fill-color": "#f6c85f",
          "fill-opacity": 0.06,
        },
      });
    }

    if (!map.getLayer(DISTRICT_LINE_LAYER_ID)) {
      map.addLayer({
        id: DISTRICT_LINE_LAYER_ID,
        type: "line",
        source: DISTRICT_SOURCE_ID,
        paint: {
          "line-color": "#8b5e13",
          "line-width": 2.2,
          "line-opacity": 0.85,
        },
      });
    }

    const countyData = countyBoundariesRef.current ?? buildEmptyCollection();
    const countySource = getGeoJsonSource(map, COUNTY_SOURCE_ID);
    if (countySource) {
      countySource.setData(countyData);
    } else {
      map.addSource(COUNTY_SOURCE_ID, {
        type: "geojson",
        data: countyData,
      });
    }

    if (!map.getLayer(COUNTY_LINE_LAYER_ID)) {
      map.addLayer({
        id: COUNTY_LINE_LAYER_ID,
        type: "line",
        source: COUNTY_SOURCE_ID,
        paint: {
          "line-color": "#5f6e73",
          "line-width": 0.9,
          "line-opacity": 0.5,
        },
      });
    }

    const nextData = combineRoadwayChunks(roadwayChunksRef.current);
    const source = getRoadwaySource(map);

    if (source) {
      source.setData(nextData);
    } else {
      map.addSource(SOURCE_ID, {
        type: "geojson",
        data: nextData,
      });
    }

    if (!map.getLayer(CASING_LAYER_ID)) {
      map.addLayer({
        id: CASING_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": "#0b4050",
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            1.8,
            8,
            2.8,
            11,
            4.5,
            14,
            7.5,
            17,
            10,
          ],
          "line-opacity": 0.9,
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
        },
      });
    }

    if (!map.getLayer(LINE_LAYER_ID)) {
      map.addLayer({
        id: LINE_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": "#1490a7",
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            1.1,
            8,
            1.8,
            11,
            3.2,
            14,
            5.2,
            17,
            7.2,
          ],
          "line-opacity": 0.98,
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
        },
      });
    }

    if (!map.getLayer(HIT_LAYER_ID)) {
      map.addLayer({
        id: HIT_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": "#1490a7",
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            11,
            8,
            14,
            11,
            18,
            14,
            22,
            17,
            26,
          ],
          "line-opacity": 0,
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
        },
      });

      map.on("mouseenter", HIT_LAYER_ID, () => {
        map.getCanvas().style.cursor = "pointer";
      });

      map.on("mouseleave", HIT_LAYER_ID, () => {
        map.getCanvas().style.cursor = "";
      });

      map.on("click", HIT_LAYER_ID, (event) => {
        const feature = event.features?.[0];
        const rawProperties = (feature?.properties ?? {}) as Record<string, unknown>;
        const uniqueId = typeof rawProperties.unique_id === "string" ? rawProperties.unique_id : "";

        if (!uniqueId) {
          return;
        }

        void openRoadwayPopup(event, {
          uniqueId,
          roadName:
            typeof rawProperties.road_name === "string"
              ? rawProperties.road_name
              : "Roadway segment",
          county:
            typeof rawProperties.county === "string"
              ? rawProperties.county
              : "Unknown",
          districtLabel:
            typeof rawProperties.district_label === "string"
              ? rawProperties.district_label
              : "District",
        });
      });
    }

    if (renderedLoadTokenRef.current !== loadTokenRef.current) {
      renderedLoadTokenRef.current = loadTokenRef.current;
      detailCacheRef.current.clear();
      popupRef.current?.remove();
    }
  });

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return;
    }

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE_URL,
      center: [-83.8, 33.3],
      zoom: 6.2,
    });

    popupRef.current = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: true,
      maxWidth: "420px",
      offset: 12,
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");

    map.on("load", () => {
      ensureRoadwayLayers();
      syncBounds();
    });

    mapRef.current = map;

    return () => {
      popupRef.current?.remove();
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    ensureRoadwayLayers();
  }, [countyBoundaries, districtBoundaries, loadToken, roadwayChunks, ensureRoadwayLayers]);

  useEffect(() => {
    syncBounds();
  }, [bounds, loadToken, syncBounds]);

  return (
    <div
      ref={containerRef}
      style={{
        height: "100%",
        minHeight: "100%",
        width: "100%",
      }}
    />
  );
}
