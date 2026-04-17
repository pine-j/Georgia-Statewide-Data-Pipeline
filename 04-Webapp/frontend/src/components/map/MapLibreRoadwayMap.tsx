import { useEffect, useEffectEvent, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import {
  GeoJsonFeatureCollection,
  RoadwayFeatureCollection,
  RoadwayVisualizationOption,
} from "../../types/api";
import {
  buildLegendHighlightOpacityExpression,
  buildRoadwayLineColorExpression,
  buildRoadwayLineOpacityExpression,
  buildRoadwayLineSortKeyExpression,
} from "./roadwayVisualization";

const DISTRICT_SOURCE_ID = "district-boundaries";
const DISTRICT_FILL_LAYER_ID = "district-boundaries-fill";
const DISTRICT_LINE_LAYER_ID = "district-boundaries-line";
const COUNTY_SOURCE_ID = "county-boundaries";
const COUNTY_LINE_LAYER_ID = "county-boundaries-line";
const SOURCE_ID = "roadways";
const HIGHLIGHT_LAYER_ID = "roadways-highlight";
const CASING_LAYER_ID = "roadways-casing";
const LINE_LAYER_ID = "roadways-line";
const HIT_LAYER_ID = "roadways-hit";
const UNIQUE_ID_PROPERTY_CANDIDATES = ["unique_id", "UNIQUE_ID", "UniqueId"] as const;

interface MapLibreRoadwayMapProps {
  roadwayChunks: RoadwayFeatureCollection[];
  countyBoundaries?: GeoJsonFeatureCollection;
  districtBoundaries?: GeoJsonFeatureCollection;
  loadToken: number;
  bounds?: [number, number, number, number] | null;
  selectedVisualization?: RoadwayVisualizationOption;
  selectedRoadwayId?: string | null;
  hoveredLegendValue?: string | null;
  onSegmentClick?: (uniqueId: string) => void;
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

function getUniqueIdPropertyName(
  collection: RoadwayFeatureCollection,
): (typeof UNIQUE_ID_PROPERTY_CANDIDATES)[number] {
  const featureProperties = collection.features[0]?.properties;

  if (!featureProperties) {
    return "unique_id";
  }

  const propertyName = UNIQUE_ID_PROPERTY_CANDIDATES.find((candidate) =>
    Object.prototype.hasOwnProperty.call(featureProperties, candidate),
  );

  return propertyName ?? "unique_id";
}

export function MapLibreRoadwayMap({
  roadwayChunks,
  countyBoundaries,
  districtBoundaries,
  loadToken,
  bounds,
  selectedVisualization,
  selectedRoadwayId,
  hoveredLegendValue,
  onSegmentClick,
}: MapLibreRoadwayMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const roadwayChunksRef = useRef<RoadwayFeatureCollection[]>(roadwayChunks);
  const countyBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(countyBoundaries);
  const districtBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(districtBoundaries);
  const boundsRef = useRef(bounds);
  const loadTokenRef = useRef(loadToken);
  const selectedVisualizationRef = useRef<RoadwayVisualizationOption | undefined>(
    selectedVisualization,
  );
  const renderedLoadTokenRef = useRef(loadToken);
  const selectedRoadwayIdRef = useRef(selectedRoadwayId);
  const hoveredLegendValueRef = useRef(hoveredLegendValue);
  const onSegmentClickRef = useRef(onSegmentClick);

  roadwayChunksRef.current = roadwayChunks;
  countyBoundariesRef.current = countyBoundaries;
  districtBoundariesRef.current = districtBoundaries;
  boundsRef.current = bounds;
  loadTokenRef.current = loadToken;
  selectedVisualizationRef.current = selectedVisualization;
  selectedRoadwayIdRef.current = selectedRoadwayId;
  hoveredLegendValueRef.current = hoveredLegendValue;
  onSegmentClickRef.current = onSegmentClick;

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

  const handleSegmentClick = useEffectEvent(
    (uniqueId: string) => {
      onSegmentClickRef.current?.(uniqueId);
    },
  );

  const ensureRoadwayLayers = useEffectEvent(() => {
    const map = mapRef.current;
    const styleLoaded = map?.isStyleLoaded() ?? false;

    if (!map || !styleLoaded) {
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
    const uniqueIdPropertyName = getUniqueIdPropertyName(nextData);
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
          "line-opacity-transition": { duration: 150, delay: 0 },
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
          "line-sort-key": buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
        },
      });
    }

    if (!map.getLayer(LINE_LAYER_ID)) {
      map.addLayer({
        id: LINE_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": buildRoadwayLineColorExpression(selectedVisualizationRef.current),
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
          "line-opacity": buildRoadwayLineOpacityExpression(selectedVisualizationRef.current),
          "line-opacity-transition": { duration: 150, delay: 0 },
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
          "line-sort-key": buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
        },
      });
    }

    // Compute hover-aware opacity for both line and casing layers
    const hoverVal = hoveredLegendValueRef.current;
    const lineOpacity = hoverVal
      ? buildLegendHighlightOpacityExpression(selectedVisualizationRef.current, hoverVal)
      : buildRoadwayLineOpacityExpression(selectedVisualizationRef.current);
    const casingOpacity = hoverVal
      ? buildLegendHighlightOpacityExpression(selectedVisualizationRef.current, hoverVal)
      : 0.9;

    if (map.getLayer(LINE_LAYER_ID)) {
      map.setPaintProperty(
        LINE_LAYER_ID,
        "line-color",
        buildRoadwayLineColorExpression(selectedVisualizationRef.current),
      );
      map.setPaintProperty(LINE_LAYER_ID, "line-opacity", lineOpacity);
      map.setLayoutProperty(
        LINE_LAYER_ID,
        "line-sort-key",
        buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
      );
    }

    if (map.getLayer(CASING_LAYER_ID)) {
      map.setLayoutProperty(
        CASING_LAYER_ID,
        "line-sort-key",
        buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
      );
      map.setPaintProperty(CASING_LAYER_ID, "line-opacity", casingOpacity);
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
          "line-sort-key": buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
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
        const uniqueIdValue = UNIQUE_ID_PROPERTY_CANDIDATES.map(
          (candidate) => rawProperties[candidate],
        ).find((value) => typeof value === "string" && value.length > 0);
        const uniqueId = typeof uniqueIdValue === "string" ? uniqueIdValue : "";

        if (!uniqueId) {
          return;
        }

        handleSegmentClick(uniqueId);
      });
    }

    if (map.getLayer(HIT_LAYER_ID)) {
      map.setLayoutProperty(
        HIT_LAYER_ID,
        "line-sort-key",
        buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
      );
    }

    if (!map.getLayer(HIGHLIGHT_LAYER_ID)) {
      map.addLayer(
        {
          id: HIGHLIGHT_LAYER_ID,
          type: "line",
          source: SOURCE_ID,
          filter: ["==", uniqueIdPropertyName, "__highlight_unselected__"],
          paint: {
            "line-color": "#FFD600",
            "line-width": [
              "interpolate",
              ["linear"],
              ["zoom"],
              5,
              6,
              8,
              8,
              11,
              12,
              14,
              16,
              17,
              20,
            ],
            "line-opacity": 0.95,
          },
          layout: {
            "line-cap": "round",
            "line-join": "round",
            "line-sort-key": buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
          },
        },
        HIT_LAYER_ID,
      );
    }

    if (map.getLayer(HIGHLIGHT_LAYER_ID)) {
      const selectedId = selectedRoadwayIdRef.current;
      map.setFilter(
        HIGHLIGHT_LAYER_ID,
        selectedId
          ? (["==", uniqueIdPropertyName, selectedId] as maplibregl.FilterSpecification)
          : (["==", uniqueIdPropertyName, "__highlight_unselected__"] as maplibregl.FilterSpecification),
      );
    }

    if (renderedLoadTokenRef.current !== loadTokenRef.current) {
      renderedLoadTokenRef.current = loadTokenRef.current;
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

    map.addControl(new maplibregl.NavigationControl(), "top-right");

    map.on("load", () => {
      ensureRoadwayLayers();
      syncBounds();
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    ensureRoadwayLayers();
  }, [
    countyBoundaries,
    districtBoundaries,
    loadToken,
    roadwayChunks,
    selectedVisualization,
    selectedRoadwayId,
    ensureRoadwayLayers,
  ]);

  // Lightweight effect: only update paint properties when legend hover changes.
  // Avoids the heavy GeoJSON re-push that ensureRoadwayLayers performs.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) {
      return;
    }

    if (hoveredLegendValue) {
      const lineOpacity = buildLegendHighlightOpacityExpression(
        selectedVisualization,
        hoveredLegendValue,
      );
      const casingOpacity = buildLegendHighlightOpacityExpression(
        selectedVisualization,
        hoveredLegendValue,
      );
      if (map.getLayer(LINE_LAYER_ID)) {
        map.setPaintProperty(LINE_LAYER_ID, "line-opacity", lineOpacity);
      }
      if (map.getLayer(CASING_LAYER_ID)) {
        map.setPaintProperty(CASING_LAYER_ID, "line-opacity", casingOpacity);
      }
    } else {
      if (map.getLayer(LINE_LAYER_ID)) {
        map.setPaintProperty(
          LINE_LAYER_ID,
          "line-opacity",
          buildRoadwayLineOpacityExpression(selectedVisualization),
        );
      }
      if (map.getLayer(CASING_LAYER_ID)) {
        map.setPaintProperty(CASING_LAYER_ID, "line-opacity", 0.9);
      }
    }
  }, [hoveredLegendValue, selectedVisualization]);

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
