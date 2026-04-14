import { useEffect, useEffectEvent, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import {
  GeoJsonFeatureCollection,
  RoadwayFeatureCollection,
  RoadwayVisualizationOption,
} from "../../types/api";
import {
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
const CASING_LAYER_ID = "roadways-casing";
const LINE_LAYER_ID = "roadways-line";
const HIT_LAYER_ID = "roadways-hit";

interface MapLibreRoadwayMapProps {
  roadwayChunks: RoadwayFeatureCollection[];
  countyBoundaries?: GeoJsonFeatureCollection;
  districtBoundaries?: GeoJsonFeatureCollection;
  loadToken: number;
  bounds?: [number, number, number, number] | null;
  selectedVisualization?: RoadwayVisualizationOption;
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

export function MapLibreRoadwayMap({
  roadwayChunks,
  countyBoundaries,
  districtBoundaries,
  loadToken,
  bounds,
  selectedVisualization,
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
  const onSegmentClickRef = useRef(onSegmentClick);

  roadwayChunksRef.current = roadwayChunks;
  countyBoundariesRef.current = countyBoundaries;
  districtBoundariesRef.current = districtBoundaries;
  boundsRef.current = bounds;
  loadTokenRef.current = loadToken;
  selectedVisualizationRef.current = selectedVisualization;
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
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
          "line-sort-key": buildRoadwayLineSortKeyExpression(selectedVisualizationRef.current),
        },
      });
    }

    if (map.getLayer(LINE_LAYER_ID)) {
      map.setPaintProperty(
        LINE_LAYER_ID,
        "line-color",
        buildRoadwayLineColorExpression(selectedVisualizationRef.current),
      );
      map.setPaintProperty(
        LINE_LAYER_ID,
        "line-opacity",
        buildRoadwayLineOpacityExpression(selectedVisualizationRef.current),
      );
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
        const uniqueId = typeof rawProperties.unique_id === "string" ? rawProperties.unique_id : "";

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
    ensureRoadwayLayers,
  ]);

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
