import { useEffect, useEffectEvent, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import {
  GeoJsonFeatureCollection,
  RoadwayFeatureCollection,
  RoadwayVisualizationOption,
} from "../../types/api";
import type {
  BoundaryOverlayVisibility,
  ThemeFilterValue,
} from "../../store/useAppStore";
import { DEFAULT_BOUNDARY_OVERLAY_VISIBILITY } from "../../store/useAppStore";
import {
  buildRoadwayLineSortKeyExpression,
  buildThemeContextFilterColorExpression,
  buildThemeContextFilterOpacityExpression,
} from "./roadwayVisualization";

const DISTRICT_SOURCE_ID = "district-boundaries";
const DISTRICT_FILL_LAYER_ID = "district-boundaries-fill";
const DISTRICT_LINE_LAYER_ID = "district-boundaries-line";
const COUNTY_SOURCE_ID = "county-boundaries";
const COUNTY_LINE_LAYER_ID = "county-boundaries-line";

const AREA_OFFICE_SOURCE_ID = "area-office-boundaries";
const AREA_OFFICE_FILL_LAYER_ID = "area-office-boundaries-fill";
const AREA_OFFICE_LINE_LAYER_ID = "area-office-boundaries-line";
const MPO_SOURCE_ID = "mpo-boundaries";
const MPO_FILL_LAYER_ID = "mpo-boundaries-fill";
const MPO_LINE_LAYER_ID = "mpo-boundaries-line";
const REGIONAL_COMMISSION_SOURCE_ID = "regional-commission-boundaries";
const REGIONAL_COMMISSION_FILL_LAYER_ID = "regional-commission-boundaries-fill";
const REGIONAL_COMMISSION_LINE_LAYER_ID = "regional-commission-boundaries-line";
const STATE_HOUSE_SOURCE_ID = "state-house-boundaries";
const STATE_HOUSE_LINE_LAYER_ID = "state-house-boundaries-line";
const STATE_SENATE_SOURCE_ID = "state-senate-boundaries";
const STATE_SENATE_LINE_LAYER_ID = "state-senate-boundaries-line";
const CONGRESSIONAL_SOURCE_ID = "congressional-boundaries";
const CONGRESSIONAL_LINE_LAYER_ID = "congressional-boundaries-line";

// Shared palette for the 8 boundary overlays. Muted tones, distinct from the
// roadway thematic ramps so overlays read as chrome rather than data.
export const BOUNDARY_OVERLAY_COLORS = {
  districts: "#8b5e13",
  counties: "#5f6e73",
  areaOffices: "#b4673f",
  mpos: "#4a7a6c",
  regionalCommissions: "#7a6d4a",
  stateHouse: "#6b4a7a",
  stateSenate: "#4a5e7a",
  congressional: "#8b3a5c",
} as const;

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
  areaOfficeBoundaries?: GeoJsonFeatureCollection;
  mpoBoundaries?: GeoJsonFeatureCollection;
  regionalCommissionBoundaries?: GeoJsonFeatureCollection;
  stateHouseBoundaries?: GeoJsonFeatureCollection;
  stateSenateBoundaries?: GeoJsonFeatureCollection;
  congressionalBoundaries?: GeoJsonFeatureCollection;
  boundaryOverlayVisibility?: BoundaryOverlayVisibility;
  loadToken: number;
  bounds?: [number, number, number, number] | null;
  selectedVisualization?: RoadwayVisualizationOption;
  themeFilterState?: ThemeFilterValue;
  selectedRoadwayId?: string | null;
  onSegmentClick?: (uniqueId: string) => void;
  onBackgroundClick?: () => void;
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
  areaOfficeBoundaries,
  mpoBoundaries,
  regionalCommissionBoundaries,
  stateHouseBoundaries,
  stateSenateBoundaries,
  congressionalBoundaries,
  boundaryOverlayVisibility,
  loadToken,
  bounds,
  selectedVisualization,
  themeFilterState,
  selectedRoadwayId,
  onSegmentClick,
  onBackgroundClick,
}: MapLibreRoadwayMapProps) {
  const effectiveOverlayVisibility =
    boundaryOverlayVisibility ?? DEFAULT_BOUNDARY_OVERLAY_VISIBILITY;

  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const roadwayChunksRef = useRef<RoadwayFeatureCollection[]>(roadwayChunks);
  const countyBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(countyBoundaries);
  const districtBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(districtBoundaries);
  const areaOfficeBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(
    areaOfficeBoundaries,
  );
  const mpoBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(mpoBoundaries);
  const regionalCommissionBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(
    regionalCommissionBoundaries,
  );
  const stateHouseBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(
    stateHouseBoundaries,
  );
  const stateSenateBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(
    stateSenateBoundaries,
  );
  const congressionalBoundariesRef = useRef<GeoJsonFeatureCollection | undefined>(
    congressionalBoundaries,
  );
  const overlayVisibilityRef = useRef<BoundaryOverlayVisibility>(effectiveOverlayVisibility);
  const boundsRef = useRef(bounds);
  const loadTokenRef = useRef(loadToken);
  const selectedVisualizationRef = useRef<RoadwayVisualizationOption | undefined>(
    selectedVisualization,
  );
  const themeFilterStateRef = useRef<ThemeFilterValue | undefined>(themeFilterState);
  const renderedLoadTokenRef = useRef(loadToken);
  const selectedRoadwayIdRef = useRef(selectedRoadwayId);
  const onSegmentClickRef = useRef(onSegmentClick);
  const onBackgroundClickRef = useRef(onBackgroundClick);

  roadwayChunksRef.current = roadwayChunks;
  countyBoundariesRef.current = countyBoundaries;
  districtBoundariesRef.current = districtBoundaries;
  areaOfficeBoundariesRef.current = areaOfficeBoundaries;
  mpoBoundariesRef.current = mpoBoundaries;
  regionalCommissionBoundariesRef.current = regionalCommissionBoundaries;
  stateHouseBoundariesRef.current = stateHouseBoundaries;
  stateSenateBoundariesRef.current = stateSenateBoundaries;
  congressionalBoundariesRef.current = congressionalBoundaries;
  overlayVisibilityRef.current = effectiveOverlayVisibility;
  boundsRef.current = bounds;
  loadTokenRef.current = loadToken;
  selectedVisualizationRef.current = selectedVisualization;
  themeFilterStateRef.current = themeFilterState;
  selectedRoadwayIdRef.current = selectedRoadwayId;
  onSegmentClickRef.current = onSegmentClick;
  onBackgroundClickRef.current = onBackgroundClick;

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
          "line-color": BOUNDARY_OVERLAY_COLORS.districts,
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
          "line-color": BOUNDARY_OVERLAY_COLORS.counties,
          "line-width": 0.9,
          "line-opacity": 0.5,
        },
      });
    }

    // Area office boundaries (Engineering family) - fill + line.
    const areaOfficeData = areaOfficeBoundariesRef.current ?? buildEmptyCollection();
    const areaOfficeSource = getGeoJsonSource(map, AREA_OFFICE_SOURCE_ID);
    if (areaOfficeSource) {
      areaOfficeSource.setData(areaOfficeData);
    } else {
      map.addSource(AREA_OFFICE_SOURCE_ID, {
        type: "geojson",
        data: areaOfficeData,
      });
    }

    if (!map.getLayer(AREA_OFFICE_FILL_LAYER_ID)) {
      map.addLayer({
        id: AREA_OFFICE_FILL_LAYER_ID,
        type: "fill",
        source: AREA_OFFICE_SOURCE_ID,
        paint: {
          "fill-color": BOUNDARY_OVERLAY_COLORS.areaOffices,
          "fill-opacity": 0.05,
        },
      });
    }

    if (!map.getLayer(AREA_OFFICE_LINE_LAYER_ID)) {
      map.addLayer({
        id: AREA_OFFICE_LINE_LAYER_ID,
        type: "line",
        source: AREA_OFFICE_SOURCE_ID,
        paint: {
          "line-color": BOUNDARY_OVERLAY_COLORS.areaOffices,
          "line-width": 1.6,
          "line-opacity": 0.8,
        },
      });
    }

    // MPO boundaries (Planning family) - fill + line.
    const mpoData = mpoBoundariesRef.current ?? buildEmptyCollection();
    const mpoSource = getGeoJsonSource(map, MPO_SOURCE_ID);
    if (mpoSource) {
      mpoSource.setData(mpoData);
    } else {
      map.addSource(MPO_SOURCE_ID, {
        type: "geojson",
        data: mpoData,
      });
    }

    if (!map.getLayer(MPO_FILL_LAYER_ID)) {
      map.addLayer({
        id: MPO_FILL_LAYER_ID,
        type: "fill",
        source: MPO_SOURCE_ID,
        paint: {
          "fill-color": BOUNDARY_OVERLAY_COLORS.mpos,
          "fill-opacity": 0.05,
        },
      });
    }

    if (!map.getLayer(MPO_LINE_LAYER_ID)) {
      map.addLayer({
        id: MPO_LINE_LAYER_ID,
        type: "line",
        source: MPO_SOURCE_ID,
        paint: {
          "line-color": BOUNDARY_OVERLAY_COLORS.mpos,
          "line-width": 1.4,
          "line-opacity": 0.8,
        },
      });
    }

    // Regional commission boundaries (Planning family) - fill + line.
    const regionalCommissionData =
      regionalCommissionBoundariesRef.current ?? buildEmptyCollection();
    const regionalCommissionSource = getGeoJsonSource(map, REGIONAL_COMMISSION_SOURCE_ID);
    if (regionalCommissionSource) {
      regionalCommissionSource.setData(regionalCommissionData);
    } else {
      map.addSource(REGIONAL_COMMISSION_SOURCE_ID, {
        type: "geojson",
        data: regionalCommissionData,
      });
    }

    if (!map.getLayer(REGIONAL_COMMISSION_FILL_LAYER_ID)) {
      map.addLayer({
        id: REGIONAL_COMMISSION_FILL_LAYER_ID,
        type: "fill",
        source: REGIONAL_COMMISSION_SOURCE_ID,
        paint: {
          "fill-color": BOUNDARY_OVERLAY_COLORS.regionalCommissions,
          "fill-opacity": 0.05,
        },
      });
    }

    if (!map.getLayer(REGIONAL_COMMISSION_LINE_LAYER_ID)) {
      map.addLayer({
        id: REGIONAL_COMMISSION_LINE_LAYER_ID,
        type: "line",
        source: REGIONAL_COMMISSION_SOURCE_ID,
        paint: {
          "line-color": BOUNDARY_OVERLAY_COLORS.regionalCommissions,
          "line-width": 1.4,
          "line-opacity": 0.8,
        },
      });
    }

    // State house boundaries (Legislative family) - line only.
    const stateHouseData = stateHouseBoundariesRef.current ?? buildEmptyCollection();
    const stateHouseSource = getGeoJsonSource(map, STATE_HOUSE_SOURCE_ID);
    if (stateHouseSource) {
      stateHouseSource.setData(stateHouseData);
    } else {
      map.addSource(STATE_HOUSE_SOURCE_ID, {
        type: "geojson",
        data: stateHouseData,
      });
    }

    if (!map.getLayer(STATE_HOUSE_LINE_LAYER_ID)) {
      map.addLayer({
        id: STATE_HOUSE_LINE_LAYER_ID,
        type: "line",
        source: STATE_HOUSE_SOURCE_ID,
        paint: {
          "line-color": BOUNDARY_OVERLAY_COLORS.stateHouse,
          "line-width": 0.8,
          "line-opacity": 0.7,
        },
      });
    }

    // State senate boundaries (Legislative family) - line only.
    const stateSenateData = stateSenateBoundariesRef.current ?? buildEmptyCollection();
    const stateSenateSource = getGeoJsonSource(map, STATE_SENATE_SOURCE_ID);
    if (stateSenateSource) {
      stateSenateSource.setData(stateSenateData);
    } else {
      map.addSource(STATE_SENATE_SOURCE_ID, {
        type: "geojson",
        data: stateSenateData,
      });
    }

    if (!map.getLayer(STATE_SENATE_LINE_LAYER_ID)) {
      map.addLayer({
        id: STATE_SENATE_LINE_LAYER_ID,
        type: "line",
        source: STATE_SENATE_SOURCE_ID,
        paint: {
          "line-color": BOUNDARY_OVERLAY_COLORS.stateSenate,
          "line-width": 1.0,
          "line-opacity": 0.75,
        },
      });
    }

    // Congressional boundaries (Legislative family) - line only.
    const congressionalData = congressionalBoundariesRef.current ?? buildEmptyCollection();
    const congressionalSource = getGeoJsonSource(map, CONGRESSIONAL_SOURCE_ID);
    if (congressionalSource) {
      congressionalSource.setData(congressionalData);
    } else {
      map.addSource(CONGRESSIONAL_SOURCE_ID, {
        type: "geojson",
        data: congressionalData,
      });
    }

    if (!map.getLayer(CONGRESSIONAL_LINE_LAYER_ID)) {
      map.addLayer({
        id: CONGRESSIONAL_LINE_LAYER_ID,
        type: "line",
        source: CONGRESSIONAL_SOURCE_ID,
        paint: {
          "line-color": BOUNDARY_OVERLAY_COLORS.congressional,
          "line-width": 1.4,
          "line-opacity": 0.8,
        },
      });
    }

    // Apply visibility layout property for each overlay. We keep the sources
    // & layers installed and toggle `visibility` so that re-enabling an
    // overlay is instant when data was already fetched.
    const visibility = overlayVisibilityRef.current;
    const overlayLayerGroups: Array<{
      visible: boolean;
      layerIds: readonly string[];
    }> = [
      {
        visible: visibility.districts,
        layerIds: [DISTRICT_FILL_LAYER_ID, DISTRICT_LINE_LAYER_ID],
      },
      { visible: visibility.counties, layerIds: [COUNTY_LINE_LAYER_ID] },
      {
        visible: visibility.areaOffices,
        layerIds: [AREA_OFFICE_FILL_LAYER_ID, AREA_OFFICE_LINE_LAYER_ID],
      },
      { visible: visibility.mpos, layerIds: [MPO_FILL_LAYER_ID, MPO_LINE_LAYER_ID] },
      {
        visible: visibility.regionalCommissions,
        layerIds: [REGIONAL_COMMISSION_FILL_LAYER_ID, REGIONAL_COMMISSION_LINE_LAYER_ID],
      },
      { visible: visibility.stateHouse, layerIds: [STATE_HOUSE_LINE_LAYER_ID] },
      { visible: visibility.stateSenate, layerIds: [STATE_SENATE_LINE_LAYER_ID] },
      { visible: visibility.congressional, layerIds: [CONGRESSIONAL_LINE_LAYER_ID] },
    ];

    for (const group of overlayLayerGroups) {
      const layoutValue = group.visible ? "visible" : "none";
      for (const layerId of group.layerIds) {
        if (map.getLayer(layerId)) {
          map.setLayoutProperty(layerId, "visibility", layoutValue);
        }
      }
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

    const lineColor = buildThemeContextFilterColorExpression(
      selectedVisualizationRef.current,
      themeFilterStateRef.current,
    );
    const lineOpacity = buildThemeContextFilterOpacityExpression(
      selectedVisualizationRef.current,
      themeFilterStateRef.current,
    );
    const casingOpacity = lineOpacity;

    if (!map.getLayer(CASING_LAYER_ID)) {
      map.addLayer({
        id: CASING_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": lineColor,
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
          "line-opacity": casingOpacity,
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
          "line-color": lineColor,
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
          "line-opacity": lineOpacity,
          "line-opacity-transition": { duration: 150, delay: 0 },
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
        lineColor,
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
      map.setPaintProperty(
        CASING_LAYER_ID,
        "line-color",
        lineColor,
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

      map.on("click", (event) => {
        if (!map.getLayer(HIT_LAYER_ID)) {
          return;
        }
        const hits = map.queryRenderedFeatures(event.point, {
          layers: [HIT_LAYER_ID],
        });
        if (hits.length === 0) {
          onBackgroundClickRef.current?.();
        }
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
    areaOfficeBoundaries,
    mpoBoundaries,
    regionalCommissionBoundaries,
    stateHouseBoundaries,
    stateSenateBoundaries,
    congressionalBoundaries,
    boundaryOverlayVisibility,
    loadToken,
    roadwayChunks,
    selectedVisualization,
    selectedRoadwayId,
    ensureRoadwayLayers,
  ]);

  // Lightweight effect: update paint properties when theme filters change.
  // Avoids the heavy GeoJSON re-push that ensureRoadwayLayers performs.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) {
      return;
    }

    const lineColor = buildThemeContextFilterColorExpression(
      selectedVisualization,
      themeFilterState,
    );
    const lineOpacity = buildThemeContextFilterOpacityExpression(
      selectedVisualization,
      themeFilterState,
    );

    if (map.getLayer(LINE_LAYER_ID)) {
      map.setPaintProperty(LINE_LAYER_ID, "line-color", lineColor);
      map.setPaintProperty(LINE_LAYER_ID, "line-opacity", lineOpacity);
    }
    if (map.getLayer(CASING_LAYER_ID)) {
      map.setPaintProperty(CASING_LAYER_ID, "line-color", lineColor);
      map.setPaintProperty(CASING_LAYER_ID, "line-opacity", lineOpacity);
    }
  }, [selectedVisualization, themeFilterState]);

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
