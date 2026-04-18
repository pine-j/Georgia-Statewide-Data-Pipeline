import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, Box, Stack, Typography } from "@mui/material";

import { FiltersPanel } from "../filters/FiltersPanel";
import { MapPanel } from "../map/MapPanel";
import { RoadwayDetailSidebar } from "../map/RoadwayDetailSidebar";
import { useBoundaryLayersQuery } from "../../hooks/useBoundaryLayersQuery";
import { useGeorgiaFiltersQuery } from "../../hooks/useGeorgiaFiltersQuery";
import { useRoadwayLoader } from "../../hooks/useRoadwayLoader";
import { useRoadwayVisualizationCatalogQuery } from "../../hooks/useRoadwayVisualizationCatalogQuery";
import {
  DEFAULT_HIGHWAY_TYPES,
  useAppStore,
} from "../../store/useAppStore";
import type { ThemeFilterValue } from "../../store/useAppStore";
import { getRoadwayDetail } from "../../services/api";
import { RoadwayDetail, RoadwayVisualizationOption } from "../../types/api";
import {
  computeLegendPresence,
  computeLegendPresenceFiltered,
} from "../map/roadwayVisualization";

function getThemeFilterBinKey(bin: { value?: string | null; label: string }): string {
  return typeof bin.value === "string" ? bin.value : bin.label;
}

function buildDefaultThemeFilterValue(
  option?: RoadwayVisualizationOption,
): ThemeFilterValue | null {
  if (!option || option.filters.length === 0) {
    return null;
  }

  const firstFilter = option.filters.find((spec) => spec.control !== "none");
  if (!firstFilter) {
    return null;
  }

  const rangeFilter = option.filters.find(
    (spec) =>
      spec.control === "range_slider" &&
      typeof spec.min_bound === "number" &&
      typeof spec.max_bound === "number",
  );

  return {
    selectedValues: option.filters.flatMap((spec) =>
      spec.bins.flatMap((bin) =>
        bin.default_selected ? [getThemeFilterBinKey(bin)] : [],
      ),
    ),
    range:
      rangeFilter &&
      typeof rangeFilter.min_bound === "number" &&
      typeof rangeFilter.max_bound === "number"
        ? [rangeFilter.min_bound, rangeFilter.max_bound]
        : null,
    includeNoData: firstFilter.include_no_data_default,
  };
}

export function AppShell() {
  const selectedDistricts = useAppStore((state) => state.selectedDistricts);
  const selectedCounties = useAppStore((state) => state.selectedCounties);
  const selectedHighwayTypes = useAppStore((state) => state.selectedHighwayTypes);
  const selectedVisualizationId = useAppStore((state) => state.selectedVisualizationId);
  const themeFilters = useAppStore((state) => state.themeFilters);
  const setSelectedDistricts = useAppStore((state) => state.setSelectedDistricts);
  const setSelectedCounties = useAppStore((state) => state.setSelectedCounties);
  const setSelectedHighwayTypes = useAppStore((state) => state.setSelectedHighwayTypes);
  const setSelectedVisualizationId = useAppStore(
    (state) => state.setSelectedVisualizationId,
  );
  const setThemeFilter = useAppStore((state) => state.setThemeFilter);
  const resetThemeFilter = useAppStore((state) => state.resetThemeFilter);
  const resetAllThemeFilters = useAppStore((state) => state.resetAllThemeFilters);
  const selectedRoadwayId = useAppStore((state) => state.selectedRoadwayId);
  const roadwayDetail = useAppStore((state) => state.roadwayDetail);
  const isLoadingDetail = useAppStore((state) => state.isLoadingDetail);
  const detailError = useAppStore((state) => state.detailError);
  const openRoadwayDetail = useAppStore((state) => state.openRoadwayDetail);
  const setRoadwayDetail = useAppStore((state) => state.setRoadwayDetail);
  const setDetailError = useAppStore((state) => state.setDetailError);
  const closeRoadwayDetail = useAppStore((state) => state.closeRoadwayDetail);

  const [hoveredLegendValue, setHoveredLegendValue] = useState<string | null>(null);

  const georgiaFiltersQuery = useGeorgiaFiltersQuery();
  const roadwayVisualizationsQuery = useRoadwayVisualizationCatalogQuery();
  const districts = georgiaFiltersQuery.data?.districts ?? [];
  const counties = georgiaFiltersQuery.data?.counties ?? [];
  const highwayTypes = georgiaFiltersQuery.data?.highway_types ?? [];
  const roadwayVisualizationCatalog = roadwayVisualizationsQuery.data;
  const thematicOptions = roadwayVisualizationCatalog?.thematic_options ?? [];
  const selectedVisualization =
    thematicOptions.find((option) => option.id === selectedVisualizationId) ?? thematicOptions[0];
  const selectedThemeFilterState = useMemo(() => {
    if (!selectedVisualization) {
      return undefined;
    }

    return (
      themeFilters[selectedVisualization.id] ??
      buildDefaultThemeFilterValue(selectedVisualization) ??
      undefined
    );
  }, [selectedVisualization, themeFilters]);
  const roadwayLoader = useRoadwayLoader(
    selectedDistricts,
    selectedCounties,
    selectedHighwayTypes,
    true,
  );
  const boundaryLayersQuery = useBoundaryLayersQuery(
    selectedDistricts,
    selectedCounties,
    selectedHighwayTypes,
    true,
  );

  useEffect(() => {
    if (thematicOptions.length === 0) {
      return;
    }

    const hasSelectedVisualization = thematicOptions.some(
      (option) => option.id === selectedVisualizationId,
    );
    if (hasSelectedVisualization) {
      return;
    }

    setSelectedVisualizationId(
      roadwayVisualizationCatalog?.default_option_id ?? thematicOptions[0].id,
    );
  }, [
    roadwayVisualizationCatalog?.default_option_id,
    selectedVisualizationId,
    setSelectedVisualizationId,
    thematicOptions,
  ]);

  useEffect(() => {
    for (const option of thematicOptions) {
      if (themeFilters[option.id] || option.filters.length === 0) {
        continue;
      }

      const defaultThemeFilter = buildDefaultThemeFilterValue(option);
      if (!defaultThemeFilter) {
        continue;
      }

      setThemeFilter(option.id, defaultThemeFilter);
    }
  }, [setThemeFilter, thematicOptions, themeFilters]);

  // Detail fetch with caching and abort-controller for race prevention
  const detailCacheRef = useRef<Map<string, RoadwayDetail>>(new Map());
  const detailAbortRef = useRef<AbortController | null>(null);

  const handleSegmentClick = useCallback(
    (uniqueId: string) => {
      // Abort any in-flight request
      detailAbortRef.current?.abort();

      openRoadwayDetail(uniqueId);

      const cached = detailCacheRef.current.get(uniqueId);
      if (cached) {
        setRoadwayDetail(cached);
        return;
      }

      const controller = new AbortController();
      detailAbortRef.current = controller;

      getRoadwayDetail(uniqueId)
        .then((detail) => {
          if (controller.signal.aborted) return;
          detailCacheRef.current.set(uniqueId, detail);
          setRoadwayDetail(detail);
        })
        .catch(() => {
          if (controller.signal.aborted) return;
          setDetailError();
        });
    },
    [openRoadwayDetail, setRoadwayDetail, setDetailError],
  );

  // Clear detail cache when filters change
  useEffect(() => {
    detailCacheRef.current.clear();
    detailAbortRef.current?.abort();
    closeRoadwayDetail();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDistricts, selectedCounties, selectedHighwayTypes]);

  const handleDistrictChange = (districts: number[]) => {
    const nextSelectedCounties = selectedCounties.filter((countyName) =>
      counties.some(
        (county) =>
          county.county === countyName &&
          (districts.length === 0 || districts.includes(county.district)),
      ),
    );

    setSelectedDistricts(districts);
    setSelectedCounties(nextSelectedCounties);
  };

  const handleDistrictDelete = (districtId: number) => {
    handleDistrictChange(selectedDistricts.filter((district) => district !== districtId));
  };

  const handleCountyDelete = (countyName: string) => {
    setSelectedCounties(selectedCounties.filter((county) => county !== countyName));
  };

  const handleHighwayTypeDelete = (highwayTypeId: string) => {
    setSelectedHighwayTypes(
      selectedHighwayTypes.filter((highwayType) => highwayType !== highwayTypeId),
    );
  };

  const handleResetFilters = () => {
    setSelectedDistricts([]);
    setSelectedCounties([]);
    setSelectedHighwayTypes([...DEFAULT_HIGHWAY_TYPES]);
    resetAllThemeFilters();
  };

  const handleVisualizationChange = (visualizationId: string) => {
    setHoveredLegendValue(null);
    setSelectedVisualizationId(visualizationId);
  };

  const legendPresence = useMemo(
    () =>
      roadwayLoader.isLoading
        ? null
        : computeLegendPresence(roadwayLoader.roadwayChunks, selectedVisualization),
    [roadwayLoader.isLoading, roadwayLoader.roadwayChunks, selectedVisualization],
  );

  const themeCoveragePercent = useMemo(() => {
    if (!legendPresence || legendPresence.total === 0) {
      return null;
    }
    return Math.round((legendPresence.withData / legendPresence.total) * 100);
  }, [legendPresence]);

  const themeViewPercent = useMemo(() => {
    if (roadwayLoader.isLoading || !selectedVisualization) {
      return null;
    }

    const filteredPresence = computeLegendPresenceFiltered(
      roadwayLoader.roadwayChunks,
      selectedVisualization,
      selectedThemeFilterState,
    );
    if (!filteredPresence || filteredPresence.total === 0) {
      return null;
    }

    return Math.round((filteredPresence.filterPassing / filteredPresence.total) * 100);
  }, [
    roadwayLoader.isLoading,
    roadwayLoader.roadwayChunks,
    selectedThemeFilterState,
    selectedVisualization,
  ]);

  const hasApiError =
    georgiaFiltersQuery.isError ||
    roadwayVisualizationsQuery.isError ||
    Boolean(roadwayLoader.error) ||
    boundaryLayersQuery.countiesQuery.isError ||
    boundaryLayersQuery.districtsQuery.isError;

  return (
    <Box
      sx={{
        height: "100vh",
        overflow: "hidden",
        display: "grid",
        gridTemplateRows: "auto 1fr",
        bgcolor: "#eef2f3",
      }}
    >
      <Box
        sx={{
          px: { xs: 2, md: 2.5 },
          py: 2,
          borderBottom: "1px solid rgba(17, 61, 73, 0.1)",
          bgcolor: "rgba(255, 255, 255, 0.94)",
          backdropFilter: "blur(12px)",
        }}
      >
        <Typography variant="h5" sx={{ fontWeight: 700 }}>
          Georgia Statewide Web App
        </Typography>
      </Box>

      <Box sx={{ px: { xs: 0, md: 0 }, py: 0, minHeight: 0, overflow: "hidden" }}>
        <Stack spacing={0} sx={{ height: "100%", minHeight: 0 }}>
          {hasApiError && (
            <Alert severity="warning" sx={{ mx: { xs: 2, md: 2.5 }, my: 1.5 }}>
              One or more requests failed while loading the roadway data.
            </Alert>
          )}

          <Box
            sx={{
              display: "grid",
              gap: 0,
              minHeight: 0,
              flex: 1,
              gridTemplateColumns: {
                xs: "1fr",
                lg: selectedRoadwayId
                  ? "320px minmax(0, 1fr) 380px"
                  : "320px minmax(0, 1fr)",
              },
              gridTemplateRows: {
                xs: "minmax(0, auto) minmax(58vh, 1fr)",
                lg: "minmax(0, 1fr)",
              },
              alignItems: "stretch",
              height: "100%",
              overflowY: { xs: "auto", lg: "hidden" },
              transition: "grid-template-columns 0.25s ease",
            }}
          >
            <Box
              sx={{
                minWidth: 0,
                minHeight: 0,
                overflow: "hidden",
                borderRight: { xs: "none", lg: "1px solid rgba(17, 61, 73, 0.12)" },
              }}
            >
              <FiltersPanel
                districts={districts}
                counties={counties}
                highwayTypes={highwayTypes}
                selectedDistricts={selectedDistricts}
                selectedCounties={selectedCounties}
                selectedHighwayTypes={selectedHighwayTypes}
                themeFilters={themeFilters}
                roadwayVisualizationCatalog={roadwayVisualizationCatalog}
                selectedVisualizationId={selectedVisualization?.id ?? selectedVisualizationId}
                selectedVisualization={selectedVisualization}
                legendPresence={legendPresence}
                themeViewPercent={themeViewPercent}
                themeCoveragePercent={themeCoveragePercent}
                onDistrictChange={handleDistrictChange}
                onDistrictDelete={handleDistrictDelete}
                onCountyChange={setSelectedCounties}
                onCountyDelete={handleCountyDelete}
                onHighwayTypeChange={setSelectedHighwayTypes}
                onHighwayTypeDelete={handleHighwayTypeDelete}
                setThemeFilter={setThemeFilter}
                resetThemeFilter={resetThemeFilter}
                onResetFilters={handleResetFilters}
                onVisualizationChange={handleVisualizationChange}
                onLegendItemHover={setHoveredLegendValue}
              />
            </Box>

            <Box sx={{ minWidth: 0, minHeight: 0, overflow: "hidden" }}>
              <MapPanel
                roadwayChunks={roadwayLoader.roadwayChunks}
                countyBoundaries={boundaryLayersQuery.countiesQuery.data}
                districtBoundaries={boundaryLayersQuery.districtsQuery.data}
                loadToken={roadwayLoader.loadToken}
                bounds={roadwayLoader.bounds}
                isLoading={roadwayLoader.isLoading}
                isManifestLoading={roadwayLoader.isManifestLoading}
                loadedSegments={roadwayLoader.loadedSegments}
                totalSegments={roadwayLoader.totalSegments}
                progressPercent={roadwayLoader.progressPercent}
                etaSeconds={roadwayLoader.etaSeconds}
                selectedVisualization={selectedVisualization}
                themeFilterState={selectedThemeFilterState}
                selectedRoadwayId={selectedRoadwayId}
                hoveredLegendValue={hoveredLegendValue}
                onSegmentClick={handleSegmentClick}
                onBackgroundClick={closeRoadwayDetail}
              />
            </Box>

            {selectedRoadwayId && (
              <Box sx={{ minWidth: 0, minHeight: 0, overflow: "hidden" }}>
                <RoadwayDetailSidebar
                  detail={roadwayDetail}
                  isLoading={isLoadingDetail}
                  hasError={detailError}
                  onClose={closeRoadwayDetail}
                />
              </Box>
            )}
          </Box>
        </Stack>
      </Box>
    </Box>
  );
}
