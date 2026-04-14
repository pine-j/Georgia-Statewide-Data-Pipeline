import { useCallback, useEffect, useMemo, useRef } from "react";
import { Alert, Box, Stack, Typography } from "@mui/material";

import { FiltersPanel } from "../filters/FiltersPanel";
import { MapPanel } from "../map/MapPanel";
import { RoadwayDetailSidebar } from "../map/RoadwayDetailSidebar";
import { useBoundaryLayersQuery } from "../../hooks/useBoundaryLayersQuery";
import { useGeorgiaFiltersQuery } from "../../hooks/useGeorgiaFiltersQuery";
import { useRoadwayLoader } from "../../hooks/useRoadwayLoader";
import { useRoadwayVisualizationCatalogQuery } from "../../hooks/useRoadwayVisualizationCatalogQuery";
import { DEFAULT_HIGHWAY_TYPES, useAppStore } from "../../store/useAppStore";
import { getRoadwayDetail } from "../../services/api";
import { RoadwayDetail } from "../../types/api";

export function AppShell() {
  const selectedDistricts = useAppStore((state) => state.selectedDistricts);
  const selectedCounties = useAppStore((state) => state.selectedCounties);
  const selectedHighwayTypes = useAppStore((state) => state.selectedHighwayTypes);
  const selectedVisualizationId = useAppStore((state) => state.selectedVisualizationId);
  const setSelectedDistricts = useAppStore((state) => state.setSelectedDistricts);
  const setSelectedCounties = useAppStore((state) => state.setSelectedCounties);
  const setSelectedHighwayTypes = useAppStore((state) => state.setSelectedHighwayTypes);
  const setSelectedVisualizationId = useAppStore(
    (state) => state.setSelectedVisualizationId,
  );
  const selectedRoadwayId = useAppStore((state) => state.selectedRoadwayId);
  const roadwayDetail = useAppStore((state) => state.roadwayDetail);
  const isLoadingDetail = useAppStore((state) => state.isLoadingDetail);
  const detailError = useAppStore((state) => state.detailError);
  const openRoadwayDetail = useAppStore((state) => state.openRoadwayDetail);
  const setRoadwayDetail = useAppStore((state) => state.setRoadwayDetail);
  const setDetailError = useAppStore((state) => state.setDetailError);
  const closeRoadwayDetail = useAppStore((state) => state.closeRoadwayDetail);

  const georgiaFiltersQuery = useGeorgiaFiltersQuery();
  const roadwayVisualizationsQuery = useRoadwayVisualizationCatalogQuery();
  const districts = georgiaFiltersQuery.data?.districts ?? [];
  const counties = georgiaFiltersQuery.data?.counties ?? [];
  const highwayTypes = georgiaFiltersQuery.data?.highway_types ?? [];
  const roadwayVisualizationCatalog = roadwayVisualizationsQuery.data;
  const thematicOptions = roadwayVisualizationCatalog?.thematic_options ?? [];
  const selectedVisualization =
    thematicOptions.find((option) => option.id === selectedVisualizationId) ?? thematicOptions[0];
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
  };

  const handleVisualizationChange = (visualizationId: string) => {
    setSelectedVisualizationId(visualizationId);
  };

  const themeCoveragePercent = useMemo(() => {
    if (roadwayLoader.isLoading) {
      return null;
    }

    const propertyName = selectedVisualization?.property_name;
    if (!propertyName || roadwayLoader.roadwayChunks.length === 0) {
      return null;
    }

    let total = 0;
    let withData = 0;
    for (const chunk of roadwayLoader.roadwayChunks) {
      for (const feature of chunk.features) {
        total += 1;
        const value = (feature.properties as Record<string, unknown>)[propertyName];
        if (value !== null && value !== undefined && value !== "") {
          withData += 1;
        }
      }
    }

    return total > 0 ? Math.round((withData / total) * 100) : null;
  }, [roadwayLoader.isLoading, roadwayLoader.roadwayChunks, selectedVisualization?.property_name]);

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
                roadwayVisualizationCatalog={roadwayVisualizationCatalog}
                selectedVisualizationId={selectedVisualization?.id ?? selectedVisualizationId}
                selectedVisualization={selectedVisualization}
                themeCoveragePercent={themeCoveragePercent}
                onDistrictChange={handleDistrictChange}
                onDistrictDelete={handleDistrictDelete}
                onCountyChange={setSelectedCounties}
                onCountyDelete={handleCountyDelete}
                onHighwayTypeChange={setSelectedHighwayTypes}
                onHighwayTypeDelete={handleHighwayTypeDelete}
                onResetFilters={handleResetFilters}
                onVisualizationChange={handleVisualizationChange}
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
                selectedRoadwayId={selectedRoadwayId}
                onSegmentClick={handleSegmentClick}
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
