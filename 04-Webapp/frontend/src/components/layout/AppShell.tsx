import { useCallback, useEffect, useMemo, useRef } from "react";
import { Alert, Box, Stack, Typography } from "@mui/material";

import { FiltersPanel } from "../filters/FiltersPanel";
import { MapPanel } from "../map/MapPanel";
import { RoadwayDetailSidebar } from "../map/RoadwayDetailSidebar";
import { useBoundaryLayersQuery } from "../../hooks/useBoundaryLayersQuery";
import { useGeorgiaFiltersQuery } from "../../hooks/useGeorgiaFiltersQuery";
import { useRoadwayLoader } from "../../hooks/useRoadwayLoader";
import { useRoadwayVisualizationCatalogQuery } from "../../hooks/useRoadwayVisualizationCatalogQuery";
import { useUrlSyncedFilters } from "../../hooks/useUrlSyncedFilters";
import { useAppStore } from "../../store/useAppStore";
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
  // Bidirectional URL<->store sync for the 9 admin-geography selections
  // plus include_unincorporated. Writes to URL on every selection change
  // via history.replaceState; reads the URL once on mount to seed state
  // so shared/bookmarked links reproduce filter combinations.
  useUrlSyncedFilters();

  const selectedDistricts = useAppStore((state) => state.selectedDistricts);
  const selectedCounties = useAppStore((state) => state.selectedCounties);
  const selectedHighwayTypes = useAppStore((state) => state.selectedHighwayTypes);
  const selectedAreaOffices = useAppStore((state) => state.selectedAreaOffices);
  const selectedMpos = useAppStore((state) => state.selectedMpos);
  const selectedRegionalCommissions = useAppStore(
    (state) => state.selectedRegionalCommissions,
  );
  const selectedStateHouseDistricts = useAppStore(
    (state) => state.selectedStateHouseDistricts,
  );
  const selectedStateSenateDistricts = useAppStore(
    (state) => state.selectedStateSenateDistricts,
  );
  const selectedCongressionalDistricts = useAppStore(
    (state) => state.selectedCongressionalDistricts,
  );
  const selectedCities = useAppStore((state) => state.selectedCities);
  const includeUnincorporated = useAppStore((state) => state.includeUnincorporated);
  const boundaryOverlayVisibility = useAppStore(
    (state) => state.boundaryOverlayVisibility,
  );
  const setBoundaryOverlayVisibility = useAppStore(
    (state) => state.setBoundaryOverlayVisibility,
  );
  const selectedVisualizationId = useAppStore((state) => state.selectedVisualizationId);
  const themeFilters = useAppStore((state) => state.themeFilters);
  const setSelectedDistricts = useAppStore((state) => state.setSelectedDistricts);
  const setSelectedCounties = useAppStore((state) => state.setSelectedCounties);
  const setSelectedHighwayTypes = useAppStore((state) => state.setSelectedHighwayTypes);
  const setSelectedAreaOffices = useAppStore((state) => state.setSelectedAreaOffices);
  const setSelectedMpos = useAppStore((state) => state.setSelectedMpos);
  const setSelectedRegionalCommissions = useAppStore(
    (state) => state.setSelectedRegionalCommissions,
  );
  const setSelectedStateHouseDistricts = useAppStore(
    (state) => state.setSelectedStateHouseDistricts,
  );
  const setSelectedStateSenateDistricts = useAppStore(
    (state) => state.setSelectedStateSenateDistricts,
  );
  const setSelectedCongressionalDistricts = useAppStore(
    (state) => state.setSelectedCongressionalDistricts,
  );
  const setSelectedCities = useAppStore((state) => state.setSelectedCities);
  const setIncludeUnincorporated = useAppStore(
    (state) => state.setIncludeUnincorporated,
  );
  const resetAllAdminFilters = useAppStore((state) => state.resetAllAdminFilters);
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

  const georgiaFiltersQuery = useGeorgiaFiltersQuery();
  const roadwayVisualizationsQuery = useRoadwayVisualizationCatalogQuery();
  const districts = georgiaFiltersQuery.data?.districts ?? [];
  const counties = georgiaFiltersQuery.data?.counties ?? [];
  const highwayTypes = georgiaFiltersQuery.data?.highway_types ?? [];
  const areaOffices = georgiaFiltersQuery.data?.area_offices ?? [];
  const mpos = georgiaFiltersQuery.data?.mpos ?? [];
  const regionalCommissions = georgiaFiltersQuery.data?.regional_commissions ?? [];
  const stateHouseDistricts = georgiaFiltersQuery.data?.state_house_districts ?? [];
  const stateSenateDistricts = georgiaFiltersQuery.data?.state_senate_districts ?? [];
  const congressionalDistricts =
    georgiaFiltersQuery.data?.congressional_districts ?? [];
  const cities = georgiaFiltersQuery.data?.cities ?? [];
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
    {
      districts: selectedDistricts,
      counties: selectedCounties,
      highwayTypes: selectedHighwayTypes,
      areaOffices: selectedAreaOffices,
      mpos: selectedMpos,
      regionalCommissions: selectedRegionalCommissions,
      stateHouseDistricts: selectedStateHouseDistricts,
      stateSenateDistricts: selectedStateSenateDistricts,
      congressionalDistricts: selectedCongressionalDistricts,
      cities: selectedCities,
      includeUnincorporated,
    },
    boundaryOverlayVisibility,
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

  const handleAreaOfficeDelete = (id: number) => {
    setSelectedAreaOffices(selectedAreaOffices.filter((areaOffice) => areaOffice !== id));
  };

  const handleMpoDelete = (id: string) => {
    setSelectedMpos(selectedMpos.filter((mpo) => mpo !== id));
  };

  const handleRegionalCommissionDelete = (id: number) => {
    setSelectedRegionalCommissions(
      selectedRegionalCommissions.filter((rc) => rc !== id),
    );
  };

  const handleStateHouseDelete = (id: number) => {
    setSelectedStateHouseDistricts(
      selectedStateHouseDistricts.filter((district) => district !== id),
    );
  };

  const handleStateSenateDelete = (id: number) => {
    setSelectedStateSenateDistricts(
      selectedStateSenateDistricts.filter((district) => district !== id),
    );
  };

  const handleCongressionalDelete = (id: number) => {
    setSelectedCongressionalDistricts(
      selectedCongressionalDistricts.filter((district) => district !== id),
    );
  };

  const handleCityDelete = (id: number) => {
    setSelectedCities(selectedCities.filter((city) => city !== id));
  };

  const handleResetFilters = () => {
    resetAllAdminFilters();
    resetAllThemeFilters();
  };

  const handleVisualizationChange = (visualizationId: string) => {
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
    boundaryLayersQuery.districtsQuery.isError ||
    boundaryLayersQuery.areaOfficesQuery.isError ||
    boundaryLayersQuery.mposQuery.isError ||
    boundaryLayersQuery.regionalCommissionsQuery.isError ||
    boundaryLayersQuery.stateHouseQuery.isError ||
    boundaryLayersQuery.stateSenateQuery.isError ||
    boundaryLayersQuery.congressionalQuery.isError;

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
                areaOffices={areaOffices}
                mpos={mpos}
                regionalCommissions={regionalCommissions}
                stateHouseDistricts={stateHouseDistricts}
                stateSenateDistricts={stateSenateDistricts}
                congressionalDistricts={congressionalDistricts}
                cities={cities}
                selectedDistricts={selectedDistricts}
                selectedCounties={selectedCounties}
                selectedHighwayTypes={selectedHighwayTypes}
                selectedAreaOffices={selectedAreaOffices}
                selectedMpos={selectedMpos}
                selectedRegionalCommissions={selectedRegionalCommissions}
                selectedStateHouseDistricts={selectedStateHouseDistricts}
                selectedStateSenateDistricts={selectedStateSenateDistricts}
                selectedCongressionalDistricts={selectedCongressionalDistricts}
                selectedCities={selectedCities}
                includeUnincorporated={includeUnincorporated}
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
                onAreaOfficeChange={setSelectedAreaOffices}
                onAreaOfficeDelete={handleAreaOfficeDelete}
                onMpoChange={setSelectedMpos}
                onMpoDelete={handleMpoDelete}
                onRegionalCommissionChange={setSelectedRegionalCommissions}
                onRegionalCommissionDelete={handleRegionalCommissionDelete}
                onStateHouseChange={setSelectedStateHouseDistricts}
                onStateHouseDelete={handleStateHouseDelete}
                onStateSenateChange={setSelectedStateSenateDistricts}
                onStateSenateDelete={handleStateSenateDelete}
                onCongressionalChange={setSelectedCongressionalDistricts}
                onCongressionalDelete={handleCongressionalDelete}
                onCityChange={setSelectedCities}
                onCityDelete={handleCityDelete}
                onIncludeUnincorporatedChange={setIncludeUnincorporated}
                setThemeFilter={setThemeFilter}
                resetThemeFilter={resetThemeFilter}
                boundaryOverlayVisibility={boundaryOverlayVisibility}
                onBoundaryOverlayToggle={setBoundaryOverlayVisibility}
                onResetFilters={handleResetFilters}
                onVisualizationChange={handleVisualizationChange}
              />
            </Box>

            <Box sx={{ minWidth: 0, minHeight: 0, overflow: "hidden" }}>
              <MapPanel
                roadwayChunks={roadwayLoader.roadwayChunks}
                countyBoundaries={boundaryLayersQuery.countiesQuery.data}
                districtBoundaries={boundaryLayersQuery.districtsQuery.data}
                areaOfficeBoundaries={boundaryLayersQuery.areaOfficesQuery.data}
                mpoBoundaries={boundaryLayersQuery.mposQuery.data}
                regionalCommissionBoundaries={
                  boundaryLayersQuery.regionalCommissionsQuery.data
                }
                stateHouseBoundaries={boundaryLayersQuery.stateHouseQuery.data}
                stateSenateBoundaries={boundaryLayersQuery.stateSenateQuery.data}
                congressionalBoundaries={boundaryLayersQuery.congressionalQuery.data}
                boundaryOverlayVisibility={boundaryOverlayVisibility}
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
