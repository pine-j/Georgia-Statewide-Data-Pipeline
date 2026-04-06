import { Alert, Box, Stack, Typography } from "@mui/material";

import { FiltersPanel } from "../filters/FiltersPanel";
import { MapPanel } from "../map/MapPanel";
import { useBoundaryLayersQuery } from "../../hooks/useBoundaryLayersQuery";
import { useGeorgiaFiltersQuery } from "../../hooks/useGeorgiaFiltersQuery";
import { useRoadwayLoader } from "../../hooks/useRoadwayLoader";
import { useAppStore } from "../../store/useAppStore";

export function AppShell() {
  const selectedDistrict = useAppStore((state) => state.selectedDistrict);
  const selectedCounties = useAppStore((state) => state.selectedCounties);
  const setSelectedDistrict = useAppStore((state) => state.setSelectedDistrict);
  const setSelectedCounties = useAppStore((state) => state.setSelectedCounties);

  const georgiaFiltersQuery = useGeorgiaFiltersQuery();
  const districts = georgiaFiltersQuery.data?.districts ?? [];
  const counties = georgiaFiltersQuery.data?.counties ?? [];
  const roadwayLoader = useRoadwayLoader(selectedDistrict, selectedCounties, true);
  const boundaryLayersQuery = useBoundaryLayersQuery(selectedDistrict, selectedCounties, true);

  const handleDistrictChange = (district: number | null) => {
    const nextSelectedCounties =
      district === null
        ? selectedCounties
        : selectedCounties.filter((countyName) =>
            counties.some(
              (county) =>
                county.county === countyName && county.district === district,
            ),
          );

    setSelectedDistrict(district);
    setSelectedCounties(nextSelectedCounties);
  };

  const handleCountyDelete = (countyName: string) => {
    setSelectedCounties(selectedCounties.filter((county) => county !== countyName));
  };

  const handleResetFilters = () => {
    setSelectedDistrict(null);
    setSelectedCounties([]);
  };

  const hasApiError =
    georgiaFiltersQuery.isError ||
    Boolean(roadwayLoader.error) ||
    boundaryLayersQuery.countiesQuery.isError ||
    boundaryLayersQuery.districtsQuery.isError;

  return (
    <Box
      sx={{
        minHeight: "100vh",
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

      <Box sx={{ px: { xs: 0, md: 0 }, py: 0, minHeight: 0 }}>
        <Stack spacing={0} sx={{ height: "100%" }}>
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
                lg: "320px minmax(0, 1fr)",
              },
              gridTemplateRows: {
                xs: "auto minmax(58vh, 1fr)",
                lg: "minmax(0, 1fr)",
              },
              alignItems: "stretch",
              height: "100%",
            }}
          >
            <Box
              sx={{
                minWidth: 0,
                minHeight: 0,
                borderRight: { xs: "none", lg: "1px solid rgba(17, 61, 73, 0.12)" },
              }}
            >
              <FiltersPanel
                districts={districts}
                counties={counties}
                selectedDistrict={selectedDistrict}
                selectedCounties={selectedCounties}
                onDistrictChange={handleDistrictChange}
                onCountyChange={setSelectedCounties}
                onCountyDelete={handleCountyDelete}
                onResetFilters={handleResetFilters}
              />
            </Box>

            <Box sx={{ minWidth: 0, minHeight: 0 }}>
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
              />
            </Box>
          </Box>
        </Stack>
      </Box>
    </Box>
  );
}
