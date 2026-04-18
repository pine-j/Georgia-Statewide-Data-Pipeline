import { Box, Chip, LinearProgress, Paper, Stack, Typography } from "@mui/material";

import {
  BoundsResponse,
  GeoJsonFeatureCollection,
  RoadwayFeatureCollection,
  RoadwayVisualizationOption,
} from "../../types/api";
import type { ThemeFilterValue } from "../../store/useAppStore";
import { MapLibreRoadwayMap } from "./MapLibreRoadwayMap";

interface MapPanelProps {
  roadwayChunks: RoadwayFeatureCollection[];
  countyBoundaries?: GeoJsonFeatureCollection;
  districtBoundaries?: GeoJsonFeatureCollection;
  loadToken: number;
  bounds?: BoundsResponse["bounds"];
  isLoading: boolean;
  isManifestLoading: boolean;
  loadedSegments: number;
  totalSegments: number;
  progressPercent: number;
  etaSeconds: number | null;
  selectedVisualization?: RoadwayVisualizationOption;
  themeFilterState?: ThemeFilterValue;
  selectedRoadwayId?: string | null;
  onSegmentClick?: (uniqueId: string) => void;
  onBackgroundClick?: () => void;
}

function formatEta(etaSeconds: number | null): string {
  if (etaSeconds === null || !Number.isFinite(etaSeconds)) {
    return "Estimating remaining load time";
  }

  if (etaSeconds < 60) {
    return `About ${Math.max(1, Math.ceil(etaSeconds))}s remaining`;
  }

  const minutes = Math.ceil(etaSeconds / 60);
  return `About ${minutes}m remaining`;
}

export function MapPanel({
  roadwayChunks,
  countyBoundaries,
  districtBoundaries,
  loadToken,
  bounds,
  isLoading,
  isManifestLoading,
  loadedSegments,
  totalSegments,
  progressPercent,
  etaSeconds,
  selectedVisualization,
  themeFilterState,
  selectedRoadwayId,
  onSegmentClick,
  onBackgroundClick,
}: MapPanelProps) {
  const isShowingProgress = isManifestLoading || isLoading;
  const hasLoadedSegments = loadedSegments > 0;

  return (
    <Paper
      elevation={0}
      sx={{
        position: "relative",
        height: "100%",
        minHeight: { xs: "58vh", lg: 0 },
        borderRadius: 0,
        border: "0",
        overflow: "hidden",
        bgcolor: "#dfe7ea",
      }}
    >
      <Box sx={{ position: "absolute", inset: 0 }}>
        <MapLibreRoadwayMap
          roadwayChunks={roadwayChunks}
          countyBoundaries={countyBoundaries}
          districtBoundaries={districtBoundaries}
          loadToken={loadToken}
          bounds={bounds}
          selectedVisualization={selectedVisualization}
          themeFilterState={themeFilterState}
          selectedRoadwayId={selectedRoadwayId}
          onSegmentClick={onSegmentClick}
          onBackgroundClick={onBackgroundClick}
        />
      </Box>

      {isShowingProgress && (
        <Box
          sx={{
            position: "absolute",
            inset: 0,
            display: "grid",
            placeItems: "center",
            bgcolor: "rgba(241, 245, 246, 0.72)",
            backdropFilter: "blur(2px)",
            zIndex: 1,
          }}
        >
          <Paper
            elevation={0}
            sx={{
              width: "min(420px, calc(100% - 32px))",
              p: 2,
              borderRadius: 0,
              border: "1px solid rgba(17, 61, 73, 0.12)",
              bgcolor: "rgba(255, 255, 255, 0.98)",
            }}
          >
            <Stack spacing={1}>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Typography variant="subtitle2">
                  {isManifestLoading ? "Preparing roadway load" : "Loading roadway segments"}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {totalSegments > 0
                    ? `${loadedSegments.toLocaleString()} / ${totalSegments.toLocaleString()}`
                    : "Counting segments"}
                </Typography>
              </Stack>

              <LinearProgress
                variant={totalSegments > 0 ? "determinate" : "indeterminate"}
                value={progressPercent}
                sx={{ height: 8, borderRadius: 999 }}
              />

              <Typography variant="caption" color="text.secondary">
                {totalSegments > 0
                  ? `${progressPercent.toFixed(1)}% complete. ${formatEta(etaSeconds)}.`
                  : "Gathering the current segment count and map extent."}
              </Typography>
            </Stack>
          </Paper>
        </Box>
      )}

      {!isShowingProgress && hasLoadedSegments && (
        <Box
          sx={{
            position: "absolute",
            left: 16,
            bottom: 16,
            zIndex: 2,
          }}
        >
          <Chip
            label={`${loadedSegments.toLocaleString()} segments loaded`}
            color="success"
            variant="filled"
          />
        </Box>
      )}
    </Paper>
  );
}
