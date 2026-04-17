import { Box, Paper, Stack, Typography } from "@mui/material";

import { RoadwayVisualizationOption } from "../../types/api";
import { getLegendItemsForDisplay } from "./roadwayVisualization";

interface RoadwayLegendCardProps {
  visualization: RoadwayVisualizationOption;
  onLegendItemHover?: (value: string | null) => void;
}

const SWATCH_GRID = {
  display: "grid",
  gridTemplateColumns: "16px minmax(0, 1fr)",
  gap: 1,
  alignItems: "center",
  borderRadius: "4px",
  px: 0.5,
  mx: -0.5,
  cursor: "pointer",
  transition: "background-color 0.15s ease",
  "&:hover": {
    bgcolor: "rgba(17, 61, 73, 0.07)",
  },
} as const;

export function RoadwayLegendCard({ visualization, onLegendItemHover }: RoadwayLegendCardProps) {
  const legendItems = getLegendItemsForDisplay(visualization);

  const handleEnter = (value: string | null) => () => onLegendItemHover?.(value);
  const handleLeave = () => onLegendItemHover?.(null);

  return (
    <Paper
      elevation={0}
      sx={{
        width: "100%",
        p: 1.5,
        borderRadius: 0,
        border: "1px solid rgba(17, 61, 73, 0.12)",
        bgcolor: "rgba(246, 248, 249, 0.96)",
      }}
    >
      <Stack spacing={1.1}>
        <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
          {visualization.label}
        </Typography>

        <Typography variant="caption" color="text.secondary">
          {visualization.description}
        </Typography>

        <Stack spacing={0.75}>
          {legendItems.map((item) => {
            // For categorical, use the `value` field; for numeric, use the label
            const hoverKey =
              visualization.kind === "categorical"
                ? (item.value ?? item.label)
                : item.label;

            return (
              <Box
                key={`${visualization.id}-${item.label}-${item.value ?? item.min_value ?? "base"}`}
                sx={SWATCH_GRID}
                onMouseEnter={handleEnter(hoverKey)}
                onMouseLeave={handleLeave}
              >
                <Box
                  sx={{
                    width: 16,
                    height: 16,
                    borderRadius: "2px",
                    bgcolor: item.color,
                    border: "1px solid rgba(16, 35, 47, 0.12)",
                  }}
                />
                <Typography variant="caption" sx={{ lineHeight: 1.35 }}>
                  {item.label}
                </Typography>
              </Box>
            );
          })}

          {/* "No data" row */}
          <Box
            sx={SWATCH_GRID}
            onMouseEnter={handleEnter("__NO_DATA__")}
            onMouseLeave={handleLeave}
          >
            <Box
              sx={{
                width: 16,
                height: 16,
                borderRadius: "2px",
                bgcolor: visualization.no_data_color,
                border: "1px solid rgba(16, 35, 47, 0.12)",
              }}
            />
            <Typography variant="caption" sx={{ lineHeight: 1.35 }}>
              No data
            </Typography>
          </Box>
        </Stack>

        {visualization.notes && (
          <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.45 }}>
            {visualization.notes}
          </Typography>
        )}
      </Stack>
    </Paper>
  );
}
