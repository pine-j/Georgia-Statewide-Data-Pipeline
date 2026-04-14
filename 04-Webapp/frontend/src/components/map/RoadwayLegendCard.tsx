import { Box, Chip, Paper, Stack, Typography } from "@mui/material";

import { RoadwayVisualizationOption } from "../../types/api";
import { getImplementationStatusLabel, getLegendItemsForDisplay } from "./roadwayVisualization";

interface RoadwayLegendCardProps {
  visualization: RoadwayVisualizationOption;
}

export function RoadwayLegendCard({ visualization }: RoadwayLegendCardProps) {
  const legendItems = getLegendItemsForDisplay(visualization);

  return (
    <Paper
      elevation={0}
      sx={{
        width: "min(320px, calc(100vw - 32px))",
        p: 1.5,
        borderRadius: 0,
        border: "1px solid rgba(17, 61, 73, 0.12)",
        bgcolor: "rgba(255, 255, 255, 0.96)",
        backdropFilter: "blur(12px)",
      }}
    >
      <Stack spacing={1.1}>
        <Stack direction="row" justifyContent="space-between" alignItems="flex-start" spacing={1}>
          <Box sx={{ minWidth: 0 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
              {visualization.label}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Texas {visualization.texas_header}
              {visualization.georgia_header ? ` -> Georgia ${visualization.georgia_header}` : ""}
            </Typography>
          </Box>

          <Chip
            size="small"
            label={getImplementationStatusLabel(visualization.implementation_status)}
            sx={{ height: 22 }}
          />
        </Stack>

        <Typography variant="caption" color="text.secondary">
          {visualization.description}
        </Typography>

        <Stack spacing={0.75}>
          {legendItems.map((item) => (
            <Box
              key={`${visualization.id}-${item.label}-${item.value ?? item.min_value ?? "base"}`}
              sx={{
                display: "grid",
                gridTemplateColumns: "16px minmax(0, 1fr)",
                gap: 1,
                alignItems: "center",
              }}
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
          ))}

          <Box
            sx={{
              display: "grid",
              gridTemplateColumns: "16px minmax(0, 1fr)",
              gap: 1,
              alignItems: "center",
            }}
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
