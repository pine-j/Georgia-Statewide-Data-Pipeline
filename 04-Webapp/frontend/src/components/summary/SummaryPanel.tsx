import {
  Alert,
  Divider,
  List,
  ListItem,
  ListItemText,
  Paper,
  Stack,
  Typography,
} from "@mui/material";

import { AnalyticsSummary } from "../../types/api";

interface SummaryPanelProps {
  summary?: AnalyticsSummary;
  bounds?: [number, number, number, number] | null;
  isLoading: boolean;
  selectedDistrict: number | null;
  selectedCounties: string[];
}

const UPCOMING_LAYERS = [
  "Traffic counts",
  "Safety",
  "Bridges",
  "Transit and connectivity",
];

export function SummaryPanel({
  summary,
  bounds,
  isLoading,
  selectedDistrict,
  selectedCounties,
}: SummaryPanelProps) {
  return (
    <Paper
      elevation={0}
      sx={{
        p: 3,
        border: "1px solid rgba(34, 64, 74, 0.12)",
        bgcolor: "#ffffff",
      }}
    >
      <Stack spacing={2}>
        <div>
          <Typography variant="h6">Georgia Layer Summary</Typography>
          <Typography variant="body2" color="text.secondary">
            A Georgia-specific side panel for the pipeline app, while the backend
            architecture stays aligned with the Raptor-style service split.
          </Typography>
        </div>

        {isLoading && <Alert severity="info">Loading roadway summary...</Alert>}

        <div>
          <Typography variant="subtitle2">Selection</Typography>
          <Typography variant="body2" color="text.secondary">
            {selectedDistrict ? `District ${selectedDistrict}` : "All districts"}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {selectedCounties.length
              ? `${selectedCounties.length} county filters applied`
              : "All counties"}
          </Typography>
        </div>

        {summary && (
          <>
            <Divider />
            <Typography variant="body1">
              <strong>{summary.roadway_count}</strong> roadway segments across{" "}
              <strong>{summary.total_miles.toFixed(2)}</strong> miles.
            </Typography>

            <List dense disablePadding>
              {summary.classes.map((item) => (
                <ListItem key={item.functional_class} disableGutters>
                  <ListItemText
                    primary={item.functional_class}
                    secondary={`${item.segment_count} segments | ${item.total_miles.toFixed(2)} miles`}
                  />
                </ListItem>
              ))}
            </List>
          </>
        )}

        {bounds && (
          <>
            <Divider />
            <Typography variant="subtitle2">Current Map Bounds</Typography>
            <Typography variant="body2" color="text.secondary">
              {bounds.map((value) => value.toFixed(4)).join(", ")}
            </Typography>
          </>
        )}

        <Divider />

        <div>
          <Typography variant="subtitle2">Layer Roadmap</Typography>
          <List dense disablePadding>
            {UPCOMING_LAYERS.map((layer) => (
              <ListItem key={layer} disableGutters>
                <ListItemText primary={layer} secondary="Placeholder for upcoming Georgia data layers" />
              </ListItem>
            ))}
          </List>
        </div>
      </Stack>
    </Paper>
  );
}
