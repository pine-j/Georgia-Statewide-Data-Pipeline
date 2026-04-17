import RestartAltRoundedIcon from "@mui/icons-material/RestartAltRounded";
import CloseRoundedIcon from "@mui/icons-material/CloseRounded";
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import {
  Autocomplete,
  Checkbox,
  Box,
  Button,
  IconButton,
  MenuItem,
  Paper,
  Stack,
  TextField,
  Typography,
  AutocompleteRenderGroupParams,
} from "@mui/material";

import {
  CountyOption,
  DistrictOption,
  HighwayTypeOption,
  RoadwayVisualizationCatalog,
  RoadwayVisualizationOption,
} from "../../types/api";
import { RoadwayLegendCard } from "../map/RoadwayLegendCard";

function displayDistrictLabel(label: string): string {
  const separatorIndex = label.indexOf(" - ");
  return separatorIndex >= 0 ? label.slice(separatorIndex + 3) : label;
}

/** Sentinel district option representing "Show all districts". */
const ALL_DISTRICTS_OPTION: DistrictOption = { id: -1, label: "All Districts" };

interface FiltersPanelProps {
  districts: DistrictOption[];
  counties: CountyOption[];
  highwayTypes: HighwayTypeOption[];
  selectedDistricts: number[];
  selectedCounties: string[];
  selectedHighwayTypes: string[];
  roadwayVisualizationCatalog?: RoadwayVisualizationCatalog;
  selectedVisualizationId: string;
  themeCoveragePercent: number | null;
  onDistrictChange: (districts: number[]) => void;
  onDistrictDelete: (districtId: number) => void;
  onCountyChange: (counties: string[]) => void;
  onCountyDelete: (county: string) => void;
  onHighwayTypeChange: (highwayTypes: string[]) => void;
  onHighwayTypeDelete: (highwayTypeId: string) => void;
  selectedVisualization?: RoadwayVisualizationOption;
  onResetFilters: () => void;
  onVisualizationChange: (visualizationId: string) => void;
  onLegendItemHover?: (value: string | null) => void;
}

export function FiltersPanel({
  districts,
  counties,
  highwayTypes,
  selectedDistricts,
  selectedCounties,
  selectedHighwayTypes,
  roadwayVisualizationCatalog,
  selectedVisualizationId,
  selectedVisualization,
  themeCoveragePercent,
  onDistrictChange,
  onDistrictDelete,
  onCountyChange,
  onCountyDelete,
  onHighwayTypeChange,
  onHighwayTypeDelete,
  onResetFilters,
  onVisualizationChange,
  onLegendItemHover,
}: FiltersPanelProps) {
  const isAllDistricts = selectedDistricts.length === 0;
  const districtOptionsWithAll = [ALL_DISTRICTS_OPTION, ...districts];
  // When "all", don't include the sentinel in the Autocomplete value —
  // it just shows as a checked option in the dropdown.
  const selectedDistrictOptions = isAllDistricts
    ? []
    : districts.filter((district) => selectedDistricts.includes(district.id));
  const countyOptions =
    selectedDistricts.length > 0
      ? counties.filter((county) => selectedDistricts.includes(county.district))
      : counties;

  // Build a district-id → label map for county grouping
  const districtLabelMap = new Map(districts.map((d) => [d.id, displayDistrictLabel(d.label)]));

  // Sort counties by district, then alphabetically within district
  const sortedCountyOptions = [...countyOptions].sort((a, b) => {
    if (a.district !== b.district) return a.district - b.district;
    return a.county.localeCompare(b.county);
  });

  const selectedHighwayTypeOptions = highwayTypes.filter((highwayType) =>
    selectedHighwayTypes.includes(highwayType.id),
  );
  const hasActiveFilters = Boolean(
    selectedDistricts.length || selectedCounties.length || selectedHighwayTypeOptions.length,
  );
  const thematicOptions = roadwayVisualizationCatalog?.thematic_options ?? [];

  const handleDistrictAutocompleteChange = (_: unknown, values: DistrictOption[]) => {
    const hasAllOption = values.some((v) => v.id === ALL_DISTRICTS_OPTION.id);
    if (hasAllOption) {
      // "Show all districts" was clicked → clear all specific selections
      onDistrictChange([]);
    } else {
      // Specific districts selected (only real district ids, never -1)
      onDistrictChange(values.filter((v) => v.id !== ALL_DISTRICTS_OPTION.id).map((v) => v.id));
    }
  };

  return (
    <Paper
      elevation={0}
      sx={{
        borderRadius: 0,
        border: "0",
        bgcolor: "#ffffff",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {/* Scrollable filters area */}
      <Box sx={{ flex: 1, overflowY: "auto", p: 1.5, pb: 1 }}>
      <Stack spacing={1}>
        {thematicOptions.length > 0 && (
          <Stack spacing={0.5}>
            <TextField
              select
              label="Map Theme"
              size="small"
              value={selectedVisualizationId}
              onChange={(event) => onVisualizationChange(event.target.value)}
              SelectProps={{
                MenuProps: {
                  PaperProps: {
                    sx: { maxHeight: "80vh" },
                  },
                },
              }}
              inputProps={{ sx: { fontSize: "0.78rem", py: "5px" } }}
              InputLabelProps={{ sx: { fontSize: "0.78rem" } }}
              fullWidth
            >
              {thematicOptions.map((option) => (
                <MenuItem key={option.id} value={option.id} sx={{ fontSize: "0.78rem", py: 0.5 }}>
                  {option.label}
                </MenuItem>
              ))}
            </TextField>

            <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.3, fontStyle: "italic", fontSize: "0.68rem" }}>
              2024 traffic data
              {themeCoveragePercent !== null && (
                <> · <strong>{themeCoveragePercent}%</strong> coverage</>
              )}
            </Typography>
          </Stack>
        )}

        {hasActiveFilters && (
          <Stack spacing={0.25} sx={{ pt: 0 }}>
            <Box
              sx={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 1,
              }}
            >
              <Typography variant="caption" color="text.secondary" sx={{ letterSpacing: 0.3 }}>
                Active Filters
              </Typography>
              <Button
                size="small"
                color="inherit"
                startIcon={<RestartAltRoundedIcon fontSize="small" />}
                onClick={onResetFilters}
                sx={{ minWidth: 0, px: 0.5, py: 0, fontSize: "0.7rem", lineHeight: 1.2 }}
              >
                Reset all
              </Button>
            </Box>

            {selectedHighwayTypeOptions.length > 0 && (
              <Box
                sx={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: 0.5,
                  minWidth: 0,
                }}
              >
                <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                  Highway Type:
                </Typography>
                <Box
                  sx={{
                    minWidth: 0,
                    flex: 1,
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 0.5,
                  }}
                >
                  {selectedHighwayTypeOptions.map((highwayType) => (
                    <Box
                      key={highwayType.id}
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 0.5,
                        minWidth: 0,
                        px: 0.5,
                        py: 0.125,
                        borderRadius: "4px",
                        bgcolor: "rgba(17, 61, 73, 0.06)",
                      }}
                    >
                      <Typography
                        variant="caption"
                        sx={{
                          color: "text.primary",
                          fontWeight: 600,
                          minWidth: 0,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {highwayType.label}
                      </Typography>
                      <IconButton
                        size="small"
                        onClick={() => onHighwayTypeDelete(highwayType.id)}
                        aria-label={`Remove ${highwayType.label} highway type filter`}
                        sx={{ p: 0.25 }}
                      >
                        <CloseRoundedIcon fontSize="inherit" />
                      </IconButton>
                    </Box>
                  ))}
                </Box>
              </Box>
            )}

            {selectedDistrictOptions.length > 0 && (
              <Box
                sx={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: 0.5,
                  minWidth: 0,
                }}
              >
                <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                  District:
                </Typography>
                <Box
                  sx={{
                    minWidth: 0,
                    flex: 1,
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 0.5,
                  }}
                >
                  {selectedDistrictOptions.map((district) => (
                    <Box
                      key={district.id}
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 0.5,
                        minWidth: 0,
                        px: 0.5,
                        py: 0.125,
                        borderRadius: "4px",
                        bgcolor: "rgba(17, 61, 73, 0.06)",
                      }}
                    >
                      <Typography
                        variant="caption"
                        sx={{
                          color: "text.primary",
                          fontWeight: 600,
                          minWidth: 0,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {displayDistrictLabel(district.label)}
                      </Typography>
                      <IconButton
                        size="small"
                        onClick={() => onDistrictDelete(district.id)}
                        aria-label={`Remove ${displayDistrictLabel(district.label)} district filter`}
                        sx={{ p: 0.25 }}
                      >
                        <CloseRoundedIcon fontSize="inherit" />
                      </IconButton>
                    </Box>
                  ))}
                </Box>
              </Box>
            )}

            {selectedCounties.length > 0 && (
              <Box
                sx={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: 0.5,
                  minWidth: 0,
                }}
              >
                <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                  County:
                </Typography>
                <Box
                  sx={{
                    minWidth: 0,
                    flex: 1,
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 0.5,
                  }}
                >
                  {selectedCounties.map((county) => (
                    <Box
                      key={county}
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 0.5,
                        minWidth: 0,
                        px: 0.5,
                        py: 0.125,
                        borderRadius: "4px",
                        bgcolor: "rgba(17, 61, 73, 0.06)",
                      }}
                    >
                      <Typography
                        variant="caption"
                        sx={{
                          color: "text.primary",
                          fontWeight: 600,
                          minWidth: 0,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {county}
                      </Typography>
                      <IconButton
                        size="small"
                        onClick={() => onCountyDelete(county)}
                        aria-label={`Remove ${county} county filter`}
                        sx={{ p: 0.25 }}
                      >
                        <CloseRoundedIcon fontSize="inherit" />
                      </IconButton>
                    </Box>
                  ))}
                </Box>
              </Box>
            )}
          </Stack>
        )}

        <Autocomplete
          multiple
          size="small"
          options={highwayTypes}
          value={selectedHighwayTypeOptions}
          onChange={(_, values) => onHighwayTypeChange(values.map((value) => value.id))}
          disableCloseOnSelect
          renderOption={(props, option, { selected }) => (
            <li {...props} style={{ fontSize: "0.78rem", paddingTop: 2, paddingBottom: 2 }}>
              <Checkbox
                icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                checkedIcon={<CheckBoxIcon fontSize="small" />}
                style={{ marginRight: 6, padding: 2 }}
                checked={selected}
              />
              {option.label} ({option.id})
            </li>
          )}
          isOptionEqualToValue={(option, value) => option.id === value.id}
          getOptionLabel={(option) => `${option.label} (${option.id})`}
          renderTags={() => null}
          renderInput={(params) => (
            <TextField
              {...params}
              label="Highway Type"
              placeholder="Select highway types"
              inputProps={{ ...params.inputProps, sx: { fontSize: "0.78rem" } }}
              InputLabelProps={{ sx: { fontSize: "0.78rem" } }}
            />
          )}
        />

        <Autocomplete
          multiple
          size="small"
          options={districtOptionsWithAll}
          value={selectedDistrictOptions}
          onChange={handleDistrictAutocompleteChange}
          disableCloseOnSelect
          renderOption={(props, option, { selected }) => {
            const isAll = option.id === ALL_DISTRICTS_OPTION.id;
            return (
              <li {...props} style={{ fontSize: "0.78rem", paddingTop: 2, paddingBottom: 2 }}>
                <Checkbox
                  icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                  checkedIcon={<CheckBoxIcon fontSize="small" />}
                  style={{ marginRight: 6, padding: 2 }}
                  checked={isAll ? isAllDistricts : selected}
                />
                <span style={{ fontWeight: isAll ? 600 : 400 }}>
                  {isAll ? "Show all districts" : displayDistrictLabel(option.label)}
                </span>
              </li>
            );
          }}
          isOptionEqualToValue={(option, value) => option.id === value.id}
          getOptionLabel={(option) =>
            option.id === ALL_DISTRICTS_OPTION.id ? "All Districts" : displayDistrictLabel(option.label)
          }
          renderTags={() => null}
          renderInput={(params) => (
            <TextField
              {...params}
              label="District"
              placeholder={isAllDistricts ? "All districts shown" : "Select districts"}
              inputProps={{ ...params.inputProps, sx: { fontSize: "0.78rem" } }}
              InputLabelProps={{ sx: { fontSize: "0.78rem" } }}
            />
          )}
        />

        <Autocomplete
          multiple
          size="small"
          options={sortedCountyOptions}
          value={sortedCountyOptions.filter((co) => selectedCounties.includes(co.county))}
          onChange={(_, values) => onCountyChange(values.map((v) => v.county))}
          groupBy={(option) => districtLabelMap.get(option.district) ?? `District ${option.district}`}
          getOptionLabel={(option) => option.county}
          isOptionEqualToValue={(option, value) => option.county === value.county}
          disableCloseOnSelect
          renderGroup={(params: AutocompleteRenderGroupParams) => (
            <li key={params.key}>
              <Box
                sx={{
                  position: "sticky",
                  top: -8,
                  zIndex: 1,
                  px: 1.5,
                  py: 0.5,
                  bgcolor: "#f0f2f3",
                  borderBottom: "1px solid rgba(17, 61, 73, 0.1)",
                }}
              >
                <Typography
                  variant="caption"
                  sx={{
                    fontWeight: 700,
                    color: "#47626b",
                    textTransform: "uppercase",
                    fontSize: "0.68rem",
                    letterSpacing: 0.4,
                  }}
                >
                  {params.group}
                </Typography>
              </Box>
              <ul style={{ padding: 0 }}>{params.children}</ul>
            </li>
          )}
          renderOption={(props, option, { selected }) => (
            <li {...props} style={{ fontSize: "0.78rem", paddingTop: 2, paddingBottom: 2 }}>
              <Checkbox
                icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                checkedIcon={<CheckBoxIcon fontSize="small" />}
                style={{ marginRight: 6, padding: 2 }}
                checked={selected}
              />
              {option.county}
            </li>
          )}
          renderTags={() => null}
          renderInput={(params) => (
            <TextField
              {...params}
              label="County"
              placeholder="Search counties"
              helperText="Includes segments crossing county boundaries."
              inputProps={{ ...params.inputProps, sx: { fontSize: "0.78rem" } }}
              InputLabelProps={{ sx: { fontSize: "0.78rem" } }}
              FormHelperTextProps={{ sx: { fontSize: "0.65rem", mt: 0.25, lineHeight: 1.3 } }}
            />
          )}
        />

      </Stack>
      </Box>

      {/* Legend pinned to bottom, always visible */}
      {selectedVisualization && (
        <Box sx={{ flexShrink: 0 }}>
          <RoadwayLegendCard visualization={selectedVisualization} onLegendItemHover={onLegendItemHover} />
        </Box>
      )}
    </Paper>
  );
}
