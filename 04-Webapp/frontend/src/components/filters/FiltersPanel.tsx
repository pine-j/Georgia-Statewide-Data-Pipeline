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
} from "../../types/api";

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
  onResetFilters: () => void;
  onVisualizationChange: (visualizationId: string) => void;
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
  themeCoveragePercent,
  onDistrictChange,
  onDistrictDelete,
  onCountyChange,
  onCountyDelete,
  onHighwayTypeChange,
  onHighwayTypeDelete,
  onResetFilters,
  onVisualizationChange,
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
        p: 2,
        borderRadius: 0,
        border: "0",
        bgcolor: "#ffffff",
        height: "100%",
        overflowY: "auto",
      }}
    >
      <Stack spacing={2}>
        <Stack
          direction="row"
          justifyContent="space-between"
          alignItems="center"
          spacing={1}
        >
          <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
            Filters
          </Typography>
        </Stack>

        {thematicOptions.length > 0 && (
          <Stack spacing={1}>
            <TextField
              select
              label="Map Theme"
              size="small"
              value={selectedVisualizationId}
              onChange={(event) => onVisualizationChange(event.target.value)}
              SelectProps={{ native: true }}
              fullWidth
            >
              {thematicOptions.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.label}
                </option>
              ))}
            </TextField>

            <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.45, fontStyle: "italic" }}>
              All traffic data represents 2024 values unless otherwise noted.
            </Typography>

            {themeCoveragePercent !== null && (
              <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.45 }}>
                Data coverage: <strong>{themeCoveragePercent}%</strong> of shown road network
              </Typography>
            )}
          </Stack>
        )}

        {hasActiveFilters && (
          <Stack spacing={0.5} sx={{ pt: 0.25 }}>
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
            <li {...props}>
              <Checkbox
                icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                checkedIcon={<CheckBoxIcon fontSize="small" />}
                style={{ marginRight: 8 }}
                checked={selected}
              />
              {option.label}
            </li>
          )}
          isOptionEqualToValue={(option, value) => option.id === value.id}
          getOptionLabel={(option) => option.label}
          renderTags={() => null}
          renderInput={(params) => (
            <TextField
              {...params}
              label="Highway Type"
              placeholder="Select highway types"
              helperText="IH = Interstate, US = U.S. Route, SH = State Route, Local = Local/Other."
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
              <li {...props}>
                <Checkbox
                  icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                  checkedIcon={<CheckBoxIcon fontSize="small" />}
                  style={{ marginRight: 8 }}
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
                  bgcolor: "rgba(17, 61, 73, 0.06)",
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
            <li {...props}>
              <Checkbox
                icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                checkedIcon={<CheckBoxIcon fontSize="small" />}
                style={{ marginRight: 8 }}
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
              helperText="Matches segments crossing the selected county, including segments continuing beyond it."
            />
          )}
        />
      </Stack>
    </Paper>
  );
}
