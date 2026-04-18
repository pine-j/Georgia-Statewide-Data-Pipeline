import RestartAltRoundedIcon from "@mui/icons-material/RestartAltRounded";
import CloseRoundedIcon from "@mui/icons-material/CloseRounded";
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import {
  Autocomplete,
  AutocompleteRenderGroupParams,
  Box,
  Button,
  Checkbox,
  IconButton,
  MenuItem,
  Paper,
  Stack,
  TextField,
  Typography,
} from "@mui/material";

import {
  CountyOption,
  DistrictOption,
  HighwayTypeOption,
  RoadwayVisualizationCatalog,
  RoadwayVisualizationOption,
} from "../../types/api";
import type { ThemeFilterState, ThemeFilterValue } from "../../store/useAppStore";
import { RoadwayLegendCard } from "../map/RoadwayLegendCard";
import { LegendPresence } from "../map/roadwayVisualization";
import { ThemeContextFilter } from "./ThemeContextFilter";

const ALL_DISTRICTS_OPTION: DistrictOption = { id: -1, label: "All Districts" };

interface ActiveThemeFilterChip {
  key: string;
  label: string;
  onDelete: () => void;
}

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
    includeNoData: firstFilter?.include_no_data_default ?? true,
  };
}

function getEffectiveThemeFilterValue(
  option?: RoadwayVisualizationOption,
  themeFilterValue?: ThemeFilterValue,
): ThemeFilterValue | null {
  const defaults = buildDefaultThemeFilterValue(option);
  if (!defaults) {
    return null;
  }

  return {
    selectedValues: themeFilterValue?.selectedValues ?? defaults.selectedValues,
    range: themeFilterValue?.range ?? defaults.range,
    includeNoData: themeFilterValue?.includeNoData ?? defaults.includeNoData,
  };
}

function formatThemeRange(range: [number, number], unit: string | null): string {
  const formatValue = (value: number) =>
    Number.isInteger(value)
      ? value.toLocaleString()
      : value.toLocaleString(undefined, { maximumFractionDigits: 1 });

  const base = `${formatValue(range[0])} to ${formatValue(range[1])}`;
  return unit ? `${base} ${unit}` : base;
}

interface FiltersPanelProps {
  districts: DistrictOption[];
  counties: CountyOption[];
  highwayTypes: HighwayTypeOption[];
  selectedDistricts: number[];
  selectedCounties: string[];
  selectedHighwayTypes: string[];
  themeFilters: ThemeFilterState;
  roadwayVisualizationCatalog?: RoadwayVisualizationCatalog;
  selectedVisualizationId: string;
  themeViewPercent: number | null;
  themeCoveragePercent: number | null;
  onDistrictChange: (districts: number[]) => void;
  onDistrictDelete: (districtId: number) => void;
  onCountyChange: (counties: string[]) => void;
  onCountyDelete: (county: string) => void;
  onHighwayTypeChange: (highwayTypes: string[]) => void;
  onHighwayTypeDelete: (highwayTypeId: string) => void;
  setThemeFilter: (visualizationId: string, patch: Partial<ThemeFilterValue>) => void;
  resetThemeFilter: (visualizationId: string) => void;
  selectedVisualization?: RoadwayVisualizationOption;
  legendPresence?: LegendPresence | null;
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
  themeFilters,
  roadwayVisualizationCatalog,
  selectedVisualizationId,
  selectedVisualization,
  legendPresence,
  themeViewPercent,
  themeCoveragePercent,
  onDistrictChange,
  onDistrictDelete,
  onCountyChange,
  onCountyDelete,
  onHighwayTypeChange,
  onHighwayTypeDelete,
  setThemeFilter,
  resetThemeFilter,
  onResetFilters,
  onVisualizationChange,
  onLegendItemHover,
}: FiltersPanelProps) {
  const isAllDistricts = selectedDistricts.length === 0;
  const districtOptionsWithAll = [ALL_DISTRICTS_OPTION, ...districts];
  const selectedDistrictOptions = isAllDistricts
    ? []
    : districts.filter((district) => selectedDistricts.includes(district.id));
  const countyOptions =
    selectedDistricts.length > 0
      ? counties.filter((county) => selectedDistricts.includes(county.district))
      : counties;

  const districtLabelMap = new Map(districts.map((district) => [district.id, district.label]));

  const sortedCountyOptions = [...countyOptions].sort((left, right) => {
    if (left.district !== right.district) {
      return left.district - right.district;
    }
    return left.county.localeCompare(right.county);
  });

  const selectedHighwayTypeOptions = highwayTypes.filter((highwayType) =>
    selectedHighwayTypes.includes(highwayType.id),
  );
  const thematicOptions = roadwayVisualizationCatalog?.thematic_options ?? [];
  const defaultThemeFilterValue = buildDefaultThemeFilterValue(selectedVisualization);
  const effectiveThemeFilterValue = getEffectiveThemeFilterValue(
    selectedVisualization,
    selectedVisualization ? themeFilters[selectedVisualization.id] : undefined,
  );
  const themeFilterChips: ActiveThemeFilterChip[] = [];

  if (
    selectedVisualization &&
    defaultThemeFilterValue &&
    effectiveThemeFilterValue
  ) {
    const defaultSelectedValues = new Set(defaultThemeFilterValue.selectedValues);
    const currentSelectedValues = new Set(effectiveThemeFilterValue.selectedValues);

    for (const spec of selectedVisualization.filters) {
      if (
        spec.control === "toggle_chips" ||
        spec.control === "multi_select" ||
        spec.control === "bin_multi_select"
      ) {
        for (const bin of spec.bins) {
          const selectionKey = getThemeFilterBinKey(bin);
          const defaultSelected = defaultSelectedValues.has(selectionKey);
          const currentSelected = currentSelectedValues.has(selectionKey);
          if (defaultSelected === currentSelected) {
            continue;
          }

          themeFilterChips.push({
            key: `${spec.property_name}-${selectionKey}`,
            label: bin.label,
            onDelete: () => {
              const nextSelectedValues = currentSelected
                ? effectiveThemeFilterValue.selectedValues.filter((value) => value !== selectionKey)
                : [...effectiveThemeFilterValue.selectedValues, selectionKey];

              setThemeFilter(selectedVisualization.id, {
                selectedValues: nextSelectedValues,
                range: effectiveThemeFilterValue.range,
                includeNoData: effectiveThemeFilterValue.includeNoData,
              });
            },
          });
        }
      }

      if (
        spec.control === "range_slider" &&
        defaultThemeFilterValue.range &&
        effectiveThemeFilterValue.range &&
        (
          effectiveThemeFilterValue.range[0] !== defaultThemeFilterValue.range[0] ||
          effectiveThemeFilterValue.range[1] !== defaultThemeFilterValue.range[1]
        )
      ) {
        themeFilterChips.push({
          key: `${spec.property_name}-range`,
          label: `${spec.label ?? "Range"}: ${formatThemeRange(
            effectiveThemeFilterValue.range,
            selectedVisualization.unit,
          )}`,
          onDelete: () =>
            setThemeFilter(selectedVisualization.id, {
              selectedValues: effectiveThemeFilterValue.selectedValues,
              range: defaultThemeFilterValue.range,
              includeNoData: effectiveThemeFilterValue.includeNoData,
            }),
        });
      }
    }

    if (effectiveThemeFilterValue.includeNoData !== defaultThemeFilterValue.includeNoData) {
      themeFilterChips.push({
        key: `${selectedVisualization.id}-include-no-data`,
        label: defaultThemeFilterValue.includeNoData ? "No data hidden" : "No data",
        onDelete: () =>
          setThemeFilter(selectedVisualization.id, {
            selectedValues: effectiveThemeFilterValue.selectedValues,
            range: effectiveThemeFilterValue.range,
            includeNoData: defaultThemeFilterValue.includeNoData,
          }),
      });
    }
  }

  const hasActiveFilters = Boolean(
    selectedDistricts.length ||
      selectedCounties.length ||
      selectedHighwayTypeOptions.length ||
      themeFilterChips.length,
  );

  const handleDistrictAutocompleteChange = (_: unknown, values: DistrictOption[]) => {
    if (values.some((value) => value.id === ALL_DISTRICTS_OPTION.id)) {
      onDistrictChange([]);
      return;
    }

    onDistrictChange(values.filter((value) => value.id !== ALL_DISTRICTS_OPTION.id).map((value) => value.id));
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

              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ lineHeight: 1.3, fontStyle: "italic", fontSize: "0.68rem" }}
              >
                2024 traffic data
                {themeViewPercent !== null && (
                  <> · <strong>{themeViewPercent}%</strong> of view</>
                )}
                {themeCoveragePercent !== null && (
                  <> · <strong>{themeCoveragePercent}%</strong> of data</>
                )}
              </Typography>

              <ThemeContextFilter
                option={selectedVisualization}
                themeFilters={themeFilters}
                setThemeFilter={setThemeFilter}
                resetThemeFilter={resetThemeFilter}
              />
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

              {selectedVisualization && themeFilterChips.length > 0 && (
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "flex-start",
                    gap: 0.5,
                    minWidth: 0,
                  }}
                >
                  <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                    {selectedVisualization.label}:
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
                    {themeFilterChips.map((chip) => (
                      <Box
                        key={chip.key}
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
                          {chip.label}
                        </Typography>
                        <IconButton
                          size="small"
                          onClick={chip.onDelete}
                          aria-label={`Remove ${chip.label} theme filter`}
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
                          {district.label}
                        </Typography>
                        <IconButton
                          size="small"
                          onClick={() => onDistrictDelete(district.id)}
                          aria-label={`Remove ${district.label} district filter`}
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
                    {isAll ? "Show all districts" : option.label}
                  </span>
                </li>
              );
            }}
            isOptionEqualToValue={(option, value) => option.id === value.id}
            getOptionLabel={(option) =>
              option.id === ALL_DISTRICTS_OPTION.id ? "All Districts" : option.label
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
            value={sortedCountyOptions.filter((county) => selectedCounties.includes(county.county))}
            onChange={(_, values) => onCountyChange(values.map((value) => value.county))}
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

      {selectedVisualization && (
        <Box sx={{ flexShrink: 0, px: 1.5, pt: 1, pb: 1.5 }}>
          <RoadwayLegendCard
            visualization={selectedVisualization}
            legendPresence={legendPresence}
            onLegendItemHover={onLegendItemHover}
            themeFilterValue={themeFilters[selectedVisualization.id]}
            defaultThemeFilterValue={defaultThemeFilterValue}
            setThemeFilter={setThemeFilter}
          />
        </Box>
      )}
    </Paper>
  );
}
