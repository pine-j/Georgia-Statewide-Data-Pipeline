import RestartAltRoundedIcon from "@mui/icons-material/RestartAltRounded";
import CloseRoundedIcon from "@mui/icons-material/CloseRounded";
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import ExpandMoreRoundedIcon from "@mui/icons-material/ExpandMoreRounded";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Autocomplete,
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
  AreaOfficeOption,
  CityOption,
  CongressionalOption,
  CountyOption,
  DistrictOption,
  HighwayTypeOption,
  MpoOption,
  RegionalCommissionOption,
  RoadwayVisualizationCatalog,
  RoadwayVisualizationOption,
  StateHouseOption,
  StateSenateOption,
} from "../../types/api";
import type {
  BoundaryOverlayVisibility,
  ThemeFilterState,
  ThemeFilterValue,
} from "../../store/useAppStore";
import { RoadwayLegendCard } from "../map/RoadwayLegendCard";
import { LegendPresence } from "../map/roadwayVisualization";
import { GeographyAutocomplete } from "./GeographyAutocomplete";
import { ThemeContextFilter } from "./ThemeContextFilter";

const ALL_DISTRICTS_OPTION: DistrictOption = { id: -1, label: "All Districts" };

interface ActiveThemeFilterChip {
  key: string;
  label: string;
  onDelete: () => void;
}

interface FilterChipSpec {
  key: string | number;
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
  areaOffices: AreaOfficeOption[];
  mpos: MpoOption[];
  regionalCommissions: RegionalCommissionOption[];
  stateHouseDistricts: StateHouseOption[];
  stateSenateDistricts: StateSenateOption[];
  congressionalDistricts: CongressionalOption[];
  cities: CityOption[];
  selectedDistricts: number[];
  selectedCounties: string[];
  selectedHighwayTypes: string[];
  selectedAreaOffices: number[];
  selectedMpos: string[];
  selectedRegionalCommissions: number[];
  selectedStateHouseDistricts: number[];
  selectedStateSenateDistricts: number[];
  selectedCongressionalDistricts: number[];
  selectedCities: number[];
  includeUnincorporated: boolean;
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
  onAreaOfficeChange: (ids: number[]) => void;
  onAreaOfficeDelete: (id: number) => void;
  onMpoChange: (ids: string[]) => void;
  onMpoDelete: (id: string) => void;
  onRegionalCommissionChange: (ids: number[]) => void;
  onRegionalCommissionDelete: (id: number) => void;
  onStateHouseChange: (ids: number[]) => void;
  onStateHouseDelete: (id: number) => void;
  onStateSenateChange: (ids: number[]) => void;
  onStateSenateDelete: (id: number) => void;
  onCongressionalChange: (ids: number[]) => void;
  onCongressionalDelete: (id: number) => void;
  onCityChange: (ids: number[]) => void;
  onCityDelete: (id: number) => void;
  onIncludeUnincorporatedChange: (value: boolean) => void;
  setThemeFilter: (visualizationId: string, patch: Partial<ThemeFilterValue>) => void;
  resetThemeFilter: (visualizationId: string) => void;
  selectedVisualization?: RoadwayVisualizationOption;
  legendPresence?: LegendPresence | null;
  boundaryOverlayVisibility?: BoundaryOverlayVisibility;
  onBoundaryOverlayToggle?: (
    overlay: keyof BoundaryOverlayVisibility,
    visible: boolean,
  ) => void;
  roadwayNetworkVisible?: boolean;
  onRoadwayNetworkVisibleChange?: (visible: boolean) => void;
  onResetFilters: () => void;
  onVisualizationChange: (visualizationId: string) => void;
}

export function FiltersPanel({
  districts,
  counties,
  highwayTypes,
  areaOffices,
  mpos,
  regionalCommissions,
  stateHouseDistricts,
  stateSenateDistricts,
  congressionalDistricts,
  cities,
  selectedDistricts,
  selectedCounties,
  selectedHighwayTypes,
  selectedAreaOffices,
  selectedMpos,
  selectedRegionalCommissions,
  selectedStateHouseDistricts,
  selectedStateSenateDistricts,
  selectedCongressionalDistricts,
  selectedCities,
  includeUnincorporated,
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
  onAreaOfficeChange,
  onAreaOfficeDelete,
  onMpoChange,
  onMpoDelete,
  onRegionalCommissionChange,
  onRegionalCommissionDelete,
  onStateHouseChange,
  onStateHouseDelete,
  onStateSenateChange,
  onStateSenateDelete,
  onCongressionalChange,
  onCongressionalDelete,
  onCityChange,
  onCityDelete,
  onIncludeUnincorporatedChange,
  setThemeFilter,
  resetThemeFilter,
  boundaryOverlayVisibility,
  onBoundaryOverlayToggle,
  roadwayNetworkVisible,
  onRoadwayNetworkVisibleChange,
  onResetFilters,
  onVisualizationChange,
}: FiltersPanelProps) {
  const isAllDistricts = selectedDistricts.length === 0;
  const districtOptionsWithAll = [ALL_DISTRICTS_OPTION, ...districts];
  const selectedDistrictOptions = isAllDistricts
    ? []
    : districts.filter((district) => selectedDistricts.includes(district.id));

  const districtLabelMap = new Map(districts.map((district) => [district.id, district.label]));

  const selectedHighwayTypeOptions = highwayTypes.filter((highwayType) =>
    selectedHighwayTypes.includes(highwayType.id),
  );

  const areaOfficeLabelMap = new Map(areaOffices.map((ao) => [ao.id, ao.label]));
  const mpoLabelMap = new Map(mpos.map((m) => [m.id, m.label]));
  const rcLabelMap = new Map(regionalCommissions.map((rc) => [rc.id, rc.label]));
  const stateHouseLabelMap = new Map(
    stateHouseDistricts.map((sh) => [sh.id, sh.label]),
  );
  const stateSenateLabelMap = new Map(
    stateSenateDistricts.map((ss) => [ss.id, ss.label]),
  );
  const congressionalLabelMap = new Map(
    congressionalDistricts.map((cd) => [cd.id, cd.label]),
  );
  const cityLabelMap = new Map(cities.map((c) => [c.id, c.label]));

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
      selectedAreaOffices.length ||
      selectedMpos.length ||
      selectedRegionalCommissions.length ||
      selectedStateHouseDistricts.length ||
      selectedStateSenateDistricts.length ||
      selectedCongressionalDistricts.length ||
      selectedCities.length ||
      includeUnincorporated ||
      themeFilterChips.length,
  );

  const handleDistrictAutocompleteChange = (_: unknown, values: DistrictOption[]) => {
    if (values.some((value) => value.id === ALL_DISTRICTS_OPTION.id)) {
      onDistrictChange([]);
      return;
    }

    onDistrictChange(values.filter((value) => value.id !== ALL_DISTRICTS_OPTION.id).map((value) => value.id));
  };

  const renderFilterChipRow = (label: string, chips: FilterChipSpec[]) => {
    if (chips.length === 0) {
      return null;
    }

    return (
      <Box
        sx={{
          display: "flex",
          alignItems: "flex-start",
          gap: 0.5,
          minWidth: 0,
        }}
      >
        <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
          {label}:
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
          {chips.map((chip) => (
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
                aria-label={`Remove ${chip.label} ${label} filter`}
                sx={{ p: 0.25 }}
              >
                <CloseRoundedIcon fontSize="inherit" />
              </IconButton>
            </Box>
          ))}
        </Box>
      </Box>
    );
  };

  const highwayTypeChips: FilterChipSpec[] = selectedHighwayTypeOptions.map((highwayType) => ({
    key: highwayType.id,
    label: highwayType.label,
    onDelete: () => onHighwayTypeDelete(highwayType.id),
  }));

  const districtChips: FilterChipSpec[] = selectedDistrictOptions.map((district) => ({
    key: district.id,
    label: district.label,
    onDelete: () => onDistrictDelete(district.id),
  }));

  const areaOfficeChips: FilterChipSpec[] = selectedAreaOffices.map((id) => ({
    key: id,
    label: areaOfficeLabelMap.get(id) ?? `Area Office ${id}`,
    onDelete: () => onAreaOfficeDelete(id),
  }));

  const countyChips: FilterChipSpec[] = selectedCounties.map((county) => ({
    key: county,
    label: county,
    onDelete: () => onCountyDelete(county),
  }));

  const mpoChips: FilterChipSpec[] = selectedMpos.map((id) => ({
    key: id,
    label: mpoLabelMap.get(id) ?? id,
    onDelete: () => onMpoDelete(id),
  }));

  const rcChips: FilterChipSpec[] = selectedRegionalCommissions.map((id) => ({
    key: id,
    label: rcLabelMap.get(id) ?? `RC ${id}`,
    onDelete: () => onRegionalCommissionDelete(id),
  }));

  const cityChipsFromSelection: FilterChipSpec[] = selectedCities.map((id) => ({
    key: id,
    label: cityLabelMap.get(id) ?? `City ${id}`,
    onDelete: () => onCityDelete(id),
  }));

  const cityChips: FilterChipSpec[] = includeUnincorporated
    ? [
        {
          key: "__unincorporated__",
          label: "Unincorporated",
          onDelete: () => onIncludeUnincorporatedChange(false),
        },
        ...cityChipsFromSelection,
      ]
    : cityChipsFromSelection;

  const stateHouseChips: FilterChipSpec[] = selectedStateHouseDistricts.map((id) => ({
    key: id,
    label: stateHouseLabelMap.get(id) ?? `HD ${id}`,
    onDelete: () => onStateHouseDelete(id),
  }));

  const stateSenateChips: FilterChipSpec[] = selectedStateSenateDistricts.map((id) => ({
    key: id,
    label: stateSenateLabelMap.get(id) ?? `SD ${id}`,
    onDelete: () => onStateSenateDelete(id),
  }));

  const congressionalChips: FilterChipSpec[] = selectedCongressionalDistricts.map((id) => ({
    key: id,
    label: congressionalLabelMap.get(id) ?? `CD ${id}`,
    onDelete: () => onCongressionalDelete(id),
  }));

  const accordionSx = {
    "&:before": { display: "none" },
    boxShadow: "none",
    border: "1px solid rgba(17, 61, 73, 0.12)",
    borderRadius: 1,
    "&.Mui-expanded": { margin: 0 },
  } as const;

  const accordionSummarySx = {
    minHeight: 32,
    px: 1.25,
    "&.Mui-expanded": { minHeight: 32 },
    "& .MuiAccordionSummary-content": {
      my: 0.25,
      "&.Mui-expanded": { my: 0.25 },
    },
  } as const;

  const accordionDetailsSx = {
    px: 1,
    pt: 0.5,
    pb: 0.75,
    display: "flex",
    flexDirection: "column",
    gap: 0.75,
  } as const;

  const accordionTitleSx = {
    fontWeight: 700,
    fontSize: "0.72rem",
    letterSpacing: 0.4,
    textTransform: "uppercase",
    color: "#47626b",
  } as const;

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
      <Box
        sx={{
          flex: 1,
          overflowY: "auto",
          overflowX: "hidden",
          minWidth: 0,
          p: 1.5,
          pb: 1,
        }}
      >
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

              {renderFilterChipRow("Highway Type", highwayTypeChips)}

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

              {renderFilterChipRow("District", districtChips)}
              {renderFilterChipRow("Area Office", areaOfficeChips)}
              {renderFilterChipRow("County", countyChips)}
              {renderFilterChipRow("MPO", mpoChips)}
              {renderFilterChipRow("Regional Commission", rcChips)}
              {renderFilterChipRow("City", cityChips)}
              {renderFilterChipRow("State House", stateHouseChips)}
              {renderFilterChipRow("State Senate", stateSenateChips)}
              {renderFilterChipRow("Congressional", congressionalChips)}
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

          {/*
            Single combined "Geographies" accordion wraps the four sub-groups
            (Engineering, Planning, Local, Legislative). Collapsed by default
            so the panel opens compact; users expand this outer accordion to
            access any of the inner ones.
          */}
          <Accordion
            disableGutters
            sx={{
              ...accordionSx,
              mt: 1,
              "&.Mui-expanded": { margin: 0, mt: 1 },
            }}
          >
            <AccordionSummary
              expandIcon={<ExpandMoreRoundedIcon fontSize="small" />}
              sx={accordionSummarySx}
            >
              <Typography sx={accordionTitleSx}>Geographies</Typography>
            </AccordionSummary>
            <AccordionDetails
              sx={{ ...accordionDetailsSx, gap: 0.5, px: 0.5, pt: 0.75, pb: 0.75 }}
            >
          {/*
            Engineering Geographies accordion.
            District kept as an inline Autocomplete to preserve the
            "All Districts" pseudo-option UX (an extra row at the top whose
            checkbox reflects the empty-selection state and whose click
            clears the entire selection). GeographyAutocomplete has a
            pseudoOption slot but it models a boolean toggle, not a
            "clear-selection" action, so reusing it here would require
            contorting the toggle semantics. Inline keeps the existing UX.
          */}
          <Accordion defaultExpanded disableGutters sx={accordionSx}>
            <AccordionSummary
              expandIcon={<ExpandMoreRoundedIcon fontSize="small" />}
              sx={accordionSummarySx}
            >
              <Typography sx={accordionTitleSx}>Engineering Geographies</Typography>
            </AccordionSummary>
            <AccordionDetails sx={accordionDetailsSx}>
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
              <GeographyAutocomplete<AreaOfficeOption, number>
                label="Area Office"
                placeholder="Select area offices"
                options={areaOffices}
                selected={selectedAreaOffices}
                onChange={onAreaOfficeChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
                parentIds={selectedDistricts}
                getOptionParentId={(o) => o.parent_district}
              />
            </AccordionDetails>
          </Accordion>

          <Accordion defaultExpanded disableGutters sx={accordionSx}>
            <AccordionSummary
              expandIcon={<ExpandMoreRoundedIcon fontSize="small" />}
              sx={accordionSummarySx}
            >
              <Typography sx={accordionTitleSx}>Planning Geographies</Typography>
            </AccordionSummary>
            <AccordionDetails sx={accordionDetailsSx}>
              <GeographyAutocomplete<CountyOption, string>
                label="County"
                placeholder="Search counties"
                options={[...counties].sort((left, right) => {
                  if (left.district !== right.district) {
                    return left.district - right.district;
                  }
                  return left.county.localeCompare(right.county);
                })}
                selected={selectedCounties}
                onChange={onCountyChange}
                getOptionId={(o) => o.county}
                getOptionLabel={(o) => o.county}
                parentIds={selectedDistricts}
                getOptionParentId={(o) => o.district}
                groupBy={(o) =>
                  districtLabelMap.get(o.district) ?? `District ${o.district}`
                }
              />
              <GeographyAutocomplete<MpoOption, string>
                label="MPO"
                placeholder="Select MPOs"
                options={mpos}
                selected={selectedMpos}
                onChange={onMpoChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
              />
              <GeographyAutocomplete<RegionalCommissionOption, number>
                label="Regional Commission"
                placeholder="Select regional commissions"
                options={regionalCommissions}
                selected={selectedRegionalCommissions}
                onChange={onRegionalCommissionChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
              />
            </AccordionDetails>
          </Accordion>

          <Accordion disableGutters sx={accordionSx}>
            <AccordionSummary
              expandIcon={<ExpandMoreRoundedIcon fontSize="small" />}
              sx={accordionSummarySx}
            >
              <Typography sx={accordionTitleSx}>Local Geographies</Typography>
            </AccordionSummary>
            <AccordionDetails sx={accordionDetailsSx}>
              <GeographyAutocomplete<CityOption, number>
                label="City"
                placeholder="Select cities"
                options={cities}
                selected={selectedCities}
                onChange={onCityChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
                parentIds={selectedDistricts}
                getOptionParentId={(o) => o.district ?? undefined}
                secondaryParentIds={selectedCounties}
                getOptionSecondaryParentId={(o) => o.county ?? undefined}
                pseudoOption={{
                  label: "Unincorporated",
                  selected: includeUnincorporated,
                  onToggle: onIncludeUnincorporatedChange,
                  helperText: "Roadway segments outside any incorporated city",
                }}
              />
            </AccordionDetails>
          </Accordion>

          <Accordion disableGutters sx={accordionSx}>
            <AccordionSummary
              expandIcon={<ExpandMoreRoundedIcon fontSize="small" />}
              sx={accordionSummarySx}
            >
              <Typography sx={accordionTitleSx}>Legislative Geographies</Typography>
            </AccordionSummary>
            <AccordionDetails sx={accordionDetailsSx}>
              <GeographyAutocomplete<StateHouseOption, number>
                label="State House"
                placeholder="Select state house districts"
                options={stateHouseDistricts}
                selected={selectedStateHouseDistricts}
                onChange={onStateHouseChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
              />
              <GeographyAutocomplete<StateSenateOption, number>
                label="State Senate"
                placeholder="Select state senate districts"
                options={stateSenateDistricts}
                selected={selectedStateSenateDistricts}
                onChange={onStateSenateChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
              />
              <GeographyAutocomplete<CongressionalOption, number>
                label="Congressional"
                placeholder="Select congressional districts"
                options={congressionalDistricts}
                selected={selectedCongressionalDistricts}
                onChange={onCongressionalChange}
                getOptionId={(o) => o.id}
                getOptionLabel={(o) => o.label}
              />
            </AccordionDetails>
          </Accordion>
            </AccordionDetails>
          </Accordion>
        </Stack>
      </Box>

      {selectedVisualization && (
        <Box sx={{ flexShrink: 0, px: 1.5, pt: 1, pb: 1.5 }}>
          <RoadwayLegendCard
            visualization={selectedVisualization}
            legendPresence={legendPresence}
            themeFilterValue={themeFilters[selectedVisualization.id]}
            defaultThemeFilterValue={defaultThemeFilterValue}
            setThemeFilter={setThemeFilter}
            boundaryOverlayVisibility={boundaryOverlayVisibility}
            onBoundaryOverlayToggle={onBoundaryOverlayToggle}
            roadwayNetworkVisible={roadwayNetworkVisible}
            onRoadwayNetworkVisibleChange={onRoadwayNetworkVisibleChange}
          />
        </Box>
      )}
    </Paper>
  );
}
