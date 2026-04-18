import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import {
  Autocomplete,
  Avatar,
  Box,
  Checkbox,
  Chip,
  Slider,
  Stack,
  TextField,
  Typography,
} from "@mui/material";

import type { ThemeFilterState, ThemeFilterValue } from "../../store/useAppStore";
import { RoadwayVisualizationOption, ThemeFilterSpec } from "../../types/api";

interface ThemeContextFilterProps {
  option?: RoadwayVisualizationOption;
  themeFilters: ThemeFilterState;
  setThemeFilter: (
    visualizationId: string,
    patch: Partial<ThemeFilterValue>,
  ) => void;
  resetThemeFilter: (visualizationId: string) => void;
}

interface FilterControlProps {
  option: RoadwayVisualizationOption;
  spec: ThemeFilterSpec;
  themeFilterValue?: ThemeFilterValue;
  setThemeFilter: (
    visualizationId: string,
    patch: Partial<ThemeFilterValue>,
  ) => void;
  resetThemeFilter: (visualizationId: string) => void;
}

interface SelectableBinOption {
  key: string;
  label: string;
  color: string;
}

function getThemeFilterBinKey(bin: { value?: string | null; label: string }): string {
  return typeof bin.value === "string" ? bin.value : bin.label;
}

function buildDefaultThemeFilterValue(
  option: RoadwayVisualizationOption,
): ThemeFilterValue {
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
  option: RoadwayVisualizationOption,
  themeFilterValue?: ThemeFilterValue,
): ThemeFilterValue {
  const defaults = buildDefaultThemeFilterValue(option);

  return {
    selectedValues: themeFilterValue?.selectedValues ?? defaults.selectedValues,
    range: themeFilterValue?.range ?? defaults.range,
    includeNoData: themeFilterValue?.includeNoData ?? defaults.includeNoData,
  };
}

function getLegendColor(
  option: RoadwayVisualizationOption,
  value: string | null | undefined,
  fallbackColor: string,
): string {
  if (typeof value !== "string") {
    return fallbackColor;
  }

  return (
    option.legend_items.find(
      (item) => item.value === value || item.label === value,
    )?.color ?? fallbackColor
  );
}

function getChipTextColor(color: string): string {
  const normalized = color.replace("#", "");
  if (normalized.length !== 6) {
    return "#10232f";
  }

  const red = Number.parseInt(normalized.slice(0, 2), 16);
  const green = Number.parseInt(normalized.slice(2, 4), 16);
  const blue = Number.parseInt(normalized.slice(4, 6), 16);
  const luminance = (0.299 * red + 0.587 * green + 0.114 * blue) / 255;

  return luminance >= 0.68 ? "#10232f" : "#ffffff";
}

function ColorSwatch({ color }: { color: string }) {
  return (
    <Avatar
      variant="rounded"
      sx={{
        width: 16,
        height: 16,
        bgcolor: color,
        border: "1px solid rgba(16, 35, 47, 0.16)",
      }}
    />
  );
}

function buildSelectableBinOptions(
  option: RoadwayVisualizationOption,
  spec: ThemeFilterSpec,
): SelectableBinOption[] {
  return spec.bins.map((bin) => {
    const selectionKey = getThemeFilterBinKey(bin);
    const legendValue = typeof bin.value === "string" ? bin.value : bin.label;

    return {
      key: selectionKey,
      label: bin.label,
      color: getLegendColor(option, legendValue, option.no_data_color),
    };
  });
}

function updateThemeFilter(
  optionId: string,
  filterValue: ThemeFilterValue,
  setThemeFilter: FilterControlProps["setThemeFilter"],
) {
  setThemeFilter(optionId, {
    selectedValues: filterValue.selectedValues,
    range: filterValue.range,
    includeNoData: filterValue.includeNoData,
  });
}

function NoDataChip({
  color,
  isSelected,
  onClick,
}: {
  color: string;
  isSelected: boolean;
  onClick: () => void;
}) {
  return (
    <Chip
      clickable
      size="small"
      label="No data"
      avatar={<ColorSwatch color={color} />}
      onClick={onClick}
      variant={isSelected ? "filled" : "outlined"}
      sx={{
        borderColor: isSelected ? color : "rgba(17, 61, 73, 0.22)",
        bgcolor: isSelected ? color : "rgba(255, 255, 255, 0.78)",
        color: isSelected ? getChipTextColor(color) : "#35505a",
        "& .MuiChip-label": {
          fontWeight: 600,
        },
        "& .MuiChip-avatar": {
          bgcolor: color,
        },
        "&:hover": {
          bgcolor: isSelected ? color : "rgba(17, 61, 73, 0.06)",
        },
      }}
    />
  );
}

function MultiSelectFilter({
  option,
  spec,
  themeFilterValue,
  setThemeFilter,
}: FilterControlProps) {
  const filterValue = getEffectiveThemeFilterValue(option, themeFilterValue);
  const selectOptions = buildSelectableBinOptions(option, spec);
  const selectedSet = new Set(filterValue.selectedValues);
  const selectedOptions = selectOptions.filter((item) => selectedSet.has(item.key));

  return (
    <Stack spacing={0.5}>
      <Autocomplete
        multiple
        size="small"
        options={selectOptions}
        value={selectedOptions}
        onChange={(_, values) =>
          updateThemeFilter(
            option.id,
            {
              ...filterValue,
              selectedValues: values.map((value) => value.key),
            },
            setThemeFilter,
          )
        }
        disableCloseOnSelect
        isOptionEqualToValue={(left, right) => left.key === right.key}
        getOptionLabel={(item) => item.label}
        renderTags={() => null}
        renderOption={(props, item, { selected }) => (
          <li {...props} style={{ fontSize: "0.78rem", paddingTop: 2, paddingBottom: 2 }}>
            <Checkbox
              icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
              checkedIcon={<CheckBoxIcon fontSize="small" />}
              style={{ marginRight: 6, padding: 2 }}
              checked={selected}
            />
            <Box
              sx={{
                mr: 0.75,
                display: "inline-flex",
                alignItems: "center",
              }}
            >
              <ColorSwatch color={item.color} />
            </Box>
            {item.label}
          </li>
        )}
        renderInput={(params) => (
          <TextField
            {...params}
            label={spec.label ?? "Category"}
            placeholder="Select categories"
            inputProps={{ ...params.inputProps, sx: { fontSize: "0.78rem" } }}
            InputLabelProps={{ sx: { fontSize: "0.78rem" } }}
          />
        )}
      />

      {spec.no_data_selectable && (
        <Stack direction="row" spacing={0.75}>
          <NoDataChip
            color={option.no_data_color}
            isSelected={filterValue.includeNoData}
            onClick={() =>
              updateThemeFilter(
                option.id,
                {
                  ...filterValue,
                  includeNoData: !filterValue.includeNoData,
                },
                setThemeFilter,
              )
            }
          />
        </Stack>
      )}
    </Stack>
  );
}

function formatSliderValue(value: number, unit: string | null): string {
  const formatted = Number.isInteger(value)
    ? value.toLocaleString()
    : value.toLocaleString(undefined, { maximumFractionDigits: 1 });
  return unit ? `${formatted} ${unit}` : formatted;
}

function getSliderStep(
  option: RoadwayVisualizationOption,
  spec: ThemeFilterSpec,
): number {
  if (typeof spec.step === "number" && spec.step > 0) {
    return spec.step;
  }

  if (
    typeof spec.min_bound === "number" &&
    typeof spec.max_bound === "number" &&
    Number.isInteger(spec.min_bound) &&
    Number.isInteger(spec.max_bound)
  ) {
    return 1;
  }

  if (
    typeof spec.min_bound === "number" &&
    typeof spec.max_bound === "number" &&
    spec.max_bound > spec.min_bound
  ) {
    return Math.max((spec.max_bound - spec.min_bound) / 100, 0.1);
  }

  return option.kind === "numeric" ? 1 : 0.1;
}

function RangeSliderFilter({
  option,
  spec,
  themeFilterValue,
  setThemeFilter,
}: FilterControlProps) {
  const filterValue = getEffectiveThemeFilterValue(option, themeFilterValue);
  if (
    typeof spec.min_bound !== "number" ||
    typeof spec.max_bound !== "number"
  ) {
    return null;
  }

  const currentRange = filterValue.range ?? [spec.min_bound, spec.max_bound];
  const step = getSliderStep(option, spec);

  return (
    <Stack spacing={0.5}>
      {spec.label && (
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ letterSpacing: 0.3 }}
        >
          {spec.label}
        </Typography>
      )}

      <Box sx={{ px: 1 }}>
        <Slider
          value={currentRange}
          min={spec.min_bound}
          max={spec.max_bound}
          step={step}
          onChange={(_, value) => {
            if (!Array.isArray(value) || value.length !== 2) {
              return;
            }

            updateThemeFilter(
              option.id,
              {
                ...filterValue,
                range: [Number(value[0]), Number(value[1])],
              },
              setThemeFilter,
            );
          }}
          valueLabelDisplay="auto"
          valueLabelFormat={(value) => formatSliderValue(value, option.unit)}
          sx={{ mt: 0.75 }}
        />
      </Box>
    </Stack>
  );
}

function HwyDesMatrixFilter(_props: FilterControlProps) {
  return null;
}

export function ThemeContextFilter({
  option,
  themeFilters,
  setThemeFilter,
  resetThemeFilter,
}: ThemeContextFilterProps) {
  if (!option || option.filters.length === 0) {
    return null;
  }

  return (
    <Stack spacing={0.75}>
      {option.filters.map((spec, index) => {
        const sharedProps: FilterControlProps = {
          option,
          spec,
          themeFilterValue: themeFilters[option.id],
          setThemeFilter,
          resetThemeFilter,
        };

        switch (spec.control) {
          case "toggle_chips":
          case "multi_select":
          case "bin_multi_select":
            return (
              <MultiSelectFilter
                key={`${option.id}-${spec.property_name}-${index}`}
                {...sharedProps}
              />
            );
          case "range_slider":
            return (
              <RangeSliderFilter
                key={`${option.id}-${spec.property_name}-${index}`}
                {...sharedProps}
              />
            );
          case "hwy_des_matrix":
            return (
              <HwyDesMatrixFilter
                key={`${option.id}-${spec.property_name}-${index}`}
                {...sharedProps}
              />
            );
          case "none":
          default:
            return null;
        }
      })}
    </Stack>
  );
}
