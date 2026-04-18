import type { ReactElement } from "react";
import { Box, Slider, Stack, Typography } from "@mui/material";

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

export function ThemeContextFilter({
  option,
  themeFilters,
  setThemeFilter,
  resetThemeFilter,
}: ThemeContextFilterProps) {
  if (!option || option.filters.length === 0) {
    return null;
  }

  const rendered = option.filters
    .map((spec, index) => {
      if (spec.control !== "range_slider") {
        return null;
      }
      return (
        <RangeSliderFilter
          key={`${option.id}-${spec.property_name}-${index}`}
          option={option}
          spec={spec}
          themeFilterValue={themeFilters[option.id]}
          setThemeFilter={setThemeFilter}
          resetThemeFilter={resetThemeFilter}
        />
      );
    })
    .filter((node): node is ReactElement => node !== null);

  if (rendered.length === 0) {
    return null;
  }

  return <Stack spacing={0.75}>{rendered}</Stack>;
}
