import type { ExpressionSpecification } from "maplibre-gl";

import type { ThemeFilterValue } from "../../store/useAppStore";
import {
  RoadwayFeature,
  RoadwayFeatureCollection,
  RoadwayLegendItem,
  RoadwayVisualizationOption,
} from "../../types/api";

export const DEFAULT_ROADWAY_LINE_COLOR = "#1490a7";
export const DEFAULT_ROADWAY_LINE_OPACITY = 0.96;
export const DEFAULT_ROADWAY_LINE_SORT_KEY = 0;
const NO_DATA_OPACITY = 0.52;

function hasNumericBounds(item: RoadwayLegendItem): item is RoadwayLegendItem & { min_value: number } {
  return typeof item.min_value === "number";
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
  themeFilterState?: ThemeFilterValue,
): ThemeFilterValue | null {
  const defaults = buildDefaultThemeFilterValue(option);
  if (!defaults) {
    return null;
  }

  return {
    selectedValues: themeFilterState?.selectedValues ?? defaults.selectedValues,
    range: themeFilterState?.range ?? defaults.range,
    includeNoData: themeFilterState?.includeNoData ?? defaults.includeNoData,
  };
}

function getFeaturePropertyValue(
  feature: RoadwayFeature,
  propertyName: string,
): unknown {
  return (feature.properties as unknown as Record<string, unknown>)[propertyName];
}

export function getLegendItemsForDisplay(
  option: RoadwayVisualizationOption,
): RoadwayLegendItem[] {
  if (option.kind !== "numeric") {
    return option.legend_items;
  }

  return [...option.legend_items].sort((left, right) => {
    const leftMin = typeof left.min_value === "number" ? left.min_value : Number.NEGATIVE_INFINITY;
    const rightMin = typeof right.min_value === "number" ? right.min_value : Number.NEGATIVE_INFINITY;
    return rightMin - leftMin;
  });
}

export interface LegendPresence {
  presentKeys: Set<string>;
  hasNoData: boolean;
  total: number;
  withData: number;
}

export interface FilteredLegendPresence extends LegendPresence {
  filterPassing: number;
}

/**
 * Single pass over the currently-loaded roadway features that returns which
 * legend buckets are actually represented in the visible data, whether any
 * segment is missing data, and the counts needed for coverage display.
 *
 * Returns `null` when the inputs are not yet ready so callers can fall back
 * to showing the full legend instead of flashing an empty one.
 */
export function computeLegendPresence(
  chunks: RoadwayFeatureCollection[],
  option: RoadwayVisualizationOption | undefined,
): LegendPresence | null {
  if (!option?.property_name || !option.kind || chunks.length === 0) {
    return null;
  }

  const presentKeys = new Set<string>();
  let hasNoData = false;
  let total = 0;
  let withData = 0;
  const prop = option.property_name;

  if (option.kind === "categorical") {
    for (const chunk of chunks) {
      for (const feature of chunk.features) {
        total += 1;
        const raw = getFeaturePropertyValue(feature, prop);
        if (raw === null || raw === undefined || raw === "") {
          hasNoData = true;
        } else {
          withData += 1;
          presentKeys.add(String(raw));
        }
      }
    }
    return { presentKeys, hasNoData, total, withData };
  }

  const numericItems = option.legend_items
    .filter(hasNumericBounds)
    .slice()
    .sort((left, right) => left.min_value - right.min_value);

  for (const chunk of chunks) {
    for (const feature of chunk.features) {
      total += 1;
      const raw = getFeaturePropertyValue(feature, prop);
      if (raw === null || raw === undefined || raw === "") {
        hasNoData = true;
        continue;
      }
      const num = Number(raw);
      if (!Number.isFinite(num)) {
        hasNoData = true;
        continue;
      }
      withData += 1;
      let matchedLabel: string | null = null;
      for (const item of numericItems) {
        if (num < item.min_value) {
          break;
        }
        const max = typeof item.max_value === "number" ? item.max_value : Infinity;
        if (num <= max) {
          matchedLabel = item.label;
        }
      }
      if (matchedLabel !== null) {
        presentKeys.add(matchedLabel);
      }
    }
  }

  return { presentKeys, hasNoData, total, withData };
}

interface ThemeFilterCriteria {
  propertyName: string;
  kind: NonNullable<RoadwayVisualizationOption["kind"]>;
  includeNoData: boolean;
  unselectedCategoricalValues: string[];
  unselectedNumericBins: Array<{
    minValue: number;
    maxValue: number | null;
  }>;
  range: [number, number] | null;
}

function compileThemeFilterCriteria(
  option?: RoadwayVisualizationOption,
  themeFilterState?: ThemeFilterValue,
): ThemeFilterCriteria | null {
  if (!option?.property_name || !option.kind || option.filters.length === 0) {
    return null;
  }

  const effectiveThemeFilter = getEffectiveThemeFilterValue(option, themeFilterState);
  if (!effectiveThemeFilter) {
    return null;
  }

  const selectedValues = new Set(effectiveThemeFilter.selectedValues);
  const criteria: ThemeFilterCriteria = {
    propertyName: option.property_name,
    kind: option.kind,
    includeNoData: effectiveThemeFilter.includeNoData,
    unselectedCategoricalValues: [],
    unselectedNumericBins: [],
    range: null,
  };

  for (const spec of option.filters) {
    switch (spec.control) {
      case "toggle_chips":
      case "multi_select":
        for (const bin of spec.bins) {
          const selectionKey = getThemeFilterBinKey(bin);
          const rawValue = typeof bin.value === "string" ? bin.value : bin.label;
          if (!selectedValues.has(selectionKey)) {
            criteria.unselectedCategoricalValues.push(rawValue);
          }
        }
        break;
      case "bin_multi_select":
        for (const bin of spec.bins) {
          if (typeof bin.min_value !== "number") {
            continue;
          }
          const selectionKey = getThemeFilterBinKey(bin);
          if (!selectedValues.has(selectionKey)) {
            criteria.unselectedNumericBins.push({
              minValue: bin.min_value,
              maxValue: typeof bin.max_value === "number" ? bin.max_value : null,
            });
          }
        }
        break;
      case "range_slider":
        if (
          Array.isArray(effectiveThemeFilter.range) &&
          effectiveThemeFilter.range.length === 2 &&
          Number.isFinite(effectiveThemeFilter.range[0]) &&
          Number.isFinite(effectiveThemeFilter.range[1])
        ) {
          const [rangeMin, rangeMax] = effectiveThemeFilter.range;
          criteria.range = criteria.range
            ? [
                Math.max(criteria.range[0], rangeMin),
                Math.min(criteria.range[1], rangeMax),
              ]
            : [rangeMin, rangeMax];
        }
        break;
      case "hwy_des_matrix":
      case "none":
      default:
        break;
    }
  }

  return criteria;
}

function matchesNumericBin(
  value: number,
  minValue: number,
  maxValue: number | null,
): boolean {
  if (value < minValue) {
    return false;
  }

  return maxValue === null ? true : value <= maxValue;
}

function evaluateThemeFilterCriteria(
  feature: RoadwayFeature,
  criteria: ThemeFilterCriteria | null,
): boolean {
  if (!criteria) {
    return true;
  }

  const raw = getFeaturePropertyValue(feature, criteria.propertyName);
  if (raw === null || raw === undefined || raw === "") {
    return criteria.includeNoData;
  }

  if (criteria.kind === "categorical") {
    return !criteria.unselectedCategoricalValues.includes(String(raw));
  }

  const numericValue = Number(raw);
  if (!Number.isFinite(numericValue)) {
    return criteria.includeNoData;
  }

  if (
    criteria.range &&
    (numericValue < criteria.range[0] || numericValue > criteria.range[1])
  ) {
    return false;
  }

  for (const bin of criteria.unselectedNumericBins) {
    if (matchesNumericBin(numericValue, bin.minValue, bin.maxValue)) {
      return false;
    }
  }

  return true;
}

export interface ThemeFilterPredicate {
  (feature: RoadwayFeature): boolean;
  criteria: ThemeFilterCriteria | null;
}

export function buildThemeFilterPredicate(
  option?: RoadwayVisualizationOption,
  themeFilterState?: ThemeFilterValue,
): ThemeFilterPredicate {
  const criteria = compileThemeFilterCriteria(option, themeFilterState);
  const predicate = ((feature: RoadwayFeature) =>
    evaluateThemeFilterCriteria(feature, criteria)) as ThemeFilterPredicate;
  predicate.criteria = criteria;
  return predicate;
}

export function computeLegendPresenceFiltered(
  chunks: RoadwayFeatureCollection[],
  option: RoadwayVisualizationOption | undefined,
  themeFilterState?: ThemeFilterValue,
): FilteredLegendPresence | null {
  const basePresence = computeLegendPresence(chunks, option);
  if (!basePresence || !option) {
    return null;
  }

  const predicate = buildThemeFilterPredicate(option, themeFilterState);
  let filterPassing = 0;

  for (const chunk of chunks) {
    for (const feature of chunk.features) {
      if (predicate(feature)) {
        filterPassing += 1;
      }
    }
  }

  return {
    ...basePresence,
    filterPassing,
  };
}

export function getLegendItemPresenceKey(
  option: RoadwayVisualizationOption,
  item: RoadwayLegendItem,
): string | null {
  if (option.kind === "categorical") {
    return typeof item.value === "string" ? item.value : null;
  }
  return item.label;
}

export function getImplementationStatusLabel(
  implementationStatus: RoadwayVisualizationOption["implementation_status"],
): string {
  switch (implementationStatus) {
    case "derived":
      return "Derived in web app";
    case "popup_only":
      return "Popup only";
    case "unavailable":
      return "Not yet available";
    case "staged":
    default:
      return "In staged data";
  }
}

export function buildRoadwayLineColorExpression(
  option?: RoadwayVisualizationOption,
): string | ExpressionSpecification {
  if (!option?.property_name || !option.kind) {
    return DEFAULT_ROADWAY_LINE_COLOR;
  }

  if (option.kind === "numeric") {
    const numericStops = option.legend_items.filter(hasNumericBounds);
    if (numericStops.length === 0) {
      return DEFAULT_ROADWAY_LINE_COLOR;
    }

    const [firstStop, ...remainingStops] = numericStops;
    return ([
      "case",
      ["==", ["get", option.property_name], null],
      option.no_data_color,
      [
        "step",
        ["to-number", ["get", option.property_name]],
        firstStop.color,
        ...remainingStops.flatMap((item) => [item.min_value, item.color]),
      ],
    ] as unknown) as ExpressionSpecification;
  }

  return ([
    "match",
    ["coalesce", ["get", option.property_name], "__NO_DATA__"],
    ...option.legend_items
      .filter((item) => typeof item.value === "string")
      .flatMap((item) => [item.value as string, item.color]),
    option.no_data_color,
  ] as unknown) as ExpressionSpecification;
}

function buildCategoricalValueMatchCondition(
  propertyName: string,
  values: string[],
): ExpressionSpecification | null {
  if (values.length === 0) {
    return null;
  }

  return ([
    "match",
    ["coalesce", ["get", propertyName], "__NO_MATCH__"],
    ...values.flatMap((value) => [value, true]),
    false,
  ] as unknown) as ExpressionSpecification;
}

function buildNumericRangeFailCondition(
  propertyName: string,
  range: [number, number],
): ExpressionSpecification {
  return ([
    "any",
    ["<", ["to-number", ["get", propertyName]], range[0]],
    [">", ["to-number", ["get", propertyName]], range[1]],
  ] as unknown) as ExpressionSpecification;
}

function buildNumericBinFailCondition(
  propertyName: string,
  bin: { minValue: number; maxValue: number | null },
): ExpressionSpecification {
  if (bin.maxValue === null) {
    return ([
      "all",
      ["!=", ["get", propertyName], null],
      [">=", ["to-number", ["get", propertyName]], bin.minValue],
    ] as unknown) as ExpressionSpecification;
  }

  return ([
    "all",
    ["!=", ["get", propertyName], null],
    [">=", ["to-number", ["get", propertyName]], bin.minValue],
    ["<=", ["to-number", ["get", propertyName]], bin.maxValue],
  ] as unknown) as ExpressionSpecification;
}

function buildThemeFilterFailCondition(
  criteria: ThemeFilterCriteria | null,
): ExpressionSpecification | null {
  if (!criteria) {
    return null;
  }

  const conditions: ExpressionSpecification[] = [];

  if (criteria.kind === "categorical") {
    const matchCondition = buildCategoricalValueMatchCondition(
      criteria.propertyName,
      criteria.unselectedCategoricalValues,
    );
    if (matchCondition) {
      conditions.push(matchCondition);
    }
  } else {
    if (criteria.range) {
      conditions.push(buildNumericRangeFailCondition(criteria.propertyName, criteria.range));
    }
    conditions.push(
      ...criteria.unselectedNumericBins.map((bin) =>
        buildNumericBinFailCondition(criteria.propertyName, bin),
      ),
    );
  }

  if (conditions.length === 0) {
    return null;
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  return (["any", ...conditions] as unknown) as ExpressionSpecification;
}

export function buildThemeContextFilterColorExpression(
  option?: RoadwayVisualizationOption,
  themeFilterState?: ThemeFilterValue,
): string | ExpressionSpecification {
  const baseColorExpression = buildRoadwayLineColorExpression(option);
  if (!option?.property_name) {
    return baseColorExpression;
  }

  const predicate = buildThemeFilterPredicate(option, themeFilterState);
  const failCondition = buildThemeFilterFailCondition(predicate.criteria);
  if (!failCondition) {
    return baseColorExpression;
  }

  return ([
    "case",
    ["==", ["get", option.property_name], null],
    option.no_data_color,
    failCondition,
    option.no_data_color,
    baseColorExpression,
  ] as unknown) as ExpressionSpecification;
}

export function buildRoadwayLineOpacityExpression(
  option?: RoadwayVisualizationOption,
): number | ExpressionSpecification {
  if (!option?.property_name) {
    return DEFAULT_ROADWAY_LINE_OPACITY;
  }

  return ([
    "case",
    ["==", ["get", option.property_name], null],
    NO_DATA_OPACITY,
    DEFAULT_ROADWAY_LINE_OPACITY,
  ] as unknown) as ExpressionSpecification;
}

export function buildThemeContextFilterOpacityExpression(
  option?: RoadwayVisualizationOption,
  themeFilterState?: ThemeFilterValue,
): number | ExpressionSpecification {
  if (!option?.property_name) {
    return buildRoadwayLineOpacityExpression(option);
  }

  const predicate = buildThemeFilterPredicate(option, themeFilterState);
  const criteria = predicate.criteria;
  if (!criteria) {
    return buildRoadwayLineOpacityExpression(option);
  }

  const noDataOpacity = criteria.includeNoData ? NO_DATA_OPACITY : 0;
  const failCondition = buildThemeFilterFailCondition(criteria);
  if (!failCondition) {
    if (criteria.includeNoData) {
      return buildRoadwayLineOpacityExpression(option);
    }

    return ([
      "case",
      ["==", ["get", option.property_name], null],
      0,
      DEFAULT_ROADWAY_LINE_OPACITY,
    ] as unknown) as ExpressionSpecification;
  }

  return ([
    "case",
    ["==", ["get", option.property_name], null],
    noDataOpacity,
    failCondition,
    NO_DATA_OPACITY,
    DEFAULT_ROADWAY_LINE_OPACITY,
  ] as unknown) as ExpressionSpecification;
}

/**
 * When a legend item is hovered, dim all segments that don't match the
 * hovered value so the matching ones visually pop.
 */
export function buildLegendHighlightOpacityExpression(
  option: RoadwayVisualizationOption | undefined,
  hoveredValue: string | null,
): number | ExpressionSpecification {
  if (!hoveredValue || !option?.property_name) {
    return DEFAULT_ROADWAY_LINE_OPACITY;
  }

  const prop = option.property_name;
  const DIMMED = 0.18;
  const FULL = DEFAULT_ROADWAY_LINE_OPACITY;

  if (hoveredValue === "__NO_DATA__") {
    return ([
      "case",
      ["==", ["get", prop], null],
      FULL,
      DIMMED,
    ] as unknown) as ExpressionSpecification;
  }

  if (option.kind === "categorical") {
    return ([
      "case",
      ["==", ["coalesce", ["get", prop], "__NO_DATA__"], hoveredValue],
      FULL,
      DIMMED,
    ] as unknown) as ExpressionSpecification;
  }

  const item = option.legend_items.find((li) => li.label === hoveredValue);
  if (!item || typeof item.min_value !== "number") {
    return DEFAULT_ROADWAY_LINE_OPACITY;
  }

  const minVal = item.min_value;
  const maxVal = typeof item.max_value === "number" ? item.max_value : Infinity;

  if (maxVal === minVal) {
    return ([
      "case",
      [
        "all",
        ["!=", ["get", prop], null],
        ["==", ["to-number", ["get", prop]], minVal],
      ],
      FULL,
      DIMMED,
    ] as unknown) as ExpressionSpecification;
  }

  if (maxVal === Infinity) {
    return ([
      "case",
      [
        "all",
        ["!=", ["get", prop], null],
        [">=", ["to-number", ["get", prop]], minVal],
      ],
      FULL,
      DIMMED,
    ] as unknown) as ExpressionSpecification;
  }

  return ([
    "case",
    [
      "all",
      ["!=", ["get", prop], null],
      [">=", ["to-number", ["get", prop]], minVal],
      ["<=", ["to-number", ["get", prop]], maxVal],
    ],
    FULL,
    DIMMED,
  ] as unknown) as ExpressionSpecification;
}

export function buildLegendHighlightColorExpression(
  option: RoadwayVisualizationOption | undefined,
  hoveredValue: string | null,
  fallbackColorExpression: string | ExpressionSpecification,
): string | ExpressionSpecification {
  if (
    !hoveredValue ||
    hoveredValue === "__NO_DATA__" ||
    !option?.property_name ||
    option.kind !== "categorical"
  ) {
    return fallbackColorExpression;
  }

  const hoveredItem = option.legend_items.find(
    (item) => item.value === hoveredValue,
  );
  if (!hoveredItem) {
    return fallbackColorExpression;
  }

  return ([
    "case",
    ["==", ["coalesce", ["get", option.property_name], "__NO_DATA__"], hoveredValue],
    hoveredItem.color,
    fallbackColorExpression,
  ] as unknown) as ExpressionSpecification;
}

export function buildRoadwayLineSortKeyExpression(
  option?: RoadwayVisualizationOption,
): number | ExpressionSpecification {
  if (!option?.property_name || !option.kind) {
    return DEFAULT_ROADWAY_LINE_SORT_KEY;
  }

  if (option.kind === "numeric") {
    return ([
      "case",
      ["==", ["get", option.property_name], null],
      -1,
      ["to-number", ["get", option.property_name]],
    ] as unknown) as ExpressionSpecification;
  }

  const stringItems = option.legend_items.filter(
    (item) => typeof item.value === "string",
  );
  if (stringItems.length === 0) {
    return DEFAULT_ROADWAY_LINE_SORT_KEY;
  }

  return ([
    "match",
    ["coalesce", ["get", option.property_name], "__NO_DATA__"],
    ...stringItems.flatMap((item, idx) => [
      item.value as string,
      stringItems.length - idx,
    ]),
    0,
  ] as unknown) as ExpressionSpecification;
}
