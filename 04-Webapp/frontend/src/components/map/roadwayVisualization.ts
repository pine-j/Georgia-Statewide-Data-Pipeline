import type { ExpressionSpecification } from "maplibre-gl";

import { RoadwayLegendItem, RoadwayVisualizationOption } from "../../types/api";

export const DEFAULT_ROADWAY_LINE_COLOR = "#1490a7";
export const DEFAULT_ROADWAY_LINE_OPACITY = 0.96;
export const DEFAULT_ROADWAY_LINE_SORT_KEY = 0;

function hasNumericBounds(item: RoadwayLegendItem): item is RoadwayLegendItem & { min_value: number } {
  return typeof item.min_value === "number";
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

export function buildRoadwayLineOpacityExpression(
  option?: RoadwayVisualizationOption,
): number | ExpressionSpecification {
  if (!option?.property_name) {
    return DEFAULT_ROADWAY_LINE_OPACITY;
  }

  return ([
    "case",
    ["==", ["get", option.property_name], null],
    0.52,
    DEFAULT_ROADWAY_LINE_OPACITY,
  ] as unknown) as ExpressionSpecification;
}

export function buildRoadwayLineSortKeyExpression(
  option?: RoadwayVisualizationOption,
): number | ExpressionSpecification {
  if (!option?.property_name || option.kind !== "numeric") {
    return DEFAULT_ROADWAY_LINE_SORT_KEY;
  }

  return ([
    "case",
    ["==", ["get", option.property_name], null],
    -1,
    ["to-number", ["get", option.property_name]],
  ] as unknown) as ExpressionSpecification;
}
