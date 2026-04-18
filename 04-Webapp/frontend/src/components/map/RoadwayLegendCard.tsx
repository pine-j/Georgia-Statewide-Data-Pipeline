import type { ReactNode } from "react";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import { Box, Checkbox, Link, Paper, Stack, Typography } from "@mui/material";

import type { ThemeFilterValue } from "../../store/useAppStore";
import {
  RoadwayLegendItem,
  RoadwayVisualizationOption,
  ThemeFilterSpec,
} from "../../types/api";
import {
  LegendPresence,
  getLegendItemPresenceKey,
  getLegendItemsForDisplay,
} from "./roadwayVisualization";

interface RoadwayLegendCardProps {
  visualization: RoadwayVisualizationOption;
  legendPresence?: LegendPresence | null;
  onLegendItemHover?: (value: string | null) => void;
  themeFilterValue?: ThemeFilterValue;
  defaultThemeFilterValue?: ThemeFilterValue | null;
  setThemeFilter?: (
    visualizationId: string,
    patch: Partial<ThemeFilterValue>,
  ) => void;
}

const NO_DATA_HOVER_KEY = "__NO_DATA__";

const SWATCH_GRID = {
  display: "grid",
  gridTemplateColumns: "12px minmax(0, 1fr)",
  gap: 0.75,
  alignItems: "center",
  borderRadius: "4px",
  px: 0.5,
  mx: -0.5,
  cursor: "pointer",
  transition: "background-color 0.15s ease",
  "&:hover": {
    bgcolor: "rgba(17, 61, 73, 0.07)",
  },
} as const;

const FILTER_ROW_GRID = {
  display: "grid",
  gridTemplateColumns: "18px 12px minmax(0, 1fr)",
  columnGap: 0.5,
  alignItems: "center",
  borderRadius: "4px",
  px: 0.5,
  mx: -0.5,
  cursor: "pointer",
  transition: "background-color 0.15s ease",
  "&:hover": {
    bgcolor: "rgba(17, 61, 73, 0.07)",
  },
} as const;

function getToggleableFilterSpec(
  visualization: RoadwayVisualizationOption,
): ThemeFilterSpec | null {
  return (
    visualization.filters.find(
      (spec) =>
        spec.control === "toggle_chips" ||
        spec.control === "multi_select" ||
        spec.control === "bin_multi_select",
    ) ?? null
  );
}

function getBinSelectionKey(bin: {
  value?: string | null;
  label: string;
}): string {
  return typeof bin.value === "string" ? bin.value : bin.label;
}

function getHoverKeyForLegendItem(
  visualization: RoadwayVisualizationOption,
  item: RoadwayLegendItem,
): string {
  if (visualization.kind === "categorical") {
    return item.value ?? item.label;
  }
  return item.label;
}

function findLegendItemForBin(
  visualization: RoadwayVisualizationOption,
  bin: { value?: string | null; label: string; min_value?: number | null; max_value?: number | null },
): RoadwayLegendItem | undefined {
  if (visualization.kind === "categorical") {
    return visualization.legend_items.find((item) => item.value === bin.value);
  }
  return visualization.legend_items.find(
    (item) =>
      item.label === bin.label ||
      (typeof bin.min_value === "number" &&
        typeof item.min_value === "number" &&
        item.min_value === bin.min_value &&
        (item.max_value ?? null) === (bin.max_value ?? null)),
  );
}

export function RoadwayLegendCard({
  visualization,
  legendPresence,
  onLegendItemHover,
  themeFilterValue,
  defaultThemeFilterValue,
  setThemeFilter,
}: RoadwayLegendCardProps) {
  const filterSpec = getToggleableFilterSpec(visualization);
  const isFilterable = Boolean(
    filterSpec &&
      defaultThemeFilterValue &&
      setThemeFilter &&
      filterSpec.bins.length > 0,
  );

  const handleEnter = (value: string | null) => () => onLegendItemHover?.(value);
  const handleLeave = () => onLegendItemHover?.(null);

  let content: ReactNode;

  if (isFilterable && filterSpec && defaultThemeFilterValue && setThemeFilter) {
    const effective: ThemeFilterValue = {
      selectedValues:
        themeFilterValue?.selectedValues ?? defaultThemeFilterValue.selectedValues,
      range: themeFilterValue?.range ?? defaultThemeFilterValue.range,
      includeNoData:
        themeFilterValue?.includeNoData ?? defaultThemeFilterValue.includeNoData,
    };
    const selectedSet = new Set(effective.selectedValues);

    const binRows = filterSpec.bins.map((bin) => {
      const selectionKey = getBinSelectionKey(bin);
      const legendItem = findLegendItemForBin(visualization, bin);
      const color = legendItem?.color ?? visualization.no_data_color;
      const hoverKey = legendItem
        ? getHoverKeyForLegendItem(visualization, legendItem)
        : selectionKey;
      return {
        key: `bin-${selectionKey}`,
        selectionKey,
        label: bin.label,
        color,
        hoverKey,
        checked: selectedSet.has(selectionKey),
      };
    });

    const toggleBin = (selectionKey: string, checked: boolean) => {
      const next = checked
        ? Array.from(new Set([...effective.selectedValues, selectionKey]))
        : effective.selectedValues.filter((value) => value !== selectionKey);
      setThemeFilter(visualization.id, {
        selectedValues: next,
        range: effective.range,
        includeNoData: effective.includeNoData,
      });
    };

    const setAll = (checkAll: boolean) => {
      setThemeFilter(visualization.id, {
        selectedValues: checkAll
          ? filterSpec.bins.map(getBinSelectionKey)
          : [],
        range: effective.range,
        includeNoData: filterSpec.no_data_selectable
          ? checkAll
          : effective.includeNoData,
      });
    };

    const toggleNoData = (checked: boolean) => {
      setThemeFilter(visualization.id, {
        selectedValues: effective.selectedValues,
        range: effective.range,
        includeNoData: checked,
      });
    };

    const showNoDataRow =
      filterSpec.no_data_selectable ||
      (legendPresence ? legendPresence.hasNoData : true);

    const totalBins = filterSpec.bins.length;
    const allChecked = binRows.every((row) => row.checked) &&
      (!filterSpec.no_data_selectable || effective.includeNoData);
    const noneChecked =
      binRows.every((row) => !row.checked) &&
      (!filterSpec.no_data_selectable || !effective.includeNoData);

    content = (
      <>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 0.5,
          }}
        >
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ fontSize: "0.66rem", letterSpacing: 0.3, textTransform: "uppercase" }}
          >
            Filter · {selectedSet.size}
            {filterSpec.no_data_selectable && effective.includeNoData ? " + ND" : ""} / {totalBins}
            {filterSpec.no_data_selectable ? " + 1" : ""}
          </Typography>
          <Box sx={{ display: "flex", gap: 0.75 }}>
            <Link
              component="button"
              type="button"
              variant="caption"
              underline="hover"
              onClick={() => setAll(true)}
              disabled={allChecked}
              sx={{
                fontSize: "0.66rem",
                lineHeight: 1.2,
                color: allChecked ? "text.disabled" : "primary.main",
                pointerEvents: allChecked ? "none" : "auto",
              }}
            >
              All
            </Link>
            <Typography variant="caption" color="text.disabled" sx={{ fontSize: "0.66rem" }}>
              ·
            </Typography>
            <Link
              component="button"
              type="button"
              variant="caption"
              underline="hover"
              onClick={() => setAll(false)}
              disabled={noneChecked}
              sx={{
                fontSize: "0.66rem",
                lineHeight: 1.2,
                color: noneChecked ? "text.disabled" : "primary.main",
                pointerEvents: noneChecked ? "none" : "auto",
              }}
            >
              None
            </Link>
          </Box>
        </Box>

        <Stack spacing={0.25}>
          {binRows.map((row) => (
            <Box
              key={row.key}
              role="button"
              onClick={() => toggleBin(row.selectionKey, !row.checked)}
              sx={FILTER_ROW_GRID}
              onMouseEnter={handleEnter(row.hoverKey)}
              onMouseLeave={handleLeave}
            >
              <Checkbox
                icon={<CheckBoxOutlineBlankIcon fontSize="inherit" />}
                checkedIcon={<CheckBoxIcon fontSize="inherit" />}
                checked={row.checked}
                onChange={(event) =>
                  toggleBin(row.selectionKey, event.target.checked)
                }
                onClick={(event) => event.stopPropagation()}
                size="small"
                sx={{ p: 0, fontSize: "0.95rem" }}
              />
              <Box
                sx={{
                  width: 12,
                  height: 12,
                  borderRadius: "2px",
                  bgcolor: row.color,
                  border: "1px solid rgba(16, 35, 47, 0.12)",
                }}
              />
              <Typography
                variant="caption"
                sx={{ lineHeight: 1.35, fontSize: "0.68rem" }}
              >
                {row.label}
              </Typography>
            </Box>
          ))}

          {showNoDataRow && (
            <Box
              role={filterSpec.no_data_selectable ? "button" : undefined}
              onClick={
                filterSpec.no_data_selectable
                  ? () => toggleNoData(!effective.includeNoData)
                  : undefined
              }
              sx={filterSpec.no_data_selectable ? FILTER_ROW_GRID : SWATCH_GRID}
              onMouseEnter={handleEnter(NO_DATA_HOVER_KEY)}
              onMouseLeave={handleLeave}
            >
              {filterSpec.no_data_selectable && (
                <Checkbox
                  icon={<CheckBoxOutlineBlankIcon fontSize="inherit" />}
                  checkedIcon={<CheckBoxIcon fontSize="inherit" />}
                  checked={effective.includeNoData}
                  onChange={(event) => toggleNoData(event.target.checked)}
                  onClick={(event) => event.stopPropagation()}
                  size="small"
                  sx={{ p: 0, fontSize: "0.95rem" }}
                />
              )}
              <Box
                sx={{
                  width: 12,
                  height: 12,
                  borderRadius: "2px",
                  bgcolor: visualization.no_data_color,
                  border: "1px solid rgba(16, 35, 47, 0.12)",
                }}
              />
              <Typography
                variant="caption"
                sx={{ lineHeight: 1.35, fontSize: "0.68rem" }}
              >
                No data
              </Typography>
            </Box>
          )}
        </Stack>
      </>
    );
  } else {
    const allLegendItems = getLegendItemsForDisplay(visualization);
    const legendItems = legendPresence
      ? allLegendItems.filter((item) => {
          const key = getLegendItemPresenceKey(visualization, item);
          return key === null ? true : legendPresence.presentKeys.has(key);
        })
      : allLegendItems;
    const showNoDataRow = legendPresence ? legendPresence.hasNoData : true;

    content = (
      <Stack spacing={0.4}>
        {legendItems.map((item) => {
          const hoverKey = getHoverKeyForLegendItem(visualization, item);
          return (
            <Box
              key={`${visualization.id}-${item.label}-${item.value ?? item.min_value ?? "base"}`}
              sx={SWATCH_GRID}
              onMouseEnter={handleEnter(hoverKey)}
              onMouseLeave={handleLeave}
            >
              <Box
                sx={{
                  width: 12,
                  height: 12,
                  borderRadius: "2px",
                  bgcolor: item.color,
                  border: "1px solid rgba(16, 35, 47, 0.12)",
                }}
              />
              <Typography variant="caption" sx={{ lineHeight: 1.35, fontSize: "0.68rem" }}>
                {item.label}
              </Typography>
            </Box>
          );
        })}

        {showNoDataRow && (
          <Box
            sx={SWATCH_GRID}
            onMouseEnter={handleEnter(NO_DATA_HOVER_KEY)}
            onMouseLeave={handleLeave}
          >
            <Box
              sx={{
                width: 12,
                height: 12,
                borderRadius: "2px",
                bgcolor: visualization.no_data_color,
                border: "1px solid rgba(16, 35, 47, 0.12)",
              }}
            />
            <Typography variant="caption" sx={{ lineHeight: 1.35, fontSize: "0.68rem" }}>
              No data
            </Typography>
          </Box>
        )}
      </Stack>
    );
  }

  return (
    <Paper
      elevation={0}
      sx={{
        width: "100%",
        p: 1,
        borderRadius: "6px",
        border: "1px solid rgba(17, 61, 73, 0.12)",
        bgcolor: "rgba(246, 248, 249, 0.96)",
      }}
    >
      <Stack spacing={0.6}>
        <Typography variant="subtitle2" sx={{ fontWeight: 700, fontSize: "0.75rem" }}>
          {visualization.label}
        </Typography>

        <Typography variant="caption" color="text.secondary" sx={{ fontSize: "0.68rem", lineHeight: 1.3 }}>
          {visualization.description}
        </Typography>

        {content}

        {visualization.notes && (
          <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.3, fontSize: "0.68rem" }}>
            {visualization.notes}
          </Typography>
        )}
      </Stack>
    </Paper>
  );
}
