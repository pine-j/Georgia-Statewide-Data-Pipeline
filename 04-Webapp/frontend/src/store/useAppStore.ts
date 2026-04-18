import { create } from 'zustand';
import { RoadwayDetail } from '../types/api';

type LayerKey = 'roadways';
export const DEFAULT_HIGHWAY_TYPES = ['IH', 'US', 'SH'];

export interface ThemeFilterValue {
  selectedValues: string[];
  range: [number, number] | null;
  includeNoData: boolean;
}

export type ThemeFilterState = Record<string, ThemeFilterValue>;

interface AppState {
  selectedDistricts: number[];
  selectedCounties: string[];
  selectedHighwayTypes: string[];
  selectedVisualizationId: string;
  themeFilters: ThemeFilterState;
  activeLayers: Record<LayerKey, boolean>;
  selectedRoadwayId: string | null;
  roadwayDetail: RoadwayDetail | null;
  isLoadingDetail: boolean;
  detailError: boolean;
  setSelectedDistricts: (districts: number[]) => void;
  setSelectedCounties: (counties: string[]) => void;
  setSelectedHighwayTypes: (highwayTypes: string[]) => void;
  setSelectedVisualizationId: (visualizationId: string) => void;
  setThemeFilter: (visualizationId: string, patch: Partial<ThemeFilterValue>) => void;
  resetThemeFilter: (visualizationId: string) => void;
  resetAllThemeFilters: () => void;
  setLayerVisibility: (layer: LayerKey, visible: boolean) => void;
  openRoadwayDetail: (uniqueId: string) => void;
  setRoadwayDetail: (detail: RoadwayDetail) => void;
  setDetailError: () => void;
  closeRoadwayDetail: () => void;
}
export const useAppStore = create<AppState>((set) => ({
  selectedDistricts: [],
  selectedCounties: [],
  selectedHighwayTypes: [...DEFAULT_HIGHWAY_TYPES],
  selectedVisualizationId: 'aadt',
  themeFilters: {},
  activeLayers: { roadways: true },
  selectedRoadwayId: null,
  roadwayDetail: null,
  isLoadingDetail: false,
  detailError: false,
  setSelectedDistricts: (selectedDistricts) => set({ selectedDistricts }),
  setSelectedCounties: (counties) => set({ selectedCounties: counties }),
  setSelectedHighwayTypes: (selectedHighwayTypes) => set({ selectedHighwayTypes }),
  setSelectedVisualizationId: (selectedVisualizationId) => set({ selectedVisualizationId }),
  setThemeFilter: (visualizationId, patch) =>
    set((current) => {
      const existingFilter = current.themeFilters[visualizationId] ?? {
        selectedValues: [],
        range: null,
        includeNoData: true,
      };

      return {
        themeFilters: {
          ...current.themeFilters,
          [visualizationId]: {
            ...existingFilter,
            selectedValues: 'selectedValues' in patch ? patch.selectedValues ?? existingFilter.selectedValues : existingFilter.selectedValues,
            range: 'range' in patch ? patch.range ?? existingFilter.range : existingFilter.range,
            includeNoData: 'includeNoData' in patch ? patch.includeNoData ?? existingFilter.includeNoData : existingFilter.includeNoData,
          },
        },
      };
    }),
  resetThemeFilter: (visualizationId) =>
    set((current) => {
      const { [visualizationId]: _removed, ...themeFilters } = current.themeFilters;
      return { themeFilters };
    }),
  resetAllThemeFilters: () => set({ themeFilters: {} }),
  setLayerVisibility: (layer, visible) => set((current) => ({ activeLayers: { ...current.activeLayers, [layer]: visible } })),
  openRoadwayDetail: (uniqueId) => set({ selectedRoadwayId: uniqueId, roadwayDetail: null, isLoadingDetail: true, detailError: false }),
  setRoadwayDetail: (detail) => set({ roadwayDetail: detail, isLoadingDetail: false }),
  setDetailError: () => set({ isLoadingDetail: false, detailError: true }),
  closeRoadwayDetail: () => set({ selectedRoadwayId: null, roadwayDetail: null, isLoadingDetail: false, detailError: false }),
}));
