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

// Toggles for the 6 new admin-boundary map overlays. City is intentionally
// absent - cities are filter-only, not a map overlay.
export interface BoundaryOverlayVisibility {
  statewide: boolean;
  districts: boolean;
  counties: boolean;
  areaOffices: boolean;
  mpos: boolean;
  regionalCommissions: boolean;
  stateHouse: boolean;
  stateSenate: boolean;
  congressional: boolean;
}

export const DEFAULT_BOUNDARY_OVERLAY_VISIBILITY: BoundaryOverlayVisibility = {
  statewide: false,
  districts: true,
  counties: true,
  areaOffices: false,
  mpos: false,
  regionalCommissions: false,
  stateHouse: false,
  stateSenate: false,
  congressional: false,
};

interface AppState {
  selectedDistricts: number[];
  selectedCounties: string[];
  selectedHighwayTypes: string[];
  // Step 2 split-driving geographies.
  selectedAreaOffices: number[];
  selectedMpos: string[];
  selectedRegionalCommissions: number[];
  // Step 4 overlay flag selections.
  selectedStateHouseDistricts: number[];
  selectedStateSenateDistricts: number[];
  selectedCongressionalDistricts: number[];
  selectedCities: number[];
  includeUnincorporated: boolean;
  boundaryOverlayVisibility: BoundaryOverlayVisibility;
  roadwayNetworkVisible: boolean;

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
  setSelectedAreaOffices: (areaOffices: number[]) => void;
  setSelectedMpos: (mpos: string[]) => void;
  setSelectedRegionalCommissions: (regionalCommissions: number[]) => void;
  setSelectedStateHouseDistricts: (stateHouseDistricts: number[]) => void;
  setSelectedStateSenateDistricts: (stateSenateDistricts: number[]) => void;
  setSelectedCongressionalDistricts: (congressionalDistricts: number[]) => void;
  setSelectedCities: (cities: number[]) => void;
  setIncludeUnincorporated: (includeUnincorporated: boolean) => void;
  setBoundaryOverlayVisibility: (
    overlay: keyof BoundaryOverlayVisibility,
    visible: boolean,
  ) => void;
  setRoadwayNetworkVisible: (visible: boolean) => void;
  resetAllAdminFilters: () => void;

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
  selectedAreaOffices: [],
  selectedMpos: [],
  selectedRegionalCommissions: [],
  selectedStateHouseDistricts: [],
  selectedStateSenateDistricts: [],
  selectedCongressionalDistricts: [],
  selectedCities: [],
  includeUnincorporated: false,
  boundaryOverlayVisibility: { ...DEFAULT_BOUNDARY_OVERLAY_VISIBILITY },
  roadwayNetworkVisible: true,

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
  setSelectedAreaOffices: (selectedAreaOffices) => set({ selectedAreaOffices }),
  setSelectedMpos: (selectedMpos) => set({ selectedMpos }),
  setSelectedRegionalCommissions: (selectedRegionalCommissions) =>
    set({ selectedRegionalCommissions }),
  setSelectedStateHouseDistricts: (selectedStateHouseDistricts) =>
    set({ selectedStateHouseDistricts }),
  setSelectedStateSenateDistricts: (selectedStateSenateDistricts) =>
    set({ selectedStateSenateDistricts }),
  setSelectedCongressionalDistricts: (selectedCongressionalDistricts) =>
    set({ selectedCongressionalDistricts }),
  setSelectedCities: (selectedCities) => set({ selectedCities }),
  setIncludeUnincorporated: (includeUnincorporated) => set({ includeUnincorporated }),
  setBoundaryOverlayVisibility: (overlay, visible) =>
    set((current) => ({
      boundaryOverlayVisibility: {
        ...current.boundaryOverlayVisibility,
        [overlay]: visible,
      },
    })),
  setRoadwayNetworkVisible: (roadwayNetworkVisible) =>
    set({ roadwayNetworkVisible }),
  resetAllAdminFilters: () =>
    set({
      selectedDistricts: [],
      selectedCounties: [],
      selectedHighwayTypes: [...DEFAULT_HIGHWAY_TYPES],
      selectedAreaOffices: [],
      selectedMpos: [],
      selectedRegionalCommissions: [],
      selectedStateHouseDistricts: [],
      selectedStateSenateDistricts: [],
      selectedCongressionalDistricts: [],
      selectedCities: [],
      includeUnincorporated: false,
      boundaryOverlayVisibility: { ...DEFAULT_BOUNDARY_OVERLAY_VISIBILITY },
    }),

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
            selectedValues:
              'selectedValues' in patch
                ? patch.selectedValues ?? existingFilter.selectedValues
                : existingFilter.selectedValues,
            range: 'range' in patch ? patch.range ?? existingFilter.range : existingFilter.range,
            includeNoData:
              'includeNoData' in patch
                ? patch.includeNoData ?? existingFilter.includeNoData
                : existingFilter.includeNoData,
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
  setLayerVisibility: (layer, visible) =>
    set((current) => ({ activeLayers: { ...current.activeLayers, [layer]: visible } })),
  openRoadwayDetail: (uniqueId) =>
    set({
      selectedRoadwayId: uniqueId,
      roadwayDetail: null,
      isLoadingDetail: true,
      detailError: false,
    }),
  setRoadwayDetail: (detail) => set({ roadwayDetail: detail, isLoadingDetail: false }),
  setDetailError: () => set({ isLoadingDetail: false, detailError: true }),
  closeRoadwayDetail: () =>
    set({
      selectedRoadwayId: null,
      roadwayDetail: null,
      isLoadingDetail: false,
      detailError: false,
    }),
}));
