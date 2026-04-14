import { create } from 'zustand';
import { RoadwayDetail } from '../types/api';

type LayerKey = 'roadways';
export const DEFAULT_HIGHWAY_TYPES = ['IH'];
interface AppState {
  selectedDistricts: number[];
  selectedCounties: string[];
  selectedHighwayTypes: string[];
  selectedVisualizationId: string;
  activeLayers: Record<LayerKey, boolean>;
  selectedRoadwayId: string | null;
  roadwayDetail: RoadwayDetail | null;
  isLoadingDetail: boolean;
  detailError: boolean;
  setSelectedDistricts: (districts: number[]) => void;
  setSelectedCounties: (counties: string[]) => void;
  setSelectedHighwayTypes: (highwayTypes: string[]) => void;
  setSelectedVisualizationId: (visualizationId: string) => void;
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
  activeLayers: { roadways: true },
  selectedRoadwayId: null,
  roadwayDetail: null,
  isLoadingDetail: false,
  detailError: false,
  setSelectedDistricts: (selectedDistricts) => set({ selectedDistricts }),
  setSelectedCounties: (counties) => set({ selectedCounties: counties }),
  setSelectedHighwayTypes: (selectedHighwayTypes) => set({ selectedHighwayTypes }),
  setSelectedVisualizationId: (selectedVisualizationId) => set({ selectedVisualizationId }),
  setLayerVisibility: (layer, visible) => set((current) => ({ activeLayers: { ...current.activeLayers, [layer]: visible } })),
  openRoadwayDetail: (uniqueId) => set({ selectedRoadwayId: uniqueId, roadwayDetail: null, isLoadingDetail: true, detailError: false }),
  setRoadwayDetail: (detail) => set({ roadwayDetail: detail, isLoadingDetail: false }),
  setDetailError: () => set({ isLoadingDetail: false, detailError: true }),
  closeRoadwayDetail: () => set({ selectedRoadwayId: null, roadwayDetail: null, isLoadingDetail: false, detailError: false }),
}));
