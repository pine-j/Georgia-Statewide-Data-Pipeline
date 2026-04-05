import { create } from "zustand";

type LayerKey = "roadways";

interface AppState {
  selectedDistrict: number | null;
  selectedCounties: string[];
  activeLayers: Record<LayerKey, boolean>;
  setSelectedDistrict: (district: number | null) => void;
  setSelectedCounties: (counties: string[]) => void;
  setLayerVisibility: (layer: LayerKey, visible: boolean) => void;
}

export const useAppStore = create<AppState>((set) => ({
  selectedDistrict: null,
  selectedCounties: [],
  activeLayers: {
    roadways: true,
  },
  setSelectedDistrict: (district) =>
    set({
      selectedDistrict: district,
      selectedCounties: [],
    }),
  setSelectedCounties: (counties) => set({ selectedCounties: counties }),
  setLayerVisibility: (layer, visible) =>
    set((current) => ({
      activeLayers: {
        ...current.activeLayers,
        [layer]: visible,
      },
    })),
}));
