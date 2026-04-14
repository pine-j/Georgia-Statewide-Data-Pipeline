export interface StateOption {
  code: string;
  name: string;
}

export interface DistrictOption {
  id: number;
  label: string;
}

export interface CountyOption {
  county: string;
  county_fips: string;
  district: number;
}

export interface HighwayTypeOption {
  id: string;
  label: string;
  route_family: string;
}

export interface GeorgiaFilterOptions {
  districts: DistrictOption[];
  counties: CountyOption[];
  highway_types: HighwayTypeOption[];
}

export interface AppConfig {
  default_state: string;
  map_provider: string;
  chat_enabled: boolean;
}

export interface FunctionalClassSummary {
  functional_class: string;
  segment_count: number;
  total_miles: number;
}

export interface AnalyticsSummary {
  state_code: string;
  roadway_count: number;
  total_miles: number;
  classes: FunctionalClassSummary[];
}

export interface RoadwayFeatureProperties {
  id: number;
  unique_id: string;
  road_name: string;
  functional_class: string;
  aadt: number | null;
  length_miles: number;
  district: number;
  district_label: string;
  county: string;
  system_code_label?: string | null;
  direction_label?: string | null;
  num_lanes?: number | null;
  future_aadt_2044?: number | null;
  k_factor?: number | null;
  d_factor?: number | null;
  truck_aadt?: number | null;
  pct_sadt?: number | null;
  pct_cadt?: number | null;
  vmt?: number | null;
  nhs_ind_label?: string | null;
  median_type_label?: string | null;
  hwy_des?: string | null;
  speed_limit?: number | null;
  truck_pct?: number | null;
  functional_class_viz?: string | null;
  surface_type_label?: string | null;
  ownership_label?: string | null;
  facility_type_label?: string | null;
  sec_evac?: string | null;
}

export interface RoadwayFeature {
  type: "Feature";
  geometry: GeoJSON.Geometry;
  properties: RoadwayFeatureProperties;
}

export interface RoadwayFeatureCollection {
  type: "FeatureCollection";
  features: RoadwayFeature[];
}

export interface GeoJsonFeature {
  type: "Feature";
  geometry: GeoJSON.Geometry;
  properties: Record<string, string | number | boolean | null>;
}

export interface GeoJsonFeatureCollection {
  type: "FeatureCollection";
  features: GeoJsonFeature[];
}

export interface RoadwayManifest {
  state_code: string;
  total_segments: number;
  chunk_size: number;
  chunk_count: number;
  bounds: [number, number, number, number] | null;
}

export interface RoadwayDetail {
  unique_id: string;
  road_name: string;
  district: number;
  district_label: string;
  county: string;
  attributes: Record<string, string | number | boolean | null>;
}

export interface BoundsResponse {
  bounds: [number, number, number, number] | null;
}

export interface RoadwayLegendItem {
  color: string;
  label: string;
  value?: string | null;
  min_value?: number | null;
  max_value?: number | null;
}

export type RoadwayVisualizationKind = "numeric" | "categorical";
export type RoadwayVisualizationMapMode = "thematic" | "details_only" | "unavailable";
export type RoadwayVisualizationImplementationStatus =
  | "staged"
  | "derived"
  | "popup_only"
  | "unavailable";

export interface RoadwayVisualizationOption {
  id: string;
  texas_header: string;
  georgia_header: string | null;
  label: string;
  description: string;
  notes: string | null;
  kind: RoadwayVisualizationKind | null;
  map_mode: RoadwayVisualizationMapMode;
  implementation_status: RoadwayVisualizationImplementationStatus;
  property_name: string | null;
  status: string;
  unit: string | null;
  default: boolean;
  no_data_color: string;
  legend_items: RoadwayLegendItem[];
}

export interface RoadwayVisualizationCatalog {
  default_option_id: string;
  thematic_options: RoadwayVisualizationOption[];
  details_only_options: RoadwayVisualizationOption[];
  unavailable_options: RoadwayVisualizationOption[];
}
