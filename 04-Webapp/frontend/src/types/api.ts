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

export interface GeorgiaFilterOptions {
  districts: DistrictOption[];
  counties: CountyOption[];
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
