import {
  AnalyticsSummary,
  AppConfig,
  BoundsResponse,
  GeoJsonFeatureCollection,
  GeorgiaFilterOptions,
  RoadwayDetail,
  RoadwayFeatureCollection,
  RoadwayManifest,
  RoadwayVisualizationCatalog,
  StateOption,
} from "../types/api";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
const DEFAULT_STATE = "ga";

export interface QueryFilters {
  districts?: number[];
  counties?: string[];
  highwayTypes?: string[];
  areaOffices?: number[];
  mpos?: string[];
  regionalCommissions?: number[];
  stateHouseDistricts?: number[];
  stateSenateDistricts?: number[];
  congressionalDistricts?: number[];
  cities?: number[];
  includeUnincorporated?: boolean;
}

export type BoundaryType =
  | "counties"
  | "districts"
  | "area_offices"
  | "mpos"
  | "regional_commissions"
  | "state_house"
  | "state_senate"
  | "congressional";

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, { signal });

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

export function getGeorgiaFilterOptions(): Promise<GeorgiaFilterOptions> {
  return fetchJson<GeorgiaFilterOptions>("/meta/georgia-filters");
}

export function getAppConfig(): Promise<AppConfig> {
  return fetchJson<AppConfig>("/meta/config");
}

export function getStates(): Promise<StateOption[]> {
  return fetchJson<StateOption[]>("/meta/states");
}

function appendAll<T>(
  params: URLSearchParams,
  key: string,
  values: readonly T[] | undefined,
): void {
  for (const value of values ?? []) {
    params.append(key, String(value));
  }
}

export function buildFilterQuery(filters: QueryFilters): string {
  const params = new URLSearchParams({ state: DEFAULT_STATE });

  appendAll(params, "district", filters.districts);
  appendAll(params, "county", filters.counties);
  appendAll(params, "highway_type", filters.highwayTypes);
  appendAll(params, "area_office", filters.areaOffices);
  appendAll(params, "mpo", filters.mpos);
  appendAll(params, "regional_commission", filters.regionalCommissions);
  appendAll(params, "state_house", filters.stateHouseDistricts);
  appendAll(params, "state_senate", filters.stateSenateDistricts);
  appendAll(params, "congressional", filters.congressionalDistricts);
  appendAll(params, "city", filters.cities);
  if (filters.includeUnincorporated) {
    params.set("include_unincorporated", "true");
  }

  return params.toString();
}

export function getAnalyticsSummary(filters: QueryFilters): Promise<AnalyticsSummary> {
  return fetchJson<AnalyticsSummary>(`/analytics/summary?${buildFilterQuery(filters)}`);
}

export function getRoadwayManifest(
  filters: QueryFilters,
  chunkSize: number,
  signal?: AbortSignal,
): Promise<RoadwayManifest> {
  const params = new URLSearchParams(buildFilterQuery(filters));
  params.set("chunk_size", String(chunkSize));
  return fetchJson<RoadwayManifest>(
    `/layers/roadways/manifest?${params.toString()}`,
    signal,
  );
}

export function getRoadways(
  filters: QueryFilters,
  limit = 500,
  offset = 0,
  signal?: AbortSignal,
): Promise<RoadwayFeatureCollection> {
  const params = new URLSearchParams(buildFilterQuery(filters));
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  return fetchJson<RoadwayFeatureCollection>(
    `/layers/roadways?${params.toString()}`,
    signal,
  );
}

export function getRoadwayVisualizationCatalog(): Promise<RoadwayVisualizationCatalog> {
  return fetchJson<RoadwayVisualizationCatalog>("/layers/roadways/visualizations");
}

export function getRoadwayDetail(
  uniqueId: string,
  signal?: AbortSignal,
): Promise<RoadwayDetail> {
  const params = new URLSearchParams({ unique_id: uniqueId });
  return fetchJson<RoadwayDetail>(`/layers/roadways/detail?${params.toString()}`, signal);
}

export function getBounds(filters: QueryFilters): Promise<BoundsResponse> {
  return fetchJson<BoundsResponse>(`/geospatial/bounds?${buildFilterQuery(filters)}`);
}

export function getBoundaryLayer(
  boundaryType: BoundaryType,
  filters: QueryFilters,
  signal?: AbortSignal,
): Promise<GeoJsonFeatureCollection> {
  return fetchJson<GeoJsonFeatureCollection>(
    `/layers/boundaries/${boundaryType}?${buildFilterQuery(filters)}`,
    signal,
  );
}
