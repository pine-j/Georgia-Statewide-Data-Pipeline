import {
  AnalyticsSummary,
  AppConfig,
  BoundsResponse,
  GeoJsonFeatureCollection,
  GeorgiaFilterOptions,
  RoadwayDetail,
  RoadwayFeatureCollection,
  RoadwayManifest,
  StateOption,
} from "../types/api";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
const DEFAULT_STATE = "ga";

interface QueryFilters {
  district?: number | null;
  counties?: string[];
}

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

function buildFilterQuery({ district, counties }: QueryFilters): string {
  const params = new URLSearchParams({ state: DEFAULT_STATE });

  if (district) {
    params.set("district", String(district));
  }

  for (const county of counties ?? []) {
    params.append("county", county);
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
  boundaryType: "counties" | "districts",
  filters: QueryFilters,
  signal?: AbortSignal,
): Promise<GeoJsonFeatureCollection> {
  return fetchJson<GeoJsonFeatureCollection>(
    `/layers/boundaries/${boundaryType}?${buildFilterQuery(filters)}`,
    signal,
  );
}
