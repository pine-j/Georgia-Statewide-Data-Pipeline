import { useQueries, type UseQueryResult } from "@tanstack/react-query";

import { getBoundaryLayer } from "../services/api";
import type { BoundaryOverlayVisibility } from "../store/useAppStore";
import type { GeoJsonFeatureCollection } from "../types/api";

interface BoundaryFilterInputs {
  districts: number[];
  counties: string[];
  highwayTypes: string[];
  areaOffices: number[];
  mpos: string[];
  regionalCommissions: number[];
  stateHouseDistricts: number[];
  stateSenateDistricts: number[];
  congressionalDistricts: number[];
  cities: number[];
  includeUnincorporated: boolean;
}

type BoundaryQueryResult = UseQueryResult<GeoJsonFeatureCollection, Error>;

export interface BoundaryLayerQueries {
  countiesQuery: BoundaryQueryResult;
  districtsQuery: BoundaryQueryResult;
  areaOfficesQuery: BoundaryQueryResult;
  mposQuery: BoundaryQueryResult;
  regionalCommissionsQuery: BoundaryQueryResult;
  stateHouseQuery: BoundaryQueryResult;
  stateSenateQuery: BoundaryQueryResult;
  congressionalQuery: BoundaryQueryResult;
}

export function useBoundaryLayersQuery(
  filters: BoundaryFilterInputs,
  overlayVisibility: BoundaryOverlayVisibility,
  enabled: boolean,
): BoundaryLayerQueries {
  const {
    districts,
    counties,
    highwayTypes,
    areaOffices,
    mpos,
    regionalCommissions,
    stateHouseDistricts,
    stateSenateDistricts,
    congressionalDistricts,
    cities,
    includeUnincorporated,
  } = filters;

  const adminFilters = {
    districts,
    counties,
    highwayTypes,
    areaOffices,
    mpos,
    regionalCommissions,
    stateHouseDistricts,
    stateSenateDistricts,
    congressionalDistricts,
    cities,
    includeUnincorporated,
  };

  const sharedFilterKey = [
    districts,
    counties,
    highwayTypes,
    areaOffices,
    mpos,
    regionalCommissions,
    stateHouseDistricts,
    stateSenateDistricts,
    congressionalDistricts,
    cities,
    includeUnincorporated,
  ] as const;

  const [
    countiesQuery,
    districtsQuery,
    areaOfficesQuery,
    mposQuery,
    regionalCommissionsQuery,
    stateHouseQuery,
    stateSenateQuery,
    congressionalQuery,
  ] = useQueries({
    queries: [
      {
        queryKey: ["boundary-layer", "counties", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("counties", adminFilters, signal),
        enabled: enabled && overlayVisibility.counties,
      },
      {
        queryKey: ["boundary-layer", "districts", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("districts", adminFilters, signal),
        enabled: enabled && overlayVisibility.districts,
      },
      {
        queryKey: ["boundary-layer", "area_offices", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("area_offices", adminFilters, signal),
        enabled: enabled && overlayVisibility.areaOffices,
      },
      {
        queryKey: ["boundary-layer", "mpos", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("mpos", adminFilters, signal),
        enabled: enabled && overlayVisibility.mpos,
      },
      {
        queryKey: ["boundary-layer", "regional_commissions", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("regional_commissions", adminFilters, signal),
        enabled: enabled && overlayVisibility.regionalCommissions,
      },
      {
        queryKey: ["boundary-layer", "state_house", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("state_house", adminFilters, signal),
        enabled: enabled && overlayVisibility.stateHouse,
      },
      {
        queryKey: ["boundary-layer", "state_senate", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("state_senate", adminFilters, signal),
        enabled: enabled && overlayVisibility.stateSenate,
      },
      {
        queryKey: ["boundary-layer", "congressional", ...sharedFilterKey, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("congressional", adminFilters, signal),
        enabled: enabled && overlayVisibility.congressional,
      },
    ],
  }) as [
    BoundaryQueryResult,
    BoundaryQueryResult,
    BoundaryQueryResult,
    BoundaryQueryResult,
    BoundaryQueryResult,
    BoundaryQueryResult,
    BoundaryQueryResult,
    BoundaryQueryResult,
  ];

  return {
    countiesQuery,
    districtsQuery,
    areaOfficesQuery,
    mposQuery,
    regionalCommissionsQuery,
    stateHouseQuery,
    stateSenateQuery,
    congressionalQuery,
  };
}
