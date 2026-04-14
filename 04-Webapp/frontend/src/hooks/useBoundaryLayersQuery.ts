import { useQueries } from "@tanstack/react-query";

import { getBoundaryLayer } from "../services/api";

export function useBoundaryLayersQuery(
  districts: number[],
  counties: string[],
  highwayTypes: string[],
  enabled: boolean,
) {
  const [countiesQuery, districtsQuery] = useQueries({
    queries: [
      {
        queryKey: ["boundary-layer", "counties", districts, counties, highwayTypes, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("counties", { districts, counties, highwayTypes }, signal),
        enabled,
      },
      {
        queryKey: ["boundary-layer", "districts", districts, counties, highwayTypes, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("districts", { districts, counties, highwayTypes }, signal),
        enabled,
      },
    ],
  });

  return {
    countiesQuery,
    districtsQuery,
  };
}
