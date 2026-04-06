import { useQueries } from "@tanstack/react-query";

import { getBoundaryLayer } from "../services/api";

export function useBoundaryLayersQuery(
  district: number | null,
  counties: string[],
  enabled: boolean,
) {
  const [countiesQuery, districtsQuery] = useQueries({
    queries: [
      {
        queryKey: ["boundary-layer", "counties", district, counties, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("counties", { district, counties }, signal),
        enabled,
      },
      {
        queryKey: ["boundary-layer", "districts", district, counties, enabled],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          getBoundaryLayer("districts", { district, counties }, signal),
        enabled,
      },
    ],
  });

  return {
    countiesQuery,
    districtsQuery,
  };
}
