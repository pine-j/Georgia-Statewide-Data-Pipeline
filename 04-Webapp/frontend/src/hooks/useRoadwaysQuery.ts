import { useQuery } from "@tanstack/react-query";

import { getRoadways } from "../services/api";

export function useRoadwaysQuery(
  district: number | null,
  counties: string[],
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["roadways", district, counties, enabled],
    queryFn: () => getRoadways({ district, counties }),
    enabled,
  });
}
