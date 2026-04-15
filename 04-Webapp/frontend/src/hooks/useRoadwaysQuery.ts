import { useQuery } from "@tanstack/react-query";

import { getRoadways } from "../services/api";

export function useRoadwaysQuery(
  districts: number[],
  counties: string[],
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["roadways", districts, counties, enabled],
    queryFn: () => getRoadways({ districts, counties }),
    enabled,
  });
}
