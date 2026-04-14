import { useQuery } from "@tanstack/react-query";

import { getBounds } from "../services/api";

export function useBoundsQuery(
  districts: number[],
  counties: string[],
  highwayTypes: string[],
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["bounds", districts, counties, highwayTypes, enabled],
    queryFn: () => getBounds({ districts, counties, highwayTypes }),
    enabled,
  });
}
