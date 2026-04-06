import { useQuery } from "@tanstack/react-query";

import { getBounds } from "../services/api";

export function useBoundsQuery(
  district: number | null,
  counties: string[],
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["bounds", district, counties, enabled],
    queryFn: () => getBounds({ district, counties }),
    enabled,
  });
}
