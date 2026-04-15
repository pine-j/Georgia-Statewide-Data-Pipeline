import { useQuery } from "@tanstack/react-query";

import { getAnalyticsSummary } from "../services/api";

export function useAnalyticsSummaryQuery(
  districts: number[],
  counties: string[],
  highwayTypes: string[],
) {
  return useQuery({
    queryKey: ["analytics-summary", districts, counties, highwayTypes],
    queryFn: () => getAnalyticsSummary({ districts, counties, highwayTypes }),
  });
}
