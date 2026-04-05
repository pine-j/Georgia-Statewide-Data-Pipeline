import { useQuery } from "@tanstack/react-query";

import { getAnalyticsSummary } from "../services/api";

export function useAnalyticsSummaryQuery(
  district: number | null,
  counties: string[],
) {
  return useQuery({
    queryKey: ["analytics-summary", district, counties],
    queryFn: () => getAnalyticsSummary({ district, counties }),
  });
}
