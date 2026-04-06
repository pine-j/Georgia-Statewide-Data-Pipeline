import { useQuery } from "@tanstack/react-query";

import { getGeorgiaFilterOptions } from "../services/api";

export function useGeorgiaFiltersQuery() {
  return useQuery({
    queryKey: ["georgia-filters"],
    queryFn: getGeorgiaFilterOptions,
  });
}
