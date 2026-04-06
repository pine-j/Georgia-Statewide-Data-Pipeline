import { useQuery } from "@tanstack/react-query";

import { getStates } from "../services/api";

export function useStatesQuery() {
  return useQuery({
    queryKey: ["states"],
    queryFn: getStates,
  });
}

