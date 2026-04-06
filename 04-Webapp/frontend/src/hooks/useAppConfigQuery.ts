import { useQuery } from "@tanstack/react-query";

import { getAppConfig } from "../services/api";

export function useAppConfigQuery() {
  return useQuery({
    queryKey: ["app-config"],
    queryFn: getAppConfig,
  });
}

