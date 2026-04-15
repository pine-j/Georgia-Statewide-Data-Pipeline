import { useQuery } from "@tanstack/react-query";

import { getRoadwayVisualizationCatalog } from "../services/api";

export function useRoadwayVisualizationCatalogQuery() {
  return useQuery({
    queryKey: ["roadway-visualizations"],
    queryFn: getRoadwayVisualizationCatalog,
  });
}
