import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";

import { bridge } from "../bridge/bridge";
import { parseModelCatalog } from "../lib/modelCatalog";
import { qk } from "../lib/queryKeys";

export function useModelCatalog(scope: string, enabled = true) {
  const query = useQuery({
    queryKey: qk.extensions(scope),
    queryFn: () => bridge.extensions(),
    enabled,
    staleTime: 15_000,
  });
  const catalog = useMemo(() => parseModelCatalog(query.data), [query.data]);
  return { ...query, catalog };
}
