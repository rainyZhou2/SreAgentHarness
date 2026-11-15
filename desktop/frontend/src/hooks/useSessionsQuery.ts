import { useQuery } from "@tanstack/react-query";

import { bridge } from "../bridge/bridge";
import { qk } from "../lib/queryKeys";

export interface SessionsFilter {
  status?: "active" | "archived";
  q?: string;
}

export function useSessionsQuery(
  scope: string,
  enabled: boolean,
  filter: SessionsFilter = {},
) {
  const status = filter.status ?? "active";
  const query = filter.q?.trim() ?? "";
  return useQuery({
    queryKey: [...qk.sessions(scope), status, query],
    queryFn: () => bridge.searchSessions(status, query),
    enabled,
    staleTime: 10_000,
  });
}
