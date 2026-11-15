import { useQuery } from "@tanstack/react-query";

import { bridge } from "../bridge/bridge";
import { qk } from "../lib/queryKeys";

export type WorksFilter = "active" | "closed" | "all";

const ACTIVE_STATUSES = "NEW,PLANNING,ACTIVE,WAITING,VERIFYING,BLOCKED,SUSPENDED";
const CLOSED_STATUSES = "SUCCEEDED,FAILED,CANCELLED";

function statuses(filter: WorksFilter): string {
  if (filter === "active") {
    return ACTIVE_STATUSES;
  }
  if (filter === "closed") {
    return CLOSED_STATUSES;
  }
  return "";
}

export function useWorksQuery(scope: string, enabled: boolean, filter: WorksFilter) {
  return useQuery({
    queryKey: [...qk.works(scope), filter],
    queryFn: () => bridge.listWorks(statuses(filter), ""),
    enabled,
    staleTime: 5_000,
    refetchInterval: 10_000,
  });
}

export function useWorkViewQuery(scope: string, workID: string | null) {
  return useQuery({
    queryKey: qk.work(scope, workID ?? ""),
    queryFn: () => bridge.workView(workID ?? ""),
    enabled: workID !== null && workID !== "",
    staleTime: 5_000,
  });
}
