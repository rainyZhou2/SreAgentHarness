import { useQuery } from "@tanstack/react-query";

import { bridge } from "../bridge/bridge";
import { qk } from "../lib/queryKeys";

export function useApprovalsQuery(scope: string, enabled: boolean) {
  return useQuery({
    queryKey: qk.approvals(scope),
    queryFn: () => bridge.pendingApprovals(),
    enabled,
    staleTime: 3_000,
    refetchInterval: 8_000,
    refetchOnWindowFocus: true,
  });
}
