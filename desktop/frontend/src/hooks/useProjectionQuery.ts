import { useQuery } from "@tanstack/react-query";

import { bridge } from "../bridge/bridge";
import { isTerminalRunStatus } from "../lib/status";
import { qk } from "../lib/queryKeys";

/**
 * Loads the authoritative run projection. While the run is active it also
 * polls as a safety net in case the event stream is interrupted; events
 * additionally invalidate this query immediately.
 */
export function useProjectionQuery(scope: string, runID: string, enabled = true) {
  return useQuery({
    queryKey: qk.projection(scope, runID),
    queryFn: () => bridge.getRunProjection(runID),
    enabled: enabled && runID !== "",
    staleTime: 3_000,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data && isTerminalRunStatus(data.status)) {
        return false;
      }
      return 3_000;
    },
  });
}
