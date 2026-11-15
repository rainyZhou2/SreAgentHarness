import { useQuery } from "@tanstack/react-query";

import { bridge } from "../bridge/bridge";
import { qk } from "../lib/queryKeys";

export function useTranscriptQuery(scope: string, sessionID: string | null) {
  return useQuery({
    queryKey: qk.transcript(scope, sessionID ?? "none"),
    queryFn: () => {
      if (!sessionID) {
        throw new Error("a session id is required");
      }
      return bridge.transcript(sessionID);
    },
    enabled: sessionID !== null,
    staleTime: 5_000,
  });
}
