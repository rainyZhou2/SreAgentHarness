import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { bridge, hasAppBridge } from "../bridge/bridge";
import { qk } from "../lib/queryKeys";

/**
 * Polls the bridge's connection flag so an interrupted server shows as
 * offline instead of silently failing writes. The browser `online`/`offline`
 * events are folded in as an immediate hint.
 */
export function useConnectionHealth(scope: string, enabled: boolean) {
  const [browserOnline, setBrowserOnline] = useState(() =>
    typeof navigator === "undefined" ? true : navigator.onLine,
  );

  useEffect(() => {
    const goOnline = () => setBrowserOnline(true);
    const goOffline = () => setBrowserOnline(false);
    window.addEventListener("online", goOnline);
    window.addEventListener("offline", goOffline);
    return () => {
      window.removeEventListener("online", goOnline);
      window.removeEventListener("offline", goOffline);
    };
  }, []);

  const query = useQuery({
    queryKey: qk.health(scope),
    queryFn: async () => {
      await bridge.ping();
      return true;
    },
    enabled: enabled && hasAppBridge(),
    refetchInterval: 15_000,
    refetchOnWindowFocus: true,
    staleTime: 5_000,
    retry: false,
  });

  const offline = !browserOnline || query.isError || query.data === false;

  return {
    offline,
    lastSyncedAt: query.dataUpdatedAt,
    refresh: query.refetch,
  };
}
