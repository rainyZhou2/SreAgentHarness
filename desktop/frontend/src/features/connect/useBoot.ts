import { useQuery } from "@tanstack/react-query";

import { bridge } from "../../bridge/bridge";
import type { Settings, Whoami } from "../../bridge/types";
import { qk } from "../../lib/queryKeys";

export interface BootData {
  settings: Settings;
  connected: boolean;
  whoami: Whoami | null;
}

async function loadBoot(): Promise<BootData> {
  const settings = await bridge.getSettings();
  if (await bridge.connected()) {
    return { settings, connected: true, whoami: await bridge.whoami().catch(() => null) };
  }
  // Local mode owns the bundled runtime: start it before showing the shell.
  if ((settings.mode ?? "local") !== "remote") {
    await bridge.connectLocal();
    const whoami = await bridge.whoami();
    return { settings: await bridge.getSettings(), connected: true, whoami };
  }
  return { settings, connected: false, whoami: null };
}

export function useBootQuery() {
  return useQuery({
    queryKey: qk.boot(),
    queryFn: loadBoot,
    retry: false,
    staleTime: Number.POSITIVE_INFINITY,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
}
