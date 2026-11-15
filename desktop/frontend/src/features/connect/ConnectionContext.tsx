import { createContext, useContext } from "react";

import type { Settings, Whoami } from "../../bridge/types";
import type { useConnectionHealth } from "../../hooks/useConnectionHealth";

export interface ConnectionContextValue {
  settings: Settings;
  whoami: Whoami | null;
  /** Stable cache namespace: mode/server/tenant/subject, never the token. */
  scope: string;
  mode: "local" | "remote";
  offline: boolean;
  lastSyncedAt: number | undefined;
  reconnect: () => Promise<void>;
  refreshHealth: ReturnType<typeof useConnectionHealth>["refresh"];
}

export const ConnectionContext = createContext<ConnectionContextValue | null>(null);

export function useConnection(): ConnectionContextValue {
  const value = useContext(ConnectionContext);
  if (!value) {
    throw new Error("useConnection must be used inside the connected app");
  }
  return value;
}

export function connectionScope(settings: Settings, whoami: Whoami | null): string {
  const mode = settings.mode ?? "local";
  const server = settings.server_url ?? "";
  const tenant = whoami?.tenant ?? settings.creds.tenant ?? "";
  const subject = whoami?.subject ?? "";
  return [mode, server, tenant, subject].join("|");
}
