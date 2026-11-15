import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { bridge } from "../bridge/bridge";
import {
  ConnectionContext,
  connectionScope,
  type ConnectionContextValue,
} from "../features/connect/ConnectionContext";
import type { BootData } from "../features/connect/useBoot";
import { useConnectionHealth } from "../hooks/useConnectionHealth";
import { useIdleLock } from "../hooks/useIdleLock";
import { useRunEvents } from "../hooks/useRunEvents";
import { useStreamStore } from "../state/streamStore";
import { LockScreen } from "../features/lock/LockScreen";
import { AppShell } from "./AppShell";

/**
 * Mounts the shell for a connected identity. The query cache and event
 * subscriptions are scoped to the connection namespace; changing account,
 * tenant or server clears both.
 */
export function ConnectedApp({
  boot,
  refreshBoot,
}: {
  boot: BootData;
  refreshBoot: () => Promise<void>;
}) {
  const queryClient = useQueryClient();
  const scope = useMemo(
    () => connectionScope(boot.settings, boot.whoami),
    [boot.settings, boot.whoami],
  );
  const health = useConnectionHealth(scope, true);
  useRunEvents(scope);
  const [locked, setLocked] = useState(false);

  const lock = useCallback(() => {
    void bridge.unwatchRun().catch(() => undefined);
    void bridge
      .lock()
      .then(() => setLocked(true))
      .catch(() => undefined);
  }, []);

  useIdleLock(boot.settings.oidc !== undefined, 15 * 60 * 1000, lock);

  const previousScope = useRef<string | null>(null);
  useEffect(() => {
    const previous = previousScope.current;
    previousScope.current = scope;
    if (previous !== null && previous !== scope) {
      queryClient.clear();
      useStreamStore.getState().clearAll();
      void bridge.unwatchRun().catch(() => undefined);
    }
  }, [queryClient, scope]);

  const refreshHealth = health.refresh;
  const mode = boot.settings.mode === "remote" ? "remote" : "local";
  const reconnect = useCallback(async () => {
    await refreshBoot();
    await refreshHealth();
  }, [refreshBoot, refreshHealth]);

  const value = useMemo<ConnectionContextValue>(
    () => ({
      settings: boot.settings,
      whoami: boot.whoami,
      scope,
      mode,
      offline: health.offline,
      lastSyncedAt: health.lastSyncedAt > 0 ? health.lastSyncedAt : undefined,
      reconnect,
      refreshHealth,
    }),
    [
      boot.settings,
      boot.whoami,
      scope,
      mode,
      health.offline,
      health.lastSyncedAt,
      reconnect,
      refreshHealth,
    ],
  );

  if (locked) {
    return <LockScreen />;
  }

  return (
    <ConnectionContext.Provider value={value}>
      <AppShell />
    </ConnectionContext.Provider>
  );
}
