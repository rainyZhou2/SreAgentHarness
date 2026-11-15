import { useCallback, useEffect, useRef, useState } from "react";

import { bridge } from "../bridge/bridge";
import { toDesktopError, type DesktopError } from "../bridge/errors";
import { getRunCursor } from "./useRunEvents";

/**
 * Follows one run's durable event stream. Go replaces any previous watch, so
 * switching runs only needs a new WatchRun; UnwatchRun is reserved for
 * leaving the conversation or unmounting the app.
 */
export function useWatchRun(runID: string | undefined, enabled: boolean) {
  const [error, setError] = useState<DesktopError | null>(null);
  const activeRun = useRef<string | undefined>(undefined);

  const start = useCallback(
    async (targetRunID: string) => {
      setError(null);
      try {
        await bridge.watchRun(targetRunID, getRunCursor(targetRunID));
      } catch (cause) {
        setError(toDesktopError(cause));
      }
    },
    [],
  );

  useEffect(() => {
    if (!enabled) {
      return;
    }
    if (!runID) {
      activeRun.current = undefined;
      void bridge.unwatchRun().catch(() => undefined);
      return;
    }
    activeRun.current = runID;
    void start(runID);
  }, [enabled, runID, start]);

  useEffect(
    () => () => {
      void bridge.unwatchRun().catch(() => undefined);
    },
    [],
  );

  const reconnect = useCallback(() => {
    if (runID) {
      void start(runID);
    }
  }, [runID, start]);

  return { error, reconnect };
}
