import { useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";

import {
  asRunDeltaPayload,
  asRunEventPayload,
  asRunStreamErrorPayload,
  subscribeRuntimeEvent,
} from "../bridge/events";
import { qk } from "../lib/queryKeys";
import { useStreamStore } from "../state/streamStore";

const INVALIDATE_DELAY_MS = 200;
const DELTA_FLUSH_MS = 60;

const TERMINAL_EVENT_TYPES = new Set(["run.completed", "run.failed", "run.cancelled"]);

const runCursors = new Map<string, number>();
const runSessions = new Map<string, string>();

export function recordRunCursor(runID: string, seq: number | undefined): void {
  if (seq === undefined || !Number.isFinite(seq)) {
    return;
  }
  runCursors.set(runID, Math.max(runCursors.get(runID) ?? 0, seq));
}

export function getRunCursor(runID: string): number {
  return runCursors.get(runID) ?? 0;
}

/** Learns run → session ownership from projections so events can refresh the transcript. */
export function indexRunSession(runID: string, sessionID: string | undefined): void {
  if (sessionID !== undefined && sessionID !== "") {
    runSessions.set(runID, sessionID);
  }
}

/**
 * Bridges the Wails runtime events into the query cache and the streaming
 * store. Durable events invalidate the run projection (coalesced); token
 * deltas are batched so the page does not re-render per token.
 */
export function useRunEvents(scope: string | null): void {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!scope) {
      return;
    }
    const invalidationTimers = new Map<string, number>();
    const deltaBuffers = new Map<string, { text: string; seq: number }>();
    let flushTimer: number | null = null;

    const flushDeltas = () => {
      flushTimer = null;
      const streams = useStreamStore.getState();
      for (const [runID, buffer] of deltaBuffers) {
        if (buffer.text !== "") {
          streams.appendDelta(runID, buffer.text, buffer.seq);
        }
      }
      deltaBuffers.clear();
    };

    const scheduleDeltaFlush = () => {
      if (flushTimer !== null) {
        return;
      }
      flushTimer = window.setTimeout(flushDeltas, DELTA_FLUSH_MS);
    };

    const refreshRun = (runID: string) => {
      void queryClient.invalidateQueries({ queryKey: qk.projection(scope, runID) });
      const sessionID = runSessions.get(runID);
      if (sessionID !== undefined) {
        void queryClient.invalidateQueries({ queryKey: qk.transcript(scope, sessionID) });
      }
    };

    const scheduleRefresh = (runID: string, immediate: boolean) => {
      const existing = invalidationTimers.get(runID);
      if (existing !== undefined) {
        window.clearTimeout(existing);
        invalidationTimers.delete(runID);
      }
      if (immediate) {
        refreshRun(runID);
        return;
      }
      const timer = window.setTimeout(() => {
        invalidationTimers.delete(runID);
        refreshRun(runID);
      }, INVALIDATE_DELAY_MS);
      invalidationTimers.set(runID, timer);
    };

    const offRunEvent = subscribeRuntimeEvent("run:event", (payload) => {
      const parsed = asRunEventPayload(payload);
      if (!parsed) {
        return;
      }
      const { run_id: runID, event } = parsed;
      recordRunCursor(runID, event.event_seq);
      const eventType = event.event_type ?? "";
      const terminal = TERMINAL_EVENT_TYPES.has(eventType);
      scheduleRefresh(runID, terminal);
      if (terminal) {
        useStreamStore.getState().clearStream(runID);
      }
    });

    const offDelta = subscribeRuntimeEvent("run:delta", (payload) => {
      const parsed = asRunDeltaPayload(payload);
      if (!parsed) {
        return;
      }
      const { run_id: runID, delta } = parsed;
      const text = delta.event?.text ?? "";
      recordRunCursor(runID, delta.delta_seq);
      if (text === "") {
        return;
      }
      const buffer = deltaBuffers.get(runID) ?? { text: "", seq: 0 };
      buffer.text += text;
      buffer.seq = Math.max(buffer.seq, delta.delta_seq ?? 0);
      deltaBuffers.set(runID, buffer);
      scheduleDeltaFlush();
    });

    const offStreamError = subscribeRuntimeEvent("run:stream-error", (payload) => {
      const parsed = asRunStreamErrorPayload(payload);
      if (!parsed) {
        return;
      }
      useStreamStore.getState().setStreamError(parsed.run_id, parsed.error);
      scheduleRefresh(parsed.run_id, true);
    });

    const offApprovals = subscribeRuntimeEvent("approvals:update", () => {
      void queryClient.invalidateQueries({ queryKey: qk.approvals(scope) });
      void queryClient.invalidateQueries({ queryKey: qk.projections(scope) });
    });

    return () => {
      offRunEvent();
      offDelta();
      offStreamError();
      offApprovals();
      for (const timer of invalidationTimers.values()) {
        window.clearTimeout(timer);
      }
      invalidationTimers.clear();
      if (flushTimer !== null) {
        window.clearTimeout(flushTimer);
        flushTimer = null;
      }
      flushDeltas();
    };
  }, [queryClient, scope]);
}
