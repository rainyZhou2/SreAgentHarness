import type {
  RunDeltaPayload,
  RunEventPayload,
  RunStreamErrorPayload,
  RuntimeEventName,
  RuntimeEvents,
  RunDelta,
  RunEvent,
} from "./types";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function asRunEventPayload(data: unknown): RunEventPayload | null {
  if (!isRecord(data)) {
    return null;
  }
  const runID = data.run_id;
  const event = data.event;
  if (typeof runID !== "string" || runID === "" || !isRecord(event)) {
    return null;
  }
  return { run_id: runID, event: event as RunEvent };
}

export function asRunDeltaPayload(data: unknown): RunDeltaPayload | null {
  if (!isRecord(data)) {
    return null;
  }
  const runID = data.run_id;
  const delta = data.delta;
  if (typeof runID !== "string" || runID === "" || !isRecord(delta)) {
    return null;
  }
  return { run_id: runID, delta: delta as RunDelta };
}

export function asRunStreamErrorPayload(data: unknown): RunStreamErrorPayload | null {
  if (!isRecord(data)) {
    return null;
  }
  const runID = data.run_id;
  if (typeof runID !== "string" || runID === "") {
    return null;
  }
  return {
    run_id: runID,
    error: typeof data.error === "string" ? data.error : "event stream stopped",
  };
}

/**
 * Subscribes to one runtime event. Returns a no-op unsubscribe when the
 * runtime is absent (browser preview) or the event bus is unavailable.
 */
export function subscribeRuntimeEvent<K extends RuntimeEventName>(
  name: K,
  handler: (payload: RuntimeEvents[K]) => void,
): () => void {
  const runtime = window.runtime;
  if (!runtime || typeof runtime.EventsOn !== "function") {
    return () => undefined;
  }
  const off = runtime.EventsOn(name, (data: unknown) => {
    handler(data as RuntimeEvents[K]);
  });
  if (typeof off === "function") {
    return off;
  }
  return () => {
    runtime.EventsOff?.(name);
  };
}
