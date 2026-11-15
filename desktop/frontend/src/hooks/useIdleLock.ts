import { useEffect } from "react";

const ACTIVITY_EVENTS = ["mousemove", "mousedown", "keydown", "touchstart", "scroll"] as const;

export function useIdleLock(
  enabled: boolean,
  timeoutMs: number,
  onIdle: () => void,
): void {
  useEffect(() => {
    if (!enabled) {
      return;
    }
    let timer: number | null = null;
    const reset = () => {
      if (timer !== null) {
        window.clearTimeout(timer);
      }
      timer = window.setTimeout(onIdle, timeoutMs);
    };
    for (const event of ACTIVITY_EVENTS) {
      window.addEventListener(event, reset, { passive: true });
    }
    reset();
    return () => {
      if (timer !== null) {
        window.clearTimeout(timer);
      }
      for (const event of ACTIVITY_EVENTS) {
        window.removeEventListener(event, reset);
      }
    };
  }, [enabled, timeoutMs, onIdle]);
}
