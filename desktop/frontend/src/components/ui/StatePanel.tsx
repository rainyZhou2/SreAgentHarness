import type { ReactNode } from "react";
import "./StatePanel.css";

export type StateTone = "loading" | "empty" | "offline" | "warning" | "info";

export function StatePanel({
  tone,
  title,
  body,
  action,
  compact = false,
}: {
  tone: StateTone;
  title: string;
  body?: string;
  action?: ReactNode;
  compact?: boolean;
}) {
  const live = tone === "loading" ? "polite" : undefined;
  return (
    <div
      className={compact ? "state-panel state-panel--compact" : "state-panel"}
      data-tone={tone}
      role={tone === "warning" ? "status" : undefined}
      aria-live={live}
    >
      {tone === "loading" ? <span className="spinner" aria-hidden="true" /> : null}
      <div className="state-panel__text">
        <strong>{title}</strong>
        {body ? <p>{body}</p> : null}
      </div>
      {action ? <div className="state-panel__action">{action}</div> : null}
    </div>
  );
}

export function SkeletonLines({ rows = 3, compact = false }: { rows?: number; compact?: boolean }) {
  return (
    <div className={compact ? "skeleton-lines skeleton-lines--compact" : "skeleton-lines"} aria-hidden="true">
      {Array.from({ length: rows }, (_, index) => (
        <span key={index} className="skeleton-line" data-short={index % 3 === 2 || undefined} />
      ))}
    </div>
  );
}
