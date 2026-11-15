import type { StatusTone } from "../../lib/status";
import "./StatusBadge.css";

export function StatusBadge({
  tone,
  label,
  className,
}: {
  tone: StatusTone;
  label: string;
  className?: string;
}) {
  return (
    <span className={className ? `status-badge ${className}` : "status-badge"} data-tone={tone}>
      <span className="status-badge__dot" aria-hidden="true" />
      <span>{label}</span>
    </span>
  );
}
