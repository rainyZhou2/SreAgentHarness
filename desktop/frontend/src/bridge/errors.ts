/**
 * Normalized error model shared by bridge failures, server errors and
 * contract violations. UI state uses `kind` to pick the recovery affordance;
 * raw messages are kept for diagnostics but never parsed for business state.
 */

export type DesktopErrorKind =
  | "bridge_unavailable"
  | "unauthorized"
  | "forbidden"
  | "not_found"
  | "conflict"
  | "rejected"
  | "unavailable"
  | "rate_limited"
  | "effect_unknown"
  | "data_contract"
  | "unknown";

export interface DesktopError {
  kind: DesktopErrorKind;
  code: string;
  message: string;
  retriable: boolean;
  status?: number;
  requestId?: string;
}

export class BridgeUnavailableError extends Error {
  readonly code = "bridge_unavailable";

  constructor() {
    super(
      "The desktop bridge is unavailable. Open this UI inside the SRE Harness desktop app.",
    );
    this.name = "BridgeUnavailableError";
  }
}

export class DataContractError extends Error {
  readonly code = "data_contract";

  constructor(message: string) {
    super(message);
    this.name = "DataContractError";
  }
}

const STATUS_PATTERN = /^(\d{3})\b/;
const EFFECT_UNKNOWN_PATTERN = /effect[_ ]unknown|effect unknown|EFFECT_UNKNOWN/i;

function messageOf(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  if (error && typeof error === "object") {
    const record = error as Record<string, unknown>;
    for (const key of ["message", "error", "detail"]) {
      const value = record[key];
      if (typeof value === "string" && value.trim() !== "") {
        return value;
      }
    }
  }
  return "Unknown error";
}

/**
 * Maps everything the bridge can reject with into a stable UI error. Wails
 * surfaces Go errors as message strings such as `409 version_conflict`; the
 * numeric prefix is the only reliable transport status the webview sees.
 */
export function toDesktopError(error: unknown): DesktopError {
  if (error instanceof BridgeUnavailableError) {
    return {
      kind: "bridge_unavailable",
      code: error.code,
      message: error.message,
      retriable: false,
    };
  }
  if (error instanceof DataContractError) {
    return {
      kind: "data_contract",
      code: error.code,
      message: error.message,
      retriable: true,
    };
  }

  const message = messageOf(error).trim();
  const statusMatch = STATUS_PATTERN.exec(message);
  const status = statusMatch?.[1] ? Number(statusMatch[1]) : undefined;
  const lower = message.toLowerCase();

  if (status === 401 || lower.includes("unauthenticated") || lower.includes("unauthorized")) {
    return {
      kind: "unauthorized",
      code: "unauthorized",
      message,
      retriable: false,
      ...(status === undefined ? {} : { status }),
    };
  }
  if (status === 403 || lower.includes("forbidden") || lower.includes("scope denied")) {
    return {
      kind: "forbidden",
      code: "forbidden",
      message,
      retriable: false,
      ...(status === undefined ? {} : { status }),
    };
  }
  if (status === 404 || lower.includes("not found")) {
    return {
      kind: "not_found",
      code: "not_found",
      message,
      retriable: false,
      ...(status === undefined ? {} : { status }),
    };
  }
  if (STATUS_PATTERN.test(message) && EFFECT_UNKNOWN_PATTERN.test(message)) {
    return { kind: "effect_unknown", code: "effect_unknown", message, retriable: false, status };
  }
  if (status === 409 || lower.includes("conflict") || lower.includes("stale state")) {
    return {
      kind: "conflict",
      code: "version_conflict",
      message,
      retriable: false,
      ...(status === undefined ? {} : { status }),
    };
  }
  if (status === 422 || lower.includes("rejected")) {
    return {
      kind: "rejected",
      code: "rejected",
      message,
      retriable: false,
      ...(status === undefined ? {} : { status }),
    };
  }
  if (status === 429 || lower.includes("rate limit") || lower.includes("too many requests")) {
    return {
      kind: "rate_limited",
      code: "rate_limited",
      message,
      retriable: true,
      ...(status === undefined ? {} : { status }),
    };
  }
  if (status !== undefined && status >= 500) {
    return { kind: "unavailable", code: `http_${status}`, message, retriable: true, status };
  }
  if (
    lower.includes("effect unknown") ||
    lower.includes("effect_unknown") ||
    lower.includes("may have dispatched")
  ) {
    return { kind: "effect_unknown", code: "effect_unknown", message, retriable: false };
  }
  if (
    lower.includes("failed to fetch") ||
    lower.includes("network") ||
    lower.includes("timeout") ||
    lower.includes("timed out") ||
    lower.includes("econn") ||
    lower.includes("connection refused") ||
    lower.includes("unavailable")
  ) {
    return { kind: "unavailable", code: "unavailable", message, retriable: true };
  }
  return { kind: "unknown", code: "unknown", message, retriable: true };
}

export function isDesktopError(value: unknown): value is DesktopError {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.kind === "string" &&
    typeof record.message === "string" &&
    typeof record.retriable === "boolean"
  );
}
