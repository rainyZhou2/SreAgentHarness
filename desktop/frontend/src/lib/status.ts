import type { MessageKey } from "../i18n/messages";

export function normalizeStatus(status: string): string {
  return status.trim().toUpperCase().replace(/[\s-]+/g, "_");
}

const TERMINAL_RUN_STATUSES = new Set(["COMPLETED", "FAILED", "CANCELLED"]);

export function isTerminalRunStatus(status: string): boolean {
  return TERMINAL_RUN_STATUSES.has(normalizeStatus(status));
}

/** Statuses in which controls are meaningful: the run is not finished. */
export function isActiveRunStatus(status: string): boolean {
  return !isTerminalRunStatus(status);
}

export function runStatusKey(status: string): MessageKey {
  switch (normalizeStatus(status)) {
    case "PENDING_ACTIVATION":
      return "run.status.PENDING_ACTIVATION";
    case "QUEUED":
      return "run.status.QUEUED";
    case "RUNNING":
      return "run.status.RUNNING";
    case "PAUSED":
      return "run.status.PAUSED";
    case "PAUSE_REQUESTED":
      return "run.status.PAUSE_REQUESTED";
    case "WAITING_APPROVAL":
      return "run.status.WAITING_APPROVAL";
    case "WAITING_CHILDREN":
      return "run.status.WAITING_CHILDREN";
    case "WAITING_INPUT":
      return "run.status.WAITING_INPUT";
    case "NEEDS_HUMAN":
      return "run.status.NEEDS_HUMAN";
    case "CANCELLED":
      return "run.status.CANCELLED";
    case "CANCELLING":
      return "run.status.CANCELLING";
    case "COMPLETED":
      return "run.status.COMPLETED";
    case "FAILED":
      return "run.status.FAILED";
    default:
      return "run.status.UNKNOWN";
  }
}

export type StatusTone = "neutral" | "positive" | "warning" | "danger" | "info";

export function runStatusTone(status: string): StatusTone {
  switch (normalizeStatus(status)) {
    case "COMPLETED":
      return "positive";
    case "FAILED":
      return "danger";
    case "CANCELLED":
      return "neutral";
    case "PAUSED":
    case "PAUSE_REQUESTED":
    case "WAITING_APPROVAL":
    case "WAITING_INPUT":
    case "WAITING_CHILDREN":
    case "NEEDS_HUMAN":
      return "warning";
    case "RUNNING":
    case "QUEUED":
    case "PENDING_ACTIVATION":
    case "CANCELLING":
      return "info";
    default:
      return "neutral";
  }
}

export function evidenceStatusKey(status: string): MessageKey {
  switch (status.trim().toLowerCase()) {
    case "ok":
      return "evidence.status.ok";
    case "partial":
      return "evidence.status.partial";
    case "empty":
      return "evidence.status.empty";
    case "stale":
      return "evidence.status.stale";
    case "unavailable":
      return "evidence.status.unavailable";
    case "unauthorized":
      return "evidence.status.unauthorized";
    case "denied":
      return "evidence.status.denied";
    case "conflict":
      return "evidence.status.conflict";
    default:
      return "evidence.status.unknown";
  }
}

export function evidenceStatusTone(status: string): StatusTone {
  switch (status.trim().toLowerCase()) {
    case "ok":
      return "positive";
    case "partial":
    case "stale":
    case "conflict":
      return "warning";
    case "unavailable":
      return "warning";
    case "unauthorized":
    case "denied":
      return "danger";
    default:
      return "neutral";
  }
}

export function signalKey(signal: string): MessageKey {
  switch (signal.trim().toLowerCase()) {
    case "metrics":
      return "evidence.signal.metrics";
    case "logs":
      return "evidence.signal.logs";
    case "traces":
      return "evidence.signal.traces";
    case "change":
      return "evidence.signal.change";
    case "topology":
      return "evidence.signal.topology";
    case "gpu":
      return "evidence.signal.gpu";
    case "inference":
      return "evidence.signal.inference";
    default:
      return "evidence.signal.unknown";
  }
}

export function hypothesisStatusKey(status: string): MessageKey {
  switch (normalizeStatus(status)) {
    case "EVALUATING":
      return "hypothesis.status.EVALUATING";
    case "SUPPORTED":
      return "hypothesis.status.SUPPORTED";
    case "REFUTED":
      return "hypothesis.status.REFUTED";
    case "UNVERIFIED":
      return "hypothesis.status.UNVERIFIED";
    default:
      return "hypothesis.status.UNKNOWN";
  }
}

export function hypothesisStatusTone(status: string): StatusTone {
  switch (normalizeStatus(status)) {
    case "SUPPORTED":
      return "positive";
    case "REFUTED":
      return "danger";
    case "EVALUATING":
      return "info";
    case "UNVERIFIED":
      return "warning";
    default:
      return "neutral";
  }
}

export function conclusionVerdictKey(verdict: string): MessageKey {
  switch (normalizeStatus(verdict)) {
    case "VERIFIED":
      return "conclusion.verdict.VERIFIED";
    case "REFUTED":
      return "conclusion.verdict.REFUTED";
    case "UNVERIFIED":
      return "conclusion.verdict.UNVERIFIED";
    case "PARTIAL":
      return "conclusion.verdict.PARTIAL";
    case "NONE":
      return "conclusion.verdict.NONE";
    default:
      return "conclusion.verdict.UNKNOWN";
  }
}

export function conclusionVerdictTone(verdict: string): StatusTone {
  switch (normalizeStatus(verdict)) {
    case "VERIFIED":
      return "positive";
    case "REFUTED":
      return "danger";
    case "UNVERIFIED":
    case "PARTIAL":
      return "warning";
    default:
      return "neutral";
  }
}

export function workStatusKey(status: string): MessageKey {
  switch (normalizeStatus(status)) {
    case "NEW":
      return "work.status.NEW";
    case "PLANNING":
      return "works.status.PLANNING";
    case "ACTIVE":
      return "works.status.ACTIVE";
    case "VERIFYING":
      return "works.status.VERIFYING";
    case "RUNNING":
      return "work.status.RUNNING";
    case "WAITING":
      return "work.status.WAITING";
    case "SUSPENDED":
      return "work.status.SUSPENDED";
    case "BLOCKED":
      return "work.status.BLOCKED";
    case "SUCCEEDED":
      return "work.status.SUCCEEDED";
    case "FAILED":
      return "work.status.FAILED";
    case "CANCELLED":
      return "work.status.CANCELLED";
    default:
      return "work.status.UNKNOWN";
  }
}

export function workStatusTone(status: string): StatusTone {
  switch (normalizeStatus(status)) {
    case "SUCCEEDED":
      return "positive";
    case "RUNNING":
    case "ACTIVE":
    case "PLANNING":
    case "VERIFYING":
      return "info";
    case "WAITING":
    case "SUSPENDED":
    case "BLOCKED":
      return "warning";
    case "FAILED":
      return "danger";
    default:
      return "neutral";
  }
}

export function actionStateKey(state: string): MessageKey {
  switch (normalizeStatus(state)) {
    case "PROPOSED":
      return "action.state.PROPOSED";
    case "WAITING_APPROVAL":
      return "action.state.WAITING_APPROVAL";
    case "READY":
      return "action.state.READY";
    case "DISPATCHING":
      return "action.state.DISPATCHING";
    case "EFFECT_UNKNOWN":
      return "action.state.EFFECT_UNKNOWN";
    case "MANUAL_RECOVERY":
      return "action.state.MANUAL_RECOVERY";
    case "APPLIED":
      return "action.state.APPLIED";
    case "REJECTED":
      return "action.state.REJECTED";
    case "EXPIRED":
      return "action.state.EXPIRED";
    default:
      return "action.state.UNKNOWN";
  }
}

export function actionStateTone(state: string): StatusTone {
  switch (normalizeStatus(state)) {
    case "WAITING_APPROVAL":
      return "warning";
    case "READY":
    case "DISPATCHING":
      return "info";
    case "APPLIED":
      return "positive";
    case "REJECTED":
    case "EXPIRED":
      return "neutral";
    case "EFFECT_UNKNOWN":
    case "MANUAL_RECOVERY":
      return "danger";
    default:
      return "neutral";
  }
}

export function isApprovalWaiting(state: string): boolean {
  return normalizeStatus(state) === "WAITING_APPROVAL";
}

export function isEffectUnknown(state: string): boolean {
  return normalizeStatus(state) === "EFFECT_UNKNOWN";
}

export function isManualRecovery(state: string): boolean {
  return normalizeStatus(state) === "MANUAL_RECOVERY";
}
