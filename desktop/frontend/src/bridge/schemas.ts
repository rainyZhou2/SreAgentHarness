import { z } from "zod";

import { DataContractError } from "./errors";
import type {
  AuditEntry,
  ParsedApprovals,
  ParsedTranscript,
  ParsedWorks,
  PendingApproval,
  Revocation,
  RoleBinding,
  SessionSummary,
  Settings,
  TurnView,
  Whoami,
  WorkAction,
  WorkStage,
  WorkSummary,
  WorkViewData,
} from "./types";

const credentialsSchema = z
  .object({
    mode: z.string().optional(),
    tenant: z.string().optional(),
    actor: z.string().optional(),
    roles: z.string().optional(),
    token: z.string().optional(),
  })
  .passthrough();

const settingsSchema = z
  .object({
    server_url: z.string().optional().default(""),
    creds: credentialsSchema.optional().default({}),
    mode: z.string().optional(),
    local_data_dir: z.string().optional(),
    model_provider: z.string().optional(),
    model_base_url: z.string().optional(),
    model_name: z.string().optional(),
    language: z.string().optional(),
    signed_out: z.boolean().optional(),
    // Preferences are embedded in the Go Settings struct, not nested.
    theme: z.string().optional(),
    no_glass: z.boolean().optional(),
    notify_off: z.boolean().optional(),
    poll_seconds: z.number().optional(),
  })
  .passthrough();

const whoamiSchema = z
  .object({
    tenant: z.string().optional(),
    subject: z.string().optional(),
    roles: z.array(z.string()).optional(),
    method: z.string().optional(),
  })
  .passthrough();

const sessionSchema = z
  .object({
    id: z.string().min(1),
    title: z.string().nullable().optional(),
    status: z.string().optional(),
    profile_id: z.string().optional(),
    default_branch_id: z.string().optional(),
    created_at: z.string().optional(),
    last_activity_at: z.string().optional(),
    turns: z.number().optional(),
    active_turns: z.number().optional(),
  })
  .passthrough();

const approvalSchema = z
  .object({
    id: z.string().min(1),
    run_id: z.string().min(1),
    session_id: z.string().optional(),
    session_title: z.string().optional(),
    initiator_id: z.string().optional(),
    action_intent_id: z.string().min(1),
    step_id: z.string().min(1),
    action_hash: z.string().min(1),
    risk_class: z.string().min(1),
    tool_name: z.string().min(1),
    target: z.string().min(1),
    required_approvals: z.number(),
    status: z.string().min(1),
    expires_at: z.string().min(1),
    created_at: z.string().min(1),
  })
  .passthrough();

const auditSchema = z
  .object({
    actor_type: z.string(),
    actor_id: z.string(),
    action: z.string(),
    resource_type: z.string(),
    resource_id: z.string().optional(),
    outcome: z.string(),
    trace_id: z.string().optional(),
    occurred_at: z.string(),
  })
  .passthrough();

const roleBindingSchema = z
  .object({
    principal_id: z.string().min(1),
    role: z.string().min(1),
    created_by: z.string(),
    created_at: z.string(),
  })
  .passthrough();

const revocationSchema = z
  .object({
    id: z.string().min(1),
    scope: z.string(),
    scope_id: z.string(),
    version: z.number(),
    actor_id: z.string(),
    reason: z.string(),
    created_at: z.string(),
  })
  .passthrough();

const workStageSchema = z
  .object({
    id: z.string().min(1),
    type: z.string().optional(),
    phase: z.string().optional(),
    status: z.string().optional(),
  })
  .passthrough();

const workActionSchema = z
  .object({
    id: z.string().min(1),
    name: z.string(),
    target: z.string(),
    state: z.string(),
    risk: z.string(),
    may_have_dispatched: z.boolean().optional(),
    needs_human: z.boolean().optional(),
  })
  .passthrough();

const workSummarySchema = z
  .object({
    id: z.string().min(1),
    title: z.string().optional().default(""),
    kind: z.string().optional(),
    status: z.string().min(1),
    queue: z.string().optional(),
    stage: z.number().optional(),
    generation: z.number().optional(),
    runs_started: z.number().optional(),
    replans: z.number().optional(),
    cost_micros: z.number().optional(),
    reason: z.string().optional(),
    updated_at: z.string().optional(),
    created_at: z.string().optional(),
    version: z.number().optional(),
    goal: z
      .object({
        intent: z.string().optional(),
        description: z.string().optional(),
        subject_refs: z.array(z.string()).optional(),
      })
      .passthrough()
      .optional(),
    template: z
      .object({ stages: z.array(workStageSchema).optional() })
      .passthrough()
      .optional(),
  })
  .passthrough();

const turnSchema = z
  .object({
    id: z.string().min(1),
    seq: z.number().optional(),
    status: z.string().optional(),
    run_id: z.string().optional(),
    text: z.string().optional(),
    created_at: z.string().optional(),
  })
  .passthrough();

export interface ParsedSessions {
  sessions: SessionSummary[];
  skipped: number;
}

function firstIssue(error: z.ZodError): string {
  const issue = error.issues[0];
  if (!issue) {
    return "invalid payload";
  }
  const path = issue.path.join(".");
  return path === "" ? issue.message : `${path}: ${issue.message}`;
}

export function parseSettings(raw: unknown): Settings {
  const result = settingsSchema.safeParse(raw ?? {});
  if (!result.success) {
    throw new DataContractError(`settings: ${firstIssue(result.error)}`);
  }
  return result.data;
}

export function parseWhoami(raw: unknown): Whoami {
  const result = whoamiSchema.safeParse(raw ?? {});
  if (!result.success) {
    throw new DataContractError(`principal: ${firstIssue(result.error)}`);
  }
  return result.data;
}

export function parseSessions(raw: unknown): ParsedSessions {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { sessions?: unknown }).sessions)) {
    throw new DataContractError("session list: sessions array is missing");
  }
  const sessions: SessionSummary[] = [];
  let skipped = 0;
  for (const item of (raw as { sessions: unknown[] }).sessions) {
    const result = sessionSchema.safeParse(item);
    if (result.success) {
      sessions.push(result.data);
    } else {
      skipped += 1;
    }
  }
  return { sessions, skipped };
}

export function parseApprovals(raw: unknown): ParsedApprovals {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { approvals?: unknown }).approvals)) {
    throw new DataContractError("approvals: approvals array is missing");
  }
  const approvals: PendingApproval[] = [];
  let skipped = 0;
  for (const item of (raw as { approvals: unknown[] }).approvals) {
    const result = approvalSchema.safeParse(item);
    if (result.success) {
      approvals.push(result.data);
    } else {
      skipped += 1;
    }
  }
  return { approvals, skipped };
}

export function parseAudit(raw: unknown): { entries: AuditEntry[]; skipped: number } {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { entries?: unknown }).entries)) {
    throw new DataContractError("audit: entries array is missing");
  }
  const entries: AuditEntry[] = [];
  let skipped = 0;
  for (const item of (raw as { entries: unknown[] }).entries) {
    const parsed = auditSchema.safeParse(item);
    if (parsed.success) {
      entries.push(parsed.data);
    } else {
      skipped += 1;
    }
  }
  return { entries, skipped };
}

export function parseRoleBindings(raw: unknown): { bindings: RoleBinding[]; skipped: number } {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { bindings?: unknown }).bindings)) {
    throw new DataContractError("role bindings: bindings array is missing");
  }
  const bindings: RoleBinding[] = [];
  let skipped = 0;
  for (const item of (raw as { bindings: unknown[] }).bindings) {
    const parsed = roleBindingSchema.safeParse(item);
    if (parsed.success) {
      bindings.push(parsed.data);
    } else {
      skipped += 1;
    }
  }
  return { bindings, skipped };
}

export function parseRevocations(raw: unknown): { revocations: Revocation[]; skipped: number } {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { revocations?: unknown }).revocations)) {
    throw new DataContractError("revocations: revocations array is missing");
  }
  const revocations: Revocation[] = [];
  let skipped = 0;
  for (const item of (raw as { revocations: unknown[] }).revocations) {
    const parsed = revocationSchema.safeParse(item);
    if (parsed.success) {
      revocations.push(parsed.data);
    } else {
      skipped += 1;
    }
  }
  return { revocations, skipped };
}

export function parseWorks(raw: unknown): ParsedWorks {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { items?: unknown }).items)) {
    throw new DataContractError("works: items array is missing");
  }
  const items: WorkSummary[] = [];
  let skipped = 0;
  for (const item of (raw as { items: unknown[] }).items) {
    const result = workSummarySchema.safeParse(item);
    if (result.success) {
      items.push(result.data);
    } else {
      skipped += 1;
    }
  }
  return { items, skipped };
}

export function parseWorkView(raw: unknown): WorkViewData {
  if (!raw || typeof raw !== "object") {
    throw new DataContractError("work view: payload is not an object");
  }
  const record = raw as Record<string, unknown>;
  const work = workSummarySchema.safeParse(record.work);
  if (!work.success) {
    throw new DataContractError("work view: work is missing or invalid");
  }
  const view =
    record.view && typeof record.view === "object"
      ? (record.view as Record<string, unknown>)
      : undefined;
  const actionsRaw = Array.isArray(record.actions)
    ? record.actions
    : view && Array.isArray(view.actions)
      ? view.actions
      : [];
  const actions: WorkAction[] = [];
  for (const item of actionsRaw) {
    const parsed = workActionSchema.safeParse(item);
    if (parsed.success) {
      actions.push(parsed.data);
    }
  }
  const stagesRaw = view && Array.isArray(view.stages) ? view.stages : work.data.template?.stages ?? [];
  const stages: WorkStage[] = [];
  for (const item of stagesRaw) {
    const parsed = workStageSchema.safeParse(item);
    if (parsed.success) {
      stages.push(parsed.data);
    }
  }
  const outcome =
    view && view.last_outcome && typeof view.last_outcome === "object"
      ? (view.last_outcome as WorkViewData["outcome"])
      : undefined;
  return {
    work: work.data,
    ...(typeof view?.waiting_on === "string" ? { waiting_on: view.waiting_on } : {}),
    ...(Array.isArray(view?.controls)
      ? { controls: view.controls.filter((item): item is string => typeof item === "string") }
      : {}),
    ...(stages.length > 0 ? { stages } : {}),
    ...(actions.length > 0 ? { actions } : {}),
    ...(outcome ? { outcome } : {}),
    ...(record.errors && typeof record.errors === "object"
      ? { errors: record.errors as Record<string, string> }
      : {}),
  };
}

export function parseTranscript(raw: unknown): ParsedTranscript {
  if (!raw || typeof raw !== "object") {
    throw new DataContractError("transcript: payload is not an object");
  }
  const record = raw as Record<string, unknown>;
  if (!Array.isArray(record.turns)) {
    throw new DataContractError("transcript: turns array is missing");
  }
  const turns: TurnView[] = [];
  let skippedTurns = 0;
  for (const item of record.turns) {
    const result = turnSchema.safeParse(item);
    if (result.success) {
      turns.push(result.data);
    } else {
      skippedTurns += 1;
    }
  }
  const branchID = typeof record.branch_id === "string" ? record.branch_id : undefined;
  return branchID === undefined ? { turns, skippedTurns } : { turns, branchID, skippedTurns };
}

export function parseCreateSession(raw: unknown): { id: string } {
  if (!raw || typeof raw !== "object") {
    throw new DataContractError("create session: payload is not an object");
  }
  const session = (raw as Record<string, unknown>).session;
  if (!session || typeof session !== "object") {
    throw new DataContractError("create session: session object is missing");
  }
  const id = (session as Record<string, unknown>).id;
  if (typeof id !== "string" || id === "") {
    throw new DataContractError("create session: session.id is missing");
  }
  return { id };
}

export function parseSendMessage(raw: unknown): { turnID?: string; runID?: string } {
  if (!raw || typeof raw !== "object") {
    throw new DataContractError("send message: payload is not an object");
  }
  const turn = (raw as Record<string, unknown>).turn;
  if (!turn || typeof turn !== "object") {
    throw new DataContractError("send message: turn object is missing");
  }
  const record = turn as Record<string, unknown>;
  return {
    ...(typeof record.id === "string" ? { turnID: record.id } : {}),
    ...(typeof record.run_id === "string" ? { runID: record.run_id } : {}),
  };
}
