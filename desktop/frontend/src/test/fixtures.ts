import type { ActionBlockData, Settings, Whoami } from "../bridge/types";

export const testSettings: Settings = {
  server_url: "http://127.0.0.1:8080",
  creds: { mode: "bearer", tenant: "tenant-test", actor: "alice", roles: "operator,approver" },
  mode: "remote",
  language: "en",
};

export const testWhoami: Whoami = {
  tenant: "tenant-test",
  subject: "alice",
  roles: ["operator", "approver"],
  method: "token",
};

export const testRunID = "run-test-1";

export function projectionEnvelope(
  blocks: unknown[],
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    projection_version: 1,
    run_id: testRunID,
    run_version: 4,
    session_id: "session-test-1",
    turn_id: "turn-test-1",
    status: "RUNNING",
    work_ids: [],
    blocks,
    ...overrides,
  };
}

export function makeActionBlock(overrides: Partial<ActionBlockData> = {}): Record<string, unknown> {
  return {
    id: "run-test-1:action:approval-1",
    kind: "action",
    ordinal: 0,
    action_id: "intent-1",
    approval_id: "approval-1",
    state: "WAITING_APPROVAL",
    risk: "R2",
    target: "deployment://prod/checkout",
    intent_hash: "sha256:abc123",
    required_approvals: 2,
    expires_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
    may_have_dispatched: false,
    controls: ["approve", "reject"],
    ...overrides,
  };
}

export const testAction: ActionBlockData = {
  action_id: "intent-1",
  approval_id: "approval-1",
  state: "WAITING_APPROVAL",
  risk: "R2",
  target: "deployment://prod/checkout",
  intent_hash: "sha256:abc123",
  required_approvals: 2,
  expires_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
  may_have_dispatched: false,
  controls: ["approve", "reject"],
};
