/**
 * Wire types of the Wails bridge. These mirror the Go desktop services and the
 * `/api` DTOs they return. They are intentionally separate from `wailsjs`
 * generated bindings: the frontend validates everything it receives, and the
 * generated bindings are build output that must not be edited.
 */

export interface Credentials {
  mode?: string;
  tenant?: string;
  actor?: string;
  roles?: string;
  token?: string;
}

export interface OIDCSettings {
  issuer: string;
  client_id: string;
  scopes?: string[];
  subject?: string;
}

export interface NetworkSettings {
  proxy_url?: string;
  ca_cert_path?: string;
}

export interface Settings {
  server_url: string;
  creds: Credentials;
  mode?: string;
  local_data_dir?: string;
  oidc?: OIDCSettings;
  network?: NetworkSettings;
  model_provider?: string;
  model_base_url?: string;
  model_name?: string;
  language?: string;
  signed_out?: boolean;
  theme?: string;
  no_glass?: boolean;
  notify_off?: boolean;
  poll_seconds?: number;
}

export interface Whoami {
  tenant?: string;
  subject?: string;
  roles?: string[];
  method?: string;
}

export interface SessionSummary {
  id: string;
  title?: string | null;
  status?: string;
  profile_id?: string;
  default_branch_id?: string;
  created_at?: string;
  last_activity_at?: string;
  turns?: number;
  active_turns?: number;
  latest_run_id?: string;
}

export interface PendingApproval {
  id: string;
  run_id: string;
  session_id?: string;
  session_title?: string;
  initiator_id?: string;
  action_intent_id: string;
  step_id: string;
  action_hash: string;
  risk_class: string;
  tool_name: string;
  target: string;
  required_approvals: number;
  status: string;
  expires_at: string;
  created_at: string;
}

export interface ParsedApprovals {
  approvals: PendingApproval[];
  skipped: number;
}

export interface AuditEntry {
  actor_type: string;
  actor_id: string;
  action: string;
  resource_type: string;
  resource_id?: string;
  outcome: string;
  trace_id?: string;
  occurred_at: string;
}

export interface RoleBinding {
  principal_id: string;
  role: string;
  created_by: string;
  created_at: string;
}

export interface Revocation {
  id: string;
  scope: string;
  scope_id: string;
  version: number;
  actor_id: string;
  reason: string;
  created_at: string;
}

export interface WorkStage {
  id: string;
  type?: string;
  phase?: string;
  status?: string;
}

export interface WorkAction {
  id: string;
  name: string;
  target: string;
  state: string;
  risk: string;
  may_have_dispatched?: boolean;
  needs_human?: boolean;
}

export interface WorkSummary {
  id: string;
  title: string;
  kind?: string;
  status: string;
  queue?: string;
  stage?: number;
  generation?: number;
  runs_started?: number;
  replans?: number;
  cost_micros?: number;
  reason?: string;
  updated_at?: string;
  created_at?: string;
  version?: number;
  goal?: {
    intent?: string;
    description?: string;
    subject_refs?: string[];
  };
  template?: { stages?: WorkStage[] };
}

export interface WorkViewData {
  work: WorkSummary;
  waiting_on?: string;
  controls?: string[];
  stages?: WorkStage[];
  actions?: WorkAction[];
  outcome?: { verdict?: string; reasons?: string[] };
  errors?: Record<string, string>;
}

export interface ParsedWorks {
  items: WorkSummary[];
  skipped: number;
}

export interface TurnView {
  id: string;
  seq?: number;
  status?: string;
  run_id?: string;
  text?: string;
  created_at?: string;
}

export interface ParsedTranscript {
  turns: TurnView[];
  branchID?: string;
  skippedTurns: number;
}

export interface RunThought {
  id?: string;
  seq: number;
  kind: string;
  summary: string;
  status?: string;
  confidence?: number;
  structured?: boolean;
  attrs?: Record<string, unknown>;
  refs?: Array<{ ref: string; relation: string }>;
}

export interface RunStep {
  id: string;
  run_id?: string;
  seq: number;
  kind: string;
  executor_name: string;
  status: string;
  created_at?: string;
  updated_at?: string;
  input?: unknown;
  output?: unknown;
  thoughts?: RunThought[];
  capabilities?: Array<{ ref: string; role: string }>;
}

export interface TurnContext {
  environment: string;
  services: string[];
  window_start?: string;
  window_end?: string;
}

export interface CommandRequest {
  type: string;
  expected_version: number;
  reason: string;
  payload: Record<string, unknown>;
}

export interface CommandResult {
  status: "APPLIED" | "PENDING" | "REJECTED" | "UNKNOWN";
  raw: Record<string, unknown>;
}

export interface AttachmentItem {
  type: string;
  title?: string;
  caption?: string;
  url?: string;
  ref?: string;
  mime_type?: string;
  size_bytes?: number;
  columns?: string[];
  rows?: string[][];
}

export interface Cost {
  model_requests?: number;
  input_tokens?: number;
  output_tokens?: number;
  cost_usd?: number;
}

export interface TimeWindow {
  start: string;
  end: string;
}

export interface EvidenceQuality {
  partial?: boolean;
  stale?: boolean;
  conflict?: boolean;
}

export interface StatusBlockData {
  status: string;
  started_at?: string;
  finished_at?: string | null;
  cost?: Cost;
}

export interface ReasoningBlockData {
  summary: string;
  truncated?: boolean;
}

export interface MarkdownBlockData {
  markdown: string;
}

export interface ToolGroupBlockData {
  step_ids: string[];
  step_count?: number;
  failed_count?: number;
}

export interface EvidenceBlockData {
  evidence_ref?: string;
  data_ref?: string;
  signal: string;
  source: string;
  status: string;
  window?: TimeWindow;
  quality?: EvidenceQuality;
  summary?: string;
}

export interface HypothesisItem {
  id: string;
  statement: string;
  status: string;
  supports?: string[];
  refutes?: string[];
}

export interface HypothesisBlockData {
  work_id?: string;
  items: HypothesisItem[];
}

export interface ConclusionBlockData {
  verdict: string;
  answer: string;
  evidence_refs?: string[];
  unverified?: string[];
}

export interface WorkBlockData {
  work_id: string;
  status: string;
  stage?: string;
  waiting_on?: string;
}

export interface ActionBlockData {
  action_id: string;
  approval_id?: string;
  state: string;
  risk: string;
  target: string;
  intent_hash: string;
  required_approvals: number;
  expires_at?: string | null;
  may_have_dispatched?: boolean;
  controls?: string[];
  /** Optional newer-server alias for the value approvals bind to. */
  action_hash?: string;
}

export interface AttachmentsBlockData {
  items: AttachmentItem[];
}

export interface UnknownBlockData {
  raw_kind?: string;
}

export interface ParsedProjection {
  schemaVersion: number;
  projectionVersion: number;
  minimumClientVersion?: string;
  capabilities: string[];
  runId: string;
  /** Server run version, used as `expected_version` for commands. */
  runVersion: number;
  sessionId?: string;
  turnId?: string;
  status: string;
  workIds: string[];
  blocks: ParsedBlock[];
}

interface BlockBase {
  id: string;
  ordinal: number;
}

export type ParsedBlock =
  | (BlockBase & { kind: "status"; data: StatusBlockData })
  | (BlockBase & { kind: "reasoning"; data: ReasoningBlockData })
  | (BlockBase & { kind: "markdown"; data: MarkdownBlockData })
  | (BlockBase & { kind: "tool-group"; data: ToolGroupBlockData })
  | (BlockBase & { kind: "evidence"; data: EvidenceBlockData })
  | (BlockBase & { kind: "hypothesis"; data: HypothesisBlockData })
  | (BlockBase & { kind: "conclusion"; data: ConclusionBlockData })
  | (BlockBase & { kind: "work"; data: WorkBlockData })
  | (BlockBase & { kind: "action"; data: ActionBlockData })
  | (BlockBase & { kind: "attachments"; data: AttachmentsBlockData })
  | (BlockBase & { kind: "unknown"; rawKind: string; raw: Record<string, unknown> })
  | (BlockBase & { kind: "invalid"; rawKind: string; reason: string; raw: Record<string, unknown> });

export type RunEvent = {
  event_seq?: number;
  event_id?: string;
  event_type?: string;
  step_id?: string | null;
  payload?: unknown;
  occurred_at?: string;
};

export type RunDelta = {
  step_id?: string;
  delta_seq?: number;
  event?: {
    type?: string;
    text?: string;
    tool_name?: string;
    args_delta?: string;
  };
};

export interface RunEventPayload {
  run_id: string;
  event: RunEvent;
}

export interface RunDeltaPayload {
  run_id: string;
  delta: RunDelta;
}

export interface RunStreamErrorPayload {
  run_id: string;
  error: string;
}

export interface RuntimeEvents {
  "run:event": RunEventPayload;
  "run:delta": RunDeltaPayload;
  "run:stream-error": RunStreamErrorPayload;
  "approvals:update": unknown;
  "approvals:open": unknown;
}

export type RuntimeEventName = keyof RuntimeEvents;

export interface WailsRuntime {
  EventsOn?(name: string, handler: (data: unknown) => void): (() => void) | void;
  EventsOff?(name: string, ...additional: string[]): void;
}

/** The exported Go methods the desktop shell provides on `window.go.main.App`. */
export interface AppBridge {
  GetSettings(): Promise<unknown>;
  Extensions(): Promise<unknown>;
  Tools(): Promise<unknown>;
  Connected(): Promise<boolean>;
  Ping(): Promise<void>;
  Connect(settings: Settings): Promise<unknown>;
  ConnectOIDC(serverURL: string, issuer: string, clientID: string): Promise<unknown>;
  Lock(): Promise<void>;
  Unlock(): Promise<unknown>;
  Locked(): Promise<boolean>;
  Disconnect(): Promise<void>;
  SetNetwork(network: NetworkSettings): Promise<void>;
  NetworkDiagnostics(): Promise<unknown>;
  ConnectLocal(): Promise<unknown>;
  Whoami(): Promise<unknown>;
  ListSessions(limit: number): Promise<unknown>;
  SearchSessions(status: string, q: string, limit: number): Promise<unknown>;
  CreateSession(title: string, profileID: string): Promise<unknown>;
  RenameSession(id: string, title: string): Promise<unknown>;
  ArchiveSession(id: string, archived: boolean): Promise<unknown>;
  DeleteSession(id: string): Promise<void>;
  ExportChat(id: string): Promise<string>;
  PendingApprovals(): Promise<unknown>;
  ApproveFromInbox(
    runID: string,
    approvalID: string,
    actionHash: string,
    approve: boolean,
    reason: string,
  ): Promise<unknown>;
  ListAudit(limit: number): Promise<unknown>;
  ListRoleBindings(): Promise<unknown>;
  ListRevocations(limit: number): Promise<unknown>;
  PutExtension(
    kind: string,
    name: string,
    spec: Record<string, unknown>,
    enabled: boolean,
    expectedVersion: number,
  ): Promise<unknown>;
  DeleteExtension(kind: string, name: string): Promise<void>;
  TestModel(name: string, spec: Record<string, unknown> | null): Promise<unknown>;
  ListModels(name: string, spec: Record<string, unknown> | null): Promise<unknown>;
  RunSteps(runID: string, expandIO: boolean): Promise<unknown>;
  ListWorks(status: string, kind: string, limit: number): Promise<unknown>;
  WorkView(id: string): Promise<unknown>;
  WorkCommand(
    id: string,
    type: string,
    payload: Record<string, unknown>,
    expectedVersion: number,
    reason: string,
  ): Promise<unknown>;
  Transcript(sessionID: string): Promise<unknown>;
  SendMessage(sessionID: string, text: string): Promise<unknown>;
  SendTurn(sessionID: string, text: string, context: TurnContext): Promise<unknown>;
  GetRunProjection(runID: string): Promise<unknown>;
  WatchRun(runID: string, afterSeq: number): Promise<void>;
  UnwatchRun(): Promise<void>;
  Command(runID: string, req: CommandRequest): Promise<unknown>;
  OpenURL(raw: string): Promise<void>;
  AttachmentImage(runID: string, index: number): Promise<string>;
  SaveAttachment(runID: string, index: number, suggested: string): Promise<string>;
  SetLanguage?(lang: "en" | "zh"): Promise<void>;
}

export interface GoBindings {
  main?: {
    App?: AppBridge;
  };
}

declare global {
  interface Window {
    go?: GoBindings;
    runtime?: WailsRuntime;
  }
}
