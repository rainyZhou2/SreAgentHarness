import type { AppBridge } from "../bridge/types";
import { projectionEnvelope, testRunID, testSettings, testWhoami } from "./fixtures";

/**
 * In-memory implementation of the Wails bridge interface for Vitest. It is
 * only imported by tests and never reaches the production bundle.
 */
export function createMockBridge(overrides: Partial<AppBridge> = {}): AppBridge {
  const base: AppBridge = {
    GetSettings: async () => testSettings,
    Extensions: async () => ({ skills: [], tools: [] }),
    Tools: async () => ({ tools: [] }),
    Connected: async () => true,
    Ping: async () => undefined,
    Connect: async () => testWhoami,
    ConnectOIDC: async () => testWhoami,
    Lock: async () => undefined,
    Unlock: async () => testWhoami,
    Locked: async () => false,
    Disconnect: async () => undefined,
    SetNetwork: async () => undefined,
    NetworkDiagnostics: async () => ({}),
    ConnectLocal: async () => ({ status: "running" }),
    Whoami: async () => testWhoami,
    ListSessions: async () => ({ sessions: [] }),
    SearchSessions: async () => ({ sessions: [] }),
    RenameSession: async () => ({}),
    ArchiveSession: async () => ({}),
    DeleteSession: async () => undefined,
    ExportChat: async () => "",
    PendingApprovals: async () => ({ approvals: [] }),
    ApproveFromInbox: async () => ({ status: "APPLIED" }),
    PutExtension: async () => ({}),
    DeleteExtension: async () => undefined,
    TestModel: async () => ({ model: "model-test", reply: "ok", latency_ms: 1 }),
    ListModels: async () => ({ models: [] }),
    RunSteps: async () => ({ steps: [] }),
    ListAudit: async () => ({ entries: [] }),
    ListRoleBindings: async () => ({ bindings: [] }),
    ListRevocations: async () => ({ revocations: [] }),
    ListWorks: async () => ({ items: [] }),
    WorkView: async () => ({ work: { id: "work-test-1", title: "Work", status: "ACTIVE" } }),
    WorkCommand: async () => ({ status: "APPLIED" }),
    CreateSession: async () => ({ session: { id: "session-test-1" } }),
    Transcript: async () => ({ branch_id: "branch-1", turns: [] }),
    SendMessage: async () => ({ turn: { id: "turn-test-1", run_id: testRunID } }),
    SendTurn: async () => ({ turn: { id: "turn-test-1", run_id: testRunID } }),
    GetRunProjection: async () => projectionEnvelope([]),
    WatchRun: async () => undefined,
    UnwatchRun: async () => undefined,
    Command: async () => ({ status: "APPLIED" }),
    OpenURL: async () => undefined,
    AttachmentImage: async () => "data:image/png;base64,iVBORw0KGgo=",
    SaveAttachment: async () => "",
    SetLanguage: async () => undefined,
  };
  return { ...base, ...overrides };
}

export function installMockBridge(overrides: Partial<AppBridge> = {}): AppBridge {
  const mock = createMockBridge(overrides);
  window.go = { main: { App: mock } };
  return mock;
}
