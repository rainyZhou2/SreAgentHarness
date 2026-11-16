import {
  BridgeUnavailableError,
  DataContractError,
  toDesktopError,
} from "./errors";
import { parseProjection } from "./projection";
import {
  parseApprovals,
  parseAudit,
  parseCreateSession,
  parseSendMessage,
  parseSessions,
  parseSettings,
  parseRoleBindings,
  parseRevocations,
  parseTranscript,
  parseWhoami,
  parseWorks,
  parseWorkView,
  type ParsedSessions,
} from "./schemas";
import type {
  AppBridge,
  CommandRequest,
  CommandResult,
  ParsedApprovals,
  ParsedProjection,
  ParsedTranscript,
  ParsedWorks,
  NetworkSettings,
  PickedFile,
  Settings,
  TurnContext,
  Whoami,
  WorkViewData,
} from "./types";

/** Returns the Go bindings, or throws a stable error when opened in a browser. */
export function getAppBridge(): AppBridge {
  const app = window.go?.main?.App;
  if (!app) {
    throw new BridgeUnavailableError();
  }
  return app;
}

export function hasAppBridge(): boolean {
  return window.go?.main?.App !== undefined;
}

export type ParsedSessionList = ParsedSessions;

function parseCommandResult(raw: unknown): CommandResult {
  if (!raw || typeof raw !== "object") {
    return { status: "UNKNOWN", raw: {} };
  }
  const record = raw as Record<string, unknown>;
  const command =
    record.command && typeof record.command === "object"
      ? (record.command as Record<string, unknown>)
      : record;
  const status = typeof command.status === "string" ? command.status.toUpperCase() : "";
  if (status === "APPLIED" || status === "PENDING" || status === "REJECTED") {
    return { status, raw: record };
  }
  return { status: "UNKNOWN", raw: record };
}

/**
 * Typed, validating facade over the Wails methods. Components never touch
 * `window.go` directly; every payload is checked before it reaches the UI.
 */
export const bridge = {
  async extensions(): Promise<unknown> {
    return getAppBridge().Extensions();
  },

  async tools(): Promise<unknown> {
    return getAppBridge().Tools();
  },

  async putExtension(
    kind: string,
    name: string,
    spec: Record<string, unknown>,
    enabled: boolean,
    expectedVersion: number,
  ): Promise<unknown> {
    return getAppBridge().PutExtension(kind, name, spec, enabled, expectedVersion);
  },

  async deleteExtension(kind: string, name: string): Promise<void> {
    await getAppBridge().DeleteExtension(kind, name);
  },

  async testModel(name: string, spec: Record<string, unknown> | null): Promise<unknown> {
    return getAppBridge().TestModel(name, spec);
  },

  async listModels(name: string, spec: Record<string, unknown> | null): Promise<unknown> {
    return getAppBridge().ListModels(name, spec);
  },

  async runSteps(runID: string, expandIO = true): Promise<unknown> {
    return getAppBridge().RunSteps(runID, expandIO);
  },

  async setToolEnabled(name: string, enabled: boolean): Promise<unknown> {
    return getAppBridge().SetToolEnabled(name, enabled);
  },

  async myMemories(): Promise<unknown> {
    return getAppBridge().MyMemories();
  },

  async setMyMemory(key: string, value: string): Promise<unknown> {
    return getAppBridge().SetMyMemory(key, value);
  },

  async deleteMyMemory(key: string): Promise<void> {
    await getAppBridge().DeleteMyMemory(key);
  },

  async getSettings(): Promise<Settings> {
    return parseSettings(await getAppBridge().GetSettings());
  },

  async connected(): Promise<boolean> {
    return getAppBridge().Connected();
  },

  async ping(): Promise<void> {
    await getAppBridge().Ping();
  },

  async connect(settings: Settings): Promise<Whoami> {
    getAppBridge();
    const raw = await getAppBridge().Connect(settings);
    return parseWhoami(raw);
  },

  async connectLocal(): Promise<void> {
    await getAppBridge().ConnectLocal();
  },

  async connectOIDC(serverURL: string, issuer: string, clientID: string): Promise<Whoami> {
    return parseWhoami(await getAppBridge().ConnectOIDC(serverURL, issuer, clientID));
  },

  async lock(): Promise<void> {
    await getAppBridge().Lock();
  },

  async unlock(): Promise<Whoami> {
    return parseWhoami(await getAppBridge().Unlock());
  },

  async locked(): Promise<boolean> {
    return getAppBridge().Locked();
  },

  async pickFiles(): Promise<PickedFile[]> {
    return getAppBridge().PickFiles();
  },

  async disconnect(): Promise<void> {
    await getAppBridge().Disconnect();
  },

  async setNetwork(network: NetworkSettings): Promise<void> {
    await getAppBridge().SetNetwork(network);
  },

  async networkDiagnostics(): Promise<unknown> {
    return getAppBridge().NetworkDiagnostics();
  },

  async whoami(): Promise<Whoami> {
    return parseWhoami(await getAppBridge().Whoami());
  },

  async listSessions(): Promise<ParsedSessionList> {
    return parseSessions(await getAppBridge().ListSessions(100));
  },

  async searchSessions(status: string, q: string): Promise<ParsedSessionList> {
    return parseSessions(await getAppBridge().SearchSessions(status, q, 100));
  },

  async createSession(title: string): Promise<{ id: string }> {
    return parseCreateSession(await getAppBridge().CreateSession(title, ""));
  },

  async renameSession(id: string, title: string): Promise<void> {
    await getAppBridge().RenameSession(id, title);
  },

  async archiveSession(id: string, archived: boolean): Promise<void> {
    await getAppBridge().ArchiveSession(id, archived);
  },

  async deleteSession(id: string): Promise<void> {
    await getAppBridge().DeleteSession(id);
  },

  async exportChat(id: string): Promise<string> {
    return getAppBridge().ExportChat(id);
  },

  async pendingApprovals(): Promise<ParsedApprovals> {
    return parseApprovals(await getAppBridge().PendingApprovals());
  },

  async approveFromInbox(
    runID: string,
    approvalID: string,
    actionHash: string,
    approve: boolean,
    reason: string,
  ): Promise<CommandResult> {
    return parseCommandResult(
      await getAppBridge().ApproveFromInbox(runID, approvalID, actionHash, approve, reason),
    );
  },

  async transcript(sessionID: string): Promise<ParsedTranscript> {
    return parseTranscript(await getAppBridge().Transcript(sessionID));
  },

  async sendMessage(sessionID: string, text: string): Promise<{ turnID?: string; runID?: string }> {
    return parseSendMessage(await getAppBridge().SendMessage(sessionID, text));
  },

  async sendTurn(
    sessionID: string,
    text: string,
    context: TurnContext,
  ): Promise<{ turnID?: string; runID?: string }> {
    return parseSendMessage(await getAppBridge().SendTurn(sessionID, text, context));
  },

  async listAudit(limit = 100) {
    return parseAudit(await getAppBridge().ListAudit(limit));
  },

  async listRoleBindings() {
    return parseRoleBindings(await getAppBridge().ListRoleBindings());
  },

  async listRevocations(limit = 100) {
    return parseRevocations(await getAppBridge().ListRevocations(limit));
  },

  async listWorks(status: string, kind: string): Promise<ParsedWorks> {
    return parseWorks(await getAppBridge().ListWorks(status, kind, 100));
  },

  async workView(id: string): Promise<WorkViewData> {
    return parseWorkView(await getAppBridge().WorkView(id));
  },

  async workCommand(
    id: string,
    type: string,
    expectedVersion: number,
    payload: Record<string, unknown> = {},
    reason = "",
  ): Promise<CommandResult> {
    return parseCommandResult(
      await getAppBridge().WorkCommand(id, type, payload, expectedVersion, reason),
    );
  },

  async getRunProjection(runID: string): Promise<ParsedProjection> {
    return parseProjection(await getAppBridge().GetRunProjection(runID), runID);
  },

  async watchRun(runID: string, afterSeq: number): Promise<void> {
    await getAppBridge().WatchRun(runID, afterSeq);
  },

  async unwatchRun(): Promise<void> {
    if (!hasAppBridge()) {
      return;
    }
    await getAppBridge().UnwatchRun();
  },

  async command(
    runID: string,
    type: string,
    payload: Record<string, unknown> = {},
    reason = "",
    expectedVersion = 0,
  ): Promise<CommandResult> {
    const request: CommandRequest = {
      type,
      expected_version: expectedVersion,
      reason,
      payload,
    };
    return parseCommandResult(await getAppBridge().Command(runID, request));
  },

  async openURL(raw: string): Promise<void> {
    await getAppBridge().OpenURL(raw);
  },

  async attachmentImage(runID: string, index: number): Promise<string> {
    const data = await getAppBridge().AttachmentImage(runID, index);
    if (typeof data !== "string" || data === "") {
      throw new DataContractError("attachment: image payload is empty");
    }
    return data;
  },

  async saveAttachment(runID: string, index: number, suggested: string): Promise<string> {
    return getAppBridge().SaveAttachment(runID, index, suggested);
  },

  async setLanguage(language: "en" | "zh"): Promise<void> {
    const app = window.go?.main?.App;
    if (!app?.SetLanguage) {
      return;
    }
    await app.SetLanguage(language);
  },
};

export { toDesktopError };
export type { ParsedProjection, ParsedTranscript, Settings, Whoami };
