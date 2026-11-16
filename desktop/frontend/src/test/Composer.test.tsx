import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Composer } from "../features/composer/Composer";
import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { useUiStore } from "../state/uiStore";
import { installMockBridge } from "./bridge";
import { projectionEnvelope, testSettings, testWhoami } from "./fixtures";
import { renderWithProviders } from "./render";

const connection: ConnectionContextValue = {
  settings: testSettings,
  whoami: testWhoami,
  scope: "test-scope",
  mode: "local",
  offline: false,
  lastSyncedAt: undefined,
  reconnect: async () => undefined,
  refreshHealth: vi.fn(async () => undefined) as unknown as ConnectionContextValue["refreshHealth"],
};

function renderComposer(
  sessionID: string | null = null,
  whoami: ConnectionContextValue["whoami"] = testWhoami,
) {
  useUiStore.setState({ currentSessionId: sessionID, drafts: {} });
  return renderWithProviders(
    <ConnectionContext.Provider value={{ ...connection, whoami }}>
      <Composer />
    </ConnectionContext.Provider>,
  );
}

describe("Composer", () => {
  it("inserts a manual skill from the slash menu", async () => {
    installMockBridge({
      Extensions: async () => ({
        skills: [
          {
            name: "deploy-check",
            enabled: true,
            spec: { description: "Check a deployment", manual: true },
          },
        ],
      }),
      Tools: async () => ({ tools: [] }),
    });

    renderComposer();
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "/dep" } });

    const option = await screen.findByRole("option", { name: /deploy-check/ });
    fireEvent.mouseDown(option);

    await waitFor(() => {
      expect((screen.getByRole("textbox") as HTMLTextAreaElement).value).toBe(
        "/deploy-check ",
      );
    });
  });

  it("shows the current goal and run metrics above the composer", async () => {
    const getProjection = vi.fn(async () =>
      projectionEnvelope(
        [
          {
            id: "run-1:status:0",
            kind: "status",
            ordinal: 0,
            status: "RUNNING",
            started_at: new Date(Date.now() - 10_000).toISOString(),
            cost: { input_tokens: 1000, output_tokens: 500 },
          },
          {
            id: "run-1:tool-group:1",
            kind: "tool-group",
            ordinal: 1,
            step_ids: ["step-1", "step-2"],
            step_count: 2,
          },
        ],
        { run_id: "run-1" },
      ),
    );
    installMockBridge({
      GetRunProjection: getProjection,
      SearchSessions: async () => ({
        sessions: [
          {
            id: "session-1",
            title: "Checkout latency",
            status: "ACTIVE",
            turns: 3,
            latest_run_id: "run-1",
          },
        ],
      }),
    });

    renderComposer("session-1");

    expect(await screen.findByText("Checkout latency")).toBeInTheDocument();
    await waitFor(() => expect(getProjection).toHaveBeenCalled());
    expect(await screen.findByText("Running")).toBeInTheDocument();
    expect(await screen.findByText(/3 turns/)).toBeInTheDocument();
    expect(await screen.findByText(/2 steps/)).toBeInTheDocument();
  });

  it("switches the workspace model from the picker", async () => {
    const putExtension = vi.fn(async () => ({}));
    installMockBridge({
      Extensions: async () => ({
        models: [
          {
            name: "model-a",
            enabled: true,
            version: 1,
            spec: {
              provider: "openai",
              preset: "openai",
              model: "model-a",
              base_url: "https://api.openai.com/v1",
              default: true,
            },
          },
          {
            name: "model-b",
            enabled: true,
            version: 2,
            spec: {
              provider: "openai",
              preset: "openai",
              model: "model-b",
              base_url: "https://api.openai.com/v1",
            },
          },
        ],
        current_model: { model: "model-a", tenant_configured: true },
      }),
      PutExtension: putExtension,
    });

    renderComposer(null, { ...testWhoami, roles: ["admin"] });

    fireEvent.click(await screen.findByRole("button", { name: /model-a/ }));
    fireEvent.click(await screen.findByRole("menuitemradio", { name: /model-b/ }));

    await waitFor(() => expect(putExtension).toHaveBeenCalledTimes(2));
    expect(putExtension).toHaveBeenLastCalledWith(
      "models",
      "model-b",
      expect.objectContaining({ model: "model-b", default: true }),
      true,
      2,
    );
  });

  it("opens effective permission and model details from the chips", async () => {
    installMockBridge({
      Extensions: async () => ({
        skills: [],
        models: [
          {
            name: "model-a",
            enabled: true,
            version: 1,
            spec: {
              provider: "openai",
              preset: "openai",
              model: "model-a",
              base_url: "https://api.openai.com/v1",
              default: true,
            },
          },
        ],
        current_model: { model: "model-a" },
      }),
      Tools: async () => ({
        tools: [{ name: "tool-a", enabled: true }],
        profile: "default",
      }),
    });

    renderComposer();

    fireEvent.click(screen.getByRole("button", { name: "Approver" }));
    expect(await screen.findByText("Effective permissions")).toBeInTheDocument();
    expect(await screen.findByText(/1 \/ 1/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Close" }));

    fireEvent.click(screen.getByRole("button", { name: /model-a/ }));
    expect(await screen.findByRole("menu")).toBeInTheDocument();
    expect(screen.getByText("OpenAI")).toBeInTheDocument();
    expect(screen.getAllByText("model-a").length).toBeGreaterThan(0);
  });

  it("sends the pinned environment, services and window with the turn", async () => {
    const sendTurn = vi.fn(async () => ({
      turn: { id: "turn-1", run_id: "run-1" },
    }));
    installMockBridge({
      CreateSession: async () => ({ session: { id: "session-1" } }),
      SendTurn: sendTurn,
    });

    renderComposer();
    fireEvent.change(screen.getByRole("textbox"), {
      target: { value: "checkout errors started after the deploy" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Context" }));
    fireEvent.change(screen.getByLabelText("Environment"), {
      target: { value: "production" },
    });
    fireEvent.change(screen.getByLabelText("Services"), {
      target: { value: "checkout, payments" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));

    await waitFor(() => expect(sendTurn).toHaveBeenCalledTimes(1));
    expect(sendTurn).toHaveBeenCalledWith(
      "session-1",
      "checkout errors started after the deploy",
      expect.objectContaining({
        environment: "production",
        services: ["checkout", "payments"],
      }),
    );
  });
});
