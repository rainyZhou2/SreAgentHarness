import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { TraceView } from "../features/trace/TraceView";
import { useUiStore } from "../state/uiStore";
import { installMockBridge } from "./bridge";
import { testSettings, testWhoami } from "./fixtures";
import { renderWithProviders } from "./render";

const connection: ConnectionContextValue = {
  settings: testSettings,
  whoami: testWhoami,
  scope: "test-scope",
  mode: "remote",
  offline: false,
  lastSyncedAt: undefined,
  reconnect: async () => undefined,
  refreshHealth: vi.fn(async () => undefined) as unknown as ConnectionContextValue["refreshHealth"],
};

describe("TraceView", () => {
  it("renders explicit thoughts and tool steps from the run trace", async () => {
    installMockBridge({
      SearchSessions: async () => ({
        sessions: [{ id: "session-1", title: "Checkout latency", latest_run_id: "run-1" }],
      }),
      RunSteps: async () => ({
        steps: [
          {
            id: "step-1",
            seq: 1,
            kind: "reason",
            executor_name: "reasoner",
            status: "SUCCEEDED",
            thoughts: [
              {
                seq: 1,
                kind: "hypothesis",
                summary: "Checkout latency may come from the payment dependency.",
                status: "UNVERIFIED",
              },
            ],
          },
          {
            id: "step-2",
            seq: 2,
            kind: "tool",
            executor_name: "metrics.query",
            status: "SUCCEEDED",
            input: { service: "checkout" },
            output: { p99: 1800 },
          },
        ],
      }),
    });
    useUiStore.setState({ currentSessionId: "session-1", workspaceView: "trace" });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <TraceView />
      </ConnectionContext.Provider>,
    );

    expect(await screen.findByText("Execution trace")).toBeInTheDocument();
    expect(
      await screen.findByText(/Checkout latency may come from the payment dependency/),
    ).toBeInTheDocument();
    expect((await screen.findAllByText("metrics.query")).length).toBeGreaterThan(0);
    expect(screen.getByText("2 steps")).toBeInTheDocument();
  });
});
