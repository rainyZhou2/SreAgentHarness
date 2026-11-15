import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { WorksInbox } from "../features/works/WorksInbox";
import { installMockBridge } from "./bridge";
import { testSettings, testWhoami } from "./fixtures";
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

describe("WorksInbox", () => {
  it("sends a pause command with the Work version", async () => {
    const command = vi.fn(async () => ({ status: "APPLIED" }));
    installMockBridge({
      ListWorks: async () => ({
        items: [
          {
            id: "work-1",
            title: "Checkout stabilization",
            kind: "incident",
            status: "ACTIVE",
            version: 3,
            updated_at: new Date().toISOString(),
          },
        ],
      }),
      WorkView: async () => ({
        work: {
          id: "work-1",
          title: "Checkout stabilization",
          kind: "incident",
          status: "ACTIVE",
          version: 3,
        },
        view: {
          controls: ["pause"],
          waiting_on: "approval pending",
          stages: [{ id: "investigate", status: "current" }],
        },
      }),
      WorkCommand: command,
    });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <WorksInbox />
      </ConnectionContext.Provider>,
    );
    expect(await screen.findByText("Checkout stabilization")).toBeInTheDocument();
    fireEvent.click(await screen.findByRole("button", { name: "Pause" }));

    await waitFor(() =>
      expect(command).toHaveBeenCalledWith("work-1", "pause", {}, 3, ""),
    );
  });
});
