import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ApprovalInbox } from "../features/approvals/ApprovalInbox";
import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { installMockBridge } from "./bridge";
import { renderWithProviders } from "./render";
import { testSettings, testWhoami } from "./fixtures";

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

function renderInbox() {
  return renderWithProviders(
    <ConnectionContext.Provider value={connection}>
      <ApprovalInbox />
    </ConnectionContext.Provider>,
  );
}

describe("ApprovalInbox", () => {
  it("sends the approval decision with the exact run and intent hash", async () => {
    const approve = vi.fn(async () => ({ status: "APPLIED" }));
    installMockBridge({
      PendingApprovals: async () => ({
        approvals: [
          {
            id: "approval-1",
            run_id: "run-1",
            action_intent_id: "intent-1",
            step_id: "step-1",
            action_hash: "sha256:abc123",
            risk_class: "R2",
            tool_name: "deployment.rollback",
            target: "deployment://prod/checkout",
            required_approvals: 2,
            status: "PENDING",
            expires_at: new Date(Date.now() + 3_600_000).toISOString(),
            created_at: new Date().toISOString(),
            session_title: "checkout incident",
          },
        ],
      }),
      ApproveFromInbox: approve,
    });

    renderInbox();
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: "Approve" }));
    expect(screen.getByText("Approve this action?")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Approve" }));

    expect(approve).toHaveBeenCalledTimes(1);
    expect(approve).toHaveBeenCalledWith(
      "run-1",
      "approval-1",
      "sha256:abc123",
      true,
      "",
    );
  });

  it("shows an empty state when no approvals are waiting", async () => {
    installMockBridge({ PendingApprovals: async () => ({ approvals: [] }) });

    renderInbox();

    expect(await screen.findByText("No approvals are waiting.")).toBeInTheDocument();
  });
});
