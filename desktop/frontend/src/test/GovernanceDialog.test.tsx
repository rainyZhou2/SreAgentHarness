import { fireEvent, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { GovernanceDialog } from "../features/settings/GovernanceDialog";
import { installMockBridge } from "./bridge";
import { testSettings } from "./fixtures";
import { renderWithProviders } from "./render";

const connection: ConnectionContextValue = {
  settings: testSettings,
  whoami: { tenant: "tenant-test", subject: "admin", roles: ["admin"], method: "token" },
  scope: "test-scope",
  mode: "remote",
  offline: false,
  lastSyncedAt: undefined,
  reconnect: async () => undefined,
  refreshHealth: vi.fn(async () => undefined) as unknown as ConnectionContextValue["refreshHealth"],
};

describe("GovernanceDialog", () => {
  it("shows audit, role and revocation records", async () => {
    installMockBridge({
      ListAudit: async () => ({
        entries: [
          {
            actor_type: "user",
            actor_id: "alice",
            action: "work.read",
            resource_type: "work",
            resource_id: "work-1",
            outcome: "allowed",
            occurred_at: new Date().toISOString(),
          },
        ],
      }),
      ListRoleBindings: async () => ({
        bindings: [{ principal_id: "bob", role: "operator", created_by: "admin", created_at: new Date().toISOString() }],
      }),
      ListRevocations: async () => ({
        revocations: [
          {
            id: "rev-1",
            scope: "subject",
            scope_id: "bob",
            version: 2,
            actor_id: "admin",
            reason: "offboarding",
            created_at: new Date().toISOString(),
          },
        ],
      }),
    });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <GovernanceDialog onClose={() => undefined} />
      </ConnectionContext.Provider>,
    );

    expect(await screen.findByText("work.read")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Roles" }));
    expect(await screen.findByText("bob")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Revocations" }));
    expect(await screen.findByText("offboarding")).toBeInTheDocument();
  });
});
