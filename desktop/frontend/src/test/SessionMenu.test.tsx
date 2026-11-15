import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { SessionMenu } from "../features/sessions/SessionMenu";
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

describe("SessionMenu", () => {
  it("renames a conversation through the bridge", async () => {
    const rename = vi.fn(async () => ({}));
    const onMutated = vi.fn();
    installMockBridge({ RenameSession: rename });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <SessionMenu
          session={{ id: "session-1", title: "Old title", status: "ACTIVE" }}
          archived={false}
          onMutated={onMutated}
        />
      </ConnectionContext.Provider>,
    );
    const user = userEvent.setup();

    await user.click(screen.getByRole("button", { name: "Conversation actions" }));
    await user.click(screen.getByRole("button", { name: "Rename" }));

    const input = screen.getByLabelText("Title");
    fireEvent.change(input, { target: { value: "New title" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(rename).toHaveBeenCalledWith("session-1", "New title"));
    expect(onMutated).toHaveBeenCalledWith("rename", "session-1");
  });
});
