import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { MemoryDialog } from "../features/memory/MemoryDialog";
import { SkillsToolsDialog } from "../features/tools/SkillsToolsDialog";
import { installMockBridge } from "./bridge";
import { testSettings, testWhoami } from "./fixtures";
import { renderWithProviders } from "./render";

const connection: ConnectionContextValue = {
  settings: testSettings,
  whoami: { ...testWhoami, roles: ["admin"] },
  scope: "test-scope",
  mode: "remote",
  offline: false,
  lastSyncedAt: undefined,
  reconnect: async () => undefined,
  refreshHealth: vi.fn(async () => undefined) as unknown as ConnectionContextValue["refreshHealth"],
};

describe("SkillsToolsDialog", () => {
  it("lists tools and skills", async () => {
    installMockBridge({
      Tools: async () => ({
        tools: [{ name: "metrics.query", description: "Query metrics", risk: "R0", enabled: true }],
        profile: "default",
      }),
      Extensions: async () => ({
        skills: [{ name: "deploy-check", enabled: true, spec: { description: "Check deploy" } }],
      }),
    });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <SkillsToolsDialog />
      </ConnectionContext.Provider>,
    );

    expect(await screen.findByText("metrics.query")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: /Skills/ })).toBeInTheDocument();
  });
});

describe("MemoryDialog", () => {
  it("lists personal memory", async () => {
    installMockBridge({
      MyMemories: async () => ({
        memories: [{ title: "language", content: "Chinese", version: 1 }],
      }),
    });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <MemoryDialog />
      </ConnectionContext.Provider>,
    );

    expect(await screen.findByText("language")).toBeInTheDocument();
    expect(screen.getByText("Chinese")).toBeInTheDocument();
  });
});
