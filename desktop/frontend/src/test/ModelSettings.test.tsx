import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ConnectionContext, type ConnectionContextValue } from "../features/connect/ConnectionContext";
import { ModelSettings } from "../features/settings/ModelSettings";
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

describe("ModelSettings", () => {
  it("saves a new model through PutExtension", async () => {
    const putExtension = vi.fn(async () => ({}));
    installMockBridge({
      Extensions: async () => ({ models: [], current_model: { model: "rule-based-sre" } }),
      PutExtension: putExtension,
    });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <ModelSettings canEdit />
      </ConnectionContext.Provider>,
    );

    fireEvent.click(await screen.findByRole("button", { name: /Add model/ }));
    fireEvent.click(await screen.findByRole("button", { name: "Save" }));

    await waitFor(() => expect(putExtension).toHaveBeenCalledTimes(1));
    expect(putExtension).toHaveBeenCalledWith(
      "models",
      "openai",
      expect.objectContaining({
        provider: "openai",
        model: "gpt-6.1-sol",
        base_url: "https://api.openai.com/v1",
        default: true,
      }),
      true,
      0,
    );
  });

  it("shows the current model and opens the editor for a configured model", async () => {
    installMockBridge({
      Extensions: async () => ({
        models: [
          {
            name: "openai-prod",
            enabled: true,
            version: 3,
            spec: {
              provider: "openai",
              preset: "openai",
              model: "gpt-6.1-sol",
              base_url: "https://api.openai.com/v1",
              default: true,
            },
          },
        ],
        current_model: { model: "gpt-6.1-sol", tenant_configured: true },
      }),
    });

    renderWithProviders(
      <ConnectionContext.Provider value={connection}>
        <ModelSettings canEdit />
      </ConnectionContext.Provider>,
    );

    expect((await screen.findAllByText("GPT-6.1-SOL")).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: /openai-prod/ }));
    expect(await screen.findByText("Edit model")).toBeInTheDocument();
    expect(screen.getByDisplayValue("gpt-6.1-sol")).toBeInTheDocument();
  });
});
