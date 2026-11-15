import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ActionBlock } from "../features/blocks/ActionBlock";
import { installMockBridge } from "./bridge";
import { makeActionBlock, projectionEnvelope, testAction, testRunID } from "./fixtures";
import { renderWithProviders } from "./render";

describe("ActionBlock", () => {
  it("shows a success state only after the server confirms the decision", async () => {
    let resolveCommand: (value: unknown) => void = () => undefined;
    const commandResult = new Promise<unknown>((resolve) => {
      resolveCommand = resolve;
    });
    const command = vi.fn(async (_runID: string, _request: unknown) => commandResult);
    installMockBridge({
      Command: command,
      GetRunProjection: async () => projectionEnvelope([makeActionBlock()]),
    });

    const user = userEvent.setup();
    renderWithProviders(
      <ActionBlock
        data={testAction}
        runId={testRunID}
        runVersion={4}
        scope="test-scope"
        offline={false}
      />,
    );

    expect(screen.queryByText(/Approved by the server/)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Approve" }));
    expect(screen.getByText("Approve this action?")).toBeInTheDocument();
    expect(screen.queryByText(/Approved by the server/)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Confirm approval" }));

    const submitting = await screen.findByRole("button", { name: /Recording decision/ });
    expect(submitting).toBeDisabled();
    // Still no success state while the request is in flight.
    expect(screen.queryByText(/Approved by the server/)).not.toBeInTheDocument();

    resolveCommand({ status: "APPLIED" });

    expect(await screen.findByText(/Approved by the server/)).toBeInTheDocument();
    expect(command).toHaveBeenCalledTimes(1);
    const call = command.mock.calls[0];
    expect(call?.[0]).toBe(testRunID);
    expect(call?.[1]).toMatchObject({
      type: "approve_tool",
      expected_version: 4,
      payload: {
        approval_request_id: "approval-1",
        action_hash: "sha256:abc123",
      },
    });
  });

  it("keeps a conflict explicit and never reports success", async () => {
    installMockBridge({
      Command: async () => {
        throw new Error("409 version_conflict");
      },
      GetRunProjection: async () => projectionEnvelope([makeActionBlock()]),
    });

    const user = userEvent.setup();
    renderWithProviders(
      <ActionBlock
        data={testAction}
        runId={testRunID}
        runVersion={4}
        scope="test-scope"
        offline={false}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Approve" }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));

    expect(
      await screen.findByText(/changed while you were deciding/),
    ).toBeInTheDocument();
    expect(screen.queryByText(/Approved by the server/)).not.toBeInTheDocument();
  });
});
