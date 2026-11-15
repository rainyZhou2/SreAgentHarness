import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { parseProjection } from "../bridge/projection";
import type { ParsedBlock } from "../bridge/types";
import { BlockRenderer } from "../features/blocks/BlockRenderer";
import { makeActionBlock, projectionEnvelope } from "./fixtures";
import { renderWithProviders } from "./render";

function renderBlock(block: ParsedBlock, runVersion = 4) {
  return renderWithProviders(
    <BlockRenderer
      block={block}
      runId="run-test-1"
      runVersion={runVersion}
      scope="test-scope"
      offline={false}
    />,
  );
}

describe("BlockRenderer", () => {
  it("degrades an unknown block kind to a read-only card and never executes it", () => {
    const projection = parseProjection(
      projectionEnvelope([
        {
          id: "run-test-1:future:0",
          kind: "future-card",
          ordinal: 0,
          raw_kind: "future-card",
          markdown: '<img src="x" onerror="alert(1)">',
          html: "<script>alert(1)</script>",
        },
      ]),
    );
    const block = projection.blocks[0];
    if (!block) {
      throw new Error("expected one parsed block");
    }

    renderBlock(block);

    expect(screen.getByText("Unsupported block")).toBeInTheDocument();
    expect(screen.getByText("future-card")).toBeInTheDocument();
    expect(screen.getByText("Read-only")).toBeInTheDocument();
    // The raw payload, including its markdown/html fields, is never rendered.
    expect(screen.queryByText(/onerror/)).not.toBeInTheDocument();
    expect(screen.queryByText(/script/)).not.toBeInTheDocument();
    // Unknown blocks expose no executable controls.
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("renders an action with missing required fields as a read-only invalid block", () => {
    const projection = parseProjection(
      projectionEnvelope([makeActionBlock({ intent_hash: undefined })]),
    );
    const block = projection.blocks[0];
    if (!block) {
      throw new Error("expected one parsed block");
    }

    expect(block.kind).toBe("invalid");
    renderBlock(block);

    expect(screen.getByText("Block could not be rendered")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Approve" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Reject" })).not.toBeInTheDocument();
  });
});
