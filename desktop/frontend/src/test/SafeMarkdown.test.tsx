import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SafeMarkdown } from "../components/ui/SafeMarkdown";
import { renderWithProviders } from "./render";

describe("SafeMarkdown", () => {
  it("renders images, link cards and code blocks", () => {
    renderWithProviders(
      <SafeMarkdown
        text={[
          "![forecast chart](https://example.com/chart.png)",
          "",
          "Source: https://api.example.com/v1/forecast?lat=39.9",
          "",
          "```ts",
          "const ok = true;",
          "```",
        ].join("\n")}
      />,
    );

    const image = screen.getByRole("img", { name: "forecast chart" });
    expect(image).toHaveAttribute("loading", "lazy");
    expect(screen.getByText("api.example.com")).toBeInTheDocument();
    expect(screen.getByText(/api\.example\.com/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy" })).toBeInTheDocument();
    expect(screen.getByText("ts")).toBeInTheDocument();
  });
});
