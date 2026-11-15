import { QueryClientProvider, type QueryClient } from "@tanstack/react-query";
import { render, type RenderOptions } from "@testing-library/react";
import type { ReactElement } from "react";

import { I18nProvider } from "../i18n/I18nProvider";
import { createQueryClient } from "../lib/queryClient";

export function renderWithProviders(
  ui: ReactElement,
  options: {
    queryClient?: QueryClient;
    locale?: "en" | "zh";
  } & Omit<RenderOptions, "wrapper"> = {},
) {
  const { queryClient, locale, ...renderOptions } = options;
  const client = queryClient ?? createQueryClient();
  const result = render(
    <QueryClientProvider client={client}>
      <I18nProvider serverLanguage={locale ?? "en"}>{ui}</I18nProvider>
    </QueryClientProvider>,
    renderOptions,
  );
  return { ...result, queryClient: client };
}
