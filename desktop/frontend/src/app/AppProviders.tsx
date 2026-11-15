import { QueryClientProvider, type QueryClient } from "@tanstack/react-query";
import { useState, type ReactNode } from "react";

import { I18nProvider } from "../i18n/I18nProvider";
import { useTheme } from "../hooks/useTheme";
import { createQueryClient } from "../lib/queryClient";

function ThemeSync() {
  useTheme();
  return null;
}

export function AppProviders({
  children,
  queryClient,
}: {
  children: ReactNode;
  queryClient?: QueryClient;
}) {
  const [fallbackClient] = useState(createQueryClient);
  const client = queryClient ?? fallbackClient;
  return (
    <QueryClientProvider client={client}>
      <I18nProvider>
        <ThemeSync />
        {children}
      </I18nProvider>
    </QueryClientProvider>
  );
}
