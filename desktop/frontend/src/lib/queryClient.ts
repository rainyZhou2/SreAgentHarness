import { QueryClient } from "@tanstack/react-query";

import { toDesktopError } from "../bridge/errors";

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 15_000,
        gcTime: 5 * 60_000,
        refetchOnWindowFocus: true,
        retry: (failureCount, error) => {
          const normalized = toDesktopError(error);
          return normalized.retriable && failureCount < 2;
        },
      },
      mutations: {
        retry: false,
      },
    },
  });
}
