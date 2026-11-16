/**
 * Query keys are scoped by a stable connection namespace (mode, server,
 * tenant, subject) that never contains the bearer token. Switching identity
 * therefore invalidates the whole cache without leaking credentials into it.
 */
export const qk = {
  boot: () => ["boot"] as const,
  health: (scope: string) => ["health", scope] as const,
  sessions: (scope: string) => ["sessions", scope] as const,
  approvals: (scope: string) => ["approvals", scope] as const,
  slash: (scope: string) => ["slash", scope] as const,
  extensions: (scope: string) => ["extensions", scope] as const,
  works: (scope: string) => ["works", scope] as const,
  work: (scope: string, workID: string) => ["work", scope, workID] as const,
  transcript: (scope: string, sessionID: string) => ["transcript", scope, sessionID] as const,
  projection: (scope: string, runID: string) => ["projection", scope, runID] as const,
  runSteps: (scope: string, runID: string) => ["run-steps", scope, runID] as const,
  projections: (scope: string) => ["projection", scope] as const,
};
