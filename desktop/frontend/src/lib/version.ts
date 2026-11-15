import type { ParsedProjection } from "../bridge/types";

export const CLIENT_VERSION = "0.4.0";

function parts(version: string): number[] {
  return version
    .trim()
    .replace(/^v/, "")
    .split(/[.+-]/)
    .map((part) => {
      const parsed = Number.parseInt(part, 10);
      return Number.isNaN(parsed) ? 0 : parsed;
    });
}

/** Returns 1 when a > b, -1 when a < b, 0 when equal. */
export function compareVersions(a: string, b: string): number {
  const left = parts(a);
  const right = parts(b);
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const l = left[index] ?? 0;
    const r = right[index] ?? 0;
    if (l > r) {
      return 1;
    }
    if (l < r) {
      return -1;
    }
  }
  return 0;
}

export interface ProjectionCompatibility {
  /** The projection is newer than this client can fully understand. */
  newerServer: boolean;
  /** The server states this client is too old for the projection. */
  clientOutdated: boolean;
}

export function assessProjectionCompatibility(
  projection: Pick<ParsedProjection, "projectionVersion" | "minimumClientVersion">,
): ProjectionCompatibility {
  const newerServer = projection.projectionVersion > 1;
  const clientOutdated =
    projection.minimumClientVersion !== undefined &&
    compareVersions(projection.minimumClientVersion, CLIENT_VERSION) > 0;
  return { newerServer, clientOutdated };
}
