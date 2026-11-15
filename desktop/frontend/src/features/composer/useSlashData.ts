import { useQuery } from "@tanstack/react-query";

import { bridge } from "../../bridge/bridge";
import { qk } from "../../lib/queryKeys";

export interface SlashSkill {
  kind: "skill";
  name: string;
  description: string;
  manual: boolean;
}

export interface SlashTool {
  kind: "tool";
  name: string;
  description: string;
  risk: string;
}

export type SlashItem = SlashSkill | SlashTool;

export interface ModelOption {
  name: string;
  enabled: boolean;
}

export interface SlashData {
  items: SlashItem[];
  currentModel: string;
  models: ModelOption[];
  toolCount: number;
  enabledToolCount: number;
  profile: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseSkills(raw: unknown): SlashSkill[] {
  if (!isRecord(raw) || !Array.isArray(raw.skills)) {
    return [];
  }
  const out: SlashSkill[] = [];
  for (const item of raw.skills) {
    if (!isRecord(item) || item.enabled === false || typeof item.name !== "string" || item.name === "") {
      continue;
    }
    const spec = isRecord(item.spec) ? item.spec : {};
    out.push({
      kind: "skill",
      name: item.name,
      description: typeof spec.description === "string" ? spec.description : "",
      manual: spec.manual === true,
    });
  }
  return out;
}

function parseTools(raw: unknown): SlashTool[] {
  if (!isRecord(raw) || !Array.isArray(raw.tools)) {
    return [];
  }
  const out: SlashTool[] = [];
  for (const item of raw.tools) {
    if (!isRecord(item) || item.enabled === false || typeof item.name !== "string" || item.name === "") {
      continue;
    }
    out.push({
      kind: "tool",
      name: item.name,
      description: typeof item.description === "string" ? item.description : "",
      risk: typeof item.risk === "string" ? item.risk : "",
    });
  }
  return out;
}

function parseCurrentModel(raw: unknown): string {
  if (!isRecord(raw) || !isRecord(raw.current_model)) {
    return "";
  }
  return typeof raw.current_model.model === "string" ? raw.current_model.model : "";
}

function parseModels(raw: unknown): ModelOption[] {
  if (!isRecord(raw) || !Array.isArray(raw.models)) {
    return [];
  }
  const out: ModelOption[] = [];
  for (const item of raw.models) {
    if (!isRecord(item) || typeof item.name !== "string" || item.name === "") {
      continue;
    }
    out.push({ name: item.name, enabled: item.enabled !== false });
  }
  return out;
}

function parseToolSummary(raw: unknown): { total: number; enabled: number; profile: string } {
  if (!isRecord(raw) || !Array.isArray(raw.tools)) {
    return { total: 0, enabled: 0, profile: "" };
  }
  let enabled = 0;
  for (const item of raw.tools) {
    if (isRecord(item) && item.enabled !== false) {
      enabled += 1;
    }
  }
  return {
    total: raw.tools.length,
    enabled,
    profile: typeof raw.profile === "string" ? raw.profile : "",
  };
}

export function useSlashData(scope: string, enabled: boolean) {
  return useQuery({
    queryKey: qk.slash(scope),
    queryFn: async (): Promise<SlashData> => {
      const [extensions, tools] = await Promise.all([bridge.extensions(), bridge.tools()]);
      const summary = parseToolSummary(tools);
      return {
        items: [...parseSkills(extensions), ...parseTools(tools)],
        currentModel: parseCurrentModel(extensions),
        models: parseModels(extensions),
        toolCount: summary.total,
        enabledToolCount: summary.enabled,
        profile: summary.profile,
      };
    },
    enabled,
    staleTime: 30_000,
  });
}
