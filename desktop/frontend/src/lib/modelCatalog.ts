export interface ModelSpec {
  provider: string;
  preset?: string;
  model: string;
  api_key?: string;
  base_url?: string;
  effort?: string;
  context_window?: number;
  max_output?: number;
  max_completion_tokens?: boolean;
  default?: boolean;
}

export interface ModelRecord {
  name: string;
  enabled: boolean;
  version: number;
  spec: ModelSpec;
  updated_at?: string;
  status?: { error?: string };
}

export interface CurrentModel {
  model?: string;
  endpoint?: string;
  tenant_configured?: boolean;
  error?: string;
}

export interface ModelCatalog {
  models: ModelRecord[];
  current?: CurrentModel;
}

export interface ModelPreset {
  id: string;
  name: string;
  mark: string;
  provider: "openai" | "anthropic" | "gemini";
  baseURL: string;
  models: string[];
  keyHint: string;
  maxCompletionTokens?: boolean;
  basePlaceholder?: string;
}

export const MODEL_PRESETS: ModelPreset[] = [
  {
    id: "openai",
    name: "OpenAI",
    mark: "AI",
    provider: "openai",
    baseURL: "https://api.openai.com/v1",
    models: ["gpt-6.1-sol", "gpt-6-luna", "gpt-5.6-terra"],
    keyHint: "sk-…",
    maxCompletionTokens: true,
  },
  {
    id: "anthropic",
    name: "Anthropic Claude",
    mark: "A",
    provider: "anthropic",
    baseURL: "",
    models: ["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"],
    keyHint: "sk-ant-…",
  },
  {
    id: "gemini",
    name: "Google Gemini",
    mark: "G",
    provider: "gemini",
    baseURL: "",
    models: ["gemini-flash-latest", "gemini-pro-latest", "gemini-3.8-flash"],
    keyHint: "AIza…",
  },
  {
    id: "deepseek",
    name: "DeepSeek",
    mark: "DS",
    provider: "openai",
    baseURL: "https://api.deepseek.com/v1",
    models: ["deepseek-v4-pro", "deepseek-v4-flash"],
    keyHint: "sk-…",
  },
  {
    id: "qwen",
    name: "Qwen (Alibaba Cloud)",
    mark: "通",
    provider: "openai",
    baseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1",
    models: ["qwen3.7-plus", "qwen3.7-max", "qwen3.6-flash"],
    keyHint: "sk-…",
  },
  {
    id: "custom",
    name: "OpenAI-compatible / self-hosted",
    mark: "</>",
    provider: "openai",
    baseURL: "",
    models: [],
    keyHint: "API key",
    basePlaceholder: "http://127.0.0.1:8000/v1",
  },
];

export function presetFor(spec: ModelSpec): ModelPreset {
  return (
    MODEL_PRESETS.find((preset) => preset.id === spec.preset) ??
    MODEL_PRESETS.find(
      (preset) =>
        preset.provider === spec.provider &&
        preset.id !== "custom" &&
        (preset.provider !== "openai" || preset.baseURL === (spec.base_url ?? "")),
    ) ??
    MODEL_PRESETS[MODEL_PRESETS.length - 1] as ModelPreset
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseSpec(value: unknown): ModelSpec | null {
  if (!isRecord(value) || typeof value.provider !== "string" || typeof value.model !== "string") {
    return null;
  }
  return {
    provider: value.provider,
    preset: typeof value.preset === "string" ? value.preset : undefined,
    model: value.model,
    api_key: typeof value.api_key === "string" ? value.api_key : undefined,
    base_url: typeof value.base_url === "string" ? value.base_url : undefined,
    effort: typeof value.effort === "string" ? value.effort : undefined,
    context_window: typeof value.context_window === "number" ? value.context_window : undefined,
    max_output: typeof value.max_output === "number" ? value.max_output : undefined,
    max_completion_tokens:
      typeof value.max_completion_tokens === "boolean" ? value.max_completion_tokens : undefined,
    default: value.default === true,
  };
}

export function parseModelCatalog(raw: unknown): ModelCatalog {
  if (!isRecord(raw)) {
    return { models: [] };
  }
  const models: ModelRecord[] = [];
  if (Array.isArray(raw.models)) {
    for (const item of raw.models) {
      if (!isRecord(item) || typeof item.name !== "string" || item.name === "") {
        continue;
      }
      const spec = parseSpec(item.spec);
      if (!spec) {
        continue;
      }
      models.push({
        name: item.name,
        enabled: item.enabled !== false,
        version: typeof item.version === "number" ? item.version : 0,
        spec,
        updated_at: typeof item.updated_at === "string" ? item.updated_at : undefined,
        status: isRecord(item.status)
          ? { error: typeof item.status.error === "string" ? item.status.error : undefined }
          : undefined,
      });
    }
  }
  models.sort(
    (left, right) =>
      Number(right.spec.default === true) - Number(left.spec.default === true) ||
      left.name.localeCompare(right.name),
  );
  const current = isRecord(raw.current_model)
    ? {
        model: typeof raw.current_model.model === "string" ? raw.current_model.model : undefined,
        endpoint:
          typeof raw.current_model.endpoint === "string" ? raw.current_model.endpoint : undefined,
        tenant_configured: raw.current_model.tenant_configured === true,
        error: typeof raw.current_model.error === "string" ? raw.current_model.error : undefined,
      }
    : undefined;
  return { models, current };
}
