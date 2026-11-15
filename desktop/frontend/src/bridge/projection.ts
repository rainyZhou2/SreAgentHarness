import { z } from "zod";

import { DataContractError } from "./errors";
import type {
  AttachmentItem,
  ConclusionBlockData,
  EvidenceBlockData,
  HypothesisBlockData,
  ParsedBlock,
  ParsedProjection,
  StatusBlockData,
  ToolGroupBlockData,
  WorkBlockData,
  ActionBlockData,
  ReasoningBlockData,
  MarkdownBlockData,
  AttachmentsBlockData,
} from "./types";

const attachmentSchema = z
  .object({
    type: z.string().min(1),
    title: z.string().optional(),
    caption: z.string().optional(),
    url: z.string().optional(),
    ref: z.string().optional(),
    mime_type: z.string().optional(),
    size_bytes: z.number().optional(),
    columns: z.array(z.string()).optional(),
    rows: z
      .array(z.array(z.unknown()))
      .optional()
      .transform((rows) =>
        rows?.map((row) => row.map((cell) => (cell == null ? "" : String(cell)))),
      ),
  })
  .passthrough();

const costSchema = z
  .object({
    model_requests: z.number().optional(),
    input_tokens: z.number().optional(),
    output_tokens: z.number().optional(),
    cost_usd: z.number().optional(),
  })
  .passthrough();

const windowSchema = z
  .object({ start: z.string(), end: z.string() })
  .passthrough();

const qualitySchema = z
  .object({
    partial: z.boolean().optional(),
    stale: z.boolean().optional(),
    conflict: z.boolean().optional(),
  })
  .passthrough();

const hypothesisItemSchema = z
  .object({
    id: z.string().min(1),
    statement: z.string(),
    status: z.string().min(1),
    supports: z.array(z.string()).optional(),
    refutes: z.array(z.string()).optional(),
  })
  .passthrough();

const blockSchemas = {
  status: z
    .object({
      status: z.string().min(1),
      started_at: z.string().optional(),
      finished_at: z.string().nullable().optional(),
      cost: costSchema.optional(),
    })
    .passthrough(),
  reasoning: z
    .object({ summary: z.string(), truncated: z.boolean().optional() })
    .passthrough(),
  markdown: z.object({ markdown: z.string() }).passthrough(),
  "tool-group": z
    .object({
      step_ids: z.array(z.string()).optional().default([]),
      step_count: z.number().optional(),
      failed_count: z.number().optional(),
    })
    .passthrough(),
  evidence: z
    .object({
      evidence_ref: z.string().optional(),
      data_ref: z.string().optional(),
      signal: z.string().min(1),
      source: z.string().min(1),
      status: z.string().min(1),
      window: windowSchema.optional(),
      quality: qualitySchema.optional(),
      summary: z.string().optional(),
    })
    .passthrough(),
  hypothesis: z
    .object({
      work_id: z.string().optional(),
      items: z.array(hypothesisItemSchema),
    })
    .passthrough(),
  conclusion: z
    .object({
      verdict: z.string().min(1),
      answer: z.string(),
      evidence_refs: z.array(z.string()).optional(),
      unverified: z.array(z.string()).optional(),
    })
    .passthrough(),
  work: z
    .object({
      work_id: z.string().min(1),
      status: z.string().min(1),
      stage: z.string().optional(),
      waiting_on: z.string().optional(),
    })
    .passthrough(),
  action: z
    .object({
      action_id: z.string().min(1),
      approval_id: z.string().optional(),
      state: z.string().min(1),
      risk: z.string().min(1),
      target: z.string().min(1),
      intent_hash: z.string().min(1),
      required_approvals: z.number(),
      expires_at: z.string().nullable().optional(),
      may_have_dispatched: z.boolean().optional(),
      controls: z.array(z.string()).optional(),
      action_hash: z.string().optional(),
    })
    .passthrough(),
  attachments: z.object({ items: z.array(attachmentSchema) }).passthrough(),
  unknown: z.object({ raw_kind: z.string().optional() }).passthrough(),
} as const;

const envelopeSchema = z
  .object({
    schema_version: z.number().optional().default(1),
    projection_version: z.number().optional().default(1),
    minimum_client_version: z.string().optional(),
    capabilities: z.array(z.string()).optional(),
    run_id: z.string().min(1),
    run_version: z.number().optional().default(0),
    session_id: z.string().optional(),
    turn_id: z.string().optional(),
    status: z.string().min(1),
    work_ids: z.array(z.string()).optional(),
    blocks: z.array(z.unknown()),
  })
  .passthrough();

const blockBaseSchema = z
  .object({
    id: z.string().min(1),
    kind: z.string().min(1),
    ordinal: z.number().optional(),
  })
  .passthrough();

function invalidBlock(
  id: string,
  ordinal: number,
  rawKind: string,
  reason: string,
  raw: Record<string, unknown>,
): ParsedBlock {
  return { kind: "invalid", id, ordinal, rawKind, reason, raw };
}

function parseBlock(raw: unknown, fallbackOrdinal: number): ParsedBlock {
  const base = blockBaseSchema.safeParse(raw);
  if (!base.success) {
    return invalidBlock(
      `invalid:${fallbackOrdinal}`,
      fallbackOrdinal,
      "invalid",
      "block is missing id or kind",
      { value: raw },
    );
  }
  const baseData = base.data;
  const id = baseData.id;
  const ordinal = baseData.ordinal ?? fallbackOrdinal;
  const rawRecord = raw as Record<string, unknown>;

  if (!(baseData.kind in blockSchemas)) {
    const rawKind =
      typeof rawRecord.raw_kind === "string" && rawRecord.raw_kind !== ""
        ? rawRecord.raw_kind
        : baseData.kind;
    return { kind: "unknown", id, ordinal, rawKind, raw: rawRecord };
  }

  const schema = blockSchemas[baseData.kind as keyof typeof blockSchemas];
  const parsed = schema.safeParse(raw);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    const field = issue?.path.filter((part) => part !== baseData.kind).join(".");
    const reason = field ? `${field}: ${issue?.message ?? "invalid"}` : (issue?.message ?? "invalid");
    return invalidBlock(id, ordinal, baseData.kind, reason, rawRecord);
  }

  switch (baseData.kind) {
    case "status":
      return { kind: "status", id, ordinal, data: parsed.data as StatusBlockData };
    case "reasoning":
      return { kind: "reasoning", id, ordinal, data: parsed.data as ReasoningBlockData };
    case "markdown":
      return { kind: "markdown", id, ordinal, data: parsed.data as MarkdownBlockData };
    case "tool-group":
      return { kind: "tool-group", id, ordinal, data: parsed.data as ToolGroupBlockData };
    case "evidence":
      return { kind: "evidence", id, ordinal, data: parsed.data as EvidenceBlockData };
    case "hypothesis":
      return { kind: "hypothesis", id, ordinal, data: parsed.data as HypothesisBlockData };
    case "conclusion":
      return { kind: "conclusion", id, ordinal, data: parsed.data as ConclusionBlockData };
    case "work":
      return { kind: "work", id, ordinal, data: parsed.data as WorkBlockData };
    case "action":
      return { kind: "action", id, ordinal, data: parsed.data as ActionBlockData };
    case "attachments":
      return { kind: "attachments", id, ordinal, data: parsed.data as AttachmentsBlockData };
    default:
      return { kind: "unknown", id, ordinal, rawKind: baseData.kind, raw: rawRecord };
  }
}

/**
 * Validates a server projection. Unknown block kinds are preserved as
 * read-only `unknown` blocks, and missing required fields become `invalid`
 * blocks instead of crashing the conversation (design §4.1.5).
 */
export function parseProjection(raw: unknown, expectedRunID?: string): ParsedProjection {
  const envelope = envelopeSchema.safeParse(raw);
  if (!envelope.success) {
    const issue = envelope.error.issues[0];
    const path = issue?.path.join(".") ?? "";
    throw new DataContractError(
      `run projection: ${path ? `${path}: ` : ""}${issue?.message ?? "invalid payload"}`,
    );
  }
  const data = envelope.data;
  if (expectedRunID !== undefined && data.run_id !== expectedRunID) {
    throw new DataContractError(
      `run projection: expected run_id ${expectedRunID}, received ${data.run_id}`,
    );
  }
  return {
    schemaVersion: data.schema_version,
    projectionVersion: data.projection_version,
    ...(data.minimum_client_version === undefined
      ? {}
      : { minimumClientVersion: data.minimum_client_version }),
    capabilities: data.capabilities ?? [],
    runId: data.run_id,
    runVersion: data.run_version,
    ...(data.session_id === undefined ? {} : { sessionId: data.session_id }),
    ...(data.turn_id === undefined ? {} : { turnId: data.turn_id }),
    status: data.status,
    workIds: data.work_ids ?? [],
    blocks: data.blocks.map((block, index) => parseBlock(block, index)),
  };
}

export function parseAttachmentItem(raw: unknown): AttachmentItem {
  const parsed = attachmentSchema.safeParse(raw);
  if (!parsed.success) {
    throw new DataContractError("attachment: invalid payload");
  }
  return parsed.data as AttachmentItem;
}
