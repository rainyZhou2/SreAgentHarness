import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { RunStep, RunThought } from "../../bridge/types";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { Icon } from "../../components/ui/Icon";
import { StatePanel } from "../../components/ui/StatePanel";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useI18n } from "../../i18n/I18nProvider";
import type { MessageKey } from "../../i18n/messages";
import { formatDateTime, formatRelativeTime } from "../../lib/format";
import { qk } from "../../lib/queryKeys";
import { normalizeStatus } from "../../lib/status";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./TraceView.css";

const STEP_KIND_KEYS: Record<string, MessageKey> = {
  reason: "trace.stepKind.reason",
  tool: "trace.stepKind.tool",
  retrieve_memory: "trace.stepKind.retrieve_memory",
  compact: "trace.stepKind.compact",
  flow: "trace.stepKind.flow",
  plan: "trace.stepKind.plan",
  plan_review: "trace.stepKind.plan_review",
};

const THOUGHT_KIND_KEYS: Record<string, MessageKey> = {
  plan: "trace.thoughtKind.plan",
  observation: "trace.thoughtKind.observation",
  hypothesis: "trace.thoughtKind.hypothesis",
  evidence: "trace.thoughtKind.evidence",
  decision: "trace.thoughtKind.decision",
  action: "trace.thoughtKind.action",
  verification: "trace.thoughtKind.verification",
  reflection: "trace.thoughtKind.reflection",
  conclusion: "trace.thoughtKind.conclusion",
};

function stepStatusTone(status: string): "positive" | "danger" | "info" | "neutral" {
  switch (normalizeStatus(status)) {
    case "SUCCEEDED":
      return "positive";
    case "FAILED":
      return "danger";
    case "RUNNING":
      return "info";
    default:
      return "neutral";
  }
}

function stepStatusLabel(t: (key: MessageKey) => string, status: string): string {
  const normalized = normalizeStatus(status);
  const key = `trace.status.${normalized}` as MessageKey;
  return t(key);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseSteps(raw: unknown): RunStep[] {
  if (!isRecord(raw) || !Array.isArray(raw.steps)) {
    return [];
  }
  const out: RunStep[] = [];
  for (const item of raw.steps) {
    if (
      !isRecord(item) ||
      typeof item.id !== "string" ||
      typeof item.seq !== "number" ||
      typeof item.kind !== "string" ||
      typeof item.executor_name !== "string" ||
      typeof item.status !== "string"
    ) {
      continue;
    }
    out.push(item as unknown as RunStep);
  }
  return out.sort((left, right) => left.seq - right.seq);
}

function pretty(value: unknown): string {
  if (value === undefined || value === null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function isToolStep(step: RunStep): boolean {
  return normalizeStatus(step.kind) === "TOOL" || step.executor_name.toLowerCase().includes("tool");
}

function TraceThoughtView({ thought }: { thought: RunThought }) {
  const { t } = useI18n();
  const kindKey = THOUGHT_KIND_KEYS[thought.kind] ?? "trace.thoughtKind.unknown";
  return (
    <div className="trace-thought" data-kind={thought.kind}>
      <div className="trace-thought__head">
        <span className="trace-thought__kind">{t(kindKey)}</span>
        {thought.status ? (
          <StatusBadge
            tone={thought.status === "FAILED" || thought.status === "REFUTED" ? "danger" : thought.status === "VERIFIED" || thought.status === "OK" ? "positive" : "neutral"}
            label={thought.status.toLowerCase()}
          />
        ) : null}
        {typeof thought.confidence === "number" && thought.confidence > 0 ? (
          <span className="trace-thought__meta">
            {t("trace.confidence", { value: thought.confidence.toFixed(2) })}
          </span>
        ) : null}
      </div>
      <p className="trace-thought__summary">{thought.summary}</p>
      {thought.refs?.length ? (
        <div className="trace-thought__refs">
          <span className="trace-label">{t("trace.references")}</span>
          {thought.refs.map((ref) => (
            <span className="chip mono" key={`${ref.relation}-${ref.ref}`} title={ref.ref}>
              {ref.relation}: {ref.ref}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function TraceIO({ input, output }: { input: unknown; output: unknown }) {
  const { t } = useI18n();
  const inputText = pretty(input);
  const outputText = pretty(output);
  return (
    <details className="trace-io">
      <summary>
        <Icon name="chevron-right" size={13} />
        {t("trace.io")}
      </summary>
      {inputText ? (
        <div className="trace-io__section">
          <span className="trace-label">{t("trace.toolInput")}</span>
          <pre>{inputText}</pre>
        </div>
      ) : null}
      {outputText ? (
        <div className="trace-io__section">
          <span className="trace-label">{t("trace.toolOutput")}</span>
          <pre>{outputText}</pre>
        </div>
      ) : null}
    </details>
  );
}

function TraceStep({ step }: { step: RunStep }) {
  const { t, locale } = useI18n();
  const kindKey = STEP_KIND_KEYS[step.kind] ?? "trace.stepKind.reason";
  const thoughts = step.thoughts ?? [];
  const tools = isToolStep(step);
  return (
    <article className="trace-step" data-kind={step.kind} data-status={normalizeStatus(step.status)}>
      <div className="trace-step__rail" aria-hidden="true">
        <span className="trace-step__seq mono">{String(step.seq).padStart(2, "0")}</span>
        <span className="trace-step__line" />
      </div>
      <div className="trace-step__body">
        <header className="trace-step__head">
          <span className="trace-step__kind">{t(kindKey)}</span>
          <span className="trace-step__executor mono">{step.executor_name}</span>
          <StatusBadge tone={stepStatusTone(step.status)} label={stepStatusLabel(t, step.status)} />
          {step.created_at ? (
            <time className="trace-step__time" title={formatDateTime(step.created_at, locale)}>
              {formatRelativeTime(step.created_at, locale)}
            </time>
          ) : null}
        </header>

        {thoughts.length === 0 ? (
          <p className="trace-step__note">{t("trace.noThoughts")}</p>
        ) : (
          <div className="trace-thoughts">
            {thoughts.map((thought) => (
              <TraceThoughtView key={`${thought.id ?? thought.seq}-${thought.kind}`} thought={thought} />
            ))}
          </div>
        )}

        {tools ? (
          <div className="trace-tool">
            <div className="trace-tool__title">
              <Icon name="sliders" />
              <span>{step.executor_name}</span>
            </div>
            <TraceIO input={step.input} output={step.output} />
          </div>
        ) : step.input !== undefined || step.output !== undefined ? (
          <TraceIO input={step.input} output={step.output} />
        ) : null}

        {step.capabilities?.length ? (
          <div className="trace-step__capabilities">
            <span className="trace-label">{t("trace.capabilities")}</span>
            {step.capabilities.map((capability) => (
              <span className="chip mono" key={`${capability.role}-${capability.ref}`}>
                {capability.role}: {capability.ref}
              </span>
            ))}
          </div>
        ) : null}
      </div>
    </article>
  );
}

export function TraceView() {
  const { t } = useI18n();
  const { scope } = useConnection();
  const currentSessionId = useUiStore((state) => state.currentSessionId);
  const sessions = useQuery({
    queryKey: qk.sessions(scope),
    queryFn: () => bridge.searchSessions("active", ""),
    enabled: currentSessionId !== null,
    staleTime: 5_000,
  });
  const session = sessions.data?.sessions.find((item) => item.id === currentSessionId);
  const runID = session?.latest_run_id ?? "";
  const trace = useQuery({
    queryKey: qk.runSteps(scope, runID),
    queryFn: () => bridge.runSteps(runID, true),
    enabled: runID !== "",
    staleTime: 1_000,
    refetchInterval: runID === "" ? false : 4_000,
  });
  const steps = useMemo(() => parseSteps(trace.data), [trace.data]);
  const toolCount = steps.filter(isToolStep).length;
  const thoughtCount = steps.reduce((sum, step) => sum + (step.thoughts?.length ?? 0), 0);
  const error: DesktopError | null = trace.error
    ? toDesktopError(trace.error)
    : sessions.error
      ? toDesktopError(sessions.error)
      : null;

  return (
    <div className="trace-view">
      <header className="trace-view__head">
        <div>
          <h2>{t("trace.title")}</h2>
          <p className="note">{t("trace.subtitle")}</p>
        </div>
        {session ? (
          <div className="trace-view__metrics">
            <span className="chip">{t("trace.steps", { count: steps.length })}</span>
            <span className="chip">{t("trace.tools", { count: toolCount })}</span>
            <span className="chip">{t("trace.thoughts", { count: thoughtCount })}</span>
          </div>
        ) : null}
      </header>

      {currentSessionId === null ? (
        <StatePanel tone="empty" title={t("trace.empty")} compact />
      ) : sessions.isPending ? (
        <StatePanel tone="loading" title={t("trace.loading")} compact />
      ) : error ? (
        <div className="trace-view__error" role="alert">
          <p className="note note--danger">{t("trace.error")}</p>
          <ErrorMessage error={error} />
          <button type="button" className="btn btn-sm" onClick={() => void trace.refetch()}>
            {t("common.retry")}
          </button>
        </div>
      ) : runID === "" ? (
        <StatePanel tone="empty" title={t("trace.noRun")} compact />
      ) : trace.isPending ? (
        <StatePanel tone="loading" title={t("trace.loading")} compact />
      ) : steps.length === 0 ? (
        <StatePanel tone="empty" title={t("trace.empty")} compact />
      ) : (
        <>
          <p className="trace-view__hint">{t("trace.reasoningNote")}</p>
          <div className="trace-timeline">
            {steps.map((step) => (
              <TraceStep key={step.id} step={step} />
            ))}
          </div>
        </>
      )}
      {session?.title ? <p className="trace-view__session mono">{session.title}</p> : null}
    </div>
  );
}
