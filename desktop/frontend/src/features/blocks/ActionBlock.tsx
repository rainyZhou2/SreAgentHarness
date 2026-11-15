import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { ActionBlockData } from "../../bridge/types";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useI18n } from "../../i18n/I18nProvider";
import { formatDateTime } from "../../lib/format";
import { qk } from "../../lib/queryKeys";
import {
  actionStateKey,
  actionStateTone,
  isApprovalWaiting,
  isEffectUnknown,
  isManualRecovery,
  normalizeStatus,
} from "../../lib/status";
import { useUiStore } from "../../state/uiStore";
import "./ActionBlock.css";

type Decision = "approve" | "reject";

type Outcome =
  | { kind: "confirmed"; decision: Decision }
  | { kind: "failed"; error: DesktopError; decision: Decision }
  | { kind: "conflict"; error: DesktopError; decision: Decision }
  | { kind: "unknown"; message: string };

export interface ActionBlockProps {
  data: ActionBlockData;
  runId: string;
  /** Authoritative server run version used as `expected_version`. */
  runVersion: number;
  scope: string;
  offline: boolean;
  onRecorded?: (actionID: string, decision: Decision) => void;
}

function isExpired(expiresAt: string | null | undefined): boolean {
  if (!expiresAt) {
    return false;
  }
  const time = Date.parse(expiresAt);
  return Number.isFinite(time) && time < Date.now();
}

/**
 * Server-authoritative approval card. The card never shows a success state
 * before the server confirms the decision, and never offers a retry for an
 * unknown effect (design §2.11.3, §4.3.3).
 */
export function ActionBlock({
  data,
  runId,
  runVersion,
  scope,
  offline,
  onRecorded,
}: ActionBlockProps) {
  const { t, locale } = useI18n();
  const queryClient = useQueryClient();
  const openDrawer = useUiStore((state) => state.openDrawer);
  const [pending, setPending] = useState<Decision | null>(null);
  const [reason, setReason] = useState("");
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [copied, setCopied] = useState(false);

  const actionHash = data.action_hash ?? data.intent_hash;
  const approvalID = data.approval_id;
  const controls = data.controls;
  const state = normalizeStatus(data.state);
  const waiting = isApprovalWaiting(data.state);
  const expired = state === "EXPIRED" || isExpired(data.expires_at);
  const effectUnknown = isEffectUnknown(data.state);
  const manualRecovery = isManualRecovery(data.state);
  const mayHaveDispatched = data.may_have_dispatched === true;
  const controlAllowed = (control: Decision): boolean =>
    Array.isArray(controls) && controls.includes(control);
  const canAct = waiting && !expired && approvalID !== undefined && actionHash !== "" && !offline;

  const mutation = useMutation({
    mutationFn: (variables: { decision: Decision; reason: string }) => {
      const payload: Record<string, unknown> = {
        approval_request_id: approvalID ?? "",
        action_hash: actionHash,
      };
      const type = variables.decision === "approve" ? "approve_tool" : "reject_tool";
      return bridge.command(runId, type, payload, variables.reason, runVersion);
    },
    onSuccess: (result, variables) => {
      setPending(null);
      setReason("");
      if (result.status === "UNKNOWN") {
        setOutcome({ kind: "unknown", message: t("run.controlUnknown") });
      } else {
        setOutcome({ kind: "confirmed", decision: variables.decision });
        onRecorded?.(data.action_id, variables.decision);
      }
      void queryClient.invalidateQueries({ queryKey: qk.projection(scope, runId) });
      void queryClient.invalidateQueries({
        predicate: (query) =>
          query.queryKey[0] === "transcript" && query.queryKey[1] === scope,
      });
    },
    onError: (error, variables) => {
      const normalized = toDesktopError(error);
      setPending(null);
      if (normalized.kind === "conflict") {
        setOutcome({ kind: "conflict", error: normalized, decision: variables.decision });
      } else if (normalized.kind === "effect_unknown") {
        setOutcome({ kind: "unknown", message: normalized.message });
      } else {
        setOutcome({ kind: "failed", error: normalized, decision: variables.decision });
      }
      // Re-read the authoritative action state before the user decides again.
      void queryClient.invalidateQueries({ queryKey: qk.projection(scope, runId) });
    },
  });

  async function copyHash() {
    if (typeof navigator === "undefined" || !navigator.clipboard) {
      return;
    }
    try {
      await navigator.clipboard.writeText(actionHash);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  }

  function confirmDecision() {
    if (!pending) {
      return;
    }
    mutation.mutate({ decision: pending, reason: reason.trim() });
  }

  return (
    <section
      className="action-block block-card"
      aria-labelledby={`action-title-${data.action_id}`}
      data-tone={actionStateTone(data.state)}
    >
      <div className="block-head">
        <span className="block-title" id={`action-title-${data.action_id}`}>
          {t("action.title")}
        </span>
        <div className="action-block__badges">
          <StatusBadge tone={actionStateTone(data.state)} label={t(actionStateKey(data.state))} />
          <span className="chip action-block__risk">{data.risk}</span>
        </div>
      </div>

      <dl className="kv-list">
        <dt className="kv-key">{t("action.target")}</dt>
        <dd className="kv-value mono">{data.target}</dd>

        <dt className="kv-key">{t("action.intentHash")}</dt>
        <dd className="kv-value">
          <span className="action-block__hash mono" title={actionHash}>
            {actionHash}
          </span>
          <button type="button" className="btn btn-ghost btn-sm" onClick={() => void copyHash()}>
            {copied ? t("common.copied") : t("common.copy")}
          </button>
        </dd>

        <dt className="kv-key">{t("action.requiredApprovals")}</dt>
        <dd className="kv-value">{data.required_approvals}</dd>

        {data.expires_at ? (
          <>
            <dt className="kv-key">{t("action.expiresAt")}</dt>
            <dd className={expired ? "kv-value note--danger" : "kv-value"}>
              {formatDateTime(data.expires_at, locale)}
              {expired ? ` · ${t("action.expired")}` : ""}
            </dd>
          </>
        ) : null}

        <dt className="kv-key mono">{t("drawer.action")}</dt>
        <dd className="kv-value mono">{data.action_id}</dd>
      </dl>

      {mayHaveDispatched ? (
        <p className="action-block__warning" role="alert">
          {t("action.mayHaveDispatched")}
        </p>
      ) : null}

      {effectUnknown ? (
        <div className="action-block__danger" role="alert">
          <strong>{t("action.effectUnknown")}</strong>
          <p>{t("action.unknownEffectHint")}</p>
        </div>
      ) : null}

      {manualRecovery ? (
        <div className="action-block__danger" role="alert">
          <strong>{t("action.manualRecovery")}</strong>
          <p>{t("action.unknownEffectHint")}</p>
        </div>
      ) : null}

      {outcome?.kind === "confirmed" ? (
        <p className="action-block__success" role="status">
          {outcome.decision === "approve"
            ? t("action.recordedApprove")
            : t("action.recordedReject")}
        </p>
      ) : null}

      {outcome?.kind === "conflict" ? (
        <div className="action-block__notice" role="alert">
          <strong>{t("action.conflict")}</strong>
          <ErrorMessage error={outcome.error} />
        </div>
      ) : null}

      {outcome?.kind === "failed" ? (
        <div className="action-block__notice" role="alert">
          <ErrorMessage error={outcome.error} />
          <p className="note">{t("action.failed")}</p>
        </div>
      ) : null}

      {outcome?.kind === "unknown" ? (
        <div className="action-block__danger" role="alert">
          <strong>{t("run.controlUnknown")}</strong>
          <p>{outcome.message}</p>
        </div>
      ) : null}

      {pending ? (
        <div className="action-confirm" role="group" aria-label={t("action.controls")}>
          <h3>
            {pending === "approve" ? t("action.confirmApproveTitle") : t("action.confirmRejectTitle")}
          </h3>
          <p className="note">
            {pending === "approve" ? t("action.confirmApproveBody") : t("action.confirmRejectBody")}
          </p>
          <p className="action-confirm__target mono">{data.target}</p>
          <p className="action-confirm__hash mono" title={actionHash}>
            {actionHash}
          </p>
          <label className="field">
            <span className="field-label">{t("action.reason")}</span>
            <textarea
              className="textarea"
              rows={2}
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              placeholder={t("action.reasonPlaceholder")}
              disabled={mutation.isPending}
            />
          </label>
          <div className="block-actions">
            <button
              type="button"
              className={pending === "approve" ? "btn btn-primary btn-sm" : "btn btn-danger btn-sm"}
              onClick={confirmDecision}
              disabled={mutation.isPending}
            >
              {mutation.isPending
                ? t("action.submitting")
                : pending === "approve"
                  ? t("action.confirmApprove")
                  : t("action.confirmReject")}
            </button>
            <button
              type="button"
              className="btn btn-sm"
              onClick={() => setPending(null)}
              disabled={mutation.isPending}
            >
              {t("common.cancel")}
            </button>
          </div>
        </div>
      ) : null}

      {canAct && (controlAllowed("approve") || controlAllowed("reject")) ? (
        <div className="block-actions">
          {controlAllowed("approve") ? (
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => {
                setOutcome(null);
                setPending("approve");
              }}
              disabled={mutation.isPending || pending !== null}
            >
              {t("action.approve")}
            </button>
          ) : null}
          {controlAllowed("reject") ? (
            <button
              type="button"
              className="btn btn-danger"
              onClick={() => {
                setOutcome(null);
                setPending("reject");
              }}
              disabled={mutation.isPending || pending !== null}
            >
              {t("action.reject")}
            </button>
          ) : null}
          <button
            type="button"
            className="btn btn-ghost"
            onClick={() => openDrawer({ type: "action", runId, runVersion, block: data })}
          >
            {t("action.inspect")}
          </button>
        </div>
      ) : (
        <div className="block-actions">
          {waiting && approvalID === undefined ? (
            <p className="note">{t("action.readOnlyNote")}</p>
          ) : null}
          {waiting && approvalID !== undefined && expired ? (
            <p className="note note--danger">{t("action.expired")}</p>
          ) : null}
          {waiting && approvalID !== undefined && !expired && offline ? (
            <p className="note note--warning">{t("connection.offline")}</p>
          ) : null}
          {waiting &&
          approvalID !== undefined &&
          !expired &&
          !offline &&
          controls !== undefined &&
          !controlAllowed("approve") &&
          !controlAllowed("reject") ? (
            <p className="note">{t("action.noControls")}</p>
          ) : null}
          <button
            type="button"
            className="btn btn-ghost btn-sm"
            onClick={() => openDrawer({ type: "action", runId, runVersion, block: data })}
          >
            {t("action.inspect")}
          </button>
        </div>
      )}
    </section>
  );
}
