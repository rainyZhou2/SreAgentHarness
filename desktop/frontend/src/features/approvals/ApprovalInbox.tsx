import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { PendingApproval } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { StatePanel } from "../../components/ui/StatePanel";
import { useApprovalsQuery } from "../../hooks/useApprovalsQuery";
import { useI18n } from "../../i18n/I18nProvider";
import { formatDateTime } from "../../lib/format";
import { qk } from "../../lib/queryKeys";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./ApprovalInbox.css";

interface Decision {
  approval: PendingApproval;
  approve: boolean;
}

export function ApprovalInbox() {
  const { t, locale } = useI18n();
  const { scope } = useConnection();
  const closeApprovals = useUiStore((state) => state.closeApprovals);
  const showNotice = useUiStore((state) => state.showNotice);
  const queryClient = useQueryClient();
  const query = useApprovalsQuery(scope, true);
  const [decision, setDecision] = useState<Decision | null>(null);
  const [reason, setReason] = useState("");
  const [error, setError] = useState<DesktopError | null>(null);

  const mutation = useMutation({
    mutationFn: (next: Decision) =>
      bridge.approveFromInbox(
        next.approval.run_id,
        next.approval.id,
        next.approval.action_hash,
        next.approve,
        reason.trim(),
      ),
    onSuccess: (result, next) => {
      if (result.status === "UNKNOWN") {
        setError(toDesktopError(new Error(t("approvals.conflict"))));
        return;
      }
      setDecision(null);
      setReason("");
      setError(null);
      showNotice(
        next.approve ? t("action.recordedApprove") : t("action.recordedReject"),
      );
      void queryClient.invalidateQueries({ queryKey: qk.approvals(scope) });
      void queryClient.invalidateQueries({ queryKey: qk.projections(scope) });
      void queryClient.invalidateQueries({ queryKey: ["transcript", scope] });
    },
    onError: (cause) => {
      setError(toDesktopError(cause));
      void queryClient.invalidateQueries({ queryKey: qk.approvals(scope) });
    },
  });

  const approvals = query.data?.approvals ?? [];
  const highRisk = approvals.filter((approval) => approval.risk_class === "R2" || approval.risk_class === "R3").length;
  const busy = mutation.isPending;

  return (
    <Dialog title={t("approvals.title")} onClose={closeApprovals} wide className="dialog--approvals">
      {query.isPending ? (
        <StatePanel tone="loading" title={t("approvals.loading")} compact />
      ) : null}
      {query.isError ? (
        <ErrorStateBlock error={toDesktopError(query.error)} onRetry={() => void query.refetch()} />
      ) : null}
      {query.data && approvals.length === 0 ? (
        <StatePanel tone="empty" title={t("approvals.empty")} compact />
      ) : null}
      {query.data && query.data.skipped > 0 ? (
        <p className="note note--warning">{t("common.partialData")}</p>
      ) : null}

      {approvals.length > 0 ? (
        <div className="approval-summary">
          <div>
            <span>{t("approvals.pending")}</span>
            <strong>{approvals.length}</strong>
          </div>
          <div data-tone={highRisk > 0 ? "danger" : undefined}>
            <span>{t("approvals.highRisk")}</span>
            <strong>{highRisk}</strong>
          </div>
        </div>
      ) : null}

      <ul className="approval-list">
        {approvals.map((approval) => {
          const active = decision?.approval.id === approval.id;
          return (
            <li
                key={approval.id}
                className="approval-card"
                data-active={active}
                data-risk={approval.risk_class.toLowerCase()}
              >
              <div className="approval-card__head">
                <span className={`chip risk risk--${approval.risk_class.toLowerCase()}`}>
                  {approval.risk_class}
                </span>
                <strong className="mono">{approval.tool_name}</strong>
                <span className="approval-card__target mono">{approval.target}</span>
                <span className="approval-card__spacer" />
                <span className="note">
                  {t("approvals.expires")}: {formatDateTime(approval.expires_at, locale)}
                </span>
              </div>
              <div className="approval-card__meta">
                {approval.session_title ? (
                  <span>
                    {t("approvals.session")}: {approval.session_title}
                  </span>
                ) : null}
                <span>{t("approvals.required", { count: approval.required_approvals })}</span>
                <span className="mono approval-card__hash" title={approval.action_hash}>
                  {approval.action_hash}
                </span>
              </div>

              {active && decision ? (
                <div className="approval-confirm" role="group">
                  <h3>
                    {decision.approve
                      ? t("approvals.confirmApprove")
                      : t("approvals.confirmReject")}
                  </h3>
                  <p className="note">{t("approvals.note")}</p>
                  <label className="field">
                    <span className="field-label">{t("approvals.reason")}</span>
                    <textarea
                      className="textarea"
                      rows={2}
                      value={reason}
                      onChange={(event) => setReason(event.target.value)}
                      disabled={busy}
                    />
                  </label>
                  {error ? <ErrorMessage error={error} /> : null}
                  <div className="approval-confirm__actions">
                    <button
                      type="button"
                      className={decision.approve ? "btn btn-primary" : "btn btn-danger"}
                      onClick={() => mutation.mutate(decision)}
                      disabled={busy}
                    >
                      {busy
                        ? t("approvals.submitting")
                        : decision.approve
                          ? t("approvals.approve")
                          : t("approvals.reject")}
                    </button>
                    <button
                      type="button"
                      className="btn"
                      onClick={() => {
                        setDecision(null);
                        setReason("");
                        setError(null);
                      }}
                      disabled={busy}
                    >
                      {t("common.cancel")}
                    </button>
                  </div>
                </div>
              ) : (
                <div className="approval-card__actions">
                  <button
                    type="button"
                    className="btn btn-primary btn-sm"
                    onClick={() => {
                      setDecision({ approval, approve: true });
                      setReason("");
                      setError(null);
                    }}
                    disabled={busy}
                  >
                    {t("approvals.approve")}
                  </button>
                  <button
                    type="button"
                    className="btn btn-danger btn-sm"
                    onClick={() => {
                      setDecision({ approval, approve: false });
                      setReason("");
                      setError(null);
                    }}
                    disabled={busy}
                  >
                    {t("approvals.reject")}
                  </button>
                </div>
              )}
            </li>
          );
        })}
      </ul>
    </Dialog>
  );
}

function ErrorStateBlock({
  error,
  onRetry,
}: {
  error: DesktopError;
  onRetry: () => void;
}) {
  const { t } = useI18n();
  return (
    <div role="alert">
      <p className="note note--danger">{t("approvals.error")}</p>
      <ErrorMessage error={error} />
      <button type="button" className="btn btn-sm" onClick={onRetry}>
        {t("common.retry")}
      </button>
    </div>
  );
}
