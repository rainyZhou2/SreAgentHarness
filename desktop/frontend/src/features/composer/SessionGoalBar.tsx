import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { SessionSummary } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { Icon } from "../../components/ui/Icon";
import { useI18n } from "../../i18n/I18nProvider";
import { qk } from "../../lib/queryKeys";
import { isTerminalRunStatus, normalizeStatus } from "../../lib/status";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./SessionGoalBar.css";

const BLOCKED_STATUSES = new Set([
  "PAUSED",
  "PAUSE_REQUESTED",
  "WAITING_APPROVAL",
  "WAITING_INPUT",
  "WAITING_CHILDREN",
  "NEEDS_HUMAN",
  "FAILED",
  "CANCELLED",
]);

function statusMeta(status: string | undefined): { key: "running" | "blocked" | "done" | "idle"; tone: string } {
  if (!status) {
    return { key: "idle", tone: "neutral" };
  }
  const normalized = normalizeStatus(status);
  if (isTerminalRunStatus(normalized)) {
    return normalized === "COMPLETED"
      ? { key: "done", tone: "positive" }
      : { key: "blocked", tone: "danger" };
  }
  if (BLOCKED_STATUSES.has(normalized)) {
    return { key: "blocked", tone: "warning" };
  }
  return { key: "running", tone: "info" };
}

export function SessionGoalBar({
  session,
  status,
}: {
  session: SessionSummary | undefined;
  status: string | undefined;
}) {
  const { t } = useI18n();
  const { scope } = useConnection();
  const queryClient = useQueryClient();
  const setCurrentSession = useUiStore((state) => state.setCurrentSession);
  const showNotice = useUiStore((state) => state.showNotice);
  const [dialog, setDialog] = useState<"rename" | "delete" | null>(null);
  const [title, setTitle] = useState(session?.title ?? "");
  const [error, setError] = useState<DesktopError | null>(null);

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: qk.sessions(scope) });
    if (session) {
      void queryClient.invalidateQueries({ queryKey: qk.transcript(scope, session.id) });
    }
  };

  const rename = useMutation({
    mutationFn: () => bridge.renameSession(session?.id ?? "", title.trim()),
    onSuccess: () => {
      setDialog(null);
      setError(null);
      invalidate();
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  const remove = useMutation({
    mutationFn: () => bridge.deleteSession(session?.id ?? ""),
    onSuccess: () => {
      setDialog(null);
      setError(null);
      setCurrentSession(null);
      invalidate();
      showNotice(t("sidebar.deleted"));
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  if (!session) {
    return null;
  }

  const meta = statusMeta(status);
  const sessionTitle =
    session.title && session.title.trim() !== "" ? session.title : t("session.untitled");

  function jumpToLatest() {
    const viewport = document.querySelector<HTMLElement>(".conversation-viewport");
    viewport?.scrollTo({ top: viewport.scrollHeight, behavior: "smooth" });
  }

  return (
    <>
      <div className="goal-bar" aria-label={t("goal.label")}>
        <span className="goal-bar__mark" aria-hidden="true">
          <Icon name="target" />
        </span>
        <span className="goal-bar__status" data-tone={meta.tone}>
          {t(`goal.status.${meta.key}`)}
        </span>
        <span className="goal-bar__title" title={sessionTitle}>
          {sessionTitle}
        </span>
        <div className="goal-bar__actions">
          <button
            type="button"
            className="goal-bar__action"
            onClick={() => {
              setTitle(session.title ?? "");
              setError(null);
              setDialog("rename");
            }}
            aria-label={t("sidebar.rename")}
            title={t("sidebar.rename")}
          >
            <Icon name="pencil" />
          </button>
          <button
            type="button"
            className="goal-bar__action"
            onClick={() => {
              setError(null);
              setDialog("delete");
            }}
            aria-label={t("sidebar.delete")}
            title={t("sidebar.delete")}
          >
            <Icon name="trash" />
          </button>
          <button
            type="button"
            className="goal-bar__action"
            onClick={jumpToLatest}
            aria-label={t("goal.jump")}
            title={t("goal.jump")}
          >
            <Icon name="target" />
          </button>
        </div>
      </div>

      {dialog === "rename" ? (
        <Dialog title={t("sidebar.renameTitle")} onClose={() => setDialog(null)}>
          <form
            onSubmit={(event) => {
              event.preventDefault();
              if (title.trim() !== "") {
                rename.mutate();
              }
            }}
          >
            <label className="field">
              <span className="field-label">{t("sidebar.renameLabel")}</span>
              <input
                className="input"
                value={title}
                onChange={(event) => setTitle(event.target.value)}
                autoFocus
                maxLength={200}
                disabled={rename.isPending}
              />
            </label>
            {error ? <ErrorMessage error={error} /> : null}
            <div className="dialog-actions">
              <button type="button" className="btn btn-sm" onClick={() => setDialog(null)}>
                {t("common.cancel")}
              </button>
              <button type="submit" className="btn btn-primary btn-sm" disabled={rename.isPending || title.trim() === ""}>
                {rename.isPending ? t("common.submitting") : t("common.save")}
              </button>
            </div>
          </form>
        </Dialog>
      ) : null}

      {dialog === "delete" ? (
        <Dialog title={t("sidebar.deleteTitle")} onClose={() => setDialog(null)}>
          <p className="note">{t("sidebar.deleteBody")}</p>
          {error ? <ErrorMessage error={error} /> : null}
          <div className="dialog-actions">
            <button type="button" className="btn btn-sm" onClick={() => setDialog(null)}>
              {t("common.cancel")}
            </button>
            <button
              type="button"
              className="btn btn-danger btn-sm"
              onClick={() => remove.mutate()}
              disabled={remove.isPending}
            >
              {remove.isPending ? t("common.submitting") : t("sidebar.delete")}
            </button>
          </div>
        </Dialog>
      ) : null}
    </>
  );
}
