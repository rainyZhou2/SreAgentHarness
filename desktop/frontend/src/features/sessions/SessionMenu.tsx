import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { SessionSummary } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { Icon } from "../../components/ui/Icon";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { useI18n } from "../../i18n/I18nProvider";
import { qk } from "../../lib/queryKeys";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./SessionMenu.css";

export type SessionMutation = "rename" | "archive" | "unarchive" | "delete";

export function SessionMenu({
  session,
  archived,
  onMutated,
}: {
  session: SessionSummary;
  archived: boolean;
  onMutated: (kind: SessionMutation, sessionID: string) => void;
}) {
  const { t } = useI18n();
  const { scope } = useConnection();
  const queryClient = useQueryClient();
  const showNotice = useUiStore((state) => state.showNotice);
  const [actionsOpen, setActionsOpen] = useState(false);
  const [dialog, setDialog] = useState<"rename" | "delete" | null>(null);
  const [title, setTitle] = useState(session.title ?? "");
  const [error, setError] = useState<DesktopError | null>(null);

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: qk.sessions(scope) });
  };

  const rename = useMutation({
    mutationFn: () => bridge.renameSession(session.id, title.trim()),
    onSuccess: () => {
      setDialog(null);
      setError(null);
      invalidate();
      void queryClient.invalidateQueries({ queryKey: qk.transcript(scope, session.id) });
      onMutated("rename", session.id);
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  const archive = useMutation({
    mutationFn: () => bridge.archiveSession(session.id, !archived),
    onSuccess: () => {
      setActionsOpen(false);
      setError(null);
      invalidate();
      onMutated(archived ? "unarchive" : "archive", session.id);
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  const remove = useMutation({
    mutationFn: () => bridge.deleteSession(session.id),
    onSuccess: () => {
      setDialog(null);
      setActionsOpen(false);
      setError(null);
      invalidate();
      onMutated("delete", session.id);
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  const exportChat = useMutation({
    mutationFn: () => bridge.exportChat(session.id),
    onSuccess: (path) => {
      setActionsOpen(false);
      setError(null);
      if (path !== "") {
        showNotice(t("sidebar.exported", { path }));
      }
    },
    onError: (cause) => {
      setActionsOpen(false);
      const normalized = toDesktopError(cause);
      setError(normalized);
      showNotice(`${t("sidebar.actionFailed")}: ${normalized.message}`);
    },
  });

  const busy = rename.isPending || archive.isPending || remove.isPending || exportChat.isPending;

  return (
    <div className="session-menu">
      <button
        type="button"
        className="session-menu__trigger"
        aria-label={t("sidebar.actions")}
        aria-haspopup="dialog"
        aria-expanded={actionsOpen}
        onClick={(event) => {
          event.stopPropagation();
          setError(null);
          setActionsOpen(true);
        }}
      >
        <Icon name="more" />
      </button>

      {actionsOpen ? (
        <Dialog title={t("sidebar.actions")} onClose={() => setActionsOpen(false)}>
          <div className="session-menu__actions">
            <button
              type="button"
              onClick={() => {
                setTitle(session.title ?? "");
                setActionsOpen(false);
                setDialog("rename");
              }}
            >
              {t("sidebar.rename")}
            </button>
            <button
              type="button"
              onClick={() => archive.mutate()}
              disabled={busy}
            >
              {archive.isPending
                ? t("common.submitting")
                : archived
                  ? t("sidebar.unarchive")
                  : t("sidebar.archive")}
            </button>
            <button
              type="button"
              onClick={() => exportChat.mutate()}
              disabled={busy}
            >
              {exportChat.isPending ? t("common.submitting") : t("sidebar.export")}
            </button>
            <button
              type="button"
              className="session-menu__danger"
              onClick={() => {
                setActionsOpen(false);
                setDialog("delete");
              }}
            >
              {t("sidebar.delete")}
            </button>
          </div>
          {error ? <ErrorMessage error={error} /> : null}
        </Dialog>
      ) : null}

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
                disabled={rename.isPending}
                maxLength={200}
              />
            </label>
            {error ? <ErrorMessage error={error} /> : null}
            <div className="dialog-actions">
              <button
                type="button"
                className="btn"
                onClick={() => setDialog(null)}
                disabled={rename.isPending}
              >
                {t("common.cancel")}
              </button>
              <button
                type="submit"
                className="btn btn-primary"
                disabled={rename.isPending || title.trim() === ""}
              >
                {rename.isPending ? t("common.submitting") : t("common.save")}
              </button>
            </div>
          </form>
        </Dialog>
      ) : null}

      {dialog === "delete" ? (
        <Dialog title={t("sidebar.deleteTitle")} onClose={() => setDialog(null)}>
          <p>{t("sidebar.deleteBody")}</p>
          {error ? <ErrorMessage error={error} /> : null}
          <div className="dialog-actions">
            <button
              type="button"
              className="btn"
              onClick={() => setDialog(null)}
              disabled={remove.isPending}
            >
              {t("common.cancel")}
            </button>
            <button
              type="button"
              className="btn btn-danger"
              onClick={() => remove.mutate()}
              disabled={remove.isPending}
            >
              {remove.isPending ? t("common.submitting") : t("sidebar.delete")}
            </button>
          </div>
        </Dialog>
      ) : null}
    </div>
  );
}
