import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { useI18n } from "../../i18n/I18nProvider";
import { qk } from "../../lib/queryKeys";
import { isActiveRunStatus, isTerminalRunStatus, normalizeStatus } from "../../lib/status";
import "./RunControls.css";

type RunCommand = "pause" | "resume" | "cancel";

type ControlNotice =
  | { kind: "accepted" }
  | { kind: "conflict"; error: DesktopError }
  | { kind: "unknown"; error: DesktopError }
  | { kind: "failed"; error: DesktopError };

export function RunControls({
  runId,
  runVersion,
  status,
  scope,
  offline,
}: {
  runId: string;
  runVersion: number;
  status: string;
  scope: string;
  offline: boolean;
}) {
  const { t } = useI18n();
  const queryClient = useQueryClient();
  const [confirmingStop, setConfirmingStop] = useState(false);
  const [notice, setNotice] = useState<ControlNotice | null>(null);
  const normalized = normalizeStatus(status);

  const mutation = useMutation({
    mutationFn: (command: RunCommand) => bridge.command(runId, command, {}, "", runVersion),
    onSuccess: () => {
      setNotice({ kind: "accepted" });
      setConfirmingStop(false);
      void queryClient.invalidateQueries({ queryKey: qk.projection(scope, runId) });
      void queryClient.invalidateQueries({
        predicate: (query) =>
          query.queryKey[0] === "transcript" && query.queryKey[1] === scope,
      });
    },
    onError: (error) => {
      setConfirmingStop(false);
      const normalizedError = toDesktopError(error);
      if (normalizedError.kind === "conflict") {
        setNotice({ kind: "conflict", error: normalizedError });
      } else if (
        normalizedError.kind === "effect_unknown" ||
        normalizedError.kind === "unknown"
      ) {
        setNotice({ kind: "unknown", error: normalizedError });
      } else {
        setNotice({ kind: "failed", error: normalizedError });
      }
      void queryClient.invalidateQueries({ queryKey: qk.projection(scope, runId) });
    },
  });

  if (isTerminalRunStatus(status)) {
    return null;
  }

  const canPause = normalized === "RUNNING";
  const canResume = normalized === "PAUSED";
  const canStop = isActiveRunStatus(status) && normalized !== "CANCELLING";
  const busy = mutation.isPending;

  return (
    <div className="run-controls" aria-label={t("run.controls")} role="group">
      <div className="run-controls__buttons">
        {canPause ? (
          <button
            type="button"
            className="btn btn-sm"
            onClick={() => mutation.mutate("pause")}
            disabled={busy || offline}
          >
            {busy ? t("run.controlPending") : t("run.pause")}
          </button>
        ) : null}
        {canResume ? (
          <button
            type="button"
            className="btn btn-sm"
            onClick={() => mutation.mutate("resume")}
            disabled={busy || offline}
          >
            {busy ? t("run.controlPending") : t("run.resume")}
          </button>
        ) : null}
        {canStop ? (
          <button
            type="button"
            className="btn btn-sm run-controls__stop"
            onClick={() => setConfirmingStop(true)}
            disabled={busy || offline}
          >
            {t("run.stop")}
          </button>
        ) : null}
      </div>

      {offline ? <p className="note note--warning">{t("connection.offline")}</p> : null}

      {confirmingStop ? (
        <div className="run-controls__confirm" role="group" aria-label={t("run.stopConfirmTitle")}>
          <strong>{t("run.stopConfirmTitle")}</strong>
          <p className="note">{t("run.stopConfirmBody")}</p>
          <div className="run-controls__buttons">
            <button
              type="button"
              className="btn btn-danger btn-sm"
              onClick={() => mutation.mutate("cancel")}
              disabled={busy}
            >
              {busy ? t("run.controlPending") : t("run.stopConfirm")}
            </button>
            <button
              type="button"
              className="btn btn-sm"
              onClick={() => setConfirmingStop(false)}
              disabled={busy}
            >
              {t("run.keepRunning")}
            </button>
          </div>
        </div>
      ) : null}

      {notice?.kind === "accepted" ? (
        <p className="run-controls__accepted" role="status">
          {t("run.controlAccepted")}
        </p>
      ) : null}
      {notice?.kind === "conflict" ? (
        <p className="note note--warning" role="alert">
          {t("run.controlConflict")}
        </p>
      ) : null}
      {notice?.kind === "failed" ? (
        <div role="alert">
          <p className="note note--danger">{t("run.controlFailed")}</p>
          <ErrorMessage error={notice.error} />
        </div>
      ) : null}
      {notice?.kind === "unknown" ? (
        <div role="alert">
          <p className="note note--danger">{t("run.controlUnknown")}</p>
          <ErrorMessage error={notice.error} />
        </div>
      ) : null}
    </div>
  );
}
