import { useEffect, useState } from "react";

import { toDesktopError } from "../../bridge/errors";
import { ErrorState } from "../../components/ui/ErrorState";
import { SkeletonLines, StatePanel } from "../../components/ui/StatePanel";
import { useI18n } from "../../i18n/I18nProvider";
import { useProjectionQuery } from "../../hooks/useProjectionQuery";
import { indexRunSession } from "../../hooks/useRunEvents";
import { runStatusKey } from "../../lib/status";
import { assessProjectionCompatibility } from "../../lib/version";
import { useStreamStore } from "../../state/streamStore";
import { BlockRenderer } from "../blocks/BlockRenderer";
import { RunControls } from "./RunControls";
import "./RunView.css";

interface DecisionNotice {
  actionID: string;
  decision: "approve" | "reject";
}

export function RunView({
  runId,
  scope,
  offline,
  streamError,
  onReconnectStream,
}: {
  runId: string;
  scope: string;
  offline: boolean;
  streamError?: string | null;
  onReconnectStream?: () => void;
}) {
  const { t } = useI18n();
  const query = useProjectionQuery(scope, runId);
  const stream = useStreamStore((state) => state.streams[runId]);
  const [notices, setNotices] = useState<DecisionNotice[]>([]);

  const projection = query.data;
  useEffect(() => {
    if (projection) {
      indexRunSession(runId, projection.sessionId);
    }
  }, [projection, runId]);

  if (query.isPending) {
    return (
      <div className="run-view" aria-busy="true">
        <StatePanel tone="loading" title={t("run.loading")} compact />
        <SkeletonLines rows={2} compact />
      </div>
    );
  }

  if (query.isError || !projection) {
    return (
      <div className="run-view">
        <ErrorState
          error={toDesktopError(query.error)}
          onRetry={() => void query.refetch()}
          compact
        />
      </div>
    );
  }

  const sortedBlocks = [...projection.blocks].sort((left, right) => left.ordinal - right.ordinal);
  const invalidBlocks = sortedBlocks.filter((block) => block.kind === "invalid").length;
  const compatibility = assessProjectionCompatibility(projection);
  const statusLabel = t(runStatusKey(projection.status));
  const streamText = stream?.text ?? "";

  return (
    <div className="run-view">
      <p className="visually-hidden" aria-live="polite">
        {statusLabel}
      </p>

      {compatibility.newerServer || compatibility.clientOutdated ? (
        <StatePanel
          tone="warning"
          title={t("run.incompatible")}
          body={compatibility.clientOutdated ? t("run.minimumClient") : undefined}
          compact
        />
      ) : null}

      {invalidBlocks > 0 ? (
        <StatePanel tone="warning" title={t("run.partial")} compact />
      ) : null}

      {streamError ? (
        <StatePanel
          tone="warning"
          title={t("run.streamError")}
          compact
          action={
            onReconnectStream ? (
              <button type="button" className="btn btn-sm" onClick={onReconnectStream}>
                {t("run.reconnect")}
              </button>
            ) : null
          }
        />
      ) : null}

      <div className="run-view__blocks">
        {sortedBlocks.length === 0 ? (
          <StatePanel tone="empty" title={t("run.empty")} compact />
        ) : (
          sortedBlocks.map((block) => (
            <BlockRenderer
              key={block.id}
              block={block}
              runId={runId}
              runVersion={projection.runVersion}
              scope={scope}
              offline={offline}
              onActionRecorded={(actionID, decision) =>
                setNotices((current) => [...current, { actionID, decision }])
              }
            />
          ))
        )}
      </div>

      {streamText !== "" ? (
        <div className="run-stream" aria-live="off">
          <span className="run-stream__label">{t("run.responding")}</span>
          <p className="run-stream__text">{streamText}</p>
        </div>
      ) : null}

      {notices.map((notice) => (
        <p
          key={`${notice.actionID}-${notice.decision}`}
          className="run-view__notice"
          role="status"
        >
          {notice.decision === "approve" ? t("action.recordedApprove") : t("action.recordedReject")}
          <span className="mono run-view__notice-id">{notice.actionID}</span>
        </p>
      ))}

      <RunControls
        runId={runId}
        runVersion={projection.runVersion}
        status={projection.status}
        scope={scope}
        offline={offline}
      />
    </div>
  );
}
