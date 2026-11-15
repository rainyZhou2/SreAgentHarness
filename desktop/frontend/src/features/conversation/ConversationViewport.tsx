import { useEffect, useMemo, useRef } from "react";

import { toDesktopError } from "../../bridge/errors";
import type { TurnView } from "../../bridge/types";
import { ErrorState } from "../../components/ui/ErrorState";
import { SkeletonLines, StatePanel } from "../../components/ui/StatePanel";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useTranscriptQuery } from "../../hooks/useTranscriptQuery";
import { useWatchRun } from "../../hooks/useWatchRun";
import { useI18n } from "../../i18n/I18nProvider";
import { formatRelativeTime } from "../../lib/format";
import { runStatusKey, runStatusTone } from "../../lib/status";
import { useStreamStore } from "../../state/streamStore";
import { useUiStore } from "../../state/uiStore";
import { RunView } from "../agent-run/RunView";
import { useConnection } from "../connect/ConnectionContext";
import { HomeView } from "./HomeView";
import "./ConversationViewport.css";

function UserTurn({ turn }: { turn: TurnView }) {
  const { t } = useI18n();
  return (
    <article className="turn turn--user">
      <header className="turn__role">{t("conversation.you")}</header>
      <p className="turn__user-text">{turn.text}</p>
    </article>
  );
}

export function ConversationViewport() {
  const { t, locale } = useI18n();
  const { scope, offline, lastSyncedAt } = useConnection();
  const currentSessionId = useUiStore((state) => state.currentSessionId);
  const transcript = useTranscriptQuery(scope, currentSessionId);
  const turns = useMemo(() => transcript.data?.turns ?? [], [transcript.data]);
  const latestRunId = useMemo(() => {
    for (let index = turns.length - 1; index >= 0; index -= 1) {
      const runID = turns[index]?.run_id;
      if (runID !== undefined && runID !== "") {
        return runID;
      }
    }
    return undefined;
  }, [turns]);

  const watch = useWatchRun(latestRunId, !offline);
  const streamLength = useStreamStore((state) =>
    latestRunId ? (state.streams[latestRunId]?.text.length ?? 0) : 0,
  );
  const streamStoreError = useStreamStore((state) =>
    latestRunId ? (state.streams[latestRunId]?.error ?? null) : null,
  );

  const scrollRef = useRef<HTMLDivElement>(null);
  const atBottomRef = useRef(true);

  useEffect(() => {
    atBottomRef.current = true;
  }, [currentSessionId]);

  useEffect(() => {
    if (!atBottomRef.current) {
      return;
    }
    const element = scrollRef.current;
    if (element) {
      element.scrollTop = element.scrollHeight;
    }
  }, [turns.length, streamLength, currentSessionId]);

  const error = transcript.error ? toDesktopError(transcript.error) : null;
  const streamError = streamStoreError !== null && streamStoreError !== ""
    ? streamStoreError
    : (watch.error?.message ?? null);
  const syncedAt = lastSyncedAt ?? 0;

  function reconnect() {
    if (latestRunId) {
      useStreamStore.getState().setStreamError(latestRunId, "");
    }
    watch.reconnect();
  }

  if (currentSessionId === null) {
    return (
      <div className="conversation-viewport" ref={scrollRef}>
        <HomeView />
      </div>
    );
  }

  const showStale = transcript.data !== undefined && transcript.isRefetchError;

  return (
    <div
      className="conversation-viewport"
      ref={scrollRef}
      onScroll={() => {
        const element = scrollRef.current;
        if (element) {
          atBottomRef.current =
            element.scrollHeight - element.scrollTop - element.clientHeight < 120;
        }
      }}
    >
      <div className="conversation-thread">
        {offline ? (
          <StatePanel
            tone="offline"
            title={t("conversation.offlineBanner")}
            body={
              syncedAt > 0
                ? t("conversation.lastSynced", {
                    time: formatRelativeTime(new Date(syncedAt).toISOString(), locale),
                  })
                : t("connection.neverSynced")
            }
            compact
          />
        ) : null}

        {showStale ? (
          <StatePanel
            tone="warning"
            title={t("conversation.staleBanner")}
            compact
            action={
              <button type="button" className="btn btn-sm" onClick={() => void transcript.refetch()}>
                {t("common.retry")}
              </button>
            }
          />
        ) : null}

        {transcript.isPending ? (
          <div aria-busy="true">
            <StatePanel tone="loading" title={t("conversation.loading")} compact />
            <SkeletonLines rows={4} />
          </div>
        ) : null}

        {transcript.isError && transcript.data === undefined ? (
          <ErrorState
            error={error ?? toDesktopError(new Error(t("conversation.error")))}
            title={
              error?.kind === "forbidden"
                ? t("conversation.forbidden")
                : error?.kind === "not_found"
                  ? t("conversation.notFound")
                  : t("conversation.error")
            }
            onRetry={() => void transcript.refetch()}
          />
        ) : null}

        {transcript.data?.skippedTurns ? (
          <StatePanel tone="warning" title={t("conversation.partial")} compact />
        ) : null}

        {transcript.data !== undefined && turns.length === 0 ? (
          <StatePanel tone="empty" title={t("conversation.empty")} compact />
        ) : null}

        {turns.map((turn) => (
          <section key={turn.id} className="turn-group">
            {turn.text ? <UserTurn turn={turn} /> : null}
            {turn.run_id ? (
              <article className="turn turn--assistant">
                <header className="turn__role">{t("conversation.agent")}</header>
                <RunView
                  runId={turn.run_id}
                  scope={scope}
                  offline={offline}
                  streamError={turn.run_id === latestRunId ? streamError : null}
                  onReconnectStream={turn.run_id === latestRunId ? reconnect : undefined}
                />
              </article>
            ) : turn.status ? (
              <p className="turn__status">
                <StatusBadge tone={runStatusTone(turn.status)} label={t(runStatusKey(turn.status))} />
              </p>
            ) : null}
          </section>
        ))}
      </div>
    </div>
  );
}
