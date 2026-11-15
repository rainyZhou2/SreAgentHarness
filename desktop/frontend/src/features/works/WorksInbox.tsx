import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { WorkAction, WorkSummary, WorkViewData } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { StatePanel } from "../../components/ui/StatePanel";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useWorkViewQuery, useWorksQuery, type WorksFilter } from "../../hooks/useWorksQuery";
import { useI18n } from "../../i18n/I18nProvider";
import type { MessageKey } from "../../i18n/messages";
import { formatRelativeTime } from "../../lib/format";
import { qk } from "../../lib/queryKeys";
import { workStatusKey, workStatusTone } from "../../lib/status";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./WorksInbox.css";

const FILTERS: WorksFilter[] = ["active", "closed", "all"];
const SAFE_CONTROLS = new Set(["pause", "resume", "wake", "reopen"]);

const CONTROL_LABELS: Record<string, MessageKey> = {
  pause: "works.pause",
  resume: "works.resume",
  wake: "works.wake",
  reopen: "works.reopen",
};

function controlLabel(control: string): MessageKey {
  return CONTROL_LABELS[control] ?? "works.controls";
}

function goalText(work: WorkSummary): string {
  const description = work.goal?.description?.trim();
  if (description) {
    return description;
  }
  return work.goal?.intent ?? "";
}

export function WorksInbox() {
  const { t, locale } = useI18n();
  const { scope } = useConnection();
  const closeWorks = useUiStore((state) => state.closeWorks);
  const showNotice = useUiStore((state) => state.showNotice);
  const queryClient = useQueryClient();
  const [filter, setFilter] = useState<WorksFilter>("active");
  const [selected, setSelected] = useState<string | null>(null);
  const [commandError, setCommandError] = useState<DesktopError | null>(null);
  const query = useWorksQuery(scope, true, filter);
  const view = useWorkViewQuery(scope, selected);
  const works = query.data?.items ?? [];

  useEffect(() => {
    if (works.length === 0) {
      setSelected(null);
      return;
    }
    if (selected === null || !works.some((work) => work.id === selected)) {
      setSelected(works[0]?.id ?? null);
    }
  }, [works, selected]);

  const command = useMutation({
    mutationFn: (control: string) => {
      const work = view.data?.work;
      return bridge.workCommand(work?.id ?? selected ?? "", control, work?.version ?? 0);
    },
    onSuccess: (result) => {
      if (result.status === "UNKNOWN") {
        setCommandError(toDesktopError(new Error(t("works.commandFailed"))));
        return;
      }
      setCommandError(null);
      showNotice(t("works.commandAccepted"));
      void queryClient.invalidateQueries({ queryKey: qk.works(scope) });
      if (selected !== null) {
        void queryClient.invalidateQueries({ queryKey: qk.work(scope, selected) });
      }
    },
    onError: (cause) => {
      const normalized = toDesktopError(cause);
      setCommandError(normalized);
      showNotice(`${t("works.commandFailed")}: ${normalized.message}`);
    },
  });

  const detail: WorkViewData | undefined = view.data;
  const work = detail?.work ?? works.find((item) => item.id === selected);
  const controls = (detail?.controls ?? []).filter((control) => SAFE_CONTROLS.has(control));
  const actions = detail?.actions ?? [];

  return (
    <Dialog title={t("works.title")} onClose={closeWorks} wide>
      <div className="works-inbox">
        <aside className="works-list" aria-label={t("works.title")}>
          <div className="works-list__filters" role="tablist">
            {FILTERS.map((value) => (
              <button
                key={value}
                type="button"
                role="tab"
                aria-selected={filter === value}
                className="works-list__filter"
                onClick={() => setFilter(value)}
              >
                {value === "active"
                  ? t("works.filterActive")
                  : value === "closed"
                    ? t("works.filterClosed")
                    : t("works.filterAll")}
              </button>
            ))}
          </div>

          {query.isPending ? (
            <StatePanel tone="loading" title={t("works.loading")} compact />
          ) : null}
          {query.isError ? (
            <div role="alert">
              <p className="note note--danger">{t("works.error")}</p>
              <ErrorMessage error={toDesktopError(query.error)} />
              <button type="button" className="btn btn-sm" onClick={() => void query.refetch()}>
                {t("common.retry")}
              </button>
            </div>
          ) : null}
          {query.data?.skipped ? (
            <p className="note note--warning">{t("works.partial")}</p>
          ) : null}
          {query.data && works.length === 0 ? (
            <StatePanel tone="empty" title={t("works.empty")} compact />
          ) : null}

          <ul className="works-list__items">
            {works.map((item) => (
              <li key={item.id}>
                <button
                  type="button"
                  className="works-list__item"
                  data-active={selected === item.id}
                  aria-current={selected === item.id ? "true" : undefined}
                  onClick={() => {
                    setSelected(item.id);
                    setCommandError(null);
                  }}
                >
                  <span className="works-list__title">{item.title || item.id}</span>
                  <span className="works-list__meta">
                    <StatusBadge
                      tone={workStatusTone(item.status)}
                      label={t(workStatusKey(item.status))}
                    />
                    <span>{item.kind}</span>
                    <span>{formatRelativeTime(item.updated_at, locale)}</span>
                  </span>
                  {item.reason ? <span className="works-list__reason">{item.reason}</span> : null}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <section className="works-detail" aria-label={t("works.title")}>
          {detail === undefined && selected !== null && view.isPending ? (
            <StatePanel tone="loading" title={t("works.loading")} compact />
          ) : null}
          {view.isError ? (
            <div role="alert">
              <p className="note note--danger">{t("works.error")}</p>
              <ErrorMessage error={toDesktopError(view.error)} />
              <button type="button" className="btn btn-sm" onClick={() => void view.refetch()}>
                {t("common.retry")}
              </button>
            </div>
          ) : null}
          {work === undefined ? (
            <StatePanel tone="empty" title={t("works.select")} compact />
          ) : (
            <>
              <header className="works-detail__head">
                <div>
                  <h3>{work.title || work.id}</h3>
                  <p className="note mono">{work.id}</p>
                </div>
                <StatusBadge tone={workStatusTone(work.status)} label={t(workStatusKey(work.status))} />
              </header>

              <div className="works-detail__meta">
                <span>{work.kind}</span>
                <span>{t("works.updated")}: {formatRelativeTime(work.updated_at, locale)}</span>
                <span>{t("works.runs")}: {work.runs_started ?? 0}</span>
                {work.version !== undefined ? <span>v{work.version}</span> : null}
              </div>

              {goalText(work) !== "" ? (
                <section>
                  <h4>{t("works.goal")}</h4>
                  <p>{goalText(work)}</p>
                  {work.goal?.subject_refs?.length ? (
                    <p className="works-detail__subjects">
                      {t("works.subjects")}:{" "}
                      {work.goal.subject_refs.map((ref) => (
                        <span key={ref} className="chip mono">{ref}</span>
                      ))}
                    </p>
                  ) : null}
                </section>
              ) : null}

              {detail?.waiting_on ? (
                <p className="works-detail__waiting">{t("work.waitingOn")}: {detail.waiting_on}</p>
              ) : null}

              {detail?.stages?.length ? (
                <section>
                  <h4>{t("works.stages")}</h4>
                  <ol className="works-stages">
                    {detail.stages.map((stage) => (
                      <li key={stage.id} data-status={stage.status}>
                        <span className="works-stages__dot" aria-hidden="true" />
                        <span>{stage.id}</span>
                        {stage.phase ? <span className="note">{stage.phase}</span> : null}
                      </li>
                    ))}
                  </ol>
                </section>
              ) : null}

              {controls.length > 0 ? (
                <section>
                  <h4>{t("works.controls")}</h4>
                  <div className="works-detail__controls">
                    {controls.map((control) => (
                      <button
                        key={control}
                        type="button"
                        className="btn btn-sm"
                        onClick={() => command.mutate(control)}
                        disabled={command.isPending}
                      >
                        {command.isPending ? t("common.submitting") : t(controlLabel(control))}
                      </button>
                    ))}
                  </div>
                </section>
              ) : null}

              {commandError ? <ErrorMessage error={commandError} /> : null}

              <section>
                <h4>{t("works.actions")}</h4>
                {actions.length === 0 ? (
                  <p className="note">{t("works.noActions")}</p>
                ) : (
                  <ul className="works-actions">
                    {actions.map((action: WorkAction) => (
                      <li key={action.id}>
                        <span className="mono">{action.name}</span>
                        <span className="mono">{action.target}</span>
                        <span className="chip">{action.risk}</span>
                        <span className="note">{action.state}</span>
                        {action.may_have_dispatched ? (
                          <span className="chip chip--warning">{t("action.mayHaveDispatched")}</span>
                        ) : null}
                      </li>
                    ))}
                  </ul>
                )}
              </section>

              {detail?.outcome ? (
                <section>
                  <h4>{t("works.outcome")}</h4>
                  <p>{detail.outcome.verdict ?? t("common.unknown")}</p>
                  {detail.outcome.reasons?.length ? (
                    <ul>
                      {detail.outcome.reasons.map((reason) => (
                        <li key={reason}>{reason}</li>
                      ))}
                    </ul>
                  ) : null}
                </section>
              ) : null}

              {detail?.errors ? (
                <p className="note note--warning">{t("common.partialData")}</p>
              ) : null}
            </>
          )}
        </section>
      </div>
    </Dialog>
  );
}
