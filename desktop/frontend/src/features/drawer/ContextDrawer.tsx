import { useRef } from "react";

import type {
  ActionBlockData,
  EvidenceBlockData,
  ToolGroupBlockData,
  WorkBlockData,
} from "../../bridge/types";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useFocusTrap } from "../../hooks/useFocusTrap";
import { Icon } from "../../components/ui/Icon";
import { useI18n } from "../../i18n/I18nProvider";
import type { MessageKey } from "../../i18n/messages";
import { formatDateTime, formatTimeRange } from "../../lib/format";
import {
  actionStateKey,
  actionStateTone,
  evidenceStatusKey,
  evidenceStatusTone,
  signalKey,
  workStatusKey,
  workStatusTone,
} from "../../lib/status";
import { useUiStore, type DrawerState } from "../../state/uiStore";
import "./ContextDrawer.css";

function EvidenceDetails({ block }: { block: EvidenceBlockData }) {
  const { t, locale } = useI18n();
  const quality = block.quality ?? {};
  return (
    <div className="drawer-details">
      <dl className="kv-list">
        <dt className="kv-key">{t("evidence.source")}</dt>
        <dd className="kv-value">{block.source}</dd>

        <dt className="kv-key">{t("evidence.signal")}</dt>
        <dd className="kv-value">{t(signalKey(block.signal))}</dd>

        <dt className="kv-key">{t("evidence.title")}</dt>
        <dd className="kv-value">
          <StatusBadge
            tone={evidenceStatusTone(block.status)}
            label={t(evidenceStatusKey(block.status))}
          />
        </dd>

        <dt className="kv-key">{t("evidence.window")}</dt>
        <dd className="kv-value">
          {block.window ? formatTimeRange(block.window, locale) : t("common.notAvailable")}
        </dd>

        {block.summary ? (
          <>
            <dt className="kv-key">{t("evidence.summary")}</dt>
            <dd className="kv-value">{block.summary}</dd>
          </>
        ) : null}

        {block.evidence_ref || block.data_ref ? (
          <>
            <dt className="kv-key">{t("evidence.refs")}</dt>
            <dd className="kv-value mono">
              {[block.evidence_ref, block.data_ref].filter(Boolean).join(" · ")}
            </dd>
          </>
        ) : null}
      </dl>
      {quality.partial || quality.stale || quality.conflict ? (
        <div className="chip-list">
          {quality.partial ? <span className="chip">{t("evidence.partial")}</span> : null}
          {quality.stale ? <span className="chip">{t("evidence.stale")}</span> : null}
          {quality.conflict ? <span className="chip">{t("evidence.conflict")}</span> : null}
        </div>
      ) : null}
      <p className="note">{t("evidence.healthyNote")}</p>
    </div>
  );
}

function ActionDetails({ block }: { block: ActionBlockData }) {
  const { t, locale } = useI18n();
  return (
    <div className="drawer-details">
      <dl className="kv-list">
        <dt className="kv-key">{t("action.target")}</dt>
        <dd className="kv-value mono">{block.target}</dd>

        <dt className="kv-key">{t("action.risk")}</dt>
        <dd className="kv-value">{block.risk}</dd>

        <dt className="kv-key">{t("action.state")}</dt>
        <dd className="kv-value">
          <StatusBadge tone={actionStateTone(block.state)} label={t(actionStateKey(block.state))} />
        </dd>

        <dt className="kv-key">{t("action.intentHash")}</dt>
        <dd className="kv-value mono">{block.action_hash ?? block.intent_hash}</dd>

        <dt className="kv-key">{t("action.requiredApprovals")}</dt>
        <dd className="kv-value">{block.required_approvals}</dd>

        {block.expires_at ? (
          <>
            <dt className="kv-key">{t("action.expiresAt")}</dt>
            <dd className="kv-value">{formatDateTime(block.expires_at, locale)}</dd>
          </>
        ) : null}

        <dt className="kv-key">{t("action.controls")}</dt>
        <dd className="kv-value">
          {block.controls?.length ? (
            <span className="chip-list">
              {block.controls.map((control) => (
                <span key={control} className="chip">
                  {control}
                </span>
              ))}
            </span>
          ) : (
            t("action.readOnlyNote")
          )}
        </dd>

        <dt className="kv-key mono">{t("drawer.action")}</dt>
        <dd className="kv-value mono">{block.action_id}</dd>
      </dl>
      {block.may_have_dispatched ? (
        <p className="note note--danger">{t("action.mayHaveDispatched")}</p>
      ) : null}
      <p className="note">{t("action.unknownEffectHint")}</p>
    </div>
  );
}

function WorkDetails({ block }: { block: WorkBlockData }) {
  const { t } = useI18n();
  return (
    <dl className="kv-list">
      <dt className="kv-key">{t("drawer.work")}</dt>
      <dd className="kv-value mono">{block.work_id}</dd>
      <dt className="kv-key">{t("work.title")}</dt>
      <dd className="kv-value">
        <StatusBadge tone={workStatusTone(block.status)} label={t(workStatusKey(block.status))} />
      </dd>
      <dt className="kv-key">{t("work.stage")}</dt>
      <dd className="kv-value">{block.stage ?? t("common.notAvailable")}</dd>
      <dt className="kv-key">{t("work.waitingOn")}</dt>
      <dd className="kv-value">{block.waiting_on ?? t("common.notAvailable")}</dd>
    </dl>
  );
}

function ToolGroupDetails({ block }: { block: ToolGroupBlockData }) {
  const { t } = useI18n();
  return (
    <div className="drawer-details">
      <p className="note">{t("run.tools", { count: block.step_count ?? block.step_ids.length })}</p>
      {block.failed_count ? (
        <p className="note note--danger">{t("run.toolsFailed", { count: block.failed_count })}</p>
      ) : null}
      {block.step_ids.length === 0 ? (
        <p className="note">{t("run.noSteps")}</p>
      ) : (
        <ul className="tool-step-list">
          {block.step_ids.map((stepID) => (
            <li key={stepID} className="chip mono">
              {stepID}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function drawerTitle(drawer: DrawerState): MessageKey {
  switch (drawer.type) {
    case "evidence":
      return "drawer.evidence";
    case "action":
      return "drawer.action";
    case "work":
      return "drawer.work";
    case "tool-group":
      return "drawer.toolGroup";
  }
}

export function ContextDrawer() {
  const { t } = useI18n();
  const drawer = useUiStore((state) => state.drawer);
  const closeDrawer = useUiStore((state) => state.closeDrawer);
  const panelRef = useRef<HTMLElement>(null);
  useFocusTrap(panelRef, drawer !== null, closeDrawer);

  if (!drawer) {
    return null;
  }

  return (
    <>
      <div className="drawer-scrim" onClick={closeDrawer} aria-hidden="true" />
      <aside
        className="context-drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby="context-drawer-title"
        ref={panelRef}
        tabIndex={-1}
      >
        <header className="context-drawer__header">
          <h2 id="context-drawer-title">{t(drawerTitle(drawer))}</h2>
          <button
            type="button"
            className="btn btn-ghost btn-sm"
            onClick={closeDrawer}
            aria-label={t("drawer.close")}
          >
            <Icon name="close" />
          </button>
        </header>
        <div className="context-drawer__body">
          {drawer.type === "evidence" ? <EvidenceDetails block={drawer.block} /> : null}
          {drawer.type === "action" ? <ActionDetails block={drawer.block} /> : null}
          {drawer.type === "work" ? <WorkDetails block={drawer.block} /> : null}
          {drawer.type === "tool-group" ? <ToolGroupDetails block={drawer.block} /> : null}
          <p className="context-drawer__ref mono">
            {t("drawer.runId")}: {drawer.runId}
          </p>
        </div>
      </aside>
    </>
  );
}
