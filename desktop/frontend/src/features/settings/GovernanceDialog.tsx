import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError } from "../../bridge/errors";
import type { AuditEntry, Revocation, RoleBinding } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { StatePanel } from "../../components/ui/StatePanel";
import { useI18n } from "../../i18n/I18nProvider";
import { formatDateTime } from "../../lib/format";
import { useConnection } from "../connect/ConnectionContext";
import "./GovernanceDialog.css";

type Tab = "audit" | "roles" | "revocations";

function useGovernanceQueries(scope: string, tab: Tab) {
  const audit = useQuery({
    queryKey: ["governance", "audit", scope],
    queryFn: () => bridge.listAudit(100),
    enabled: tab === "audit",
    staleTime: 10_000,
  });
  const roles = useQuery({
    queryKey: ["governance", "roles", scope],
    queryFn: () => bridge.listRoleBindings(),
    enabled: tab === "roles",
    staleTime: 30_000,
  });
  const revocations = useQuery({
    queryKey: ["governance", "revocations", scope],
    queryFn: () => bridge.listRevocations(100),
    enabled: tab === "revocations",
    staleTime: 30_000,
  });
  return { audit, roles, revocations };
}

export function GovernanceDialog({ onClose }: { onClose: () => void }) {
  const { t, locale } = useI18n();
  const { scope, whoami } = useConnection();
  const [tab, setTab] = useState<Tab>("audit");
  const queries = useGovernanceQueries(scope, tab);
  const admin = whoami?.roles?.includes("admin") ?? false;

  const active =
    tab === "audit" ? queries.audit : tab === "roles" ? queries.roles : queries.revocations;
  const skipped =
    tab === "audit"
      ? queries.audit.data?.skipped
      : tab === "roles"
        ? queries.roles.data?.skipped
        : queries.revocations.data?.skipped;

  return (
    <Dialog title={t("governance.title")} onClose={onClose} wide>
      <div className="governance-tabs" role="tablist">
        {(["audit", "roles", "revocations"] as Tab[]).map((value) => (
          <button
            key={value}
            type="button"
            role="tab"
            aria-selected={tab === value}
            className="governance-tab"
            onClick={() => setTab(value)}
          >
            {value === "audit"
              ? t("governance.audit")
              : value === "roles"
                ? t("governance.roles")
                : t("governance.revocations")}
          </button>
        ))}
      </div>

      {!admin ? <p className="note note--warning">{t("governance.adminOnly")}</p> : null}
      {active.isPending ? (
        <StatePanel tone="loading" title={t("governance.loading")} compact />
      ) : null}
      {active.isError ? (
        <div role="alert">
          <p className="note note--danger">{t("governance.error")}</p>
          <ErrorMessage error={toDesktopError(active.error)} />
          <button type="button" className="btn btn-sm" onClick={() => void active.refetch()}>
            {t("common.retry")}
          </button>
        </div>
      ) : null}
      {skipped ? <p className="note note--warning">{t("governance.partial")}</p> : null}

      {tab === "audit" && queries.audit.data ? (
        <AuditTable entries={queries.audit.data.entries} locale={locale} />
      ) : null}
      {tab === "roles" && queries.roles.data ? (
        <RolesTable bindings={queries.roles.data.bindings} locale={locale} />
      ) : null}
      {tab === "revocations" && queries.revocations.data ? (
        <RevocationsTable revocations={queries.revocations.data.revocations} locale={locale} />
      ) : null}
    </Dialog>
  );
}

function AuditTable({ entries, locale }: { entries: AuditEntry[]; locale: "en" | "zh" }) {
  const { t } = useI18n();
  if (entries.length === 0) {
    return <StatePanel tone="empty" title={t("governance.empty")} compact />;
  }
  return (
    <div className="governance-table-wrap">
      <table className="governance-table">
        <thead>
          <tr>
            <th>{t("governance.time")}</th>
            <th>{t("governance.actor")}</th>
            <th>{t("governance.action")}</th>
            <th>{t("governance.resource")}</th>
            <th>{t("governance.outcome")}</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((entry, index) => (
            <tr key={`${entry.occurred_at}-${entry.actor_id}-${entry.action}-${index}`}>
              <td>{formatDateTime(entry.occurred_at, locale)}</td>
              <td className="mono">{entry.actor_id}</td>
              <td className="mono">{entry.action}</td>
              <td className="mono">
                {entry.resource_type}
                {entry.resource_id ? ` / ${entry.resource_id}` : ""}
              </td>
              <td>
                <span className="chip" data-outcome={entry.outcome}>
                  {entry.outcome}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RolesTable({ bindings, locale }: { bindings: RoleBinding[]; locale: "en" | "zh" }) {
  const { t } = useI18n();
  if (bindings.length === 0) {
    return <StatePanel tone="empty" title={t("governance.empty")} compact />;
  }
  return (
    <div className="governance-table-wrap">
      <table className="governance-table">
        <thead>
          <tr>
            <th>{t("governance.principal")}</th>
            <th>{t("governance.role")}</th>
            <th>{t("governance.createdBy")}</th>
            <th>{t("governance.time")}</th>
          </tr>
        </thead>
        <tbody>
          {bindings.map((binding) => (
            <tr key={`${binding.principal_id}-${binding.role}`}>
              <td className="mono">{binding.principal_id}</td>
              <td>{binding.role}</td>
              <td className="mono">{binding.created_by}</td>
              <td>{formatDateTime(binding.created_at, locale)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RevocationsTable({
  revocations,
  locale,
}: {
  revocations: Revocation[];
  locale: "en" | "zh";
}) {
  const { t } = useI18n();
  if (revocations.length === 0) {
    return <StatePanel tone="empty" title={t("governance.empty")} compact />;
  }
  return (
    <div className="governance-table-wrap">
      <table className="governance-table">
        <thead>
          <tr>
            <th>{t("governance.time")}</th>
            <th>{t("governance.scope")}</th>
            <th>{t("governance.version")}</th>
            <th>{t("governance.actor")}</th>
            <th>{t("governance.reason")}</th>
          </tr>
        </thead>
        <tbody>
          {revocations.map((event) => (
            <tr key={event.id}>
              <td>{formatDateTime(event.created_at, locale)}</td>
              <td className="mono">
                {event.scope} / {event.scope_id}
              </td>
              <td>{event.version}</td>
              <td className="mono">{event.actor_id}</td>
              <td>{event.reason}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
