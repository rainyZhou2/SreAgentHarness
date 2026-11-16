import { useDeferredValue, useMemo, useState } from "react";

import logo from "../../assets/logo.png";
import { toDesktopError } from "../../bridge/errors";
import { Icon } from "../../components/ui/Icon";
import { useApprovalsQuery } from "../../hooks/useApprovalsQuery";
import { useI18n } from "../../i18n/I18nProvider";
import { useSessionsQuery } from "../../hooks/useSessionsQuery";
import { useWorksQuery } from "../../hooks/useWorksQuery";
import { formatRelativeTime } from "../../lib/format";
import type { SessionSummary } from "../../bridge/types";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import { SessionMenu, type SessionMutation } from "./SessionMenu";
import "./SessionSidebar.css";

function activityTime(session: SessionSummary): number {
  const value = session.last_activity_at ?? session.created_at;
  if (!value) {
    return 0;
  }
  const time = new Date(value).getTime();
  return Number.isNaN(time) ? 0 : time;
}

export function SessionSidebar() {
  const { t, locale } = useI18n();
  const { scope, offline, mode, whoami } = useConnection();
  const [status, setStatus] = useState<"active" | "archived">("active");
  const collapsed = useUiStore((state) => state.sidebarCollapsed);
  const search = useUiStore((state) => state.search);
  const setSearch = useUiStore((state) => state.setSearch);
  const toggleSidebar = useUiStore((state) => state.toggleSidebar);
  const currentSessionId = useUiStore((state) => state.currentSessionId);
  const setCurrentSession = useUiStore((state) => state.setCurrentSession);
  const openApprovals = useUiStore((state) => state.openApprovals);
  const openWorks = useUiStore((state) => state.openWorks);
  const openSettings = useUiStore((state) => state.openSettings);
  const showNotice = useUiStore((state) => state.showNotice);
  const deferredSearch = useDeferredValue(search);
  const sessionsQuery = useSessionsQuery(scope, true, { status, q: deferredSearch });
  const approvals = useApprovalsQuery(scope, true);
  const works = useWorksQuery(scope, true, "active");

  const sessions = sessionsQuery.data?.sessions ?? [];
  const error = sessionsQuery.error ? toDesktopError(sessionsQuery.error) : null;
  const subject = whoami?.subject ?? "";
  const roles = whoami?.roles?.join(", ") ?? "";
  const approvalsCount = approvals.data?.approvals.length ?? 0;
  const worksCount = works.data?.items.length ?? 0;
  const groups = useMemo(() => {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const todayStart = today.getTime();
    const labels: Array<{ key: string; label: string }> = [
      { key: "today", label: t("sidebar.groupToday") },
      { key: "yesterday", label: t("sidebar.groupYesterday") },
      { key: "previous7", label: t("sidebar.groupPrevious7") },
      { key: "older", label: t("sidebar.groupOlder") },
    ];
    const buckets = new Map<string, SessionSummary[]>(labels.map((item) => [item.key, []]));
    for (const session of sessions) {
      const time = activityTime(session);
      if (time === 0) {
        buckets.get("older")?.push(session);
        continue;
      }
      const dayStart = new Date(time);
      dayStart.setHours(0, 0, 0, 0);
      const days = Math.floor((todayStart - dayStart.getTime()) / 86_400_000);
      const key = days <= 0 ? "today" : days === 1 ? "yesterday" : days <= 7 ? "previous7" : "older";
      buckets.get(key)?.push(session);
    }
    return labels
      .map((item) => ({ ...item, sessions: buckets.get(item.key) ?? [] }))
      .filter((item) => item.sessions.length > 0);
  }, [sessions, t]);

  function handleMutated(kind: SessionMutation, sessionID: string) {
    if (sessionID !== currentSessionId) {
      return;
    }
    if (kind === "delete" || kind === "archive") {
      setCurrentSession(null);
    }
    if (kind === "archive") {
      showNotice(t("sidebar.archivedCurrent"));
    }
  }

  function renderSession(session: SessionSummary) {
    const title =
      session.title && session.title.trim() !== "" ? session.title : t("session.untitled");
    const active = session.id === currentSessionId;
    const lastActivity = formatRelativeTime(session.last_activity_at, locale);
    return (
      <li key={session.id} className="session-row" data-active={active}>
        <button
          type="button"
          className="session-item"
          data-active={active}
          aria-current={active ? "true" : undefined}
          title={collapsed ? title : undefined}
          onClick={() => setCurrentSession(session.id)}
        >
          {collapsed ? (
            <span className="session-item__initial" aria-hidden="true">
              {title.slice(0, 1).toUpperCase()}
            </span>
          ) : (
            <>
              <span className="session-item__title">{title}</span>
              <span className="session-item__meta">
                {session.active_turns ? (
                  <span className="session-item__active">
                    <span className="session-item__live" aria-hidden="true" />
                    {t("session.active")}
                  </span>
                ) : (
                  lastActivity
                )}
              </span>
            </>
          )}
        </button>
        {collapsed ? null : (
          <SessionMenu
            session={session}
            archived={status === "archived"}
            onMutated={handleMutated}
          />
        )}
      </li>
    );
  }

  return (
    <aside
      className="session-sidebar"
      data-collapsed={collapsed}
      aria-label={t("sidebar.conversations")}
    >
      <div className="session-sidebar__brand">
        <img className="session-sidebar__logo" src={logo} alt="" />
        {collapsed ? null : <span className="session-sidebar__name">{t("app.title")}</span>}
        <button
          type="button"
          className="btn btn-ghost btn-sm session-sidebar__collapse"
          onClick={toggleSidebar}
          aria-label={collapsed ? t("sidebar.expand") : t("sidebar.collapse")}
          aria-expanded={!collapsed}
        >
          <Icon name={collapsed ? "chevron-right" : "chevron-left"} />
        </button>
      </div>

      <div className="session-sidebar__actions">
        <button
          type="button"
          className="btn session-sidebar__new"
          onClick={() => {
            setCurrentSession(null);
            setSearch("");
            setStatus("active");
          }}
          aria-label={t("sidebar.newChat")}
          title={collapsed ? t("sidebar.newChat") : undefined}
        >
          <Icon name="plus" />
          {collapsed ? null : <span>{t("sidebar.newChat")}</span>}
        </button>
      </div>

      {collapsed ? null : (
        <>
          <div className="session-sidebar__search">
            <label className="visually-hidden" htmlFor="session-search">
              {t("sidebar.searchLabel")}
            </label>
            <Icon name="search" size={15} className="session-sidebar__search-icon" />
            <input
              id="session-search"
              className="input session-sidebar__search-input"
              type="search"
              placeholder={t("sidebar.searchPlaceholder")}
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            {search !== "" ? (
              <button
                type="button"
                className="session-sidebar__search-clear"
                onClick={() => setSearch("")}
                aria-label={t("sidebar.clearSearch")}
              >
                <Icon name="close" size={13} />
              </button>
            ) : null}
          </div>
          <div className="session-sidebar__filters" role="tablist" aria-label={t("sidebar.conversations")}>
            <button
              type="button"
              role="tab"
              aria-selected={status === "active"}
              className="session-sidebar__filter"
              onClick={() => setStatus("active")}
            >
              {t("sidebar.filterActive")}
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={status === "archived"}
              className="session-sidebar__filter"
              onClick={() => setStatus("archived")}
            >
              {t("sidebar.filterArchived")}
            </button>
          </div>
        </>
      )}

      <nav className="session-sidebar__list" aria-label={t("sidebar.conversations")}>
        {offline ? <p className="session-sidebar__note">{t("sidebar.offline")}</p> : null}

        {sessionsQuery.isPending ? (
          <div className="session-sidebar__skeletons" aria-hidden="true">
            {Array.from({ length: 6 }, (_, index) => (
              <span key={index} className="session-sidebar__skeleton" />
            ))}
          </div>
        ) : null}

        {error ? (
          <div className="session-sidebar__error" role="alert">
            <p>{t("sidebar.error")}</p>
            <p className="session-sidebar__error-detail mono">{error.message}</p>
            <button
              type="button"
              className="btn btn-sm"
              onClick={() => void sessionsQuery.refetch()}
            >
              {t("sidebar.retry")}
            </button>
          </div>
        ) : null}

        {sessionsQuery.data?.skipped ? (
          <p className="session-sidebar__note">{t("sidebar.partial")}</p>
        ) : null}

        {!sessionsQuery.isPending && !error && sessions.length === 0 ? (
          <div className="session-sidebar__empty">
            {deferredSearch.trim() !== "" ? (
              <p>{t("sidebar.noMatches", { query: deferredSearch.trim() })}</p>
            ) : status === "archived" ? (
              <p>{t("sidebar.noArchived")}</p>
            ) : (
              <>
                <p>{t("sidebar.empty")}</p>
                <p className="session-sidebar__note">{t("sidebar.emptyHint")}</p>
              </>
            )}
          </div>
        ) : null}

        {collapsed ? (
          <ul className="session-list">{sessions.map(renderSession)}</ul>
        ) : (
          groups.map((group) => (
            <section className="session-group" key={group.key} aria-label={group.label}>
              <div className="session-group__label">{group.label}</div>
              <ul className="session-list">{group.sessions.map(renderSession)}</ul>
            </section>
          ))
        )}
      </nav>

      <div className="session-sidebar__footer">
        <div className="session-sidebar__nav">
          <button
            type="button"
            className="session-sidebar__approvals"
            onClick={openWorks}
            aria-label={t("works.open")}
            title={collapsed ? t("works.open") : undefined}
          >
            <Icon name="pulse" />
            {collapsed ? null : <span>{t("works.title")}</span>}
            {!collapsed && worksCount > 0 ? (
              <span className="session-sidebar__badge session-sidebar__badge--muted">
                {worksCount}
              </span>
            ) : null}
          </button>
          <button
            type="button"
            className="session-sidebar__approvals"
            onClick={openApprovals}
            aria-label={t("approvals.open")}
            title={collapsed ? t("approvals.open") : undefined}
          >
            <Icon name="check-circle" />
            {collapsed ? null : <span>{t("approvals.title")}</span>}
            {approvalsCount > 0 ? (
              <span
                className="session-sidebar__badge"
                aria-label={t("approvals.count", { count: approvalsCount })}
              >
                {approvalsCount}
              </span>
            ) : null}
          </button>
        </div>

        <div className="session-sidebar__account-card" title={roles}>
          <span
            className="session-sidebar__status-dot"
            data-online={!offline}
            aria-hidden="true"
          />
          {collapsed ? null : (
            <div className="session-sidebar__account-main">
              <span className="session-sidebar__account-mode">
                {offline
                  ? t("connection.offline")
                  : mode === "remote"
                    ? t("connection.remote")
                    : t("connection.local")}
              </span>
              <span className="session-sidebar__account-name">{subject || t("common.notAvailable")}</span>
            </div>
          )}
          <button
            type="button"
            className="session-sidebar__account-settings"
            onClick={openSettings}
            aria-label={t("sidebar.settings")}
            title={t("sidebar.settings")}
          >
            <Icon name="globe" size={15} />
          </button>
        </div>
      </div>
    </aside>
  );
}
