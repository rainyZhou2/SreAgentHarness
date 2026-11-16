import { useDeferredValue, useState } from "react";

import logo from "../../assets/logo.png";
import { toDesktopError } from "../../bridge/errors";
import { Icon } from "../../components/ui/Icon";
import { useApprovalsQuery } from "../../hooks/useApprovalsQuery";
import { useI18n } from "../../i18n/I18nProvider";
import { useSessionsQuery } from "../../hooks/useSessionsQuery";
import { formatRelativeTime } from "../../lib/format";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import { SessionMenu, type SessionMutation } from "./SessionMenu";
import "./SessionSidebar.css";

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

  const sessions = sessionsQuery.data?.sessions ?? [];
  const error = sessionsQuery.error ? toDesktopError(sessionsQuery.error) : null;
  const subject = whoami?.subject ?? "";
  const roles = whoami?.roles?.join(", ") ?? "";
  const approvalsCount = approvals.data?.approvals.length ?? 0;

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
            <input
              id="session-search"
              className="input"
              type="search"
              placeholder={t("sidebar.searchPlaceholder")}
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
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

        <ul className="session-list">
          {sessions.map((session) => {
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
                          <span className="session-item__active">{t("session.active")}</span>
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
          })}
        </ul>
      </nav>

      <div className="session-sidebar__footer">
        <button
          type="button"
          className="session-sidebar__approvals"
          onClick={openWorks}
          aria-label={t("works.open")}
          title={collapsed ? t("works.open") : undefined}
        >
          <Icon name="pulse" />
          {collapsed ? null : <span>{t("works.title")}</span>}
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
            <span className="session-sidebar__badge" aria-label={t("approvals.count", { count: approvalsCount })}>
              {approvalsCount}
            </span>
          ) : null}
        </button>
        <div className="session-sidebar__connection" title={roles}>
          <span
            className="session-sidebar__status-dot"
            data-online={!offline}
            aria-hidden="true"
          />
          {collapsed ? null : (
            <span>
              {offline ? t("connection.offline") : mode === "remote" ? t("connection.remote") : t("connection.local")}
            </span>
          )}
        </div>
        {collapsed || subject === "" ? null : (
          <span className="session-sidebar__account">{subject}</span>
        )}
        <button
          type="button"
          className="btn btn-ghost btn-sm session-sidebar__settings"
          onClick={openSettings}
          aria-label={t("sidebar.settings")}
          title={t("sidebar.settings")}
        >
          <Icon name="globe" />
          {collapsed ? null : <span>{t("sidebar.settings")}</span>}
        </button>
      </div>
    </aside>
  );
}
