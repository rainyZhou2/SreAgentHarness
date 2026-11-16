import { useI18n } from "../../i18n/I18nProvider";
import { useSessionsQuery } from "../../hooks/useSessionsQuery";
import { useUiStore } from "../../state/uiStore";
import { StatusBadge } from "../../components/ui/StatusBadge";
import { useConnection } from "../connect/ConnectionContext";
import "./ConversationHeader.css";

export function ConversationHeader() {
  const { t } = useI18n();
  const { scope, mode, offline, whoami } = useConnection();
  const currentSessionId = useUiStore((state) => state.currentSessionId);
  const workspaceView = useUiStore((state) => state.workspaceView);
  const setWorkspaceView = useUiStore((state) => state.setWorkspaceView);
  const sessions = useSessionsQuery(scope, true);
  const current = sessions.data?.sessions.find((session) => session.id === currentSessionId);
  const title = current?.title && current.title.trim() !== "" ? current.title : t("session.untitled");
  const subject = whoami?.subject ?? "";
  const roles = whoami?.roles?.length ? whoami.roles.join(", ") : "";
  const account = mode === "remote" ? subject : "";

  return (
    <header className="conversation-header">
      <div className="conversation-header__row">
        <div className="conversation-header__title">
          <h1>{currentSessionId === null ? t("sidebar.newChat") : title}</h1>
          {current?.active_turns ? (
            <StatusBadge tone="info" label={t("session.active")} />
          ) : null}
        </div>
        <div className="conversation-header__meta">
          {account ? (
            <span className="conversation-header__account" title={roles}>
              {account}
            </span>
          ) : null}
          <span className="chip">{mode === "remote" ? t("connection.remote") : t("connection.local")}</span>
          <StatusBadge
            tone={offline ? "warning" : "positive"}
            label={offline ? t("connection.offline") : t("connection.connected")}
          />
        </div>
      </div>
      {currentSessionId !== null ? (
        <nav className="conversation-tabs" role="tablist" aria-label={t("sidebar.conversations")}>
          <button
            type="button"
            role="tab"
            aria-selected={workspaceView === "conversation"}
            className="conversation-tab"
            onClick={() => setWorkspaceView("conversation")}
          >
            {t("tabs.conversation")}
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={workspaceView === "trace"}
            className="conversation-tab"
            onClick={() => setWorkspaceView("trace")}
          >
            {t("tabs.trace")}
          </button>
        </nav>
      ) : null}
    </header>
  );
}
