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
  const sessions = useSessionsQuery(scope, true);
  const current = sessions.data?.sessions.find((session) => session.id === currentSessionId);
  const title = current?.title && current.title.trim() !== "" ? current.title : t("session.untitled");
  const subject = whoami?.subject ?? "";
  const roles = whoami?.roles?.length ? whoami.roles.join(", ") : "";

  return (
    <header className="conversation-header">
      <div className="conversation-header__title">
        <h1>{currentSessionId === null ? t("sidebar.newChat") : title}</h1>
        {current?.active_turns ? (
          <StatusBadge tone="info" label={t("session.active")} />
        ) : null}
      </div>
      <div className="conversation-header__meta">
        {subject ? (
          <span className="conversation-header__account" title={roles}>
            {subject}
          </span>
        ) : null}
        <span className="chip">{mode === "remote" ? t("connection.remote") : t("connection.local")}</span>
        <StatusBadge
          tone={offline ? "warning" : "positive"}
          label={offline ? t("connection.offline") : t("connection.connected")}
        />
      </div>
    </header>
  );
}
