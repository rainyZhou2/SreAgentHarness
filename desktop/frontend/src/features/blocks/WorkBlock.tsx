import { StatusBadge } from "../../components/ui/StatusBadge";
import type { WorkBlockData } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { workStatusKey, workStatusTone } from "../../lib/status";
import { useUiStore } from "../../state/uiStore";
import "./blocks.css";

export function WorkBlock({ data, runId }: { data: WorkBlockData; runId: string }) {
  const { t } = useI18n();
  const openDrawer = useUiStore((state) => state.openDrawer);
  return (
    <div className="work-block block-card">
      <div className="block-head">
        <span className="block-title">{t("work.title")}</span>
        <StatusBadge tone={workStatusTone(data.status)} label={t(workStatusKey(data.status))} />
      </div>
      <div className="kv-list">
        <span className="kv-key">{t("work.stage")}</span>
        <span className="kv-value">{data.stage ?? t("common.notAvailable")}</span>
        <span className="kv-key">{t("work.waitingOn")}</span>
        <span className="kv-value">{data.waiting_on ?? t("common.notAvailable")}</span>
        <span className="kv-key mono">{t("drawer.work")}</span>
        <span className="kv-value mono">{data.work_id}</span>
      </div>
      <div className="block-actions">
        <button
          type="button"
          className="btn btn-sm"
          onClick={() => openDrawer({ type: "work", runId, block: data })}
        >
          {t("work.details")}
        </button>
      </div>
    </div>
  );
}
