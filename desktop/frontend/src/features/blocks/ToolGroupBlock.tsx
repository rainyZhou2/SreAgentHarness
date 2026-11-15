import type { ToolGroupBlockData } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { useUiStore } from "../../state/uiStore";
import { StatusBadge } from "../../components/ui/StatusBadge";
import "./blocks.css";

export function ToolGroupBlock({
  data,
  runId,
}: {
  data: ToolGroupBlockData;
  runId: string;
}) {
  const { t } = useI18n();
  const openDrawer = useUiStore((state) => state.openDrawer);
  const count = data.step_count ?? data.step_ids.length;
  const failed = data.failed_count ?? 0;
  return (
    <div className="tool-group block-card block-card--quiet">
      <div className="block-head">
        <span className="block-title">{t("run.steps")}</span>
        <div className="meta-row">
          <span>{t("run.tools", { count })}</span>
          {failed > 0 ? (
            <StatusBadge tone="danger" label={t("run.toolsFailed", { count: failed })} />
          ) : null}
        </div>
      </div>
      <div className="block-actions">
        <button
          type="button"
          className="btn btn-sm"
          onClick={() => openDrawer({ type: "tool-group", runId, block: data })}
        >
          {t("run.steps")}
        </button>
      </div>
    </div>
  );
}
