import { useI18n } from "../../i18n/I18nProvider";
import { formatCost, formatDateTime, formatNumber, formatRelativeTime } from "../../lib/format";
import { runStatusKey, runStatusTone } from "../../lib/status";
import type { StatusBlockData } from "../../bridge/types";
import { StatusBadge } from "../../components/ui/StatusBadge";
import "./blocks.css";

export function StatusBlock({ data }: { data: StatusBlockData }) {
  const { t, locale } = useI18n();
  const cost = data.cost;
  return (
    <div className="status-block">
      <div className="status-block__main">
        <StatusBadge tone={runStatusTone(data.status)} label={t(runStatusKey(data.status))} />
      </div>
      <div className="meta-row">
        {data.started_at ? (
          <span title={formatDateTime(data.started_at, locale)}>
            {formatRelativeTime(data.started_at, locale)}
          </span>
        ) : null}
        {typeof cost?.input_tokens === "number" || typeof cost?.output_tokens === "number" ? (
          <span>
            {t("run.tokens", {
              input: formatNumber(cost.input_tokens ?? 0, locale),
              output: formatNumber(cost.output_tokens ?? 0, locale),
            })}
          </span>
        ) : null}
        {typeof cost?.model_requests === "number" ? (
          <span>{t("run.models", { count: formatNumber(cost.model_requests, locale) })}</span>
        ) : null}
        {typeof cost?.cost_usd === "number" ? (
          <span>
            {t("run.cost")} {formatCost(cost.cost_usd, locale)}
          </span>
        ) : null}
      </div>
    </div>
  );
}
