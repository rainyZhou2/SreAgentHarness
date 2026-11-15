import { StatusBadge } from "../../components/ui/StatusBadge";
import type { EvidenceBlockData } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { formatTimeRange } from "../../lib/format";
import {
  evidenceStatusKey,
  evidenceStatusTone,
  signalKey,
} from "../../lib/status";
import { useUiStore } from "../../state/uiStore";
import "./EvidenceBlock.css";

export function EvidenceBlock({ data, runId }: { data: EvidenceBlockData; runId: string }) {
  const { t, locale } = useI18n();
  const openDrawer = useUiStore((state) => state.openDrawer);
  const quality = data.quality ?? {};
  const showQuality =
    quality.partial === true || quality.stale === true || quality.conflict === true;
  const status = data.status.toLowerCase();
  const tone = evidenceStatusTone(data.status);
  const cardTone =
    status === "denied" || status === "unauthorized"
      ? " block-card--danger"
      : tone === "warning"
        ? " block-card--warning"
        : "";

  return (
    <div className={`evidence-block block-card${cardTone}`}>
      <div className="block-head">
        <span className="block-title">
          {t("evidence.title")}
          <span className="chip">{t(signalKey(data.signal))}</span>
        </span>
        <StatusBadge tone={tone} label={t(evidenceStatusKey(data.status))} />
      </div>

      <div className="block-body">
        {data.summary ? <p className="evidence-block__summary">{data.summary}</p> : null}
        <div className="meta-row">
          <span>
            <strong>{t("evidence.source")}:</strong> {data.source}
          </span>
          {data.window ? (
            <span>
              <strong>{t("evidence.window")}:</strong>{" "}
              {formatTimeRange(data.window, locale)}
            </span>
          ) : null}
        </div>
        {showQuality ? (
          <div className="chip-list">
            {quality.partial ? (
              <span className="chip chip--warning">{t("evidence.partial")}</span>
            ) : null}
            {quality.stale ? (
              <span className="chip chip--warning">{t("evidence.stale")}</span>
            ) : null}
            {quality.conflict ? (
              <span className="chip chip--warning">{t("evidence.conflict")}</span>
            ) : null}
          </div>
        ) : null}
        {status === "ok" ? (
          <p className="note">{t("evidence.healthyNote")}</p>
        ) : null}
      </div>

      <div className="block-actions">
        <button
          type="button"
          className="btn btn-sm"
          onClick={() => openDrawer({ type: "evidence", runId, block: data })}
        >
          {t("evidence.details")}
        </button>
      </div>
    </div>
  );
}
