import { SafeMarkdown } from "../../components/ui/SafeMarkdown";
import { StatusBadge } from "../../components/ui/StatusBadge";
import type { ConclusionBlockData } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { conclusionVerdictKey, conclusionVerdictTone } from "../../lib/status";
import "./blocks.css";

export function ConclusionBlock({ data }: { data: ConclusionBlockData }) {
  const { t } = useI18n();
  return (
    <div className="conclusion-block block-card">
      <div className="block-head">
        <span className="block-title">{t("conclusion.title")}</span>
        <StatusBadge
          tone={conclusionVerdictTone(data.verdict)}
          label={t(conclusionVerdictKey(data.verdict))}
        />
      </div>

      <div className="block-body">
        <h3 className="conclusion-block__label">{t("conclusion.answer")}</h3>
        <SafeMarkdown text={data.answer} />
      </div>

      {data.evidence_refs?.length ? (
        <div className="block-body">
          <h3 className="conclusion-block__label">{t("conclusion.evidence")}</h3>
          <div className="chip-list">
            {data.evidence_refs.map((reference) => (
              <span key={reference} className="chip mono" title={reference}>
                {reference}
              </span>
            ))}
          </div>
        </div>
      ) : (
        <p className="note">{t("conclusion.noEvidence")}</p>
      )}

      {data.unverified?.length ? (
        <div className="block-body">
          <h3 className="conclusion-block__label">{t("conclusion.unverified")}</h3>
          <ul className="conclusion-block__unverified">
            {data.unverified.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
