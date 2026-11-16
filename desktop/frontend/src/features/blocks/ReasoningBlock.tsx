import { Icon } from "../../components/ui/Icon";
import { useI18n } from "../../i18n/I18nProvider";
import type { ReasoningBlockData } from "../../bridge/types";
import "./blocks.css";

export function ReasoningBlock({ data }: { data: ReasoningBlockData }) {
  const { t } = useI18n();
  return (
    <details className="reasoning-block block-card block-card--quiet">
      <summary className="reasoning-block__summary">
        <Icon name="chevron-right" className="reasoning-block__chevron" />
        <span>{t("run.reasoning")}</span>
        {data.truncated ? <span className="chip">{t("run.reasoningTruncated")}</span> : null}
      </summary>
      <p className="reasoning-block__text">{data.summary}</p>
    </details>
  );
}
