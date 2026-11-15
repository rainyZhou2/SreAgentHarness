import { StatusBadge } from "../../components/ui/StatusBadge";
import type { HypothesisBlockData } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { hypothesisStatusKey, hypothesisStatusTone } from "../../lib/status";
import "./blocks.css";

export function HypothesisBlock({ data }: { data: HypothesisBlockData }) {
  const { t } = useI18n();
  return (
    <div className="hypothesis-block block-card">
      <div className="block-head">
        <span className="block-title">{t("hypothesis.title")}</span>
        {data.work_id ? <span className="chip mono">{data.work_id}</span> : null}
      </div>
      {data.items.length === 0 ? (
        <p className="note">{t("hypothesis.empty")}</p>
      ) : (
        <ul className="hypothesis-list">
          {data.items.map((item) => (
            <li key={item.id} className="hypothesis-item">
              <div className="hypothesis-item__top">
                <p className="hypothesis-item__statement">{item.statement}</p>
                <StatusBadge
                  tone={hypothesisStatusTone(item.status)}
                  label={t(hypothesisStatusKey(item.status))}
                />
              </div>
              {item.supports?.length || item.refutes?.length ? (
                <div className="meta-row">
                  {item.supports?.length ? (
                    <span>{t("hypothesis.supports", { count: item.supports.length })}</span>
                  ) : null}
                  {item.refutes?.length ? (
                    <span>{t("hypothesis.refutes", { count: item.refutes.length })}</span>
                  ) : null}
                </div>
              ) : null}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
