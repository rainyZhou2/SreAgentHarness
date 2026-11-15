import { useI18n } from "../../i18n/I18nProvider";
import "./blocks.css";

export function UnknownBlock({ rawKind }: { rawKind: string }) {
  const { t } = useI18n();
  return (
    <div className="unknown-block block-card block-card--quiet" role="note">
      <div className="block-head">
        <span className="block-title">{t("block.unknownTitle")}</span>
        <span className="chip">{t("block.readOnly")}</span>
      </div>
      <p>{t("block.unknownBody", { kind: rawKind })}</p>
      <p className="meta-row">
        <span className="mono">{rawKind}</span>
      </p>
    </div>
  );
}

export function InvalidBlock({ rawKind, reason }: { rawKind: string; reason: string }) {
  const { t } = useI18n();
  return (
    <div className="unknown-block block-card block-card--warning" role="alert">
      <div className="block-head">
        <span className="block-title">{t("block.invalidTitle")}</span>
        <span className="chip">{t("block.readOnly")}</span>
      </div>
      <p>{t("block.invalidBody", { reason })}</p>
      <p className="meta-row">
        <span className="mono">{rawKind}</span>
      </p>
    </div>
  );
}
