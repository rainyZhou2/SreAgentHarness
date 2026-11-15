import type { DesktopError, DesktopErrorKind } from "../../bridge/errors";
import { useI18n } from "../../i18n/I18nProvider";
import type { MessageKey } from "../../i18n/messages";
import "./ErrorState.css";

const titleKeys: Record<DesktopErrorKind, MessageKey> = {
  bridge_unavailable: "error.bridgeUnavailable",
  unauthorized: "error.unauthorized",
  forbidden: "error.forbidden",
  not_found: "error.notFound",
  conflict: "error.conflict",
  rejected: "error.rejected",
  unavailable: "error.unavailable",
  rate_limited: "error.rateLimited",
  effect_unknown: "error.effectUnknown",
  data_contract: "error.dataContract",
  unknown: "error.unknown",
};

const hintKeys: Record<DesktopErrorKind, MessageKey> = {
  bridge_unavailable: "error.bridgeUnavailable",
  unauthorized: "error.hintUnauthorized",
  forbidden: "error.hintForbidden",
  not_found: "error.hintForbidden",
  conflict: "error.hintConflict",
  rejected: "error.hintConflict",
  unavailable: "error.hintUnavailable",
  rate_limited: "error.hintUnavailable",
  effect_unknown: "error.hintEffectUnknown",
  data_contract: "error.hintDataContract",
  unknown: "error.hintUnknown",
};

/** Compact inline error used inside cards and forms. */
export function ErrorMessage({ error }: { error: DesktopError }) {
  const { t } = useI18n();
  return (
    <div className="error-message" role="alert">
      <strong>{t(titleKeys[error.kind])}</strong>
      {error.message ? <span className="error-message__detail mono">{error.message}</span> : null}
    </div>
  );
}

/** Block-level error with an explicit next step. */
export function ErrorState({
  error,
  onRetry,
  title,
  compact = false,
}: {
  error: DesktopError;
  onRetry?: () => void;
  title?: string;
  compact?: boolean;
}) {
  const { t } = useI18n();
  return (
    <div className={compact ? "error-state error-state--compact" : "error-state"} role="alert">
      <div className="error-state__heading">
        <span className="error-state__icon" aria-hidden="true">
          !
        </span>
        <div>
          <h3>{title ?? t(titleKeys[error.kind])}</h3>
          <p className="error-state__hint">{t(hintKeys[error.kind])}</p>
        </div>
      </div>
      {error.message ? <p className="error-state__detail mono">{error.message}</p> : null}
      <div className="error-state__actions">
        {onRetry ? (
          <button type="button" className="btn btn-sm" onClick={onRetry}>
            {t("error.retry")}
          </button>
        ) : null}
        {error.requestId ? (
          <span className="error-state__request mono">
            {t("error.requestId", { id: error.requestId })}
          </span>
        ) : null}
      </div>
    </div>
  );
}
