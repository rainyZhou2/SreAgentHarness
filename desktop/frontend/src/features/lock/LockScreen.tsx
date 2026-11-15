import { useState } from "react";

import logo from "../../assets/logo.png";
import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { useI18n } from "../../i18n/I18nProvider";
import "./LockScreen.css";

export function LockScreen() {
  const { t } = useI18n();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<DesktopError | null>(null);

  async function unlock() {
    setBusy(true);
    setError(null);
    try {
      await bridge.unlock();
      window.location.reload();
    } catch (cause) {
      setError(toDesktopError(cause));
      setBusy(false);
    }
  }

  async function signInAgain() {
    setBusy(true);
    try {
      await bridge.disconnect();
    } finally {
      window.location.reload();
    }
  }

  return (
    <div className="lock-screen">
      <div className="lock-card" role="dialog" aria-modal="true" aria-labelledby="lock-title">
        <img className="lock-mark" src={logo} alt="" />
        <h1 id="lock-title">{t("lock.title")}</h1>
        <p className="note">{t("lock.body")}</p>
        {error ? <ErrorMessage error={error} /> : null}
        <div className="lock-actions">
          <button type="button" className="btn btn-primary" onClick={() => void unlock()} disabled={busy}>
            {busy ? t("lock.unlocking") : t("lock.unlock")}
          </button>
          <button type="button" className="btn" onClick={() => void signInAgain()} disabled={busy}>
            {t("lock.signIn")}
          </button>
        </div>
      </div>
    </div>
  );
}
