import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { Dialog } from "../../components/ui/Dialog";
import { applyTheme, readThemePreference, type ThemePreference } from "../../hooks/useTheme";
import { useI18n } from "../../i18n/I18nProvider";
import type { LocalePreference, MessageKey } from "../../i18n/messages";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import { GovernanceDialog } from "./GovernanceDialog";
import "./SettingsDialog.css";

const languageOptions: Array<{ value: LocalePreference; label: MessageKey }> = [
  { value: "system", label: "settings.languageSystem" },
  { value: "en", label: "settings.languageEn" },
  { value: "zh", label: "settings.languageZh" },
];

const themeOptions: Array<{ value: ThemePreference; label: MessageKey }> = [
  { value: "system", label: "settings.themeSystem" },
  { value: "light", label: "settings.themeLight" },
  { value: "dark", label: "settings.themeDark" },
];

export function SettingsDialog() {
  const { t, preference, setPreference } = useI18n();
  const closeSettings = useUiStore((state) => state.closeSettings);
  const { settings, whoami, mode } = useConnection();
  const [theme, setTheme] = useState<ThemePreference>(() => readThemePreference());
  const [governanceOpen, setGovernanceOpen] = useState(false);
  const [proxyURL, setProxyURL] = useState(settings.network?.proxy_url ?? "");
  const [caPath, setCaPath] = useState(settings.network?.ca_cert_path ?? "");
  const [networkError, setNetworkError] = useState<DesktopError | null>(null);
  const [networkNotice, setNetworkNotice] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [diagnostics, setDiagnostics] = useState<Record<string, unknown> | null>(null);
  const roles = whoami?.roles?.join(", ") ?? "";
  const admin = whoami?.roles?.includes("admin") ?? false;

  function changeLanguage(next: LocalePreference) {
    setPreference(next);
    if (next === "en" || next === "zh") {
      // The desktop service persists the choice; the UI already switched.
      void bridge.setLanguage(next).catch(() => undefined);
    }
  }

  function changeTheme(next: ThemePreference) {
    setTheme(next);
    applyTheme(next);
  }

  async function saveNetwork() {
    setNetworkError(null);
    setNetworkNotice(null);
    try {
      await bridge.setNetwork({
        proxy_url: proxyURL.trim(),
        ca_cert_path: caPath.trim(),
      });
      setNetworkNotice(t("settings.networkSaved"));
    } catch (cause) {
      setNetworkError(toDesktopError(cause));
    }
  }

  async function testNetwork() {
    setTesting(true);
    setNetworkError(null);
    setNetworkNotice(null);
    setDiagnostics(null);
    try {
      setDiagnostics((await bridge.networkDiagnostics()) as Record<string, unknown>);
    } catch (cause) {
      setNetworkError(toDesktopError(cause));
    } finally {
      setTesting(false);
    }
  }

  return (
    <Dialog title={t("settings.title")} onClose={closeSettings}>
      <div className="settings-sections">
        <fieldset className="settings-group">
          <legend>{t("settings.language")}</legend>
          {languageOptions.map((option) => (
            <label key={option.value} className="settings-radio">
              <input
                type="radio"
                name="settings-language"
                value={option.value}
                checked={preference === option.value}
                onChange={() => changeLanguage(option.value)}
              />
              <span>{t(option.label)}</span>
            </label>
          ))}
        </fieldset>

        <fieldset className="settings-group">
          <legend>{t("settings.appearance")}</legend>
          <span className="field-label">{t("settings.theme")}</span>
          {themeOptions.map((option) => (
            <label key={option.value} className="settings-radio">
              <input
                type="radio"
                name="settings-theme"
                value={option.value}
                checked={theme === option.value}
                onChange={() => changeTheme(option.value)}
              />
              <span>{t(option.label)}</span>
            </label>
          ))}
        </fieldset>

        <section className="settings-group" aria-labelledby="settings-connection-heading">
          <h3 id="settings-connection-heading">{t("settings.connection")}</h3>
          <dl className="kv-list">
            <dt className="kv-key">{t("settings.mode")}</dt>
            <dd className="kv-value">
              {mode === "remote" ? t("connection.remote") : t("connection.local")}
            </dd>
            <dt className="kv-key">{t("connection.serverUrl")}</dt>
            <dd className="kv-value mono">{settings.server_url || t("common.notAvailable")}</dd>
            <dt className="kv-key">{t("settings.account")}</dt>
            <dd className="kv-value">{whoami?.subject ?? t("common.notAvailable")}</dd>
            <dt className="kv-key">{t("settings.tenant")}</dt>
            <dd className="kv-value">{whoami?.tenant ?? settings.creds.tenant ?? t("common.notAvailable")}</dd>
            <dt className="kv-key">{t("settings.roles")}</dt>
            <dd className="kv-value">{roles || t("common.notAvailable")}</dd>
          </dl>
        </section>
      </div>

      <section className="settings-group" aria-labelledby="settings-network-heading">
        <h3 id="settings-network-heading">{t("settings.network")}</h3>
        <label className="field">
          <span className="field-label">{t("settings.proxyUrl")}</span>
          <input
            className="input"
            value={proxyURL}
            onChange={(event) => setProxyURL(event.target.value)}
            placeholder="http://proxy.example.com:8080"
            spellCheck={false}
          />
        </label>
        <label className="field">
          <span className="field-label">{t("settings.caCertPath")}</span>
          <input
            className="input"
            value={caPath}
            onChange={(event) => setCaPath(event.target.value)}
            placeholder="/etc/ssl/certs/enterprise-ca.pem"
            spellCheck={false}
          />
        </label>
        <p className="note">{t("settings.networkHint")}</p>
        {networkError ? <ErrorMessage error={networkError} /> : null}
        {networkNotice ? <p className="note">{networkNotice}</p> : null}
        {diagnostics ? (
          <p className="note" role="status">
            {diagnostics.ok ? t("settings.networkOk") : t("settings.networkFailed")}
            {typeof diagnostics.status === "number" ? ` · HTTP ${diagnostics.status}` : ""}
            {typeof diagnostics.latency_ms === "number" ? ` · ${diagnostics.latency_ms} ms` : ""}
            {typeof diagnostics.error === "string" ? ` · ${diagnostics.error}` : ""}
          </p>
        ) : null}
        <div className="settings-actions">
          <button type="button" className="btn btn-sm" onClick={() => void testNetwork()} disabled={testing}>
            {testing ? t("common.loading") : t("settings.testNetwork")}
          </button>
          <button type="button" className="btn btn-primary btn-sm" onClick={() => void saveNetwork()}>
            {t("common.save")}
          </button>
        </div>
      </section>

      {admin ? (
        <section className="settings-group" aria-labelledby="settings-governance-heading">
          <h3 id="settings-governance-heading">{t("governance.title")}</h3>
          <button type="button" className="btn btn-sm" onClick={() => setGovernanceOpen(true)}>
            {t("governance.open")}
          </button>
        </section>
      ) : null}

      <footer className="settings-footer">
        <button type="button" className="btn btn-primary" onClick={closeSettings}>
          {t("settings.dismiss")}
        </button>
      </footer>
      {governanceOpen ? <GovernanceDialog onClose={() => setGovernanceOpen(false)} /> : null}
    </Dialog>
  );
}
