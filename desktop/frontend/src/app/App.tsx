import { useCallback, useEffect } from "react";

import { toDesktopError } from "../bridge/errors";
import { hasStoredLanguagePreference, useI18n } from "../i18n/I18nProvider";
import { isLocale } from "../i18n/messages";
import { ConnectScreen } from "../features/connect/ConnectScreen";
import { useBootQuery } from "../features/connect/useBoot";
import { ConnectedApp } from "./ConnectedApp";
import "./App.css";

function BootLoading() {
  const { t } = useI18n();
  return (
    <div className="boot-screen" role="status" aria-live="polite">
      <span className="spinner" aria-hidden="true" />
      <p>{t("app.loading")}</p>
    </div>
  );
}

export function App() {
  const boot = useBootQuery();
  const { setPreference } = useI18n();

  useEffect(() => {
    if (!boot.data || hasStoredLanguagePreference()) {
      return;
    }
    const language = boot.data.settings.language;
    if (isLocale(language)) {
      setPreference(language);
    }
  }, [boot.data, setPreference]);

  const retryBoot = useCallback(async () => {
    await boot.refetch();
  }, [boot.refetch]);

  if (boot.isPending) {
    return <BootLoading />;
  }

  if (boot.isError) {
    return (
      <ConnectScreen
        settings={null}
        initialError={toDesktopError(boot.error)}
        initialMode="local"
        onRetryBoot={() => void retryBoot()}
      />
    );
  }

  if (!boot.data) {
    return <BootLoading />;
  }

  if (!boot.data.connected) {
    return (
      <ConnectScreen
        settings={boot.data.settings}
        initialError={null}
        initialMode={boot.data.settings.mode === "remote" ? "remote" : "local"}
        onRetryBoot={() => void retryBoot()}
      />
    );
  }

  return <ConnectedApp boot={boot.data} refreshBoot={retryBoot} />;
}
