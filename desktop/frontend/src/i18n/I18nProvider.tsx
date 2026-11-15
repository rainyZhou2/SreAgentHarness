import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import {
  isLocale,
  resolveLocale,
  translate,
  type Locale,
  type LocalePreference,
  type MessageKey,
} from "./messages";

const STORAGE_KEY = "sre.ui.language";

interface I18nContextValue {
  locale: Locale;
  preference: LocalePreference;
  setPreference: (preference: LocalePreference) => void;
  t: (key: MessageKey, params?: Record<string, string | number>) => string;
}

const I18nContext = createContext<I18nContextValue | null>(null);

function readStoredPreference(): LocalePreference | null {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "system" || isLocale(stored ?? undefined)) {
      return stored as LocalePreference;
    }
  } catch {
    // Private storage is unavailable in some embedded webviews.
  }
  return null;
}

function systemPreference(): LocalePreference {
  return "system";
}

export function hasStoredLanguagePreference(): boolean {
  return readStoredPreference() !== null;
}

export function I18nProvider({
  children,
  serverLanguage,
}: {
  children: ReactNode;
  serverLanguage?: string;
}) {
  const [preference, setPreferenceState] = useState<LocalePreference>(() => {
    const stored = readStoredPreference();
    if (stored) {
      return stored;
    }
    return isLocale(serverLanguage) ? serverLanguage : systemPreference();
  });

  useEffect(() => {
    if (readStoredPreference() !== null || !isLocale(serverLanguage)) {
      return;
    }
    setPreferenceState(serverLanguage);
  }, [serverLanguage]);

  const locale = resolveLocale(preference);

  useEffect(() => {
    document.documentElement.lang = locale === "zh" ? "zh-CN" : "en";
  }, [locale]);

  const setPreference = useCallback((next: LocalePreference) => {
    setPreferenceState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // Persisting the preference is best effort.
    }
  }, []);

  const value = useMemo<I18nContextValue>(
    () => ({
      locale,
      preference,
      setPreference,
      t: (key, params) => translate(locale, key, params),
    }),
    [locale, preference, setPreference],
  );

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  const value = useContext(I18nContext);
  if (!value) {
    throw new Error("useI18n must be used inside I18nProvider");
  }
  return value;
}
