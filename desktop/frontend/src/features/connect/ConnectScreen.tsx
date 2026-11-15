import { useMutation } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";

import logo from "../../assets/logo.png";
import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { Settings } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { ErrorMessage } from "../../components/ui/ErrorState";
import "./ConnectScreen.css";

export type ConnectMode = "local" | "remote";

interface ConnectScreenProps {
  settings: Settings | null;
  initialError: DesktopError | null;
  initialMode: ConnectMode;
  onRetryBoot: () => void;
}

export function ConnectScreen({
  settings,
  initialError,
  initialMode,
  onRetryBoot,
}: ConnectScreenProps) {
  const { t } = useI18n();
  const [mode, setMode] = useState<ConnectMode>(initialMode);
  const [serverURL, setServerURL] = useState(settings?.server_url ?? "");
  const [authMode, setAuthMode] = useState<"bearer" | "dev" | "oidc">(
    settings?.oidc ? "oidc" : settings?.creds.mode === "dev" ? "dev" : "bearer",
  );
  const [token, setToken] = useState("");
  const [tenant, setTenant] = useState(settings?.creds.tenant ?? "");
  const [actor, setActor] = useState(settings?.creds.actor ?? "");
  const [roles, setRoles] = useState(settings?.creds.roles ?? "");
  const [oidcIssuer, setOidcIssuer] = useState(settings?.oidc?.issuer ?? "");
  const [oidcClientID, setOidcClientID] = useState(settings?.oidc?.client_id ?? "");
  const [validation, setValidation] = useState<string | null>(null);

  const localMutation = useMutation({
    mutationFn: () => bridge.connectLocal(),
    onSuccess: onRetryBoot,
  });

  const remoteMutation = useMutation({
    mutationFn: (next: Settings) => bridge.connect(next),
    onSuccess: onRetryBoot,
  });

  const oidcMutation = useMutation({
    mutationFn: (input: { url: string; issuer: string; clientID: string }) =>
      bridge.connectOIDC(input.url, input.issuer, input.clientID),
    onSuccess: onRetryBoot,
  });

  const localError = localMutation.error ? toDesktopError(localMutation.error) : initialError;
  const remoteError = remoteMutation.error
    ? toDesktopError(remoteMutation.error)
    : oidcMutation.error
      ? toDesktopError(oidcMutation.error)
      : null;

  function submitRemote(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setValidation(null);
    const url = serverURL.trim();
    if (url === "") {
      setValidation(t("connection.requiredUrl"));
      return;
    }
    if (authMode === "bearer" && token.trim() === "") {
      setValidation(t("connection.requiredToken"));
      return;
    }
    if (authMode === "dev" && (tenant.trim() === "" || actor.trim() === "")) {
      setValidation(t("connection.requiredDev"));
      return;
    }
    if (authMode === "oidc") {
      oidcMutation.mutate({
        url,
        issuer: oidcIssuer.trim(),
        clientID: oidcClientID.trim(),
      });
      return;
    }
    const next: Settings = {
      ...(settings ?? {}),
      server_url: url,
      mode: "remote",
      creds: {
        mode: authMode,
        ...(authMode === "bearer"
          ? { token: token.trim() }
          : { tenant: tenant.trim(), actor: actor.trim(), roles: roles.trim() }),
      },
    };
    remoteMutation.mutate(next);
  }

  const busy = localMutation.isPending || remoteMutation.isPending || oidcMutation.isPending;

  return (
    <div className="connect-screen">
      <div className="connect-card">
        <header className="connect-header">
          <img className="connect-logo" src={logo} alt="" />
          <div>
            <h1>{t("connection.signInTitle")}</h1>
            <p>{t("connection.signInSubtitle")}</p>
          </div>
        </header>

        <div className="connect-tabs" role="tablist" aria-label={t("connection.mode")}>
          <button
            type="button"
            role="tab"
            aria-selected={mode === "local"}
            className="connect-tab"
            onClick={() => setMode("local")}
            disabled={busy}
          >
            {t("connection.local")}
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={mode === "remote"}
            className="connect-tab"
            onClick={() => setMode("remote")}
            disabled={busy}
          >
            {t("connection.remote")}
          </button>
        </div>

        {mode === "local" ? (
          <section className="connect-panel" aria-label={t("connection.local")}>
            <p className="connect-hint">{t("connection.localHint")}</p>
            {localError ? <ErrorMessage error={localError} /> : null}
            <button
              type="button"
              className="btn btn-primary connect-submit"
              onClick={() => localMutation.mutate()}
              disabled={busy}
            >
              {localMutation.isPending
                ? t("connection.localStarting")
                : t("connection.localStart")}
            </button>
          </section>
        ) : (
          <form className="connect-panel" onSubmit={submitRemote} aria-label={t("connection.remote")}>
            <p className="connect-hint">{t("connection.remoteHint")}</p>
            <label className="field">
              <span className="field-label">{t("connection.serverUrl")}</span>
              <input
                className="input"
                type="url"
                inputMode="url"
                placeholder="https://harness.example.com"
                value={serverURL}
                onChange={(event) => setServerURL(event.target.value)}
                autoComplete="url"
                disabled={busy}
                required
              />
            </label>

            <fieldset className="connect-auth">
              <legend className="field-label">{t("connection.authMode")}</legend>
              <label className="radio">
                <input
                  type="radio"
                  name="auth-mode"
                  checked={authMode === "bearer"}
                  onChange={() => setAuthMode("bearer")}
                  disabled={busy}
                />
                {t("connection.bearer")}
              </label>
              <label className="radio">
                <input
                  type="radio"
                  name="auth-mode"
                  checked={authMode === "dev"}
                  onChange={() => setAuthMode("dev")}
                  disabled={busy}
                />
                {t("connection.dev")}
              </label>
              <label className="radio">
                <input
                  type="radio"
                  name="auth-mode"
                  checked={authMode === "oidc"}
                  onChange={() => setAuthMode("oidc")}
                  disabled={busy}
                />
                {t("connection.sso")}
              </label>
            </fieldset>

            {authMode === "bearer" ? (
              <label className="field">
                <span className="field-label">{t("connection.token")}</span>
                <input
                  className="input"
                  type="password"
                  value={token}
                  onChange={(event) => setToken(event.target.value)}
                  autoComplete="off"
                  disabled={busy}
                  required
                />
              </label>
            ) : authMode === "dev" ? (
              <div className="connect-dev-grid">
                <label className="field">
                  <span className="field-label">{t("connection.tenant")}</span>
                  <input
                    className="input"
                    value={tenant}
                    onChange={(event) => setTenant(event.target.value)}
                    disabled={busy}
                    required
                  />
                </label>
                <label className="field">
                  <span className="field-label">{t("connection.actor")}</span>
                  <input
                    className="input"
                    value={actor}
                    onChange={(event) => setActor(event.target.value)}
                    disabled={busy}
                    required
                  />
                </label>
                <label className="field">
                  <span className="field-label">{t("connection.roles")}</span>
                  <input
                    className="input"
                    value={roles}
                    onChange={(event) => setRoles(event.target.value)}
                    disabled={busy}
                    placeholder="operator,approver"
                  />
                </label>
              </div>
            ) : (
              <div className="connect-dev-grid">
                <p className="connect-hint span2">{t("connection.ssoHint")}</p>
                <label className="field">
                  <span className="field-label">{t("connection.issuer")}</span>
                  <input
                    className="input"
                    value={oidcIssuer}
                    onChange={(event) => setOidcIssuer(event.target.value)}
                    disabled={busy}
                    placeholder="https://idp.example.com"
                  />
                </label>
                <label className="field">
                  <span className="field-label">{t("connection.clientId")}</span>
                  <input
                    className="input"
                    value={oidcClientID}
                    onChange={(event) => setOidcClientID(event.target.value)}
                    disabled={busy}
                    placeholder="sre-desktop"
                  />
                </label>
              </div>
            )}

            {validation ? (
              <p className="form-error" role="alert">
                {validation}
              </p>
            ) : null}
            {remoteError ? <ErrorMessage error={remoteError} /> : null}

            <button type="submit" className="btn btn-primary connect-submit" disabled={busy}>
              {remoteMutation.isPending || oidcMutation.isPending
                ? t("connection.connecting")
                : t("connection.submit")}
            </button>
          </form>
        )}

      </div>
    </div>
  );
}
