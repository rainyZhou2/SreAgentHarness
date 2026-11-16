import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { useI18n } from "../../i18n/I18nProvider";
import { MODEL_PRESETS, presetFor, type ModelPreset, type ModelRecord } from "./modelCatalog";
import "./ModelEditorDialog.css";

const EFFORTS = ["", "low", "medium", "high", "xhigh", "max"];

function uniqueName(base: string, names: string[]): string {
  const taken = new Set(names);
  let candidate = base;
  let suffix = 2;
  while (taken.has(candidate)) {
    candidate = `${base}-${suffix}`;
    suffix += 1;
  }
  return candidate;
}

function stringList(raw: unknown): string[] {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { models?: unknown }).models)) {
    return [];
  }
  return (raw as { models: unknown[] }).models.filter((item): item is string => typeof item === "string");
}

export function ModelEditorDialog({
  model,
  preset: initialPreset,
  models,
  onClose,
  onSaved,
}: {
  model?: ModelRecord;
  preset?: ModelPreset;
  models: ModelRecord[];
  onClose: () => void;
  onSaved: () => void;
}) {
  const { t } = useI18n();
  const editing = model !== undefined;
  const startingPreset = initialPreset ?? (model ? presetFor(model.spec) : MODEL_PRESETS[0]!);
  const [presetID, setPresetID] = useState(startingPreset.id);
  const [provider, setProvider] = useState(model?.spec.provider ?? startingPreset.provider);
  const [name, setName] = useState(
    model?.name ?? uniqueName(startingPreset.id, models.map((item) => item.name)),
  );
  const [modelID, setModelID] = useState(model?.spec.model ?? startingPreset.models[0] ?? "");
  const [apiKey, setApiKey] = useState("");
  const [baseURL, setBaseURL] = useState(model?.spec.base_url ?? startingPreset.baseURL);
  const [effort, setEffort] = useState(model?.spec.effort ?? "");
  const [contextWindow, setContextWindow] = useState(
    model?.spec.context_window ? String(model.spec.context_window) : "",
  );
  const [maxOutput, setMaxOutput] = useState(
    model?.spec.max_output ? String(model.spec.max_output) : "",
  );
  const [maxCompletion, setMaxCompletion] = useState(
    model?.spec.max_completion_tokens ?? startingPreset.maxCompletionTokens ?? false,
  );
  const [isDefault, setIsDefault] = useState(model?.spec.default ?? models.length === 0);
  const [enabled, setEnabled] = useState(model?.enabled ?? true);
  const [listed, setListed] = useState<string[] | null>(null);
  const [testing, setTesting] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [testResult, setTestResult] = useState<{ model?: string; reply?: string; latency_ms?: number } | null>(null);
  const [error, setError] = useState<DesktopError | null>(null);

  const preset = MODEL_PRESETS.find((item) => item.id === presetID) ?? startingPreset;

  function selectPreset(nextID: string) {
    const next = MODEL_PRESETS.find((item) => item.id === nextID);
    if (!next) {
      return;
    }
    setPresetID(next.id);
    setProvider(next.provider);
    setBaseURL(next.baseURL);
    setModelID(next.models[0] ?? "");
    setMaxCompletion(next.maxCompletionTokens ?? false);
    setListed(null);
    setTestResult(null);
    if (!editing) {
      setName(uniqueName(next.id, models.map((item) => item.name)));
    }
  }

  function spec(): Record<string, unknown> {
    const out: Record<string, unknown> = {
      provider,
      preset: presetID,
      model: modelID.trim(),
      api_key: apiKey,
    };
    if (baseURL.trim() !== "") {
      out.base_url = baseURL.trim();
    }
    if (provider === "anthropic" && effort !== "") {
      out.effort = effort;
    }
    if (provider !== "anthropic") {
      if (Number(contextWindow) > 0) {
        out.context_window = Number(contextWindow);
      }
      if (Number(maxOutput) > 0) {
        out.max_output = Number(maxOutput);
      }
    }
    if (provider === "openai" && maxCompletion) {
      out.max_completion_tokens = true;
    }
    if (isDefault) {
      out.default = true;
    }
    return out;
  }

  function validate(): boolean {
    if (name.trim() === "" || modelID.trim() === "") {
      setError(toDesktopError(t("models.required")));
      return false;
    }
    if (provider === "openai" && baseURL.trim() === "") {
      setError(toDesktopError(t("models.baseRequired")));
      return false;
    }
    if (!editing && (provider === "anthropic" || provider === "gemini") && apiKey.trim() === "") {
      setError(toDesktopError(t("models.keyRequired")));
      return false;
    }
    return true;
  }

  async function fetchModels() {
    if (!validate()) {
      return;
    }
    setFetching(true);
    setError(null);
    try {
      setListed(stringList(await bridge.listModels(name.trim(), spec())));
    } catch (cause) {
      setError(toDesktopError(cause));
    } finally {
      setFetching(false);
    }
  }

  async function testConnection() {
    if (!validate()) {
      return;
    }
    setTesting(true);
    setError(null);
    setTestResult(null);
    try {
      setTestResult((await bridge.testModel(name.trim(), spec())) as typeof testResult);
    } catch (cause) {
      setError(toDesktopError(cause));
    } finally {
      setTesting(false);
    }
  }

  async function save() {
    if (!validate()) {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      if (isDefault) {
        for (const other of models) {
          if (other.name !== name.trim() && other.spec.default) {
            await bridge.putExtension(
              "models",
              other.name,
              { ...other.spec, default: false },
              other.enabled,
              other.version,
            );
          }
        }
      }
      await bridge.putExtension(
        "models",
        name.trim(),
        spec(),
        enabled,
        model?.version ?? 0,
      );
      onSaved();
    } catch (cause) {
      setError(toDesktopError(cause));
    } finally {
      setSaving(false);
    }
  }

  async function remove() {
    if (!model) {
      return;
    }
    setDeleting(true);
    setError(null);
    try {
      await bridge.deleteExtension("models", model.name);
      onSaved();
    } catch (cause) {
      setError(toDesktopError(cause));
      setDeleting(false);
    }
  }

  const choices = Array.from(new Set([...(listed ?? []), ...preset.models, modelID].filter(Boolean)));
  const keyPlaceholder = editing ? t("models.apiKeySaved") : preset.keyHint;

  return (
    <Dialog
      title={editing ? t("models.edit") : t("models.add")}
      onClose={onClose}
      wide
      className="dialog--model-editor"
    >
      <form
        className="model-editor"
        onSubmit={(event) => {
          event.preventDefault();
          void save();
        }}
      >
        <div className="model-editor__preset">
          <label className="field">
            <span className="field-label">{t("models.preset")}</span>
            <select
              className="select"
              value={presetID}
              onChange={(event) => selectPreset(event.target.value)}
              disabled={editing || saving}
            >
              {MODEL_PRESETS.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <span className="note">{provider === "openai" ? "OpenAI-compatible" : provider === "gemini" ? "Gemini API" : "Anthropic Messages"}</span>
        </div>

        <div className="model-editor__grid">
          <label className="field">
            <span className="field-label">{t("models.name")}</span>
            <input
              className="input mono"
              value={name}
              onChange={(event) => setName(event.target.value)}
              disabled={editing || saving}
              spellCheck={false}
            />
          </label>
          <label className="field">
            <span className="field-label">{t("models.model")}</span>
            <div className="model-editor__inline">
              <input
                className="input mono"
                value={modelID}
                list="model-id-choices"
                onChange={(event) => setModelID(event.target.value)}
                disabled={saving}
                spellCheck={false}
              />
              <button type="button" className="btn btn-sm" onClick={() => void fetchModels()} disabled={fetching || saving}>
                {fetching ? t("models.fetching") : t("models.fetch")}
              </button>
            </div>
            <datalist id="model-id-choices">
              {choices.map((item) => (
                <option key={item} value={item} />
              ))}
            </datalist>
          </label>
          <label className="field">
            <span className="field-label">{t("models.apiKey")}</span>
            <input
              className="input"
              type="password"
              autoComplete="off"
              value={apiKey}
              onChange={(event) => setApiKey(event.target.value)}
              placeholder={keyPlaceholder}
              disabled={saving}
            />
            <span className="note">{t("models.keyStored")}</span>
          </label>
          <label className="field">
            <span className="field-label">{t("models.baseURL")}</span>
            <input
              className="input mono"
              value={baseURL}
              onChange={(event) => setBaseURL(event.target.value)}
              placeholder={preset.basePlaceholder ?? (provider === "anthropic" ? "https://api.anthropic.com" : provider === "gemini" ? "https://generativelanguage.googleapis.com/v1beta" : "")}
              disabled={saving}
              spellCheck={false}
            />
          </label>
        </div>

        {listed !== null ? (
          listed.length === 0 ? (
            <p className="note">{t("models.fetchEmpty")}</p>
          ) : (
            <div className="model-editor__chips">
              {listed.slice(0, 40).map((item) => (
                <button
                  key={item}
                  type="button"
                  className="chip"
                  data-active={item === modelID}
                  onClick={() => setModelID(item)}
                >
                  {item}
                </button>
              ))}
            </div>
          )
        ) : (
          <p className="note">{t("models.fetchHint")}</p>
        )}

        <details className="model-editor__advanced">
          <summary>{t("models.advanced")}</summary>
          <div className="model-editor__grid">
            {provider === "anthropic" ? (
              <label className="field">
                <span className="field-label">{t("models.effort")}</span>
                <select
                  className="select"
                  value={effort}
                  onChange={(event) => setEffort(event.target.value)}
                  disabled={saving}
                >
                  {EFFORTS.map((value) => (
                    <option key={value || "default"} value={value}>
                      {value || t("models.effortDefault")}
                    </option>
                  ))}
                </select>
              </label>
            ) : (
              <>
                <label className="field">
                  <span className="field-label">{t("models.contextWindow")}</span>
                  <input
                    className="input"
                    inputMode="numeric"
                    value={contextWindow}
                    onChange={(event) => setContextWindow(event.target.value)}
                    disabled={saving}
                  />
                </label>
                <label className="field">
                  <span className="field-label">{t("models.maxOutput")}</span>
                  <input
                    className="input"
                    inputMode="numeric"
                    value={maxOutput}
                    onChange={(event) => setMaxOutput(event.target.value)}
                    disabled={saving}
                  />
                </label>
              </>
            )}
          </div>
          {provider === "openai" ? (
            <label className="model-editor__check">
              <input
                type="checkbox"
                checked={maxCompletion}
                onChange={(event) => setMaxCompletion(event.target.checked)}
                disabled={saving}
              />
              {t("models.maxCompletionTokens")}
            </label>
          ) : null}
        </details>

        <div className="model-editor__test">
          <button
            type="button"
            className="btn btn-sm"
            onClick={() => void testConnection()}
            disabled={testing || saving}
          >
            {testing ? t("models.testing") : t("models.test")}
          </button>
          {testResult ? (
            <span className="note note--success">
              {t("models.testOk", {
                model: testResult.model ?? modelID,
                reply: testResult.reply ?? "",
                ms: testResult.latency_ms ?? 0,
              })}
            </span>
          ) : null}
        </div>

        <div className="model-editor__checks">
          <label className="model-editor__check">
            <input
              type="checkbox"
              checked={isDefault}
              onChange={(event) => setIsDefault(event.target.checked)}
              disabled={saving}
            />
            {t("models.defaultCheck")}
          </label>
          <label className="model-editor__check">
            <input
              type="checkbox"
              checked={enabled}
              onChange={(event) => setEnabled(event.target.checked)}
              disabled={saving}
            />
            {t("models.enabledCheck")}
          </label>
        </div>

        {error ? <ErrorMessage error={error} /> : null}

        <footer className="model-editor__footer">
          {model ? (
            <button type="button" className="btn btn-danger btn-sm" onClick={() => setConfirmDelete(true)} disabled={saving || deleting}>
              {t("models.delete")}
            </button>
          ) : null}
          <span className="spacer" />
          <button type="button" className="btn btn-sm" onClick={onClose} disabled={saving}>
            {t("common.cancel")}
          </button>
          <button type="submit" className="btn btn-primary btn-sm" disabled={saving}>
            {saving ? t("common.submitting") : t("common.save")}
          </button>
        </footer>
      </form>

      {confirmDelete ? (
        <Dialog title={t("models.deleteTitle")} onClose={() => setConfirmDelete(false)}>
          <p className="note">{t("models.deleteBody")}</p>
          {error ? <ErrorMessage error={error} /> : null}
          <div className="dialog-actions">
            <button type="button" className="btn btn-sm" onClick={() => setConfirmDelete(false)}>
              {t("common.cancel")}
            </button>
            <button type="button" className="btn btn-danger btn-sm" onClick={() => void remove()} disabled={deleting}>
              {deleting ? t("common.submitting") : t("models.delete")}
            </button>
          </div>
        </Dialog>
      ) : null}
    </Dialog>
  );
}
