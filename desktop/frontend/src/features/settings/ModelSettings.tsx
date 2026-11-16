import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { toDesktopError } from "../../bridge/errors";
import { Icon } from "../../components/ui/Icon";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { useModelCatalog } from "../../hooks/useModelCatalog";
import { useI18n } from "../../i18n/I18nProvider";
import { qk } from "../../lib/queryKeys";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import { ModelEditorDialog } from "./ModelEditorDialog";
import { modelDisplayName, presetFor, type ModelRecord } from "../../lib/modelCatalog";
import "./ModelSettings.css";

export function ModelSettings({ canEdit }: { canEdit: boolean }) {
  const { t } = useI18n();
  const { scope } = useConnection();
  const queryClient = useQueryClient();
  const showNotice = useUiStore((state) => state.showNotice);
  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState<ModelRecord | null>(null);
  const query = useModelCatalog(scope);
  const { catalog } = query;
  const current = catalog.current;
  const currentModel = current?.model ?? "";
  const configured = current?.tenant_configured ?? false;
  const currentRecord = catalog.models.find((model) => model.spec.model === currentModel);
  const currentLabel =
    !configured && (currentModel === "" || currentModel === "rule-based-sre")
      ? t("models.notConfigured")
      : currentRecord
        ? modelDisplayName(currentRecord.spec, presetFor(currentRecord.spec))
        : currentModel;

  function refresh() {
    void queryClient.invalidateQueries({ queryKey: qk.extensions(scope) });
    void queryClient.invalidateQueries({ queryKey: qk.slash(scope) });
  }

  function saved() {
    setEditorOpen(false);
    refresh();
    showNotice(t("models.saved"));
  }

  return (
    <section className="settings-card" aria-labelledby="settings-models-heading">
      <header className="settings-card__head">
        <span className="settings-card__icon" aria-hidden="true">
          <Icon name="cpu" />
        </span>
        <div>
          <h3 id="settings-models-heading">{t("models.title")}</h3>
          <p>{t("models.hint")}</p>
        </div>
      </header>

      <div className="settings-model-current">
        <div>
          <span className="field-label">{t("models.current")}</span>
          <strong className="mono">{currentLabel}</strong>
          <p className="note">
            {configured
              ? t("models.configured")
              : currentModel === "rule-based-sre" || currentModel === ""
                ? t("models.notConfigured")
                : t("models.workspaceDefault")}
          </p>
        </div>
        <button
          type="button"
          className="btn btn-sm"
          onClick={() => {
            setEditing(null);
            setEditorOpen(true);
          }}
          disabled={!canEdit}
        >
          <Icon name="plus" />
          {t("models.add")}
        </button>
      </div>

      {!canEdit ? <p className="note">{t("models.adminOnly")}</p> : null}
      {query.isError ? <ErrorMessage error={toDesktopError(query.error)} /> : null}

      {query.isPending ? (
        <p className="note">{t("common.loading")}</p>
      ) : catalog.models.length === 0 ? (
        <p className="note">{t("models.none")}</p>
      ) : (
        <div className="settings-model-list">
          {catalog.models.map((model) => {
            const preset = presetFor(model.spec);
            return (
              <button
                key={model.name}
                type="button"
                className="settings-model-row"
                onClick={() => {
                  setEditing(model);
                  setEditorOpen(true);
                }}
                disabled={!canEdit}
                title={t("models.edit")}
              >
                <span className="settings-model-row__mark" aria-hidden="true">
                  {preset.mark}
                </span>
                <span className="settings-model-row__main">
                  <span>{modelDisplayName(model.spec, preset)}</span>
                  <span className="note">
                    {preset.name} · {model.spec.model} · {model.name}
                    {model.spec.base_url ? ` · ${model.spec.base_url}` : ""}
                  </span>
                </span>
                <span className="settings-model-row__meta">
                  {model.spec.default ? <span className="chip chip--accent">{t("models.default")}</span> : null}
                  <span className="note">{model.enabled ? t("models.enabled") : t("models.disabled")}</span>
                </span>
              </button>
            );
          })}
        </div>
      )}

      <p className="note">{t("models.keyStored")}</p>
      <p className="note">{t("models.defaultHint")}</p>

      {editorOpen ? (
        <ModelEditorDialog
          model={editing ?? undefined}
          models={catalog.models}
          onClose={() => setEditorOpen(false)}
          onSaved={saved}
        />
      ) : null}
    </section>
  );
}
