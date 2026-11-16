import { useQuery } from "@tanstack/react-query";

import { bridge } from "../../bridge/bridge";
import { Icon } from "../../components/ui/Icon";
import { useI18n } from "../../i18n/I18nProvider";
import { modelDisplayName, type ModelPreset, type ModelRecord } from "../../lib/modelCatalog";
import "./ModelAccountGroup.css";

function stringList(raw: unknown): string[] {
  if (!raw || typeof raw !== "object" || !Array.isArray((raw as { models?: unknown }).models)) {
    return [];
  }
  return (raw as { models: unknown[] }).models.filter(
    (item): item is string => typeof item === "string",
  );
}

export function ModelAccountGroup({
  scope,
  account,
  preset,
  label,
  currentModel,
  selectedRecordName,
  canEdit,
  selecting,
  onSelect,
}: {
  scope: string;
  account: ModelRecord;
  preset: ModelPreset;
  label: string;
  currentModel: string;
  selectedRecordName: string | undefined;
  canEdit: boolean;
  selecting: boolean;
  onSelect: (account: ModelRecord, modelID: string) => void;
}) {
  const { t } = useI18n();
  const query = useQuery({
    queryKey: ["provider-models", scope, account.name, account.version],
    queryFn: () => bridge.listModels(account.name, null),
    enabled: canEdit,
    staleTime: 5 * 60_000,
    retry: false,
  });
  const remote = stringList(query.data);
  const models = Array.from(new Set([account.spec.model, ...remote].filter(Boolean)));

  return (
    <div className="composer-model-group" role="group" aria-label={label}>
      <div className="composer-model-group__label">{label}</div>
      {query.isPending && canEdit ? (
        <p className="composer-model-group__note">{t("models.loading")}</p>
      ) : null}
      {query.isError ? <p className="composer-model-group__note">{t("models.listFailed")}</p> : null}
      {models.map((modelID) => {
        const selected = selectedRecordName === account.name && modelID === currentModel;
        return (
          <button
            key={modelID}
            type="button"
            role="menuitemradio"
            aria-checked={selected}
            className="composer-model-menu__item"
            data-selected={selected}
            disabled={!canEdit || !account.enabled || selecting}
            onClick={() => onSelect(account, modelID)}
          >
            <span>{modelDisplayName({ ...account.spec, model: modelID }, preset)}</span>
            {selected ? <Icon name="check" size={15} /> : null}
          </button>
        );
      })}
    </div>
  );
}
