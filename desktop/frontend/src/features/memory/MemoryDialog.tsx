import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { PersonalMemory } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { Icon } from "../../components/ui/Icon";
import { StatePanel } from "../../components/ui/StatePanel";
import { useI18n } from "../../i18n/I18nProvider";
import { qk } from "../../lib/queryKeys";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./MemoryDialog.css";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseMemories(raw: unknown): PersonalMemory[] {
  if (!isRecord(raw) || !Array.isArray(raw.memories)) {
    return [];
  }
  return raw.memories.filter(
    (item): item is PersonalMemory =>
      isRecord(item) &&
      ((typeof item.title === "string" && item.title !== "") ||
        (typeof item.subject === "string" && item.subject !== "")),
  );
}

function memoryKey(memory: PersonalMemory): string {
  if (memory.title && memory.title !== "") {
    return memory.title;
  }
  return (memory.subject ?? "").replace(/^pref:/, "");
}

export function MemoryDialog() {
  const { t } = useI18n();
  const { scope } = useConnection();
  const close = useUiStore((state) => state.closeMemory);
  const showNotice = useUiStore((state) => state.showNotice);
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: qk.memories(scope),
    queryFn: () => bridge.myMemories(),
    staleTime: 10_000,
  });
  const memories = useMemo(() => parseMemories(query.data), [query.data]);
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [error, setError] = useState<DesktopError | null>(null);
  const [deleting, setDeleting] = useState<PersonalMemory | null>(null);

  function openAdd() {
    setEditing(null);
    setKey("");
    setValue("");
    setError(null);
    setFormOpen(true);
  }

  function openEdit(memory: PersonalMemory) {
    setEditing(memoryKey(memory));
    setKey(memoryKey(memory));
    setValue(memory.content ?? "");
    setError(null);
    setFormOpen(true);
  }

  const save = useMutation({
    mutationFn: () => bridge.setMyMemory(key.trim(), value.trim()),
    onSuccess: () => {
      setFormOpen(false);
      setError(null);
      void queryClient.invalidateQueries({ queryKey: qk.memories(scope) });
      showNotice(t("memory.saved"));
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  const remove = useMutation({
    mutationFn: (memory: PersonalMemory) => bridge.deleteMyMemory(memoryKey(memory)),
    onSuccess: () => {
      setDeleting(null);
      setError(null);
      void queryClient.invalidateQueries({ queryKey: qk.memories(scope) });
      showNotice(t("memory.deleted"));
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });

  return (
    <Dialog title={t("memory.title")} onClose={close} wide className="dialog--memory">
      <div className="memory-head">
        <p className="note">{t("memory.hint")}</p>
        <button type="button" className="btn btn-sm" onClick={openAdd}>
          <Icon name="plus" />
          {t("memory.add")}
        </button>
      </div>

      {formOpen ? (
        <form
          className="memory-form"
          onSubmit={(event) => {
            event.preventDefault();
            if (key.trim() === "" || value.trim() === "") {
              setError(toDesktopError(t("memory.required")));
              return;
            }
            save.mutate();
          }}
        >
          <div className="memory-form__grid">
            <label className="field">
              <span className="field-label">{t("memory.key")}</span>
              <input
                className="input"
                value={key}
                onChange={(event) => setKey(event.target.value)}
                disabled={editing !== null || save.isPending}
                maxLength={64}
              />
            </label>
            <label className="field">
              <span className="field-label">{t("memory.value")}</span>
              <input
                className="input"
                value={value}
                onChange={(event) => setValue(event.target.value)}
                disabled={save.isPending}
                maxLength={2000}
              />
            </label>
          </div>
          {error ? <ErrorMessage error={error} /> : null}
          <div className="memory-form__actions">
            <button type="button" className="btn btn-sm" onClick={() => setFormOpen(false)}>
              {t("common.cancel")}
            </button>
            <button type="submit" className="btn btn-primary btn-sm" disabled={save.isPending}>
              {save.isPending ? t("common.submitting") : t("common.save")}
            </button>
          </div>
        </form>
      ) : null}

      {query.isPending ? (
        <StatePanel tone="loading" title={t("common.loading")} compact />
      ) : query.isError ? (
        <div className="memory-error" role="alert">
          <p className="note note--danger">{t("common.error")}</p>
          <ErrorMessage error={toDesktopError(query.error)} />
        </div>
      ) : memories.length === 0 ? (
        <StatePanel tone="empty" title={t("memory.empty")} compact />
      ) : (
        <div className="memory-list">
          {memories.map((memory) => (
            <div className="memory-row" key={memoryKey(memory)}>
              <div className="memory-row__main">
                <span className="memory-row__key mono">{memoryKey(memory)}</span>
                <span className="memory-row__value">{memory.content}</span>
              </div>
              <div className="memory-row__actions">
                <button type="button" className="btn btn-ghost btn-sm" onClick={() => openEdit(memory)}>
                  {t("memory.edit")}
                </button>
                <button type="button" className="btn btn-ghost btn-sm" onClick={() => setDeleting(memory)}>
                  {t("memory.delete")}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {deleting ? (
        <Dialog title={t("memory.deleteTitle")} onClose={() => setDeleting(null)}>
          <p className="note">{t("memory.deleteBody")}</p>
          {error ? <ErrorMessage error={error} /> : null}
          <div className="dialog-actions">
            <button type="button" className="btn btn-sm" onClick={() => setDeleting(null)}>
              {t("common.cancel")}
            </button>
            <button type="button" className="btn btn-danger btn-sm" onClick={() => remove.mutate(deleting)} disabled={remove.isPending}>
              {remove.isPending ? t("common.submitting") : t("memory.delete")}
            </button>
          </div>
        </Dialog>
      ) : null}
    </Dialog>
  );
}
