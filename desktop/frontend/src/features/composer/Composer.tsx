import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { PickedFile, TurnContext } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { Icon } from "../../components/ui/Icon";
import { useSessionsQuery } from "../../hooks/useSessionsQuery";
import { useModelCatalog } from "../../hooks/useModelCatalog";
import { useProjectionQuery } from "../../hooks/useProjectionQuery";
import { useI18n } from "../../i18n/I18nProvider";
import { formatBytes, formatNumber, truncateTitle } from "../../lib/format";
import { modelDisplayName, presetFor, type ModelRecord } from "../../lib/modelCatalog";
import { qk } from "../../lib/queryKeys";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import { ModelAccountGroup } from "./ModelAccountGroup";
import { SessionGoalBar } from "./SessionGoalBar";
import { SlashMenu } from "./SlashMenu";
import { useSlashData, type SlashItem } from "./useSlashData";
import "./Composer.css";

interface SlashState {
  start: number;
  end: number;
  query: string;
}

type RoleKey = "admin" | "approver" | "operator" | "viewer" | "member";

function roleKey(roles: string[] | undefined): RoleKey {
  const set = new Set((roles ?? []).map((role) => role.trim().toLowerCase()));
  if (set.has("admin")) {
    return "admin";
  }
  if (set.has("approver")) {
    return "approver";
  }
  if (set.has("operator")) {
    return "operator";
  }
  if (set.has("viewer")) {
    return "viewer";
  }
  return "member";
}

function localDateTimeToISO(value: string): string | undefined {
  if (value === "") {
    return undefined;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString();
}

function compactNumber(value: number, locale: "en" | "zh"): string {
  return new Intl.NumberFormat(locale === "zh" ? "zh-CN" : "en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function Composer() {
  const { t, locale } = useI18n();
  const { scope, offline, whoami } = useConnection();
  const queryClient = useQueryClient();
  const currentSessionId = useUiStore((state) => state.currentSessionId);
  const setCurrentSession = useUiStore((state) => state.setCurrentSession);
  const setDraft = useUiStore((state) => state.setDraft);
  const clearDraft = useUiStore((state) => state.clearDraft);
  const setSearch = useUiStore((state) => state.setSearch);
  const openSettings = useUiStore((state) => state.openSettings);
  const showNotice = useUiStore((state) => state.showNotice);

  const draftKey = currentSessionId ?? "home";
  const draft = useUiStore((state) => state.drafts[draftKey] ?? "");
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const composingRef = useRef(false);
  const [submitError, setSubmitError] = useState<DesktopError | null>(null);
  const [lastText, setLastText] = useState<string | null>(null);
  const [contextOpen, setContextOpen] = useState(false);
  const [environment, setEnvironment] = useState("");
  const [services, setServices] = useState("");
  const [windowStart, setWindowStart] = useState("");
  const [windowEnd, setWindowEnd] = useState("");
  const [slash, setSlash] = useState<SlashState | null>(null);
  const [slashIndex, setSlashIndex] = useState(0);
  const [menuOpen, setMenuOpen] = useState(false);
  const [infoDialog, setInfoDialog] = useState<"permission" | null>(null);
  const [modelMenuOpen, setModelMenuOpen] = useState(false);
  const [permissionMenuOpen, setPermissionMenuOpen] = useState(false);
  const modelWrapRef = useRef<HTMLDivElement>(null);
  const permissionWrapRef = useRef<HTMLDivElement>(null);
  const [attachments, setAttachments] = useState<PickedFile[]>([]);
  const slashData = useSlashData(scope, true);
  const modelCatalog = useModelCatalog(scope);
  const sessionsQuery = useSessionsQuery(scope, currentSessionId !== null);
  const session = sessionsQuery.data?.sessions.find((item) => item.id === currentSessionId);
  const latestRunID = session?.latest_run_id ?? "";
  const projection = useProjectionQuery(scope, latestRunID, latestRunID !== "");

  const slashItems = useMemo(() => {
    const items = slashData.data?.items ?? [];
    const query = slash?.query.trim().toLowerCase() ?? "";
    const filtered =
      query === ""
        ? items
        : items.filter(
            (item) =>
              item.name.toLowerCase().includes(query) ||
              item.description.toLowerCase().includes(query),
          );
    return filtered.slice(0, 12);
  }, [slashData.data, slash?.query]);

  const metrics = useMemo(() => {
    let steps = 0;
    let tokens = 0;
    let started: number | null = null;
    let finished: number | null = null;
    for (const block of projection.data?.blocks ?? []) {
      if (block.kind === "tool-group") {
        steps += block.data.step_count ?? block.data.step_ids.length;
      }
      if (block.kind === "status") {
        tokens =
          (block.data.cost?.input_tokens ?? 0) + (block.data.cost?.output_tokens ?? 0);
        const start = block.data.started_at ? Date.parse(block.data.started_at) : NaN;
        const end = block.data.finished_at ? Date.parse(block.data.finished_at) : Date.now();
        if (!Number.isNaN(start)) {
          started = start;
        }
        if (!Number.isNaN(end)) {
          finished = end;
        }
      }
    }
    const elapsed = started !== null && finished !== null ? finished - started : 0;
    const tokensPerSecond =
      elapsed > 0 && tokens > 0 ? Math.round(tokens / (elapsed / 1000)) : 0;
    return { steps, tokens, tokensPerSecond };
  }, [projection.data]);

  const mutation = useMutation({
    mutationFn: async (text: string) => {
      let sessionID = currentSessionId;
      if (!sessionID) {
        const created = await bridge.createSession(truncateTitle(text));
        sessionID = created.id;
      }
      const context: TurnContext = {
        environment: environment.trim(),
        services: services
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
      };
      if (attachments.length > 0) {
        context.attachments = attachments.map(
          (attachment) => `File: ${attachment.name}\n${attachment.text}`,
        );
      }
      const start = localDateTimeToISO(windowStart);
      const end = localDateTimeToISO(windowEnd);
      if (start !== undefined) {
        context.window_start = start;
      }
      if (end !== undefined) {
        context.window_end = end;
      }
      const sent = await bridge.sendTurn(sessionID, text, context);
      return { sessionID, ...sent };
    },
    onSuccess: ({ sessionID }) => {
      clearDraft(draftKey);
      setSubmitError(null);
      setLastText(null);
      setSlash(null);
      setMenuOpen(false);
      setAttachments([]);
      setCurrentSession(sessionID);
      void queryClient.invalidateQueries({ queryKey: qk.sessions(scope) });
      void queryClient.invalidateQueries({ queryKey: qk.transcript(scope, sessionID) });
      window.setTimeout(() => textareaRef.current?.focus(), 0);
    },
    onError: (error, text) => {
      setSubmitError(toDesktopError(error));
      setLastText(text);
    },
  });

  const exportChat = useMutation({
    mutationFn: () => bridge.exportChat(currentSessionId ?? ""),
    onSuccess: (path) => {
      setMenuOpen(false);
      if (path !== "") {
        showNotice(t("sidebar.exported", { path }));
      }
    },
    onError: (cause) => {
      const normalized = toDesktopError(cause);
      showNotice(`${t("sidebar.actionFailed")}: ${normalized.message}`);
    },
  });

  const selectModel = useMutation({
    mutationFn: async ({ account, modelID }: { account: ModelRecord; modelID: string }) => {
      for (const other of configuredModels) {
        if (other.name !== account.name && other.spec.default) {
          await bridge.putExtension(
            "models",
            other.name,
            { ...other.spec, default: false },
            other.enabled,
            other.version,
          );
        }
      }
      await bridge.putExtension(
        "models",
        account.name,
        { ...account.spec, model: modelID, default: true },
        account.enabled,
        account.version,
      );
      return { account, modelID };
    },
    onSuccess: ({ account, modelID }) => {
      void queryClient.invalidateQueries({ queryKey: qk.extensions(scope) });
      void queryClient.invalidateQueries({ queryKey: qk.slash(scope) });
      void queryClient.invalidateQueries({ queryKey: ["provider-models", scope, account.name] });
      showNotice(
        t("models.selected", {
          model: modelDisplayName({ ...account.spec, model: modelID }, presetFor(account.spec)),
        }),
      );
      setModelMenuOpen(false);
    },
  });

  useEffect(() => {
    const element = textareaRef.current;
    if (!element) {
      return;
    }
    element.style.height = "auto";
    element.style.height = `${Math.min(element.scrollHeight, 220)}px`;
  }, [draft]);

  useEffect(() => {
    setSlashIndex(0);
  }, [slash?.query, slashItems.length]);

  function submit(text: string) {
    const trimmed = text.trim();
    if (trimmed === "" || mutation.isPending || offline) {
      return;
    }
    mutation.mutate(trimmed);
  }

  function detectSlash(value: string, caret: number): SlashState | null {
    const before = value.slice(0, caret);
    const match = /(?:^|\s)\/([\p{L}\p{N}_-]*)$/u.exec(before);
    if (!match) {
      return null;
    }
    const token = match[0];
    const start = caret - token.length + (token.startsWith(" ") ? 1 : 0);
    return { start, end: caret, query: match[1] ?? "" };
  }

  function pickSlash(item: SlashItem) {
    if (!slash) {
      return;
    }
    const insertion = item.kind === "skill" ? `/${item.name} ` : `use ${item.name} {}`;
    const next = draft.slice(0, slash.start) + insertion + draft.slice(slash.end);
    setDraft(draftKey, next);
    setSlash(null);
    window.setTimeout(() => {
      const element = textareaRef.current;
      if (element) {
        element.focus();
        const caret = slash.start + insertion.length;
        element.setSelectionRange(caret, caret);
      }
    }, 0);
  }

  async function attachFiles() {
    setMenuOpen(false);
    try {
      const picked = await bridge.pickFiles();
      setAttachments((current) => {
        const merged = [...current];
        for (const file of picked) {
          if (!merged.some((item) => item.name === file.name && item.size === file.size)) {
            merged.push(file);
          }
        }
        return merged.slice(0, 8);
      });
    } catch (cause) {
      showNotice(toDesktopError(cause).message);
    }
  }

  function openSlash() {
    const element = textareaRef.current;
    const caret = element?.selectionStart ?? draft.length;
    const next = draft.slice(0, caret) + "/" + draft.slice(caret);
    setDraft(draftKey, next);
    setSlash({ start: caret, end: caret + 1, query: "" });
    setMenuOpen(false);
    window.setTimeout(() => element?.focus(), 0);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (slash !== null && slashItems.length > 0) {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        setSlashIndex((index) => (index + 1) % slashItems.length);
        return;
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        setSlashIndex((index) => (index - 1 + slashItems.length) % slashItems.length);
        return;
      }
      if (event.key === "Enter" || event.key === "Tab") {
        event.preventDefault();
        const item = slashItems[slashIndex];
        if (item) {
          pickSlash(item);
        }
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        setSlash(null);
        return;
      }
    }
    if (event.key !== "Enter" || event.shiftKey) {
      return;
    }
    const native = event.nativeEvent;
    if (native.isComposing || composingRef.current || native.keyCode === 229) {
      return;
    }
    event.preventDefault();
    submit(draft);
  }

  useEffect(() => {
    if (!modelMenuOpen) {
      return;
    }
    const onPointerDown = (event: PointerEvent) => {
      if (!modelWrapRef.current?.contains(event.target as Node)) {
        setModelMenuOpen(false);
      }
    };
    const onKeyDown = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") {
        setModelMenuOpen(false);
      }
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [modelMenuOpen]);

  useEffect(() => {
    if (!permissionMenuOpen) {
      return;
    }
    const onPointerDown = (event: PointerEvent) => {
      if (!permissionWrapRef.current?.contains(event.target as Node)) {
        setPermissionMenuOpen(false);
      }
    };
    const onKeyDown = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") {
        setPermissionMenuOpen(false);
      }
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [permissionMenuOpen]);

  const hasContext =
    environment.trim() !== "" ||
    services.trim() !== "" ||
    windowStart !== "" ||
    windowEnd !== "";
  const configuredModels = modelCatalog.catalog.models;
  const currentModel = modelCatalog.catalog.current?.model ?? slashData.data?.currentModel ?? "";
  const currentRecord =
    configuredModels.find((record) => record.spec.default) ??
    configuredModels.find((record) => record.spec.model === currentModel && record.enabled) ??
    configuredModels.find((record) => record.spec.model === currentModel);
  const modelLabel = currentRecord ? currentRecord.spec.model : currentModel;
  const canManageModels = whoami?.roles?.includes("admin") ?? false;
  const accountLabels = useMemo(() => {
    const totals = new Map<string, number>();
    for (const account of configuredModels) {
      const preset = presetFor(account.spec);
      totals.set(preset.id, (totals.get(preset.id) ?? 0) + 1);
    }
    const seen = new Map<string, number>();
    return configuredModels.map((account) => {
      const preset = presetFor(account.spec);
      const index = (seen.get(preset.id) ?? 0) + 1;
      seen.set(preset.id, index);
      return `${preset.name} ${t("models.account")}${(totals.get(preset.id) ?? 0) > 1 ? ` ${index}` : ""}`;
    });
  }, [configuredModels, t]);
  const role = roleKey(whoami?.roles);
  const turns = session?.turns ?? 0;

  return (
    <div className="composer">
      <SessionGoalBar session={session} status={projection.data?.status} />

      {submitError ? (
        <div className="composer__error" role="alert">
          <ErrorMessage error={submitError} />
          <div className="composer__error-actions">
            <span className="note">{t("composer.error")}</span>
            <button
              type="button"
              className="btn btn-sm"
              disabled={mutation.isPending || offline || lastText === null}
              onClick={() => {
                if (lastText !== null) {
                  submit(lastText);
                }
              }}
            >
              {t("composer.retry")}
            </button>
          </div>
        </div>
      ) : null}

      {contextOpen ? (
        <div className="composer-context">
          <label className="field">
            <span className="field-label">{t("composer.environment")}</span>
            <input
              className="input"
              value={environment}
              onChange={(event) => setEnvironment(event.target.value)}
              placeholder="production"
              disabled={mutation.isPending}
            />
          </label>
          <label className="field">
            <span className="field-label">{t("composer.services")}</span>
            <input
              className="input"
              value={services}
              onChange={(event) => setServices(event.target.value)}
              placeholder="checkout, payments"
              disabled={mutation.isPending}
            />
          </label>
          <label className="field">
            <span className="field-label">{t("composer.windowStart")}</span>
            <input
              className="input"
              type="datetime-local"
              value={windowStart}
              onChange={(event) => setWindowStart(event.target.value)}
              disabled={mutation.isPending}
            />
          </label>
          <label className="field">
            <span className="field-label">{t("composer.windowEnd")}</span>
            <input
              className="input"
              type="datetime-local"
              value={windowEnd}
              onChange={(event) => setWindowEnd(event.target.value)}
              disabled={mutation.isPending}
            />
          </label>
        </div>
      ) : null}

      <div className="composer__box">
        {slash !== null ? (
          <SlashMenu
            items={slashItems}
            activeIndex={slashIndex}
            onPick={pickSlash}
            onHover={setSlashIndex}
          />
        ) : null}
        {attachments.length > 0 ? (
          <div className="composer__attachments" aria-label={t("composer.attachments")}>
            {attachments.map((attachment) => (
              <span className="composer__attachment" key={`${attachment.name}-${attachment.size}`}>
                <Icon name="paperclip" size={13} />
                <span className="composer__attachment-name">{attachment.name}</span>
                <span className="composer__attachment-size">{formatBytes(attachment.size)}</span>
                <button
                  type="button"
                  className="composer__attachment-remove"
                  onClick={() =>
                    setAttachments((current) =>
                      current.filter(
                        (item) => !(item.name === attachment.name && item.size === attachment.size),
                      ),
                    )
                  }
                  aria-label={t("composer.removeAttachment", { name: attachment.name })}
                >
                  <Icon name="close" size={12} />
                </button>
              </span>
            ))}
          </div>
        ) : null}

        <label className="visually-hidden" htmlFor="composer-input">
          {t("composer.placeholder")}
        </label>
        <textarea
          id="composer-input"
          ref={textareaRef}
          className="composer__input"
          rows={1}
          value={draft}
          placeholder={t("composer.placeholder")}
          onChange={(event) => {
            setDraft(draftKey, event.target.value);
            setSlash(
              detectSlash(
                event.target.value,
                event.target.selectionStart ?? event.target.value.length,
              ),
            );
          }}
          onKeyDown={handleKeyDown}
          onCompositionStart={() => {
            composingRef.current = true;
          }}
          onCompositionEnd={() => {
            composingRef.current = false;
          }}
          disabled={mutation.isPending}
          aria-describedby="composer-hint"
          aria-autocomplete="list"
          aria-expanded={slash !== null}
        />

        <div className="composer__footer">
          <div className="composer__controls">
            <div className="composer__add-wrap">
              <button
                type="button"
                className="composer__icon-btn"
                aria-label={t("composer.add")}
                aria-haspopup="menu"
                aria-expanded={menuOpen}
                onClick={() => setMenuOpen((value) => !value)}
              >
                <Icon name="plus" />
              </button>
              {menuOpen ? (
                <div className="composer__menu" role="menu">
                  <button type="button" role="menuitem" onClick={openSlash}>
                    <Icon name="slash" /> {t("composer.slash")}
                  </button>
                  <button
                    type="button"
                    role="menuitem"
                    data-active={hasContext}
                    onClick={() => {
                      setContextOpen((value) => !value);
                      setMenuOpen(false);
                    }}
                  >
                    <Icon name="sliders" /> {t("composer.context")}
                    {hasContext ? <span className="composer__dot" aria-hidden="true" /> : null}
                  </button>
                  <button type="button" role="menuitem" onClick={() => void attachFiles()}>
                    <Icon name="paperclip" /> {t("composer.attach")}
                    {attachments.length > 0 ? (
                      <span className="composer__menu-count">{attachments.length}</span>
                    ) : null}
                  </button>
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => {
                      setCurrentSession(null);
                      setSearch("");
                      setMenuOpen(false);
                    }}
                  >
                    <Icon name="plus" /> {t("composer.newConversation")}
                  </button>
                  {currentSessionId !== null ? (
                    <button
                      type="button"
                      role="menuitem"
                      onClick={() => exportChat.mutate()}
                      disabled={exportChat.isPending}
                    >
                      <Icon name="download" /> {t("composer.export")}
                    </button>
                  ) : null}
                </div>
              ) : null}
            </div>

            <div className="composer__permission-wrap" ref={permissionWrapRef}>
              <button
                type="button"
                className="composer__pill"
                title={t("composer.permission")}
                aria-haspopup="menu"
                aria-expanded={permissionMenuOpen}
                onClick={() => {
                  setPermissionMenuOpen((open) => !open);
                  setModelMenuOpen(false);
                }}
              >
                <Icon name="shield" />
                <span>{t(`composer.role.${role}`)}</span>
                <Icon name="chevron-down" size={12} className="composer__pill-chevron" />
              </button>
              {permissionMenuOpen ? (
                <div className="composer-permission-menu" role="menu">
                  <div className="composer-permission-menu__current" data-selected="true">
                    <Icon name="shield" size={14} />
                    <span>{t(`composer.role.${role}`)}</span>
                    <Icon name="check" size={14} />
                  </div>
                  <p className="composer-permission-menu__note">
                    {t("composer.permissionLocked")}
                  </p>
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => {
                      setPermissionMenuOpen(false);
                      setInfoDialog("permission");
                    }}
                  >
                    {t("composer.permissionDetails")}
                  </button>
                </div>
              ) : null}
            </div>
          </div>

          <div className="composer__trailing">
            <div className="composer__model-wrap" ref={modelWrapRef}>
              <button
                type="button"
                className="composer__pill"
                title={t("composer.model")}
                aria-haspopup="menu"
                aria-expanded={modelMenuOpen}
                onClick={() => {
                  setModelMenuOpen((open) => !open);
                  setPermissionMenuOpen(false);
                }}
              >
                <Icon name="cpu" />
                <span>{modelLabel !== "" ? modelLabel : t("composer.model")}</span>
                <Icon name="chevron-down" size={12} className="composer__pill-chevron" />
              </button>
              {modelMenuOpen ? (
                <div className="composer-model-menu" role="menu">
                  {configuredModels.length === 0 ? (
                    <p className="composer-model-menu__empty">{t("models.noModels")}</p>
                  ) : (
                    configuredModels.map((account, index) => {
                      const preset = presetFor(account.spec);
                      return (
                        <ModelAccountGroup
                          key={account.name}
                          scope={scope}
                          account={account}
                          preset={preset}
                          label={accountLabels[index] ?? preset.name}
                          currentModel={currentModel}
                          selectedRecordName={currentRecord?.name}
                          canEdit={canManageModels}
                          selecting={selectModel.isPending}
                          onSelect={(target, modelID) => {
                            if (target.name === currentRecord?.name && modelID === currentModel) {
                              setModelMenuOpen(false);
                              return;
                            }
                            selectModel.mutate({ account: target, modelID });
                          }}
                        />
                      );
                    })
                  )}
                  {!canManageModels ? <p className="composer-model-menu__note">{t("models.adminOnly")}</p> : null}
                  {selectModel.error ? (
                    <p className="composer-model-menu__note" role="alert">
                      {toDesktopError(selectModel.error).message}
                    </p>
                  ) : null}
                  <div className="composer-model-menu__footer">
                    <button
                      type="button"
                      className="btn btn-sm"
                      onClick={() => {
                        setModelMenuOpen(false);
                        openSettings();
                      }}
                    >
                      {t("models.manage")}
                    </button>
                  </div>
                </div>
              ) : null}
            </div>

            <button
              type="button"
              className="composer__send"
              onClick={() => submit(draft)}
              disabled={offline || mutation.isPending || draft.trim() === ""}
              aria-label={mutation.isPending ? t("composer.sending") : t("composer.send")}
              title={mutation.isPending ? t("composer.sending") : t("composer.send")}
            >
              {mutation.isPending ? <span className="spinner" /> : <Icon name="arrow-up" size={18} />}
            </button>
          </div>
        </div>
      </div>

      <div className="composer__metrics" id="composer-hint">
        <span>
          <Icon name="pulse" />
          {t("composer.metrics.turns", { count: formatNumber(turns, locale) })}
          <span className="composer__metrics-sep">·</span>
          {t("composer.metrics.steps", { count: formatNumber(metrics.steps, locale) })}
        </span>
        {metrics.tokens > 0 ? (
          <span>
            {t("composer.metrics.tokens", { count: compactNumber(metrics.tokens, locale) })}
            {metrics.tokensPerSecond > 0 ? (
              <>
                <span className="composer__metrics-sep">·</span>
                {t("composer.metrics.tps", {
                  count: formatNumber(metrics.tokensPerSecond, locale),
                })}
              </>
            ) : null}
          </span>
        ) : null}
        {currentSessionId === null ? <span>{t("composer.newSessionNote")}</span> : null}
        {offline ? <span className="composer__offline">{t("composer.offline")}</span> : null}
      </div>

      {infoDialog === "permission" ? (
        <Dialog
          title={t("composer.permissionTitle")}
          onClose={() => setInfoDialog(null)}
          className="dialog--permission"
        >
          <div className="permission-summary">
            <span className="permission-summary__icon" aria-hidden="true">
              <Icon name="shield" size={20} />
            </span>
            <div>
              <strong>{t(`composer.role.${role}`)}</strong>
              <span>{t("composer.permissionLocked")}</span>
            </div>
            <span className="permission-summary__status">
              <span className="session-sidebar__status-dot" data-online="true" aria-hidden="true" />
              {t("connection.connected")}
            </span>
          </div>

          <div className="permission-grid">
            <div>
              <span>{t("composer.permissionRole")}</span>
              <strong>{t(`composer.role.${role}`)}</strong>
            </div>
            <div>
              <span>{t("composer.permissionTools")}</span>
              <strong>
                {slashData.data?.enabledToolCount ?? 0} / {slashData.data?.toolCount ?? 0}
              </strong>
            </div>
            <div>
              <span>{t("composer.permissionProfile")}</span>
              <strong className="mono">{slashData.data?.profile || "—"}</strong>
            </div>
          </div>

          <p className="permission-note">{t("composer.permissionNote")}</p>
        </Dialog>
      ) : null}

    </div>
  );
}
