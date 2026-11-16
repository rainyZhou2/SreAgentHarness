import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { ToolInfo } from "../../bridge/types";
import { Dialog } from "../../components/ui/Dialog";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { Icon } from "../../components/ui/Icon";
import { StatePanel } from "../../components/ui/StatePanel";
import { useI18n } from "../../i18n/I18nProvider";
import { qk } from "../../lib/queryKeys";
import { useUiStore } from "../../state/uiStore";
import { useConnection } from "../connect/ConnectionContext";
import "./SkillsToolsDialog.css";

interface SkillInfo {
  name: string;
  description: string;
  enabled: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseTools(raw: unknown): ToolInfo[] {
  if (!isRecord(raw) || !Array.isArray(raw.tools)) {
    return [];
  }
  return raw.tools.filter(
    (item): item is ToolInfo => isRecord(item) && typeof item.name === "string" && item.name !== "",
  );
}

function parseSkills(raw: unknown): SkillInfo[] {
  if (!isRecord(raw) || !Array.isArray(raw.skills)) {
    return [];
  }
  const out: SkillInfo[] = [];
  for (const item of raw.skills) {
    if (!isRecord(item) || typeof item.name !== "string") {
      continue;
    }
    const spec = isRecord(item.spec) ? item.spec : {};
    out.push({
      name: item.name,
      description: typeof spec.description === "string" ? spec.description : "",
      enabled: item.enabled !== false,
    });
  }
  return out;
}

export function SkillsToolsDialog() {
  const { t } = useI18n();
  const { scope, whoami } = useConnection();
  const close = useUiStore((state) => state.closeTools);
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<"tools" | "skills">("tools");
  const [search, setSearch] = useState("");
  const [error, setError] = useState<DesktopError | null>(null);
  const admin = whoami?.roles?.includes("admin") ?? false;
  const tools = useQuery({ queryKey: qk.tools(scope), queryFn: () => bridge.tools(), staleTime: 15_000 });
  const extensions = useQuery({
    queryKey: qk.extensions(scope),
    queryFn: () => bridge.extensions(),
    staleTime: 15_000,
  });
  const toolList = useMemo(() => parseTools(tools.data), [tools.data]);
  const skillList = useMemo(() => parseSkills(extensions.data), [extensions.data]);
  const needle = search.trim().toLowerCase();
  const filteredTools = toolList.filter(
    (tool) =>
      needle === "" ||
      tool.name.toLowerCase().includes(needle) ||
      (tool.description ?? "").toLowerCase().includes(needle),
  );
  const filteredSkills = skillList.filter(
    (skill) =>
      needle === "" ||
      skill.name.toLowerCase().includes(needle) ||
      skill.description.toLowerCase().includes(needle),
  );
  const toggle = useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      bridge.setToolEnabled(name, enabled),
    onSuccess: () => {
      setError(null);
      void queryClient.invalidateQueries({ queryKey: qk.tools(scope) });
    },
    onError: (cause) => setError(toDesktopError(cause)),
  });
  const loading = tools.isPending || extensions.isPending;
  const total = tab === "tools" ? filteredTools.length : filteredSkills.length;

  return (
    <Dialog title={t("tools.title")} onClose={close} wide className="dialog--tools">
      <p className="note">{t("tools.hint")}</p>
      <div className="tools-tabs" role="tablist">
        <button type="button" role="tab" aria-selected={tab === "tools"} onClick={() => setTab("tools")}>
          {t("tools.tabTools")}
          <span>{toolList.length}</span>
        </button>
        <button type="button" role="tab" aria-selected={tab === "skills"} onClick={() => setTab("skills")}>
          {t("tools.tabSkills")}
          <span>{skillList.length}</span>
        </button>
      </div>
      <label className="tools-search">
        <Icon name="search" size={15} />
        <input
          className="input"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder={t("tools.search")}
        />
      </label>
      {error ? <ErrorMessage error={error} /> : null}
      {loading ? (
        <StatePanel tone="loading" title={t("common.loading")} compact />
      ) : total === 0 ? (
        <StatePanel tone="empty" title={t("tools.empty")} compact />
      ) : (
        <div className="tools-list">
          {(tab === "tools" ? filteredTools : filteredSkills).map((item) => {
            const tool = "risk" in item ? item : undefined;
            const enabled = item.enabled !== false;
            return (
              <div className="tools-row" key={item.name}>
                <span className="tools-row__icon" aria-hidden="true">
                  <Icon name={tab === "tools" ? "sliders" : "sparkles"} />
                </span>
                <div className="tools-row__main">
                  <span className="tools-row__name mono">{item.name}</span>
                  {item.description ? <span className="tools-row__description">{item.description}</span> : null}
                </div>
                {tool?.risk ? <span className="chip">{tool.risk}</span> : null}
                {tab === "tools" ? (
                  <button
                    type="button"
                    className="tools-toggle"
                    data-on={enabled}
                    disabled={!admin || toggle.isPending}
                    onClick={() => toggle.mutate({ name: item.name, enabled: !enabled })}
                    title={admin ? undefined : t("tools.adminOnly")}
                  >
                    {enabled ? t("tools.enabled") : t("tools.disabled")}
                  </button>
                ) : (
                  <span className="tools-state">{enabled ? t("tools.enabled") : t("tools.disabled")}</span>
                )}
              </div>
            );
          })}
        </div>
      )}
      {!admin ? <p className="note">{t("tools.adminOnly")}</p> : null}
    </Dialog>
  );
}
