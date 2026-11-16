import { useEffect } from "react";

import { Icon } from "../components/ui/Icon";
import { useUiStore } from "../state/uiStore";
import { ApprovalInbox } from "../features/approvals/ApprovalInbox";
import { WorksInbox } from "../features/works/WorksInbox";
import { Composer } from "../features/composer/Composer";
import { ConversationHeader } from "../features/conversation/ConversationHeader";
import { ConversationViewport } from "../features/conversation/ConversationViewport";
import { ContextDrawer } from "../features/drawer/ContextDrawer";
import { SessionSidebar } from "../features/sessions/SessionSidebar";
import { SettingsDialog } from "../features/settings/SettingsDialog";
import { TraceView } from "../features/trace/TraceView";
import { SkillsToolsDialog } from "../features/tools/SkillsToolsDialog";
import { MemoryDialog } from "../features/memory/MemoryDialog";
import "./AppShell.css";

export function AppShell() {
  const drawer = useUiStore((state) => state.drawer);
  const workspaceView = useUiStore((state) => state.workspaceView);
  const settingsOpen = useUiStore((state) => state.settingsOpen);
  const approvalsOpen = useUiStore((state) => state.approvalsOpen);
  const worksOpen = useUiStore((state) => state.worksOpen);
  const toolsOpen = useUiStore((state) => state.toolsOpen);
  const memoryOpen = useUiStore((state) => state.memoryOpen);
  const notice = useUiStore((state) => state.notice);
  const clearNotice = useUiStore((state) => state.clearNotice);
  const setCurrentSession = useUiStore((state) => state.setCurrentSession);

  useEffect(() => {
    if (notice === null) {
      return;
    }
    const timer = window.setTimeout(clearNotice, 5_000);
    return () => window.clearTimeout(timer);
  }, [notice, clearNotice]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "k" || !(event.metaKey || event.ctrlKey)) {
        return;
      }
      event.preventDefault();
      setCurrentSession(null);
      window.setTimeout(() => document.getElementById("composer-input")?.focus(), 0);
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [setCurrentSession]);

  return (
    <div className="app-shell" data-drawer-open={drawer !== null}>
      <SessionSidebar />
      <main className="workspace">
        <ConversationHeader />
        {workspaceView === "trace" ? (
          <TraceView />
        ) : (
          <>
            <ConversationViewport />
            <Composer />
          </>
        )}
      </main>
      <ContextDrawer />
      {settingsOpen ? <SettingsDialog /> : null}
      {worksOpen ? <WorksInbox /> : null}
      {approvalsOpen ? <ApprovalInbox /> : null}
      {toolsOpen ? <SkillsToolsDialog /> : null}
      {memoryOpen ? <MemoryDialog /> : null}
      {notice ? (
        <div className="app-notice" role="status">
          <span>{notice}</span>
          <button type="button" className="btn btn-ghost btn-sm" onClick={clearNotice}>
            <Icon name="close" />
          </button>
        </div>
      ) : null}
    </div>
  );
}
