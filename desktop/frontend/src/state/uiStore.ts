import { create } from "zustand";

import type {
  ActionBlockData,
  EvidenceBlockData,
  ToolGroupBlockData,
  WorkBlockData,
} from "../bridge/types";

const SIDEBAR_KEY = "sre.ui.sidebarCollapsed";

export type DrawerState =
  | { type: "evidence"; runId: string; block: EvidenceBlockData }
  | { type: "action"; runId: string; runVersion: number; block: ActionBlockData }
  | { type: "work"; runId: string; block: WorkBlockData }
  | { type: "tool-group"; runId: string; block: ToolGroupBlockData };

interface UiState {
  sidebarCollapsed: boolean;
  search: string;
  currentSessionId: string | null;
  drawer: DrawerState | null;
  settingsOpen: boolean;
  approvalsOpen: boolean;
  worksOpen: boolean;
  notice: string | null;
  drafts: Record<string, string>;

  toggleSidebar: () => void;
  setSearch: (search: string) => void;
  setCurrentSession: (sessionID: string | null) => void;
  openDrawer: (drawer: DrawerState) => void;
  closeDrawer: () => void;
  openSettings: () => void;
  closeSettings: () => void;
  openApprovals: () => void;
  closeApprovals: () => void;
  openWorks: () => void;
  closeWorks: () => void;
  showNotice: (notice: string) => void;
  clearNotice: () => void;
  setDraft: (key: string, text: string) => void;
  clearDraft: (key: string) => void;
}

function readSidebarCollapsed(): boolean {
  try {
    return window.localStorage.getItem(SIDEBAR_KEY) === "true";
  } catch {
    return false;
  }
}

function persistSidebar(collapsed: boolean): void {
  try {
    window.localStorage.setItem(SIDEBAR_KEY, String(collapsed));
  } catch {
    // The layout preference is best effort.
  }
}

let drawerTrigger: HTMLElement | null = null;

export const useUiStore = create<UiState>((set, get) => ({
  sidebarCollapsed: readSidebarCollapsed(),
  search: "",
  currentSessionId: null,
  drawer: null,
  settingsOpen: false,
  approvalsOpen: false,
  worksOpen: false,
  notice: null,
  drafts: {},

  toggleSidebar: () => {
    const collapsed = !get().sidebarCollapsed;
    persistSidebar(collapsed);
    set({ sidebarCollapsed: collapsed });
  },

  setSearch: (search) => set({ search }),

  setCurrentSession: (sessionID) =>
    set({ currentSessionId: sessionID, search: get().search }),

  openDrawer: (drawer) => {
    const active = document.activeElement;
    drawerTrigger = active instanceof HTMLElement ? active : null;
    set({ drawer });
  },

  closeDrawer: () => {
    const trigger = drawerTrigger;
    drawerTrigger = null;
    set({ drawer: null });
    if (trigger && trigger.isConnected) {
      window.requestAnimationFrame(() => trigger.focus());
    }
  },

  openSettings: () => set({ settingsOpen: true }),
  closeSettings: () => set({ settingsOpen: false }),

  openApprovals: () => set({ approvalsOpen: true }),
  closeApprovals: () => set({ approvalsOpen: false }),
  openWorks: () => set({ worksOpen: true }),
  closeWorks: () => set({ worksOpen: false }),
  showNotice: (notice) => set({ notice }),
  clearNotice: () => set({ notice: null }),

  setDraft: (key, text) => set((state) => ({ drafts: { ...state.drafts, [key]: text } })),
  clearDraft: (key) =>
    set((state) => {
      const next = { ...state.drafts };
      delete next[key];
      return { drafts: next };
    }),
}));
