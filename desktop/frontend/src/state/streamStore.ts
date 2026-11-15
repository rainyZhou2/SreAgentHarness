import { create } from "zustand";

export interface RunStream {
  text: string;
  lastSeq: number;
  error: string | null;
  updatedAt: number;
}

interface StreamState {
  streams: Record<string, RunStream>;

  appendDelta: (runId: string, text: string, seq: number) => void;
  setStreamError: (runId: string, error: string) => void;
  clearStream: (runId: string) => void;
  clearAll: () => void;
}

const emptyStream: RunStream = { text: "", lastSeq: 0, error: null, updatedAt: 0 };

export const useStreamStore = create<StreamState>((set) => ({
  streams: {},

  appendDelta: (runId, text, seq) =>
    set((state) => {
      const previous = state.streams[runId] ?? emptyStream;
      const stream: RunStream = {
        text: previous.text + text,
        lastSeq: Math.max(previous.lastSeq, seq),
        error: previous.error,
        updatedAt: Date.now(),
      };
      return { streams: { ...state.streams, [runId]: stream } };
    }),

  setStreamError: (runId, error) =>
    set((state) => {
      const previous = state.streams[runId] ?? emptyStream;
      return { streams: { ...state.streams, [runId]: { ...previous, error, updatedAt: Date.now() } } };
    }),

  clearStream: (runId) =>
    set((state) => {
      if (!(runId in state.streams)) {
        return state;
      }
      const next = { ...state.streams };
      delete next[runId];
      return { streams: next };
    }),

  clearAll: () => set({ streams: {} }),
}));

