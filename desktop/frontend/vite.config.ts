import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The desktop shell embeds the production bundle from dist-next. The legacy
// dist directory is served by older releases and must not be touched here.
export default defineConfig({
  base: "./",
  plugins: [react()],
  build: {
    outDir: "dist-next",
    emptyOutDir: true,
    sourcemap: false,
    rollupOptions: {
      output: {
        // Keep the vendor split stable for WebView caching; the app chunk
        // holds only UI code.
        manualChunks: {
          "react-vendor": ["react", "react-dom"],
          "data-vendor": ["@tanstack/react-query", "zustand", "zod"],
          "markdown-vendor": ["react-markdown", "remark-gfm"],
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    css: false,
    restoreMocks: true,
  },
});
