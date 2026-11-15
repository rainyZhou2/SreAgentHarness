import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./app/App";
import { AppProviders } from "./app/AppProviders";
import "./styles/tokens.css";
import "./styles/base.css";

const runtime = window.runtime as (Window["runtime"] & { WindowSetTitle?: unknown }) | undefined;
document.documentElement.classList.toggle("native", typeof runtime?.WindowSetTitle === "function");

const container = document.getElementById("root");
if (!container) {
  throw new Error("Root element is missing from index.html");
}

createRoot(container).render(
  <StrictMode>
    <AppProviders>
      <App />
    </AppProviders>
  </StrictMode>,
);
