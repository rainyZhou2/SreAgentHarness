import type { MouseEvent } from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";

import { bridge, hasAppBridge } from "../../bridge/bridge";
import "./SafeMarkdown.css";

function isAllowedLink(href: string | undefined): href is string {
  if (!href) {
    return false;
  }
  return /^(https?:|mailto:)/i.test(href.trim());
}

const components: Components = {
  a({ href, children, ...rest }) {
    const allowed = isAllowedLink(href);
    const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
      event.preventDefault();
      if (!allowed || !hasAppBridge()) {
        return;
      }
      void bridge.openURL(href).catch(() => undefined);
    };
    return (
      <a {...rest} href={allowed ? href : undefined} onClick={handleClick} rel="noreferrer noopener">
        {children}
      </a>
    );
  },
  table({ children, ...rest }) {
    return (
      <div className="md-table-wrap">
        <table {...rest}>{children}</table>
      </div>
    );
  },
};

/**
 * Agent output is untrusted: raw HTML is never rendered, links are opened
 * through the Go bridge (which refuses unsafe schemes) and code is escaped.
 */
export function SafeMarkdown({ text }: { text: string }) {
  return (
    <div className="md">
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
        {text}
      </ReactMarkdown>
    </div>
  );
}
