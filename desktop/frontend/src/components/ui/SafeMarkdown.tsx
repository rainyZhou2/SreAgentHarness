import { useState, type MouseEvent, type ReactNode } from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";

import { bridge, hasAppBridge } from "../../bridge/bridge";
import { useI18n } from "../../i18n/I18nProvider";
import { Icon } from "./Icon";
import "./SafeMarkdown.css";

function isAllowedLink(href: string | undefined): href is string {
  if (!href) {
    return false;
  }
  return /^(https?:|mailto:)/i.test(href.trim());
}

function isAllowedImage(src: string | undefined): src is string {
  return src !== undefined && /^(https?:\/\/|data:image\/)/i.test(src.trim());
}

function textContent(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(textContent).join("");
  }
  if (node && typeof node === "object" && "props" in node) {
    return textContent((node as { props: { children?: ReactNode } }).props.children);
  }
  return "";
}

function openExternal(href: string) {
  if (!hasAppBridge()) {
    return;
  }
  void bridge.openURL(href).catch(() => undefined);
}

function MarkdownLink({ href, children, title }: React.ComponentProps<"a">) {
  const allowed = isAllowedLink(href);
  const text = textContent(children).trim();
  const bare = allowed && text !== "" && text === href;
  const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    if (allowed) {
      openExternal(href);
    }
  };

  if (bare) {
    let domain = href ?? "";
    let path = "";
    try {
      const parsed = new URL(href ?? "");
      domain = parsed.host;
      path = `${parsed.pathname}${parsed.search}`;
    } catch {
      // Keep the raw URL when it cannot be parsed.
    }
    return (
      <a
        className="md-link-card"
        href={href}
        onClick={handleClick}
        rel="noreferrer noopener"
        title={href}
      >
        <span className="md-link-card__domain">{domain}</span>
        <span className="md-link-card__path">{path || "/"}</span>
        <Icon name="external" size={12} />
      </a>
    );
  }

  return (
    <a href={allowed ? href : undefined} onClick={handleClick} rel="noreferrer noopener" title={title}>
      {children}
      {allowed ? <Icon name="external" size={11} className="md-link-icon" /> : null}
    </a>
  );
}

function MarkdownImage({ src, alt, title }: React.ComponentProps<"img">) {
  const { t } = useI18n();
  const [failed, setFailed] = useState(false);
  if (!isAllowedImage(src)) {
    return <span className="md-image-error">{t("markdown.imageUnavailable")}</span>;
  }
  if (failed) {
    return <span className="md-image-error">{t("markdown.imageUnavailable")}</span>;
  }
  const caption = title || alt || "";
  const openable = /^https?:\/\//i.test(src);
  return (
    <span className="md-image">
      {openable ? (
        <button
          type="button"
          className="md-image__open"
          onClick={() => openExternal(src)}
          title={caption || src}
        >
          <img
            src={src}
            alt={alt ?? ""}
            loading="lazy"
            decoding="async"
            referrerPolicy="no-referrer"
            onError={() => setFailed(true)}
          />
        </button>
      ) : (
        <img src={src} alt={alt ?? ""} loading="lazy" decoding="async" />
      )}
      {caption ? <span className="md-image__caption">{caption}</span> : null}
    </span>
  );
}

function CodeBlock({ children }: { children?: ReactNode }) {
  const { t } = useI18n();
  const [copied, setCopied] = useState(false);
  const code = textContent(children);
  const language =
    children && typeof children === "object" && "props" in children
      ? (children as { props: { className?: string } }).props.className?.match(/language-([\w-]+)/)?.[1]
      : undefined;

  async function copy() {
    if (typeof navigator === "undefined" || !navigator.clipboard) {
      return;
    }
    try {
      await navigator.clipboard.writeText(code.replace(/\n$/, ""));
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard access is best effort in embedded webviews.
    }
  }

  return (
    <div className="md-code">
      <div className="md-code__head">
        <span>{language ?? "code"}</span>
        <button type="button" className="md-code__copy" onClick={() => void copy()}>
          {copied ? t("markdown.copied") : t("markdown.copy")}
        </button>
      </div>
      <pre>{children}</pre>
    </div>
  );
}

const components: Components = {
  a: MarkdownLink,
  img: MarkdownImage,
  pre: CodeBlock,
  table({ children }) {
    return (
      <div className="md-table-wrap">
        <table>{children}</table>
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
