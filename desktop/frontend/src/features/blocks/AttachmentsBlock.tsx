import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { bridge } from "../../bridge/bridge";
import { toDesktopError, type DesktopError } from "../../bridge/errors";
import type { AttachmentItem, AttachmentsBlockData } from "../../bridge/types";
import { ErrorMessage } from "../../components/ui/ErrorState";
import { useI18n } from "../../i18n/I18nProvider";
import { formatBytes } from "../../lib/format";
import "./AttachmentsBlock.css";

function isWebURL(value: string | undefined): value is string {
  return value !== undefined && /^https?:\/\//i.test(value);
}

function toCSV(columns: string[], rows: string[][]): string {
  const escape = (cell: string) => (/[",\n]/.test(cell) ? `"${cell.replace(/"/g, '""')}"` : cell);
  return [columns, ...rows].map((row) => row.map(escape).join(",")).join("\n");
}

function AttachmentTable({ item }: { item: AttachmentItem }) {
  const { t } = useI18n();
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");
  const columns = item.columns ?? [];
  const rows = item.rows ?? [];

  async function copyCSV() {
    if (typeof navigator === "undefined" || !navigator.clipboard) {
      setCopyState("failed");
      return;
    }
    try {
      await navigator.clipboard.writeText(toCSV(columns, rows));
      setCopyState("copied");
      window.setTimeout(() => setCopyState("idle"), 1500);
    } catch {
      setCopyState("failed");
    }
  }

  return (
    <figure className="attachment attachment--table">
      <figcaption className="attachment__head">
        <span className="attachment__title">{item.title ?? t("attachments.table")}</span>
        <span className="meta-row">{t("attachments.rows", { count: rows.length })}</span>
        <button type="button" className="btn btn-ghost btn-sm" onClick={() => void copyCSV()}>
          {copyState === "copied"
            ? t("attachments.copied")
            : copyState === "failed"
              ? t("attachments.copyFailed")
              : t("attachments.copyCsv")}
        </button>
      </figcaption>
      <div className="attachment__table-wrap">
        <table>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex}>{cell}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {item.caption ? <p className="attachment__caption">{item.caption}</p> : null}
    </figure>
  );
}

function AttachmentImage({
  item,
  runId,
  index,
}: {
  item: AttachmentItem;
  runId: string;
  index: number;
}) {
  const { t } = useI18n();
  const [saveState, setSaveState] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<DesktopError | null>(null);
  const hasRef = item.ref !== undefined && item.ref !== "";
  const webURL = isWebURL(item.url) ? item.url : undefined;

  const image = useQuery({
    queryKey: ["attachment-image", runId, index],
    queryFn: () => bridge.attachmentImage(runId, index),
    enabled: hasRef,
    staleTime: Number.POSITIVE_INFINITY,
    retry: 1,
  });

  async function save() {
    setSaveError(null);
    try {
      const path = await bridge.saveAttachment(runId, index, item.title ?? `attachment-${index + 1}`);
      if (path !== "") {
        setSaveState(path);
      }
    } catch (cause) {
      setSaveError(toDesktopError(cause));
    }
  }

  return (
    <figure className="attachment attachment--image">
      <div className="attachment__image-box">
        {webURL ? (
          <img
            src={webURL}
            alt={t("attachments.imageAlt", { title: item.title ?? String(index + 1) })}
            decoding="async"
            loading="lazy"
            referrerPolicy="no-referrer"
          />
        ) : hasRef ? (
          image.isPending ? (
            <span className="note">{t("attachments.loading")}</span>
          ) : image.isError ? (
            <span className="note note--danger">{t("attachments.failed")}</span>
          ) : (
            <img
              src={image.data}
              alt={t("attachments.imageAlt", { title: item.title ?? String(index + 1) })}
              decoding="async"
            />
          )
        ) : (
          <span className="note">{t("attachments.failed")}</span>
        )}
      </div>
      <figcaption className="attachment__head">
        <span className="attachment__title">{item.title ?? t("attachments.image")}</span>
        {item.caption ? <span className="attachment__caption">{item.caption}</span> : null}
        {hasRef ? (
          <button type="button" className="btn btn-ghost btn-sm" onClick={() => void save()}>
            {t("attachments.download")}
          </button>
        ) : null}
      </figcaption>
      {saveState ? (
        <p className="note" role="status">
          {t("attachments.saved", { path: saveState })}
        </p>
      ) : null}
      {saveError ? <ErrorMessage error={saveError} /> : null}
    </figure>
  );
}

function AttachmentLinkOrFile({
  item,
  runId,
  index,
}: {
  item: AttachmentItem;
  runId: string;
  index: number;
}) {
  const { t } = useI18n();
  const [saveState, setSaveState] = useState<string | null>(null);
  const [error, setError] = useState<DesktopError | null>(null);
  const webURL = isWebURL(item.url) ? item.url : undefined;
  const hasRef = item.ref !== undefined && item.ref !== "";
  const meta = [item.mime_type, formatBytes(item.size_bytes)].filter(Boolean).join(" · ");

  async function open() {
    if (!webURL) {
      return;
    }
    setError(null);
    try {
      await bridge.openURL(webURL);
    } catch (cause) {
      setError(toDesktopError(cause));
    }
  }

  async function save() {
    setError(null);
    try {
      const path = await bridge.saveAttachment(runId, index, item.title ?? `attachment-${index + 1}`);
      if (path !== "") {
        setSaveState(path);
      }
    } catch (cause) {
      setError(toDesktopError(cause));
    }
  }

  return (
    <div className="attachment attachment--row">
      <div className="attachment__row-body">
        <span className="attachment__title">{item.title ?? t("attachments.file")}</span>
        {item.caption ? <span className="attachment__caption">{item.caption}</span> : null}
        {meta ? <span className="attachment__meta">{meta}</span> : null}
        {webURL ? <span className="attachment__meta mono">{webURL}</span> : null}
      </div>
      <div className="attachment__row-actions">
        {webURL ? (
          <button type="button" className="btn btn-ghost btn-sm" onClick={() => void open()}>
            {t("attachments.open")}
          </button>
        ) : null}
        {hasRef ? (
          <button type="button" className="btn btn-ghost btn-sm" onClick={() => void save()}>
            {t("attachments.download")}
          </button>
        ) : null}
      </div>
      {saveState ? (
        <p className="note" role="status">
          {t("attachments.saved", { path: saveState })}
        </p>
      ) : null}
      {error ? <ErrorMessage error={error} /> : null}
    </div>
  );
}

function AttachmentItemView({
  item,
  runId,
  index,
}: {
  item: AttachmentItem;
  runId: string;
  index: number;
}) {
  if (item.type === "table") {
    return <AttachmentTable item={item} />;
  }
  if (item.type === "image") {
    return <AttachmentImage item={item} runId={runId} index={index} />;
  }
  return <AttachmentLinkOrFile item={item} runId={runId} index={index} />;
}

export function AttachmentsBlock({
  data,
  runId,
}: {
  data: AttachmentsBlockData;
  runId: string;
}) {
  const { t } = useI18n();
  return (
    <div className="attachments-block block-card">
      <div className="block-head">
        <span className="block-title">{t("attachments.title")}</span>
      </div>
      {data.items.length === 0 ? (
        <p className="note">{t("attachments.empty")}</p>
      ) : (
        <div className="attachments-block__list">
          {data.items.map((item, index) => (
            <AttachmentItemView
              key={`${item.type}-${item.ref ?? item.url ?? index}`}
              item={item}
              runId={runId}
              index={index}
            />
          ))}
        </div>
      )}
    </div>
  );
}
