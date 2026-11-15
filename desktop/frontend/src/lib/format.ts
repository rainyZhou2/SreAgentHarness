import type { Locale } from "../i18n/messages";

const relativeUnits: Array<{ unit: Intl.RelativeTimeFormatUnit; seconds: number }> = [
  { unit: "year", seconds: 60 * 60 * 24 * 365 },
  { unit: "month", seconds: 60 * 60 * 24 * 30 },
  { unit: "week", seconds: 60 * 60 * 24 * 7 },
  { unit: "day", seconds: 60 * 60 * 24 },
  { unit: "hour", seconds: 60 * 60 },
  { unit: "minute", seconds: 60 },
  { unit: "second", seconds: 1 },
];

function intlLocale(locale: Locale): string {
  return locale === "zh" ? "zh-CN" : "en-US";
}

function parseTimestamp(value: string | undefined): Date | null {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function formatRelativeTime(
  value: string | undefined,
  locale: Locale,
  now: number = Date.now(),
): string {
  const date = parseTimestamp(value);
  if (!date) {
    return "";
  }
  const deltaSeconds = (date.getTime() - now) / 1000;
  const formatter = new Intl.RelativeTimeFormat(intlLocale(locale), { numeric: "auto" });
  for (const { unit, seconds } of relativeUnits) {
    if (Math.abs(deltaSeconds) >= seconds || unit === "second") {
      const amount = deltaSeconds / seconds;
      return formatter.format(Math.round(amount), unit);
    }
  }
  return formatter.format(0, "second");
}

export function formatDateTime(value: string | undefined, locale: Locale): string {
  const date = parseTimestamp(value);
  if (!date) {
    return "";
  }
  return new Intl.DateTimeFormat(intlLocale(locale), {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function formatTimeRange(
  window: { start: string; end: string } | undefined,
  locale: Locale,
): string {
  if (!window) {
    return "";
  }
  const start = takeTime(window.start);
  const end = takeTime(window.end);
  if (!start || !end) {
    return "";
  }
  const startDate = parseTimestamp(window.start);
  const endDate = parseTimestamp(window.end);
  if (!startDate || !endDate) {
    return "";
  }
  const day = new Intl.DateTimeFormat(intlLocale(locale), { dateStyle: "medium" });
  return `${day.format(startDate)} ${start} – ${end}`;
}

function takeTime(value: string): string {
  const date = parseTimestamp(value);
  if (!date) {
    return "";
  }
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

export function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined || bytes <= 0) {
    return "";
  }
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  const rounded = index === 0 ? String(value) : value.toFixed(value < 10 ? 1 : 0);
  return `${rounded} ${units[index] ?? "B"}`;
}

export function formatNumber(value: number | undefined, locale: Locale): string {
  if (value === undefined) {
    return "";
  }
  return new Intl.NumberFormat(intlLocale(locale)).format(value);
}

export function formatCost(value: number | undefined, locale: Locale): string {
  if (value === undefined) {
    return "";
  }
  return new Intl.NumberFormat(intlLocale(locale), {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: value < 1 ? 4 : 2,
  }).format(value);
}

export function truncateTitle(text: string, maxLength = 80): string {
  const firstLine = text.split("\n")[0]?.trim() ?? "";
  if (firstLine.length <= maxLength) {
    return firstLine;
  }
  return `${firstLine.slice(0, maxLength - 1)}…`;
}
