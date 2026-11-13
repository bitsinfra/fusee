// One set of formatters: relative where recency is the point, absolute for
// records, and microseconds where Fusee measures them.

import { isEncoded } from "./api";

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto", style: "short" });
const abs = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" });

export function ago(ms: number, now = Date.now()): string {
  const s = Math.round((ms - now) / 1000);
  const a = Math.abs(s);
  if (a < 5) return "just now";
  if (a < 60) return rtf.format(s, "second");
  if (a < 3600) return rtf.format(Math.round(s / 60), "minute");
  if (a < 86400) return rtf.format(Math.round(s / 3600), "hour");
  return rtf.format(Math.round(s / 86400), "day");
}

export function when(ms: number): string {
  return abs.format(new Date(ms));
}

/** A duration from milliseconds (fractions are microseconds): "38 µs", "1.2 s". */
export function duration(ms: number): string {
  if (ms <= 0) return "< 1 ms";
  if (ms < 1) return `${Math.round(ms * 1000)} µs`;
  if (ms < 1000) return `${ms < 10 ? ms.toFixed(1) : Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)} s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
  return `${Math.floor(ms / 3_600_000)}h ${Math.round((ms % 3_600_000) / 60_000)}m`;
}

export function runDuration(r: { startedMs: number; finishedMs: number | null }, now = Date.now()): string {
  return duration((r.finishedMs ?? now) - r.startedMs);
}

// A payload a codec encrypted (and no codec endpoint decoded) reads as
// "encrypted" in text.
const sealed = (_k: string, x: unknown) => (isEncoded(x) ? "encrypted" : x);

export function json(v: unknown): string {
  return typeof v === "string" ? JSON.stringify(v) : JSON.stringify(v, sealed, 2);
}

export function short(v: unknown, n = 80): string {
  const s = typeof v === "string" ? v : isEncoded(v) ? "encrypted" : JSON.stringify(v, sealed);
  return s === undefined ? "" : s.length > n ? `${s.slice(0, n)}…` : s;
}
