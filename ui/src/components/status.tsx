// A neutral pill with a hairline border and a 6px semantic dot. Chromatic
// colour means status, and nothing else.
import { cn } from "../lib/cn";

export type Tone = "success" | "warning" | "error" | "info" | "live" | "neutral";

const TONES: Record<string, Tone> = {
  ok: "success",
  running: "live",
  suspended: "info",
  parked: "neutral",
  queued: "neutral",
  failed: "error",
  stuck: "error",
  paused: "warning",
  waiting_for_code: "warning",
  cancelled: "warning",
  timed_out: "warning",
  terminated: "neutral",
  continued_as_new: "neutral",
  reset: "neutral",
};

const LABELS: Record<string, string> = {
  ok: "Succeeded",
  running: "Running",
  suspended: "Waiting",
  parked: "Waiting",
  queued: "Queued",
  failed: "Failed",
  stuck: "Stuck",
  paused: "Paused",
  waiting_for_code: "Waiting for code",
  cancelled: "Cancelled",
  timed_out: "Timed out",
  terminated: "Terminated",
};

export function toneOf(status: string): Tone {
  return TONES[status] ?? "neutral";
}

export function statusLabel(status: string): string {
  return LABELS[status] ?? status.replace(/_/g, " ");
}

const DOT: Record<Tone, string> = {
  success: "bg-ds-success",
  warning: "bg-ds-warning",
  error: "bg-ds-error",
  info: "bg-ds-info",
  live: "bg-ds-live",
  neutral: "bg-ds-mark-gray",
};

/** The fill of a tone's dot, for marks drawn in its colour (a timeline bar). */
export function toneFill(tone: Tone): string {
  return DOT[tone];
}

export function StatusDot({ tone, className, pulse }: { tone: Tone; className?: string; pulse?: boolean }) {
  return (
    <span aria-hidden="true" className={cn("relative inline-flex size-1.5 shrink-0 rounded-full", DOT[tone], className)}>
      {pulse ? <span className={cn("absolute inset-0 animate-ping rounded-full opacity-60 motion-reduce:hidden", DOT[tone])} /> : null}
    </span>
  );
}

export function StatusPill({ status, className, children }: { status: string; className?: string; children?: React.ReactNode }) {
  const tone = toneOf(status);
  return (
    <span
      className={cn(
        "inline-flex h-6 items-center gap-1.5 whitespace-nowrap rounded-full border border-ds-border-subtle bg-ds-surface px-2 text-ui-caption leading-none text-ds-text-strong",
        tone === "neutral" && "text-ds-text-muted",
        className,
      )}
    >
      <StatusDot tone={tone} pulse={status === "running"} />
      {children ?? statusLabel(status)}
    </span>
  );
}
