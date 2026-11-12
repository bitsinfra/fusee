import { AlertTriangle, Check, Copy, Info, XCircle } from "lucide-react";
import * as React from "react";
import { cn } from "../lib/cn";
import { Button } from "./button";

/** A key cap, shown in menus and tooltips. */
export function Kbd({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <kbd
      className={cn(
        "inline-flex h-5 min-w-5 items-center justify-center rounded-sm border border-ds-border bg-ds-surface-elevated px-1 font-sans text-ui-badge text-ds-text-muted",
        className,
      )}
    >
      {children}
    </kbd>
  );
}

export const MOD = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform) ? "⌘" : "Ctrl";

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("animate-pulse rounded-md bg-ds-surface-elevated motion-reduce:animate-none", className)} />;
}

/** What belongs here, why it is empty, and what to do. */
export function EmptyState({ title, children, action }: { title: string; children?: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="flex flex-col items-start gap-2 px-4 py-10 sm:px-6">
      <p className="text-ui-heading text-ds-text-strong">{title}</p>
      {children ? <div className="max-w-xl text-ui-body text-ds-text-muted">{children}</div> : null}
      {action}
    </div>
  );
}

const NOTICE = {
  info: { icon: Info, box: "border-ds-border bg-ds-surface-elevated", icon_: "text-ds-info" },
  warning: { icon: AlertTriangle, box: "border-ds-warning/40 bg-ds-warning-muted", icon_: "text-ds-warning" },
  error: { icon: XCircle, box: "border-ds-error/40 bg-ds-error-muted", icon_: "text-ds-error" },
};

/** A rounded box with a full hairline border and a muted fill. */
export function Notice({
  tone = "info",
  title,
  children,
  actions,
  className,
}: {
  tone?: keyof typeof NOTICE;
  title: React.ReactNode;
  children?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
}) {
  const t = NOTICE[tone];
  const Icon = t.icon;
  return (
    <div role={tone === "error" ? "alert" : "status"} className={cn("flex gap-2.5 rounded-lg border px-3 py-2.5", t.box, className)}>
      <Icon className={cn("mt-0.5 size-4 shrink-0", t.icon_)} aria-hidden="true" />
      <div className="min-w-0 flex-1">
        <p className="text-ui-section text-ds-text-strong">{title}</p>
        {children ? <div className="mt-0.5 text-ui-body text-ds-text-muted">{children}</div> : null}
        {actions ? <div className="mt-2 flex flex-wrap gap-1.5">{actions}</div> : null}
      </div>
    </div>
  );
}

export function MetricTile({ label, value, hint }: { label: string; value: React.ReactNode; hint?: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5 px-3 py-2">
      <span className="truncate text-ui-caption text-ds-text-muted">{label}</span>
      <span className="tabular text-ui-heading text-ds-text-strong">{value}</span>
      {hint ? <span className="truncate text-ui-caption text-ds-text-subtle">{hint}</span> : null}
    </div>
  );
}

export function Badge({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <span className={cn("inline-flex h-5 items-center rounded-full border border-ds-border-subtle px-1.5 text-ui-badge text-ds-text-muted", className)}>
      {children}
    </span>
  );
}

/** A monospace ID or name. */
export function Mono({ children, className, title }: { children: React.ReactNode; className?: string; title?: string }) {
  return (
    <span title={title} className={cn("font-mono text-ui-code", className)}>
      {children}
    </span>
  );
}

export function CopyButton({ text, label = "Copy" }: { text: string; label?: string }) {
  const [done, setDone] = React.useState(false);
  return (
    <Button
      variant="outline"
      size="sm"
      onClick={() =>
        navigator.clipboard.writeText(text).then(() => {
          setDone(true);
          setTimeout(() => setDone(false), 1200);
        })
      }
    >
      {done ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
      {done ? "Copied" : label}
    </Button>
  );
}

/** Section heading on the continuous work surface. */
export function Section({ title, actions, children, className }: { title: string; actions?: React.ReactNode; children: React.ReactNode; className?: string }) {
  return (
    <section className={cn("border-t border-ds-border-subtle first:border-t-0", className)}>
      <div className="flex h-10 items-center justify-between gap-2 px-4">
        <h2 className="text-ui-section text-ds-text-strong">{title}</h2>
        {actions}
      </div>
      {children}
    </section>
  );
}
