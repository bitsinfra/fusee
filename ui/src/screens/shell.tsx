import { Activity, AlertTriangle, Boxes, CalendarClock, Gauge, Layers, ListTree, Menu as MenuIcon, Moon, Sun, Contrast, X } from "lucide-react";
import * as React from "react";
import type { Info } from "../lib/api";
import { cn } from "../lib/cn";
import { href } from "../lib/route";
import { appearance, highContrast, setAppearance, setHighContrast } from "../lib/theme";
import { IconButton } from "../components/button";
import { Kbd, MOD } from "../components/primitives";
import { Tooltip } from "../components/overlay";

export const NAV = [
  { id: "runs", label: "Runs", icon: ListTree },
  { id: "attention", label: "Needs attention", icon: AlertTriangle },
  { id: "types", label: "Types", icon: Layers },
  { id: "schedules", label: "Schedules", icon: CalendarClock },
  { id: "objects", label: "Objects", icon: Boxes },
  { id: "limits", label: "Limits", icon: Gauge },
  { id: "health", label: "Health", icon: Activity },
] as const;

function Mark() {
  // Fusee's own mark: the fusee's cone, drawn as two strokes.
  return (
    <svg viewBox="0 0 16 16" className="size-4 text-ds-text-strong" aria-hidden="true">
      <path d="M2 3h12M4 8h8M6 13h4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
    </svg>
  );
}

function ThemeControls() {
  const [a, setA] = React.useState(appearance());
  const [hc, setHc] = React.useState(highContrast());
  const dark = a === "dark" || (a === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
  return (
    <div className="flex items-center gap-1">
      <IconButton
        size="icon-sm"
        label={dark ? "Light theme" : "Dark theme"}
        onClick={() => {
          const next = dark ? "light" : "dark";
          setAppearance(next);
          setA(next);
        }}
      >
        {dark ? <Sun /> : <Moon />}
      </IconButton>
      <IconButton
        size="icon-sm"
        label={hc ? "Standard contrast" : "High contrast"}
        aria-pressed={hc}
        onClick={() => {
          setHighContrast(!hc);
          setHc(!hc);
        }}
      >
        <Contrast />
      </IconButton>
    </div>
  );
}

export function Shell({
  info,
  current,
  attention,
  onPalette,
  children,
}: {
  info: Info | null;
  current: string;
  attention: number;
  onPalette: () => void;
  children: React.ReactNode;
}) {
  const [open, setOpen] = React.useState(false);
  const where = info?.dir ?? info?.http?.url ?? (info ? info.durability : "connecting…");
  const rail = (
    <nav aria-label="Main" className="flex h-full flex-col gap-3 px-2 py-3">
      <div className="flex items-start gap-2 px-2">
        <Mark />
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5">
            <span className="text-ui-section">Fusee</span>
            {info?.profile === "dev" ? (
              <span className="rounded-full border border-ds-border px-1.5 text-ui-badge text-ds-text-muted" title="The dev profile: history kept, strict checks, not for production">
                dev
              </span>
            ) : null}
            {info?.namespace ? (
              <span
                className="truncate rounded-full border border-ds-border px-1.5 text-ui-badge text-ds-text-muted"
                title={`This token reaches the runs under ${info.namespace}/ only`}
              >
                {info.namespace}
              </span>
            ) : null}
          </div>
          <p className="truncate font-mono text-ui-code text-ds-sidebar-text-muted" title={where}>
            {where}
          </p>
          {info?.viewer && !["http", "token"].includes(info.viewer) ? (
            <p className="truncate text-ui-caption text-ds-sidebar-text-muted" title="Signals, cancellations, terminations and resets made here record this name">
              as {info.viewer}
            </p>
          ) : null}
        </div>
      </div>
      <button
        type="button"
        onClick={onPalette}
        className="mx-1 flex h-7 items-center gap-2 rounded-md border border-ds-sidebar-border bg-ds-surface px-2 text-ui-body text-ds-text-subtle transition-interactive hover:border-ds-border"
      >
        <span className="flex-1 text-left">Search or jump to…</span>
        <Kbd>{MOD}K</Kbd>
      </button>
      <ul className="flex flex-col gap-px">
        {NAV.map((n) => {
          const Icon = n.icon;
          const active = current === n.id;
          return (
            <li key={n.id}>
              <a
                href={href([n.id])}
                onClick={() => setOpen(false)}
                aria-current={active ? "page" : undefined}
                className={cn(
                  "flex h-[30px] items-center gap-2 rounded-md px-2 text-ui-body text-ds-sidebar-text transition-interactive hover:bg-ds-sidebar-item-hover",
                  active && "bg-ds-sidebar-item-active font-semibold text-ds-sidebar-text-active",
                )}
              >
                <Icon className="size-4 shrink-0 opacity-80" strokeWidth={1.75} aria-hidden="true" />
                <span className="flex-1 truncate">{n.label}</span>
                {n.id === "attention" && attention > 0 ? <span className="tabular text-ui-caption text-ds-error">{attention}</span> : null}
              </a>
            </li>
          );
        })}
      </ul>
      <div className="mt-auto flex items-center justify-between px-2">
        <a className="text-ui-caption text-ds-sidebar-text-muted hover:text-ds-text-strong" href="/v1/openapi.json" target="_blank" rel="noreferrer">
          API
        </a>
        <ThemeControls />
      </div>
    </nav>
  );
  return (
    <div className="flex h-dvh min-h-0 bg-ds-shell-bg">
      <aside className="hidden w-sidebar shrink-0 bg-ds-sidebar-bg md:block">{rail}</aside>
      {open ? (
        <div className="fixed inset-0 z-40 md:hidden">
          <div className="absolute inset-0 bg-black/30" onClick={() => setOpen(false)} />
          <aside className="absolute inset-y-0 left-0 w-sidebar bg-ds-sidebar-bg shadow-lg">
            <div className="absolute right-2 top-2">
              <IconButton size="icon-sm" label="Close the menu" onClick={() => setOpen(false)}>
                <X />
              </IconButton>
            </div>
            {rail}
          </aside>
        </div>
      ) : null}
      <main className="flex min-w-0 flex-1 flex-col md:py-2 md:pr-2">
        <div className="flex h-10 items-center gap-2 border-b border-ds-border-subtle bg-ds-surface px-3 md:hidden">
          <IconButton size="icon-sm" label="Menu" onClick={() => setOpen(true)}>
            <MenuIcon />
          </IconButton>
          <Mark />
          <span className="text-ui-section">Fusee</span>
        </div>
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden bg-ds-surface md:rounded-xl md:border md:border-ds-border-subtle">{children}</div>
      </main>
    </div>
  );
}

/** The one header pattern: a location row (title, then actions in a fixed
 * order), then one view row (tabs, then search, filter, sort, display). */
export function Header({ title, meta, status, actions, view }: { title: React.ReactNode; meta?: React.ReactNode; status?: React.ReactNode; actions?: React.ReactNode; view?: React.ReactNode }) {
  return (
    <header className="shrink-0 border-b border-ds-border-subtle">
      <div className="flex min-h-12 flex-wrap items-center gap-x-3 gap-y-1 px-4 py-2">
        <div className="min-w-0 flex-1">
          <h1 className="truncate text-ui-page-title">{title}</h1>
          {meta ? <p className="truncate text-ui-caption text-ds-text-muted">{meta}</p> : null}
        </div>
        {status}
        {actions ? <div className="flex items-center gap-1.5">{actions}</div> : null}
      </div>
      {view ? <div className="flex flex-wrap items-center gap-2 px-4 pb-2">{view}</div> : null}
    </header>
  );
}

export function Truth({ state, staleSince, error }: { state: string; staleSince: number | null; error: string | null }) {
  if (state === "live") return null;
  if (state === "loading") return <span className="text-ui-caption text-ds-text-subtle">Loading…</span>;
  return (
    <Tooltip content={error ?? "The engine is not answering"}>
      <span className="inline-flex items-center gap-1.5 rounded-full border border-ds-warning/40 bg-ds-warning-muted px-2 text-ui-caption text-ds-text-strong">
        Offline{staleSince ? ` · stale since ${new Date(staleSince).toLocaleTimeString()}` : ""}
      </span>
    </Tooltip>
  );
}
