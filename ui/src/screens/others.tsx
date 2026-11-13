import type { ColumnDef } from "@tanstack/react-table";
import { Pause, Pencil, Play, Zap } from "lucide-react";
import * as React from "react";
import { api, errorMessage, type LimitInfo, type RunSummary, type TypeInfo } from "../lib/api";
import { ago, duration, when } from "../lib/format";
import { type Liveness, usePoll } from "../lib/live";
import { go, href } from "../lib/route";
import { Button } from "../components/button";
import { JsonView } from "../components/json";
import { EmptyState, MetricTile, Mono, Notice, Section } from "../components/primitives";
import { StatusPill } from "../components/status";
import { DataTable } from "../components/table";
import { useToast } from "../components/toast";
import { Confirm } from "../components/overlay";
import { Header, Truth } from "./shell";

const user = (r: RunSummary) => !r.type.startsWith("fusee.");

// ------------------------------------------------------------------ attention

function reason(r: RunSummary): string {
  if (r.status === "stuck") return errorMessage(r.error) ?? "The code no longer matches the journal";
  if (r.status === "waiting_for_code") return `No process runs code for ${r.type} v${r.version}`;
  if (r.status === "paused") return "A step ran out of retries: resume it once the cause is fixed";
  return errorMessage(r.error) ?? "Failed";
}

export function AttentionScreen({ runs, live }: { runs: RunSummary[]; live: Liveness }) {
  const rows = runs.filter((r) => user(r) && (r.status === "stuck" || r.status === "paused" || r.status === "waiting_for_code" || r.status === "failed"));
  const stuck = rows.filter((r) => r.status === "stuck").length;
  const paused = rows.filter((r) => r.status === "paused");
  const toast = useToast();
  const resumeAll = async () => {
    let ok = 0;
    const errors: string[] = [];
    for (const r of paused) {
      try {
        await api.resume(r.id);
        ok++;
      } catch (e) {
        errors.push(`${r.id}: ${(e as Error).message}`);
      }
    }
    toast(`Resumed ${ok} of ${paused.length}`, errors.length ? "error" : "info");
    if (errors.length) toast(errors.slice(0, 3).join("; "), "error");
  };
  const columns = React.useMemo<ColumnDef<RunSummary, unknown>[]>(
    () => [
      { id: "id", header: "Workflow ID", size: 240, cell: ({ row }) => <Mono>{row.original.id}</Mono> },
      { id: "status", header: "Status", size: 130, cell: ({ row }) => <StatusPill status={row.original.status} /> },
      { id: "reason", header: "Reason", cell: ({ row }) => <span className="text-ds-text-muted" title={reason(row.original)}>{reason(row.original)}</span> },
      { id: "type", header: "Type", size: 120, meta: { minTable: 720 }, cell: ({ row }) => `${row.original.type} v${row.original.version}` },
      { id: "when", header: "Started", size: 100, meta: { right: true, minTable: 560 }, cell: ({ row }) => <span className="text-ds-text-muted">{ago(row.original.startedMs)}</span> },
    ],
    [],
  );
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Header title="Needs attention" meta="Stuck, paused and failed runs, and runs waiting for code" status={<Truth {...live} />} />
      {stuck > 0 ? (
        <Notice tone="error" className="m-4" title={`${stuck} run${stuck === 1 ? " is" : "s are"} stuck`}>
          A stuck run's code no longer matches its journal. It waits, unchanged, until code that matches is deployed, the change is guarded with{" "}
          <Mono>patched()</Mono>, or it is reset to before the step that differs. Open one to see the difference.
        </Notice>
      ) : null}
      {paused.length > 0 ? (
        <Notice
          tone="warning"
          className="m-4"
          title={`${paused.length} run${paused.length === 1 ? " is" : "s are"} paused`}
          actions={
            <Button size="sm" variant="outline" onClick={resumeAll}>
              <Play aria-hidden="true" /> Resume {paused.length === 1 ? "it" : `all ${paused.length}`}
            </Button>
          }
        >
          A step ran out of retries and its run waits, with its callers, instead of failing. Fix the cause (deploy the fix if it is in the code), then resume: the step runs again with a fresh set of attempts.
        </Notice>
      ) : null}
      <DataTable
        rows={rows}
        columns={columns}
        rowId={(r) => r.id}
        onOpen={(r) => go(["run", r.id])}
        empty={<EmptyState title="Nothing needs attention.">No run is stuck, paused, waiting for code, or failed.</EmptyState>}
      />
    </div>
  );
}

// ------------------------------------------------------------------ types

function HealthBar({ runs }: { runs: RunSummary[] }) {
  const day = Date.now() - 86_400_000;
  const recent = runs.filter((r) => r.startedMs >= day).slice(0, 60).reverse();
  if (!recent.length) return <span className="text-ui-caption text-ds-text-subtle">No runs</span>;
  return (
    <span className="flex h-4 items-end gap-px" aria-label={`${recent.filter((r) => r.status === "failed").length} of ${recent.length} failed in 24 hours`}>
      {recent.map((r) => (
        <span key={r.id} title={`${r.id}: ${r.status}`} className={r.status === "failed" || r.status === "stuck" ? "h-4 w-1 rounded-sm bg-ds-error" : r.finishedMs ? "h-4 w-1 rounded-sm bg-ds-border-strong" : "h-2 w-1 rounded-sm bg-ds-live"} />
      ))}
    </span>
  );
}

export function TypesScreen({ runs }: { runs: RunSummary[] }) {
  const { data: types, error } = usePoll<TypeInfo[]>(api.types, 5000, []);
  const byType = new Map<string, RunSummary[]>();
  for (const r of runs.filter(user)) byType.set(r.type, [...(byType.get(r.type) ?? []), r]);
  const names = [...new Set([...(types ?? []).map((t) => t.name), ...byType.keys()])].sort();
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Header title="Types" meta="Registered workflow types, their versions, and live runs per version" />
      {error ? <Notice tone="warning" className="m-4" title="Registered types are not available">{error}</Notice> : null}
      {names.length === 0 ? (
        <EmptyState title="No workflow types yet.">An engine registers its types when it opens: <Mono>Engine.open({"{ workflows: [checkout] }"})</Mono>.</EmptyState>
      ) : (
        <ul className="min-h-0 flex-1 divide-y divide-ds-border-subtle overflow-y-auto">
          {names.map((n) => {
            const t = types?.find((x) => x.name === n);
            const rs = byType.get(n) ?? [];
            const versions = [...new Set([...(t?.versions ?? []), ...rs.map((r) => r.version)])].sort((a, b) => a - b);
            const last = rs[0];
            return (
              <li key={n} className="grid grid-cols-1 gap-2 px-4 py-2.5 sm:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)_10rem]">
                <div className="min-w-0">
                  <a className="font-mono text-ui-code hover:underline" href={href(["runs"], { type: n })} onClick={(e) => { e.preventDefault(); go(["runs"]); }}>
                    {n}
                  </a>
                  <p className="text-ui-caption text-ds-text-muted">
                    {t ? (t.object ? "object type" : "workflow") : "no code registered here"}
                    {last ? ` · last run ${ago(last.startedMs)}` : ""}
                  </p>
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {versions.map((v) => {
                    const liveRuns = rs.filter((r) => r.version === v && !r.finishedMs).length;
                    const newest = v === versions.at(-1);
                    return (
                      <span key={v} className="inline-flex h-6 items-center gap-1.5 rounded-full border border-ds-border-subtle px-2 text-ui-caption" title={liveRuns === 0 && !newest ? "No live runs: this version's code can go" : undefined}>
                        v{v}
                        <span className="tabular text-ds-text-muted">{liveRuns} live</span>
                        {t && !t.versions.includes(v) ? <span className="text-ds-warning">no code</span> : null}
                      </span>
                    );
                  })}
                </div>
                <div className="flex flex-col items-start gap-1 sm:items-end">
                  <span className="text-ui-caption text-ds-text-muted">Health · 24 hours</span>
                  <HealthBar runs={rs} />
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

// ------------------------------------------------------------------ health

function percentile(xs: number[], p: number): number {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((s.length - 1) * p))] ?? 0;
}

function Line({ points, max, label }: { points: number[]; max: number; label: string }) {
  if (points.length < 2) return <p className="px-4 py-6 text-ui-body text-ds-text-subtle">Not enough runs to draw.</p>;
  const w = 600;
  const h = 120;
  const d = points.map((v, i) => `${i ? "L" : "M"}${((i / (points.length - 1)) * w).toFixed(1)},${(h - (v / (max || 1)) * (h - 8) - 4).toFixed(1)}`).join(" ");
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="h-32 w-full" role="img" aria-label={label} preserveAspectRatio="none">
      {[0.25, 0.5, 0.75].map((f) => (
        <line key={f} x1="0" x2={w} y1={h * f} y2={h * f} className="stroke-ds-border-subtle" strokeWidth="1" vectorEffect="non-scaling-stroke" />
      ))}
      <path d={d} fill="none" className="stroke-ds-info" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

export function HealthScreen({ runs }: { runs: RunSummary[] }) {
  const { data: m } = usePoll(api.metrics, 2000, []);
  const prev = React.useRef<{ t: number; steps: number } | null>(null);
  const [rate, setRate] = React.useState<number | null>(null);
  React.useEffect(() => {
    const steps = m?.total?.steps_run;
    if (steps === undefined) return;
    const now = Date.now();
    if (prev.current) setRate(((steps - prev.current.steps) * 1000) / Math.max(1, now - prev.current.t));
    prev.current = { t: now, steps };
  }, [m]);
  const done = runs.filter((r) => user(r) && r.finishedMs);
  const ok = done.filter((r) => r.status === "ok").length;
  const failed = done.filter((r) => r.status === "failed");
  const times = done.map((r) => (r.finishedMs ?? 0) - r.startedMs);
  const t = m?.total ?? {};
  const flushes = t.flushes ?? 0;
  const failuresByType = new Map<string, number>();
  for (const r of failed) failuresByType.set(r.type, (failuresByType.get(r.type) ?? 0) + 1);
  const buckets = new Map<number, number>();
  for (const r of runs.filter(user)) {
    const b = Math.floor(r.startedMs / 60_000);
    buckets.set(b, (buckets.get(b) ?? 0) + 1);
  }
  const keys = [...buckets.keys()].sort((a, b) => a - b).slice(-60);
  const series = keys.map((k) => buckets.get(k) ?? 0);
  return (
    <div className="min-h-0 flex-1 overflow-y-auto">
      <Header title="Health" meta="From this engine's runs and counters" />
      <div className="grid grid-cols-2 divide-x divide-y divide-ds-border-subtle border-b border-ds-border-subtle sm:grid-cols-4 lg:grid-cols-7">
        <MetricTile label="Runs" value={runs.filter(user).length} />
        <MetricTile label="Success rate" value={done.length ? `${Math.round((ok / done.length) * 100)}%` : "—"} />
        <MetricTile label="Median duration" value={times.length ? duration(percentile(times, 0.5)) : "—"} />
        <MetricTile label="95th percentile" value={times.length ? duration(percentile(times, 0.95)) : "—"} />
        <MetricTile label="Steps a second" value={rate === null ? "—" : Math.round(rate).toLocaleString()} />
        <MetricTile label="Records per flush" value={flushes ? ((t.records ?? 0) / flushes).toFixed(1) : "—"} />
        <MetricTile label="Flush time" value={flushes && t.flush_us_total ? duration(t.flush_us_total / flushes / 1000) : "—"} hint={t.max_flush_us ? `max ${duration(t.max_flush_us / 1000)}` : undefined} />
      </div>
      <Section title="Runs over time" actions={<span className="text-ui-caption text-ds-text-muted">starts per minute</span>}>
        <div className="px-4 pb-3">
          <Line points={series} max={Math.max(...series, 1)} label="Run starts per minute" />
        </div>
      </Section>
      <Section title="Failures by type">
        {failuresByType.size === 0 ? (
          <p className="px-4 pb-3 text-ui-body text-ds-text-subtle">No failures.</p>
        ) : (
          <ul className="divide-y divide-ds-border-subtle">
            {[...failuresByType].sort((a, b) => b[1] - a[1]).map(([k, n]) => (
              <li key={k} className="flex h-8 items-center justify-between px-4 text-ui-body">
                <Mono>{k}</Mono>
                <span className="tabular">{n}</span>
              </li>
            ))}
          </ul>
        )}
      </Section>
      <Section title="Counters">
        <div className="px-4 pb-4">
          <JsonView value={m?.total ?? {}} />
        </div>
      </Section>
    </div>
  );
}

// ------------------------------------------------------------------ schedules

export function SchedulesScreen() {
  const toast = useToast();
  const { data, error, reload } = usePoll(api.schedules, 3000, []);
  const [open, setOpen] = React.useState<string | null>(null);
  const { data: detail } = usePoll(() => (open ? api.schedule(open) : Promise.resolve(null)), 3000, [open], !!open);
  const op = (name: string, o: "pause" | "resume" | "trigger") =>
    api
      .scheduleOp(name, o)
      .then(() => {
        toast(o === "trigger" ? `Started a run of ${name}` : `${o === "pause" ? "Paused" : "Resumed"} ${name}`);
        reload();
      })
      .catch((e: Error) => toast(e.message, "error"));
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Header title="Schedules" />
      {error ? <Notice tone="warning" className="m-4" title="Schedules are not available">{error}</Notice> : null}
      {data && data.length === 0 ? (
        <EmptyState title="No schedules.">A schedule starts a workflow on a cron expression or an interval: <Mono>engine.schedule("nightly", {"{ cron: \"0 3 * * *\" }"}, report)</Mono>.</EmptyState>
      ) : null}
      <ul className="min-h-0 flex-1 divide-y divide-ds-border-subtle overflow-y-auto">
        {(data ?? []).map((s) => (
          <li key={s.name}>
            <div className="flex h-[34px] items-center gap-3 px-4">
              <button type="button" className="min-w-0 flex-1 truncate text-left font-mono text-ui-code hover:underline" onClick={() => setOpen(open === s.name ? null : s.name)}>
                {s.name}
              </button>
              <span className="text-ui-caption text-ds-text-muted">since {ago(s.summary.startedMs)}</span>
              <Button size="sm" variant="ghost" onClick={() => op(s.name, "trigger")}>
                <Zap aria-hidden="true" /> Run now
              </Button>
              <Button size="sm" variant="ghost" onClick={() => op(s.name, "pause")}>
                <Pause aria-hidden="true" /> Pause
              </Button>
              <Button size="sm" variant="ghost" onClick={() => op(s.name, "resume")}>
                <Play aria-hidden="true" /> Resume
              </Button>
            </div>
            {open === s.name ? (
              <div className="px-4 pb-3">
                <JsonView value={detail ?? undefined} />
              </div>
            ) : null}
          </li>
        ))}
      </ul>
    </div>
  );
}

// ------------------------------------------------------------------ objects

/** The fields `after` changes from `before`, removed ones included. */
function changedFields(before: Record<string, unknown>, after: Record<string, unknown>): string[] {
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  return [...keys].filter((k) => JSON.stringify(before[k]) !== JSON.stringify(after[k])).sort();
}

/**
 * The state of a key, editable: the edit is shown as the fields it changes
 * and confirmed, then sent with the version it was made against, so a
 * change made meanwhile is never overwritten.
 */
function StateEditor({ type, objectKey, state, version, onSaved }: {
  type: string;
  objectKey: string;
  state: Record<string, unknown>;
  version?: string;
  onSaved: () => void;
}) {
  const toast = useToast();
  const [draft, setDraft] = React.useState<string | null>(null);
  const [base, setBase] = React.useState<{ state: Record<string, unknown>; version?: string } | null>(null);
  const [confirming, setConfirming] = React.useState(false);
  let parsed: Record<string, unknown> | null = null;
  let invalid: string | null = null;
  if (draft !== null) {
    try {
      const v = JSON.parse(draft);
      if (v && typeof v === "object" && !Array.isArray(v)) parsed = v;
      else invalid = "The state is a JSON object.";
    } catch (e) {
      invalid = (e as Error).message;
    }
  }
  const changes = parsed && base ? changedFields(base.state, parsed) : [];
  const save = async () => {
    if (!parsed || !base) return;
    try {
      const r = await api.editState(type, objectKey, { clearAll: true, set: parsed }, base.version);
      toast(r.changed.length ? `Saved: ${r.changed.join(", ")}` : "Saved: nothing changed");
      setDraft(null);
      onSaved();
    } catch (e) {
      const stale = e instanceof Error && "status" in e && (e as { status: number }).status === 412;
      toast(stale ? "The state changed while you edited it: it was reloaded, edit it again" : (e as Error).message, "error");
      if (stale) {
        setDraft(null);
        onSaved();
      }
    }
  };
  if (draft === null) {
    return (
      <Section
        title="State"
        actions={
          <Button size="sm" variant="outline" onClick={() => { setBase({ state, version }); setDraft(JSON.stringify(state, null, 2)); }}>
            <Pencil aria-hidden="true" /> Edit
          </Button>
        }
      >
        <div className="px-4 pb-3">
          <JsonView value={state} />
          {version ? <p className="mt-2 text-ui-caption text-ds-text-subtle">Version <Mono>{version}</Mono></p> : null}
        </div>
      </Section>
    );
  }
  return (
    <Section title="Edit the state">
      <div className="space-y-2 px-4 pb-3">
        <label className="sr-only" htmlFor="state-draft">The state, as JSON</label>
        <textarea
          id="state-draft"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          spellCheck={false}
          rows={Math.min(24, Math.max(6, draft.split("\n").length + 1))}
          className="w-full rounded-md border border-ds-border bg-ds-surface p-2 font-mono text-ui-code outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-ds-focus"
        />
        <p className={invalid ? "text-ui-caption text-ds-error" : "text-ui-caption text-ds-text-muted"} aria-live="polite">
          {invalid ?? (changes.length ? `Changes ${changes.join(", ")}` : "No change yet")}
        </p>
        <div className="flex gap-2">
          <Button size="sm" disabled={!!invalid || !changes.length} onClick={() => setConfirming(true)}>Save</Button>
          <Button size="sm" variant="ghost" onClick={() => setDraft(null)}>Discard</Button>
        </div>
      </div>
      <Confirm
        open={confirming}
        onOpenChange={setConfirming}
        title={`Change ${changes.length} field${changes.length === 1 ? "" : "s"} of ${type}/${objectKey}?`}
        action="Save the state"
        destructive={false}
        onConfirm={save}
      >
        The edit waits for a handler that is running, then commits as a handler's writes do; handlers that run after it see the new state. Fields:{" "}
        <Mono>{changes.join(", ")}</Mono>.
      </Confirm>
    </Section>
  );
}

export function ObjectsScreen({ runs, route }: { runs: RunSummary[]; route: string[] }) {
  const keys = runs.filter((r) => r.object && !r.finishedMs);
  const [type, key] = [route[1], route.slice(2).join("/")];
  const { data, error, reload } = usePoll(() => (type && key ? api.object(type, key) : Promise.resolve(null)), 2000, [type, key], !!(type && key));
  if (type && key) {
    return (
      <div className="min-h-0 flex-1 overflow-y-auto">
        <Header title={<Mono>{`${type}/${key}`}</Mono>} meta="A key of an object type" status={data ? <StatusPill status={data.status} /> : null} actions={<Button variant="outline" asChild><a href={href(["objects"])}>All keys</a></Button>} />
        {error ? <EmptyState title="This key cannot be read.">{error}</EmptyState> : null}
        <StateEditor
          type={type}
          objectKey={key}
          state={(data?.state ?? data?.objectDetail?.state ?? {}) as Record<string, unknown>}
          version={data?.objectDetail?.version}
          onSaved={reload}
        />
        <Section title="Pending calls">
          <div className="px-4 pb-3">
            <JsonView value={{ running: data?.objectDetail?.running ?? [], waiting: data?.objectDetail?.waiting ?? [] }} />
          </div>
        </Section>
        <Section title="Run">
          <div className="px-4 pb-3">
            <a className="text-ds-accent-text hover:underline" href={href(["run", `${type}/${key}`])}>
              Open the key's run
            </a>
          </div>
        </Section>
      </div>
    );
  }
  const columns: ColumnDef<RunSummary, unknown>[] = [
    { id: "key", header: "Key", cell: ({ row }) => <Mono>{row.original.id}</Mono> },
    { id: "type", header: "Type", size: 160, meta: { minTable: 480 }, cell: ({ row }) => row.original.type },
    { id: "since", header: "Since", size: 120, meta: { right: true, minTable: 560 }, cell: ({ row }) => <span className="text-ds-text-muted" title={when(row.original.startedMs)}>{ago(row.original.startedMs)}</span> },
  ];
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Header title="Objects" meta="Keys of object types: their state and pending calls" />
      <DataTable
        rows={keys}
        columns={columns}
        rowId={(r) => r.id}
        onOpen={(r) => go(["objects", ...r.id.split("/")])}
        empty={<EmptyState title="No object keys.">An object type keeps state per key: <Mono>engine.object(cart, "user-7").call("add", item)</Mono> creates one.</EmptyState>}
      />
    </div>
  );
}

// ------------------------------------------------------------------ limits

const limitColumns: ColumnDef<LimitInfo, unknown>[] = [
  { id: "name", header: "Limit", cell: ({ row }) => <Mono>{row.original.name}</Mono> },
  {
    id: "use",
    header: "In use",
    size: 200,
    cell: ({ row }) => {
      const l = row.original;
      const share = l.concurrent > 0 ? Math.min(1, l.inUse / l.concurrent) : 0;
      return (
        <span className="flex items-center gap-2">
          <span className="tabular w-14 shrink-0">{l.concurrent > 0 ? `${l.inUse} / ${l.concurrent}` : l.inUse}</span>
          {l.concurrent > 0 ? (
            <span className="h-1.5 w-24 overflow-hidden rounded-full bg-ds-hover" aria-hidden>
              <span className={share >= 1 ? "block h-full bg-ds-warning" : "block h-full bg-ds-accent"} style={{ width: `${share * 100}%` }} />
            </span>
          ) : null}
        </span>
      );
    },
  },
  { id: "rate", header: "Per second", size: 110, meta: { right: true, minTable: 560 }, cell: ({ row }) => <span className="tabular text-ds-text-muted">{row.original.perSecond > 0 ? row.original.perSecond : "—"}</span> },
  { id: "waiting", header: "Waiting", size: 90, meta: { right: true }, cell: ({ row }) => <span className={row.original.waiting > 0 ? "tabular text-ds-warning" : "tabular text-ds-text-muted"}>{row.original.waiting}</span> },
];

export function LimitsScreen() {
  const { data: m } = usePoll(api.metrics, 2000, []);
  const { data: limits, error } = usePoll(api.limits, 1000, []);
  const t = m?.total ?? {};
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Header title="Limits" meta="Concurrency and rate limits on steps" />
      <div className="grid grid-cols-2 divide-x divide-ds-border-subtle border-b border-ds-border-subtle sm:grid-cols-4">
        <MetricTile label="Permit waits" value={t.limit_waits ?? t.permit_waits ?? "—"} />
        <MetricTile label="Busy rejections" value={t.busy ?? 0} />
        <MetricTile label="Gate waits" value={t.gate_waits ?? "—"} />
        <MetricTile label="Shards" value={m?.shards?.length ?? "—"} />
      </div>
      {error ? <Notice tone="warning" className="m-4" title="Limits are not available">{error}</Notice> : null}
      <DataTable
        rows={limits ?? []}
        columns={limitColumns}
        rowId={(l) => l.name}
        empty={
          <EmptyState title="No limits defined.">
            A limit is defined with <Mono>engine.limit("stripe", {"{ concurrent: 5 }"})</Mono>; steps take a permit with <Mono>limit: "stripe"</Mono>. <Mono>fusee limits</Mono> lists them from a terminal.
          </EmptyState>
        }
      />
    </div>
  );
}
