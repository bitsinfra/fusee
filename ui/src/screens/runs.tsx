import type { ColumnDef } from "@tanstack/react-table";
import { Link2, Play, RotateCcw, Search, XCircle } from "lucide-react";
import * as React from "react";
import { api, type RunSummary } from "../lib/api";
import { ago, runDuration, when } from "../lib/format";
import { ATTENTION, type Liveness } from "../lib/live";
import { go, href } from "../lib/route";
import { Button } from "../components/button";
import { Confirm, TabsList, TabsRoot } from "../components/overlay";
import { CopyButton, EmptyState, Mono } from "../components/primitives";
import { StatusPill } from "../components/status";
import { DataTable } from "../components/table";
import { useToast } from "../components/toast";
import { Header, Truth } from "./shell";

const VIEWS = [
  { value: "all", label: "All" },
  { value: "running", label: "Running" },
  { value: "attention", label: "Needs attention" },
  { value: "failed", label: "Failed" },
] as const;

const LIVE = new Set(["running", "suspended", "parked", "queued"]);

function inView(r: RunSummary, view: string): boolean {
  switch (view) {
    case "running":
      return LIVE.has(r.status);
    case "attention":
      return ATTENTION.has(r.status);
    case "failed":
      return r.status === "failed";
    default:
      return true;
  }
}

export function FirstRun({ language }: { language?: string }) {
  const [lang, setLang] = React.useState(language ?? "typescript");
  const code: Record<string, string> = {
    typescript: `const engine = await Engine.open({ workflows: [checkout] });\nconst run = await engine.start(checkout, "order-1", order);\nconsole.log(await run.result());`,
    python: `engine = await fusee.Engine.open(workflows=[checkout])\nrun = await engine.start(checkout, "order-1", order)\nprint(await run.result())`,
    rust: `engine.register(CHECKOUT, checkout);\nlet run = engine.start(CHECKOUT, "order-1", &order).await?;\nprintln!("{}", run.result().await?);`,
  };
  return (
    <EmptyState title="No runs yet. Start one from your code:">
      <div className="mt-1 flex gap-1">
        {Object.keys(code).map((l) => (
          <Button key={l} size="xs" variant={l === lang ? "secondary" : "ghost"} onClick={() => setLang(l)}>
            {l === "typescript" ? "TypeScript" : l === "python" ? "Python" : "Rust"}
          </Button>
        ))}
      </div>
      <pre className="mt-2 overflow-x-auto rounded-md border border-ds-border-subtle bg-ds-surface-sunken px-3 py-2 font-mono text-ui-code text-ds-text-strong">{code[lang]}</pre>
      <p className="mt-2">
        Or from a terminal: <Mono>fusee start &lt;type&gt; &lt;id&gt; '&lt;input&gt;'</Mono>. The{" "}
        <a className="text-ds-accent-text underline-offset-4 hover:underline" href="https://github.com/bitsinfra/fusee#quick-start" target="_blank" rel="noreferrer">
          quick start
        </a>{" "}
        walks through one.
      </p>
    </EmptyState>
  );
}

export function RunsScreen({ runs, live, view, reload }: { runs: RunSummary[]; live: Liveness; view: string; reload: () => void }) {
  const toast = useToast();
  const [q, setQ] = React.useState("");
  const [type, setType] = React.useState("");
  const [status, setStatus] = React.useState("");
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [confirm, setConfirm] = React.useState<null | "cancel" | "terminate">(null);
  const [now, setNow] = React.useState(Date.now());
  React.useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  const types = React.useMemo(() => [...new Set(runs.map((r) => r.type))].filter((t) => !t.startsWith("fusee.")).sort(), [runs]);
  const shown = React.useMemo(
    () =>
      runs.filter(
        (r) =>
          !r.type.startsWith("fusee.") &&
          inView(r, view) &&
          (!q || r.id.toLowerCase().includes(q.toLowerCase())) &&
          (!type || r.type === type) &&
          (!status || r.status === status),
      ),
    [runs, view, q, type, status],
  );
  const filtered = q || type || status;

  const columns = React.useMemo<ColumnDef<RunSummary, unknown>[]>(
    () => [
      { id: "id", header: "Workflow ID", cell: ({ row }) => <Mono title={row.original.id}>{row.original.id}</Mono> },
      { id: "type", header: "Type", size: 140, meta: { minTable: 720 }, cell: ({ row }) => row.original.type },
      { id: "status", header: "Status", size: 130, cell: ({ row }) => <StatusPill status={row.original.status} /> },
      { id: "version", header: "Version", size: 70, meta: { minTable: 720 }, cell: ({ row }) => <span className="text-ds-text-muted">v{row.original.version}</span> },
      {
        id: "started",
        header: "Started",
        size: 110,
        meta: { right: true, minTable: 560 },
        cell: ({ row }) => (
          <span className="text-ds-text-muted" title={when(row.original.startedMs)}>
            {ago(row.original.startedMs, now)}
          </span>
        ),
      },
      {
        id: "duration",
        header: "Duration",
        size: 90,
        meta: { right: true, minTable: 440 },
        cell: ({ row }) => <span className="tabular text-ds-text-muted">{row.original.finishedMs ? runDuration(row.original) : "In progress"}</span>,
      },
    ],
    [now],
  );

  const ids = [...selected];
  const bulk = async (what: "retry" | "resume" | "cancel" | "terminate") => {
    let ok = 0;
    const errors: string[] = [];
    for (const id of ids) {
      try {
        if (what === "retry") await api.retry(id);
        else if (what === "resume") await api.resume(id);
        else if (what === "cancel") await api.cancel(id, "cancelled from the UI");
        else await api.terminate(id, "terminated from the UI");
        ok++;
      } catch (e) {
        errors.push(`${id}: ${(e as Error).message}`);
      }
    }
    const done = { retry: "Retried", resume: "Resumed", cancel: "Cancelled", terminate: "Terminated" }[what];
    toast(`${done} ${ok} of ${ids.length}`, errors.length ? "error" : "info");
    if (errors.length) toast(errors.slice(0, 3).join("; "), "error");
    setSelected(new Set());
    reload();
  };

  return (
    <TabsRoot value={view} onValueChange={(v) => go(["runs"], { view: v === "all" ? undefined : v })} className="flex min-h-0 flex-1 flex-col">
      <Header
        title="Runs"
        status={<Truth {...live} />}
        actions={<CopyButton text={location.href} label="Copy link" />}
        view={
          <>
            <TabsList tabs={VIEWS.map((v) => ({ value: v.value, label: v.label }))} />
            <div className="ml-auto flex flex-wrap items-center gap-2">
              <label className="relative">
                <span className="sr-only">Search by workflow ID</span>
                <Search className="pointer-events-none absolute left-2 top-1.5 size-4 text-ds-text-subtle" aria-hidden="true" />
                <input
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder="Search by ID"
                  className="h-7 w-48 rounded-md border border-ds-border bg-ds-surface pl-7 pr-2 text-ui-body outline-none placeholder:text-ds-text-subtle focus-visible:outline focus-visible:outline-2 focus-visible:outline-ds-focus"
                />
              </label>
              <select aria-label="Type" value={type} onChange={(e) => setType(e.target.value)} className="h-7 rounded-md border border-ds-border bg-ds-surface px-2 text-ui-body">
                <option value="">Any type</option>
                {types.map((t) => (
                  <option key={t}>{t}</option>
                ))}
              </select>
              <select aria-label="Status" value={status} onChange={(e) => setStatus(e.target.value)} className="h-7 rounded-md border border-ds-border bg-ds-surface px-2 text-ui-body">
                <option value="">Any status</option>
                {["running", "suspended", "parked", "paused", "stuck", "waiting_for_code", "ok", "failed", "cancelled", "timed_out", "terminated"].map((s) => (
                  <option key={s} value={s}>
                    {s.replace(/_/g, " ")}
                  </option>
                ))}
              </select>
              <span className="tabular text-ui-caption text-ds-text-muted">{shown.length} runs</span>
            </div>
          </>
        }
      />
      {selected.size > 0 ? (
        <div className="flex h-10 shrink-0 items-center gap-2 border-b border-ds-border-subtle bg-ds-surface-elevated px-4">
          <span className="text-ui-body">{selected.size} selected</span>
          <Button size="sm" variant="outline" onClick={() => bulk("retry")}>
            <RotateCcw aria-hidden="true" /> Retry failed step
          </Button>
          <Button size="sm" variant="outline" onClick={() => bulk("resume")}>
            <Play aria-hidden="true" /> Resume paused
          </Button>
          <Button size="sm" variant="outline" onClick={() => setConfirm("cancel")}>
            Cancel
          </Button>
          <Button size="sm" variant="destructive-outline" onClick={() => setConfirm("terminate")}>
            <XCircle aria-hidden="true" /> Terminate
          </Button>
          <Button size="sm" variant="ghost" onClick={() => setSelected(new Set())}>
            Clear selection
          </Button>
        </div>
      ) : null}
      <DataTable
        rows={shown}
        columns={columns}
        rowId={(r) => r.id}
        selectable
        selected={selected}
        onSelectedChange={setSelected}
        onOpen={(r) => go(["run", r.id])}
        empty={
          live.state === "loading" ? (
            <div className="space-y-2 p-4" aria-busy="true">
              {[0, 1, 2, 3].map((i) => (
                <div key={i} className="h-6 animate-pulse rounded-md bg-ds-surface-elevated motion-reduce:animate-none" />
              ))}
            </div>
          ) : live.state === "offline" && runs.length === 0 ? (
            <EmptyState title="The engine is not answering.">
              {live.error}. Is the app running? <Mono>fusee dev</Mono> starts it and serves this page.
            </EmptyState>
          ) : filtered || view !== "all" ? (
            <EmptyState title="No runs match." action={<Button variant="outline" size="sm" onClick={() => { setQ(""); setType(""); setStatus(""); go(["runs"]); }}>Clear filters</Button>}>
              {view === "attention" ? "Nothing is stuck, waiting for code or failed." : "Change the filters to see more."}
            </EmptyState>
          ) : (
            <FirstRun />
          )
        }
      />
      <Confirm
        open={confirm === "cancel"}
        onOpenChange={(o) => !o && setConfirm(null)}
        title={`Cancel ${selected.size} runs?`}
        action="Cancel runs"
        destructive={false}
        onConfirm={() => bulk("cancel")}
      >
        Cancellation reaches each run's waits; its code may compensate before it ends.
      </Confirm>
      <Confirm
        open={confirm === "terminate"}
        onOpenChange={(o) => !o && setConfirm(null)}
        title={`Terminate ${selected.size} runs?`}
        action="Terminate runs"
        typeToConfirm={String(selected.size)}
        onConfirm={() => bulk("terminate")}
      >
        Each run ends at once: a step still running is discarded, no compensation runs, and children follow their close policy.
      </Confirm>
      <a hidden href={href(["runs"])}>
        <Link2 />
      </a>
    </TabsRoot>
  );
}
