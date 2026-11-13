import { ArrowLeft, Download, MoreHorizontal, Play, RotateCcw, Send, StopCircle, XCircle } from "lucide-react";
import * as React from "react";
import { api, errorMessage, events, failureOf, type Failure, type JournalRecord, type RunDetail } from "../lib/api";
import { cn } from "../lib/cn";
import { ago, duration, short, when } from "../lib/format";
import { FINISHED, usePoll } from "../lib/live";
import { go, href } from "../lib/route";
import { Button, IconButton } from "../components/button";
import { JsonView } from "../components/json";
import { Confirm, MenuContent, MenuItem, MenuRoot, MenuSeparator, MenuTrigger, Modal, TabsContent, TabsList, TabsRoot } from "../components/overlay";
import { CopyButton, EmptyState, Kbd, Mono, Notice, Skeleton } from "../components/primitives";
import { StatusDot, StatusPill, toneFill, toneOf } from "../components/status";
import { useToast } from "../components/toast";
import { Header } from "./shell";

/** One operation of the run: a step, a sleep, a wait, a child, a call. */
interface Op {
  seq: number;
  kind: string;
  name: string;
  status: string;
  attempts: number;
  record?: JournalRecord;
  done?: JournalRecord;
  tries: JournalRecord[];
  /** When the step began (Unix ms) and took (ms): dev profile only. */
  began?: number;
  took?: number;
}

function operations(journal: JournalRecord[], times?: Record<string, [number, number]>): Op[] {
  const ops = new Map<number, Op>();
  const get = (r: JournalRecord, kind: string, name: string) => {
    const seq = r.seq as number;
    let op = ops.get(seq);
    if (!op) {
      op = { seq, kind, name, status: "running", attempts: 1, tries: [] };
      ops.set(seq, op);
    }
    return op;
  };
  for (const r of journal) {
    if (typeof r.seq !== "number") continue;
    switch (r.kind) {
      case "step": {
        const op = get(r, "step", String(r.name));
        op.record = r;
        op.status = r.status === "ok" ? "ok" : String(r.status);
        op.attempts = Number(r.attempt ?? 1);
        break;
      }
      case "attempt": {
        const op = get(r, "step", String(r.name));
        op.tries.push(r);
        op.attempts = Math.max(op.attempts, Number(r.attempt ?? 1) + 1);
        if (r.paused === true) {
          // Waiting for a resume, not for its next attempt.
          op.status = "paused";
          op.attempts = Number(r.attempt ?? 1);
        }
        break;
      }
      case "resume": {
        const op = ops.get(r.seq as number);
        if (op) {
          op.tries.push(r);
          op.status = "running";
          op.attempts = Number(r.attempt ?? 0) + 1;
        }
        break;
      }
      case "timer":
        get(r, "sleep", String(r.name)).record = r;
        break;
      case "fired": {
        const op = get(r, "sleep", "");
        op.status = "ok";
        op.done = r;
        break;
      }
      case "wait":
        get(r, "wait", String(r.name)).record = r;
        break;
      case "waited": {
        const op = get(r, "wait", "");
        op.done = r;
        op.status = (r.outcome as { timeout?: unknown } | undefined)?.timeout !== undefined ? "timed_out" : "ok";
        break;
      }
      case "child":
      case "call": {
        const op = get(r, r.kind, String(r.kind === "call" ? `${r.type}.${r.handler}` : r.id));
        op.record = r;
        break;
      }
      case "child_done": {
        const op = get(r, "child", "");
        op.done = r;
        op.status = String(r.status);
        break;
      }
      case "await":
        get(r, "await", String(r.name)).record = r;
        break;
      case "completed": {
        const op = get(r, "await", "");
        op.done = r;
        op.status = String(r.status);
        break;
      }
    }
  }
  if (times) {
    for (const [seq, [began, took]] of Object.entries(times)) {
      const op = ops.get(Number(seq));
      if (op) Object.assign(op, { began, took });
    }
  }
  return [...ops.values()].sort((a, b) => a.seq - b.seq);
}

/** Where a step sits in the run's time, as percentages of its span. */
function placement(op: Op, from: number, span: number): { left: string; width: string } {
  const left = Math.min(100, Math.max(0, ((op.began ?? from) - from) / span) * 100);
  const width = Math.min(100 - left, ((op.took ?? 0) / span) * 100);
  return { left: `${left}%`, width: `${width}%` };
}

/** What was asked of the run from outside, and who asked (audit). */
function requests(journal: JournalRecord[]): { what: string; detail: string; by?: string }[] {
  const out: { what: string; detail: string; by?: string }[] = [];
  for (const r of journal) {
    const by = typeof r.by === "string" ? r.by : undefined;
    if (r.kind === "signal") out.push({ what: "Signal", detail: String(r.name), by });
    else if (r.kind === "cancel") out.push({ what: "Cancel", detail: String(r.reason ?? ""), by });
    else if (r.kind === "resume") out.push({ what: "Resume", detail: `step at position ${r.seq}`, by });
    else if (r.kind === "done" && r.status === "terminated") out.push({ what: "Terminate", detail: short(r.payload ?? "", 60), by });
    else if (r.kind === "done" && r.status === "reset") out.push({ what: "Reset", detail: "a new run replaced this one", by });
    else if (r.kind === "start" && r.from === "reset") out.push({ what: "Reset", detail: "this run replaced the one before", by });
  }
  return out;
}

function FailureView({ f }: { f: Failure }) {
  const root = (() => {
    let x = f;
    while (x.cause) x = x.cause;
    return x;
  })();
  return (
    <div className="space-y-3">
      <p className="text-ui-body text-ds-text-strong">{root.message}</p>
      <dl className="grid grid-cols-[8rem_1fr] gap-x-3 gap-y-1 text-ui-body">
        {root.type ? (
          <>
            <dt className="text-ds-text-muted">Type</dt>
            <dd>
              <Mono>{root.type}</Mono>
            </dd>
          </>
        ) : null}
        {root.code ? (
          <>
            <dt className="text-ds-text-muted">Code</dt>
            <dd>
              <Mono>{root.code}</Mono>
            </dd>
          </>
        ) : null}
        <dt className="text-ds-text-muted">Retried</dt>
        <dd>{root.retryable === false ? "No: the failure is final" : "Under the step's retry policy"}</dd>
        {f !== root ? (
          <>
            <dt className="text-ds-text-muted">Raised as</dt>
            <dd className="text-ds-text-muted">{f.message}</dd>
          </>
        ) : null}
      </dl>
      {root.details !== undefined ? (
        <div>
          <p className="mb-1 text-ui-caption text-ds-text-muted">Details</p>
          <JsonView value={root.details} />
        </div>
      ) : null}
      {root.stack ? (
        <details open className="group">
          <summary className="cursor-pointer text-ui-caption text-ds-text-muted">Technical details</summary>
          <pre className="mt-1 max-h-72 overflow-auto rounded-md border border-ds-border-subtle bg-ds-surface-sunken p-2 font-mono text-ui-code text-ds-text-muted">{root.stack}</pre>
        </details>
      ) : null}
    </div>
  );
}

function OpPanel({ op, journal }: { op: Op; journal: JournalRecord[] }) {
  const failed = op.status === "failed";
  const out = op.record?.payload ?? op.done?.payload;
  const tabs = [
    ...(op.kind === "child" || op.kind === "call" ? [{ value: "input", label: "Input" }] : []),
    { value: "output", label: "Output" },
    ...(failed || op.tries.length ? [{ value: "error", label: "Error" }] : []),
    { value: "attempts", label: `Attempts (${op.attempts})` },
    { value: "records", label: "Records" },
  ];
  const [tab, setTab] = React.useState(failed ? "error" : "output");
  const lastError = failed ? failureOf(out) : failureOf(op.tries.at(-1)?.error);
  return (
    <div className="border-t border-ds-border-subtle px-4 py-3">
      <div className="mb-2 flex items-center gap-2">
        <StatusDot tone={toneOf(op.status)} />
        <span className="font-mono text-ui-code">{op.name}</span>
        <span className="text-ui-caption text-ds-text-muted">{op.kind}</span>
        {op.began !== undefined && op.took !== undefined ? (
          <span className="ml-auto text-ui-caption text-ds-text-muted">
            began {when(op.began)} · took {duration(op.took)}
          </span>
        ) : null}
      </div>
      <TabsRoot value={tab} onValueChange={setTab}>
        <TabsList tabs={tabs} className="mb-3 border-b border-ds-border-subtle pb-1.5" />
        <TabsContent value="input">
          <JsonView value={op.record?.input} />
        </TabsContent>
        <TabsContent value="output">{failed ? <p className="text-ui-body text-ds-text-subtle">The step failed; see Error.</p> : op.status === "running" ? <p className="text-ui-body text-ds-text-subtle">Not finished.</p> : <JsonView value={out} />}</TabsContent>
        <TabsContent value="error">{lastError ? <FailureView f={lastError} /> : null}</TabsContent>
        <TabsContent value="attempts">
          <ul className="divide-y divide-ds-border-subtle">
            {op.tries.map((t) => (
              <li key={String(t.attempt)} className="flex gap-3 py-1.5 text-ui-body">
                <span className="w-20 shrink-0 text-ds-text-muted">Attempt {String(t.attempt)}</span>
                <span className="text-ds-error">{failureOf(t.error)?.message}</span>
              </li>
            ))}
            {op.record ? (
              <li className="flex gap-3 py-1.5 text-ui-body">
                <span className="w-20 shrink-0 text-ds-text-muted">Attempt {String(op.record.attempt ?? op.attempts)}</span>
                <span className={failed ? "text-ds-error" : "text-ds-text-strong"}>{failed ? failureOf(out)?.message : "Succeeded"}</span>
              </li>
            ) : null}
          </ul>
        </TabsContent>
        <TabsContent value="records">
          <JsonView value={journal.filter((r) => r.seq === op.seq)} />
        </TabsContent>
      </TabsRoot>
    </div>
  );
}

function Streams({ id, names }: { id: string; names: string[] }) {
  const [name, setName] = React.useState(names[0] ?? "");
  const [chunks, setChunks] = React.useState<{ offset: number; value: unknown }[]>([]);
  const [end, setEnd] = React.useState<string | null>(null);
  React.useEffect(() => {
    if (!name) return;
    setChunks([]);
    setEnd(null);
    const es = events(`/v1/runs/${encodeURIComponent(id)}/streams/${encodeURIComponent(name)}`);
    es.addEventListener("chunk", (e) => setChunks((c) => [...c, JSON.parse((e as MessageEvent).data)]));
    es.addEventListener("end", (e) => {
      setEnd(JSON.parse((e as MessageEvent).data).status ?? "ok");
      es.close();
    });
    return () => es.close();
  }, [id, name]);
  if (!names.length) return <EmptyState title="No streams.">A step or the workflow writes one with <Mono>stream(name)</Mono>; readers follow it as it grows.</EmptyState>;
  return (
    <div className="px-4 py-3">
      <div className="mb-2 flex items-center gap-2">
        <select aria-label="Stream" value={name} onChange={(e) => setName(e.target.value)} className="h-7 rounded-md border border-ds-border bg-ds-surface px-2 text-ui-body">
          {names.map((n) => (
            <option key={n}>{n}</option>
          ))}
        </select>
        <span className="text-ui-caption text-ds-text-muted">
          {chunks.length} chunks{end ? ` · ended ${end}` : " · following"}
        </span>
      </div>
      <div className="max-h-[60vh] overflow-y-auto rounded-md border border-ds-border-subtle bg-ds-surface-sunken p-2 font-mono text-ui-code">
        {chunks.map((c) => (
          <div key={c.offset} className="flex gap-3">
            <span className="w-10 shrink-0 text-right text-ds-text-subtle">{c.offset}</span>
            <span className="whitespace-pre-wrap break-all">{typeof c.value === "string" ? c.value : JSON.stringify(c.value)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function RunScreen({ id }: { id: string }) {
  const toast = useToast();
  const [done, setDone] = React.useState(false);
  const { data: run, error, reload } = usePoll<RunDetail>(() => api.run(id), 1000, [id], !done);
  const { data: result } = usePoll(() => api.result(id), 60_000, [id, run?.status], !!run && FINISHED.has(run.status) && run.status !== "ok");
  React.useEffect(() => setDone(!!run?.finishedMs), [run?.finishedMs]);
  const [open, setOpen] = React.useState<number | null>(null);
  const [tab, setTab] = React.useState("steps");
  const [dialog, setDialog] = React.useState<null | "signal" | "terminate" | "cancel" | "reset">(null);
  const [signal, setSignal] = React.useState({ name: "", value: "null" });
  const [resetTo, setResetTo] = React.useState("");

  if (error && !run) {
    return (
      <>
        <Header title={<Mono>{id}</Mono>} actions={<Button variant="outline" asChild><a href={href(["runs"])}><ArrowLeft aria-hidden="true" /> Runs</a></Button>} />
        <EmptyState title={error.includes("not found") ? "No run with this workflow ID." : "The run cannot be read."}>{error}</EmptyState>
      </>
    );
  }
  if (!run) {
    return (
      <div className="space-y-3 p-4" aria-busy="true">
        <Skeleton className="h-6 w-64" />
        <Skeleton className="h-4 w-40" />
        <Skeleton className="h-40 w-full" />
      </div>
    );
  }
  const ops = operations(run.journal, run.stepTimes);
  const steps = ops.filter((o) => o.kind === "step");
  const children = run.journal.filter((r) => r.kind === "child");
  const streamNames = [...new Set(run.journal.filter((r) => r.kind === "chunk" || r.kind === "stream_end").map((r) => String(r.name)))];
  const failed = run.status === "failed";
  const stuck = run.status === "stuck";
  const pausedOp = run.status === "paused" ? ops.find((o) => o.status === "paused") : undefined;
  const pausedError = pausedOp?.tries.filter((t) => t.kind === "attempt").at(-1)?.error as Failure | string | undefined;
  const pausedMessage = typeof pausedError === "string" ? errorMessage(pausedError) : pausedError?.message;
  const live = !run.finishedMs;
  const took = (run.finishedMs ?? Date.now()) - run.startedMs;
  // With step times (dev profile) the step list is also a timeline.
  const timed = ops.some((o) => o.took !== undefined);
  const span = Math.max(1, took, ...ops.map((o) => (o.began ?? 0) + (o.took ?? 0) - run.startedMs));
  const act = async (f: () => Promise<unknown>, said: string) => {
    try {
      await f();
      toast(said);
      setDone(false);
      reload();
    } catch (e) {
      toast((e as Error).message, "error");
    }
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Header
        title={<Mono className="text-ui-detail-title">{run.id}</Mono>}
        meta={
          <>
            {run.type} v{run.version} · run {run.run} · {duration(took)} · started {ago(run.startedMs)}
          </>
        }
        status={<StatusPill status={run.status} />}
        actions={
          <>
            {failed ? (
              <Button onClick={() => act(() => api.retry(run.id), "Retrying from the failed step")}>
                <RotateCcw aria-hidden="true" /> Retry
              </Button>
            ) : run.status === "paused" ? (
              <Button onClick={() => act(() => api.resume(run.id), "Resumed: the step runs again")}>
                <Play aria-hidden="true" /> Resume
              </Button>
            ) : live ? (
              <Button onClick={() => setDialog("signal")}>
                <Send aria-hidden="true" /> Signal
              </Button>
            ) : null}
            <CopyButton text={location.href} label="Copy link" />
            <MenuRoot>
              <MenuTrigger asChild>
                <IconButton variant="outline" label="More actions">
                  <MoreHorizontal />
                </IconButton>
              </MenuTrigger>
              <MenuContent>
                <MenuItem disabled={!live} onSelect={() => setDialog("cancel")}>
                  <StopCircle /> Cancel
                </MenuItem>
                <MenuItem disabled={!live} destructive onSelect={() => setDialog("terminate")}>
                  <XCircle /> Terminate
                </MenuItem>
                <MenuSeparator />
                <MenuItem disabled={!steps.length} onSelect={() => setDialog("reset")}>
                  <RotateCcw /> Reset to a step
                </MenuItem>
                <MenuItem onSelect={() => window.open(api.exportUrl(run.id))}>
                  <Download /> Export journal
                </MenuItem>
              </MenuContent>
            </MenuRoot>
          </>
        }
      />
      <div className="min-h-0 flex-1 overflow-y-auto">
        {stuck ? (
          <Notice
            tone="error"
            className="m-4"
            title="This run is stuck: its code no longer matches its journal"
            actions={
              <>
                <Button size="sm" variant="outline" onClick={() => setDialog("reset")}>
                  Reset to a step
                </Button>
                <Button size="sm" variant="outline" asChild>
                  <a href="https://github.com/bitsinfra/fusee/blob/main/docs/errors.md#determinism" target="_blank" rel="noreferrer">
                    How to fix it
                  </a>
                </Button>
              </>
            }
          >
            <p className="font-mono text-ui-code text-ds-text-strong">{errorMessage(run.error)}</p>
            <p className="mt-1">Deploy code that matches the journal, guard the change with <Mono>patched("…")</Mono> so older runs take the old path, or reset the run to before the step that differs.</p>
          </Notice>
        ) : null}
        {pausedOp ? (
          <Notice
            tone="warning"
            className="m-4"
            title={`This run is paused: step ${pausedOp.name} ran out of retries`}
            actions={
              <Button size="sm" onClick={() => act(() => api.resume(run.id), "Resumed: the step runs again")}>
                <Play aria-hidden="true" /> Resume
              </Button>
            }
          >
            {pausedMessage ? <p className="font-mono text-ui-code text-ds-text-strong">{pausedMessage}</p> : null}
            <p className="mt-1">
              The run waits here, and so do its parent and anyone waiting on its result. Fix the cause (deploy the fix if it is in the code), then resume: the step runs
              again at once with a fresh set of attempts.
            </p>
          </Notice>
        ) : null}
        {failed && result?.error ? (
          <div className="m-4 rounded-lg border border-ds-error/40 bg-ds-error-muted/40 p-3">
            <FailureView f={result.error} />
          </div>
        ) : null}
        <TabsRoot value={tab} onValueChange={setTab}>
          <div className="border-b border-ds-border-subtle px-4 py-1.5">
            <TabsList
              tabs={[
                { value: "steps", label: `Steps (${steps.length})` },
                { value: "journal", label: "Journal" },
                { value: "streams", label: `Streams${streamNames.length ? ` (${streamNames.length})` : ""}` },
                { value: "children", label: `Children${children.length ? ` (${children.length})` : ""}` },
                { value: "diagnostics", label: "Diagnostics" },
              ]}
            />
          </div>
          <TabsContent value="steps">
            {ops.length === 0 ? <EmptyState title="No operations yet.">The run has started; its first step, sleep or wait shows here.</EmptyState> : null}
            <ul className="divide-y divide-ds-border-subtle">
              {ops.map((op) => (
                <li key={op.seq}>
                  <button
                    type="button"
                    onClick={() => setOpen(open === op.seq ? null : op.seq)}
                    aria-expanded={open === op.seq}
                    className={cn(
                      "grid h-[34px] w-full items-center gap-3 px-4 text-left transition-interactive hover:bg-ds-table-row-hover",
                      timed ? "grid-cols-[1rem_minmax(0,1fr)_5rem_4rem_7rem] sm:grid-cols-[1rem_minmax(0,1fr)_minmax(4rem,12rem)_5rem_4rem_7rem]" : "grid-cols-[1rem_minmax(0,1fr)_6rem_7rem]",
                      open === op.seq && "bg-ds-table-row-selected",
                    )}
                  >
                    <StatusDot tone={toneOf(op.status)} pulse={op.status === "running" && live} />
                    <span className="min-w-0 truncate">
                      <span className="font-mono text-ui-code">{op.name || op.kind}</span>
                      <span className="ml-2 text-ui-caption text-ds-text-subtle">{op.kind}</span>
                    </span>
                    {timed ? (
                      <span aria-hidden="true" className="relative hidden h-1.5 rounded-full bg-ds-surface-sunken sm:block">
                        {op.took !== undefined ? <span className={cn("absolute inset-y-0 min-w-[2px] rounded-full", toneFill(toneOf(op.status)))} style={placement(op, run.startedMs, span)} /> : null}
                      </span>
                    ) : null}
                    <span className="text-right text-ui-caption text-ds-text-muted">{op.attempts > 1 ? `${op.attempts} attempts` : ""}</span>
                    {timed ? <span className="text-right font-mono text-ui-caption tabular-nums text-ds-text-muted">{op.took !== undefined ? duration(op.took) : ""}</span> : null}
                    <span className="truncate text-right text-ui-caption text-ds-text-muted">
                      {op.status === "running" ? (op.kind === "wait" ? "waiting" : op.kind === "sleep" ? "sleeping" : "running") : op.status === "ok" ? short(op.record?.payload ?? "", 18) : op.status}
                    </span>
                  </button>
                  {open === op.seq ? <OpPanel op={op} journal={run.journal} /> : null}
                </li>
              ))}
            </ul>
          </TabsContent>
          <TabsContent value="journal" className="px-4 py-3">
            <JsonView value={run.journal} />
          </TabsContent>
          <TabsContent value="streams">
            <Streams id={run.id} names={streamNames} />
          </TabsContent>
          <TabsContent value="children">
            {children.length === 0 ? <EmptyState title="No children.">A child starts with <Mono>ctx.child(workflow, id, input)</Mono>.</EmptyState> : null}
            <ul className="divide-y divide-ds-border-subtle">
              {children.map((c) => (
                <li key={String(c.seq)}>
                  <a href={href(["run", String(c.id)])} className="flex h-[34px] items-center gap-3 px-4 hover:bg-ds-table-row-hover">
                    <Mono>{String(c.id)}</Mono>
                    <span className="text-ui-caption text-ds-text-muted">{String(c.type)}</span>
                  </a>
                </li>
              ))}
            </ul>
          </TabsContent>
          <TabsContent value="diagnostics" className="px-4 py-3">
            <dl className="grid grid-cols-[10rem_1fr] gap-x-3 gap-y-1.5 text-ui-body">
              <dt className="text-ds-text-muted">Started</dt>
              <dd>{when(run.startedMs)}</dd>
              <dt className="text-ds-text-muted">Finished</dt>
              <dd>{run.finishedMs ? when(run.finishedMs) : "—"}</dd>
              <dt className="text-ds-text-muted">Shard, run</dt>
              <dd className="font-mono text-ui-code">
                {run.shard}, {run.run}
              </dd>
              <dt className="text-ds-text-muted">Journal records</dt>
              <dd className="tabular">{run.journal.length}</dd>
              <dt className="text-ds-text-muted">Search attributes</dt>
              <dd>
                <JsonView value={run.attributes ?? {}} />
              </dd>
            </dl>
            <h3 className="mt-5 mb-1.5 text-ui-section">Requests</h3>
            {(() => {
              const asked = requests(run.journal);
              if (asked.length === 0) return <p className="text-ui-body text-ds-text-subtle">No signals, cancellations, terminations or resets.</p>;
              return (
                <ul className="divide-y divide-ds-border-subtle rounded-md border border-ds-border-subtle">
                  {asked.map((q, i) => (
                    <li key={i} className="grid grid-cols-[6rem_minmax(0,1fr)_minmax(0,12rem)] items-center gap-3 px-3 py-1.5 text-ui-body">
                      <span className="text-ds-text-muted">{q.what}</span>
                      <span className="truncate font-mono text-ui-code">{q.detail}</span>
                      <span className="truncate text-right text-ui-caption text-ds-text-muted" title={q.by ? `asked by ${q.by}` : "made in code, or before audit"}>
                        {q.by ?? "—"}
                      </span>
                    </li>
                  ))}
                </ul>
              );
            })()}
          </TabsContent>
        </TabsRoot>
      </div>

      <Modal
        open={dialog === "signal"}
        onOpenChange={(o) => !o && setDialog(null)}
        title={`Signal ${run.id}`}
        description="The value is JSON; it is recorded once the signal is durable."
        footer={
          <>
            <Button variant="outline" onClick={() => setDialog(null)}>
              Cancel
            </Button>
            <Button
              disabled={!signal.name}
              onClick={() => {
                let v: unknown;
                try {
                  v = JSON.parse(signal.value || "null");
                } catch {
                  toast("The value is not JSON", "error");
                  return;
                }
                setDialog(null);
                act(() => api.signal(run.id, signal.name, v), `Signalled ${signal.name}`);
              }}
            >
              Send signal
            </Button>
          </>
        }
      >
        <div className="space-y-2">
          <label className="block text-ui-label text-ds-text-muted">
            Name
            <input value={signal.name} onChange={(e) => setSignal({ ...signal, name: e.target.value })} className="mt-1 block h-7 w-full rounded-md border border-ds-border bg-ds-surface px-2 font-mono text-ui-code text-ds-text-strong" />
          </label>
          <label className="block text-ui-label text-ds-text-muted">
            Value
            <textarea value={signal.value} onChange={(e) => setSignal({ ...signal, value: e.target.value })} rows={3} className="mt-1 block w-full rounded-md border border-ds-border bg-ds-surface px-2 py-1 font-mono text-ui-code text-ds-text-strong" />
          </label>
        </div>
      </Modal>
      <Confirm open={dialog === "cancel"} onOpenChange={(o) => !o && setDialog(null)} title={`Cancel ${run.id}?`} action="Cancel run" destructive={false} onConfirm={() => act(() => api.cancel(run.id, "cancelled from the UI"), "Cancellation delivered")}>
        Cancellation reaches the run's waits; its code may compensate before it ends.
      </Confirm>
      <Confirm open={dialog === "terminate"} onOpenChange={(o) => !o && setDialog(null)} title={`Terminate ${run.id}?`} action="Terminate" onConfirm={() => act(() => api.terminate(run.id, "terminated from the UI"), "Terminated")}>
        A step still running is discarded; its children follow their close policy. Nothing compensates.
      </Confirm>
      <Confirm
        open={dialog === "reset"}
        onOpenChange={(o) => !o && setDialog(null)}
        title={`Reset ${run.id}?`}
        action="Reset"
        onConfirm={() => act(() => api.reset(run.id, resetTo || steps.at(-1)?.name || ""), "Reset; a new run continues")}
      >
        <p>A new run replaces this one: its journal is cut before the step you pick, and that step and those after it run again.</p>
        <select aria-label="Step" value={resetTo || steps.at(-1)?.name || ""} onChange={(e) => setResetTo(e.target.value)} className="mt-2 h-7 w-full rounded-md border border-ds-border bg-ds-surface px-2 font-mono text-ui-code text-ds-text-strong">
          {steps.map((s) => (
            <option key={s.seq} value={s.name}>
              before {s.name}
            </option>
          ))}
        </select>
      </Confirm>
      <span hidden>
        <Kbd>j</Kbd>
        {go.name}
      </span>
    </div>
  );
}
