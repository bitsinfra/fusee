// Every component and screen state, for review and screenshots (`#/fixtures`).
import * as React from "react";
import type { RunSummary } from "../lib/api";
import { Button } from "../components/button";
import { JsonView } from "../components/json";
import { EmptyState, Kbd, MetricTile, Mono, Notice, Section, Skeleton } from "../components/primitives";
import { StatusPill } from "../components/status";
import { Header, Truth } from "./shell";
import { FirstRun } from "./runs";

const STATUSES = ["running", "suspended", "parked", "queued", "stuck", "paused", "waiting_for_code", "ok", "failed", "cancelled", "timed_out", "terminated"];

export const SAMPLE: RunSummary[] = STATUSES.map((s, i) => ({
  id: `order-${100 + i}`,
  type: i % 2 ? "checkout" : "refund",
  version: 1 + (i % 3),
  status: s,
  shard: 0,
  run: i + 1,
  startedMs: Date.now() - i * 3_600_000,
  finishedMs: ["ok", "failed", "cancelled", "timed_out", "terminated"].includes(s) ? Date.now() - i * 3_600_000 + 23_000 : null,
  error: s === "failed" ? "step 'charge' failed: card declined" : s === "stuck" ? "the journal recorded step 'reserve' here; the code called 'fraud-check'" : null,
  attributes: {},
}));

export function FixturesScreen() {
  return (
    <div className="min-h-0 flex-1 overflow-y-auto">
      <Header title="Fixtures" meta="Every component and state, light and dark (switch in the rail)" />
      <Section title="Buttons">
        <div className="flex flex-wrap gap-2 px-4 pb-3">
          <Button>Primary</Button>
          <Button variant="outline">Outline</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="destructive">Destructive</Button>
          <Button variant="destructive-outline">Destructive outline</Button>
          <Button loading>Loading</Button>
          <Button disabled>Disabled</Button>
          <Button variant="link">Link</Button>
          <Kbd>⌘K</Kbd>
        </div>
      </Section>
      <Section title="Status">
        <div className="flex flex-wrap gap-2 px-4 pb-3">
          {STATUSES.map((s) => (
            <StatusPill key={s} status={s} />
          ))}
        </div>
      </Section>
      <Section title="Truth states">
        <div className="flex flex-wrap items-center gap-3 px-4 pb-3">
          <Truth state="loading" staleSince={null} error={null} />
          <Truth state="offline" staleSince={Date.now() - 90_000} error="the engine stopped answering" />
          <span className="text-ui-caption text-ds-text-muted">live shows nothing</span>
        </div>
      </Section>
      <Section title="Notices">
        <div className="space-y-2 px-4 pb-3">
          <Notice title="Information">A neutral notice.</Notice>
          <Notice tone="warning" title="Waiting for code">No process runs code for checkout v3.</Notice>
          <Notice tone="warning" title="This run is paused: step charge ran out of retries" actions={<Button size="sm">Resume</Button>}>
            payments are down (5 attempts). Fix the cause, then resume: the step runs again with a fresh set of attempts.
          </Notice>
          <Notice tone="error" title="This run is stuck" actions={<Button size="sm" variant="outline">Reset to a step</Button>}>
            The journal recorded step <Mono>reserve</Mono> here; the code called <Mono>fraud-check</Mono>.
          </Notice>
        </div>
      </Section>
      <Section title="Metric tiles">
        <div className="grid grid-cols-2 divide-x divide-ds-border-subtle border-y border-ds-border-subtle sm:grid-cols-4">
          <MetricTile label="Runs" value="129" />
          <MetricTile label="Success rate" value="97%" />
          <MetricTile label="Median duration" value="38 µs" />
          <MetricTile label="Flush time" value="27 µs" hint="max 1.2 ms" />
        </div>
      </Section>
      <Section title="Loading">
        <div className="space-y-2 px-4 pb-3">
          <Skeleton className="h-6 w-64" />
          <Skeleton className="h-6 w-full" />
        </div>
      </Section>
      <Section title="JSON">
        <div className="px-4 pb-3">
          <JsonView value={{ id: "ch_1", amount: 4200, items: ["tea", "milk"], paid: true, note: null }} />
        </div>
      </Section>
      <Section title="Empty states">
        <FirstRun />
        <EmptyState title="No runs match." action={<Button size="sm" variant="outline">Clear filters</Button>}>
          Change the filters to see more.
        </EmptyState>
      </Section>
      <Section title="Rows">
        <ul className="divide-y divide-ds-border-subtle">
          {SAMPLE.slice(0, 5).map((r) => (
            <li key={r.id} className="grid h-[34px] grid-cols-[minmax(0,1fr)_8rem_9rem] items-center gap-3 px-4 hover:bg-ds-table-row-hover">
              <Mono>{r.id}</Mono>
              <span>{r.type}</span>
              <StatusPill status={r.status} />
            </li>
          ))}
        </ul>
      </Section>
      {React.createElement("div", { className: "h-8" })}
    </div>
  );
}
