// Payloads, inputs and errors in monospace, folded past a depth.
import { ChevronRight, Lock } from "lucide-react";
import * as React from "react";
import { isEncoded } from "../lib/api";
import { cn } from "../lib/cn";

/** A payload a codec encrypted that no codec endpoint decoded. */
function Encrypted({ b64 }: { b64: string }) {
  const bytes = Math.floor((b64.length * 3) / 4) - (b64.endsWith("==") ? 2 : b64.endsWith("=") ? 1 : 0);
  return (
    <span
      className="inline-flex items-center gap-1 text-ds-text-subtle"
      title="Encrypted by the application's codec. To read it here, run a codec endpoint and pass its URL to the server (--codec-url)."
    >
      <Lock className="size-3" aria-hidden="true" />
      encrypted · {bytes} bytes
    </span>
  );
}

function Leaf({ v }: { v: unknown }) {
  if (v === null) return <span className="text-ds-text-subtle">null</span>;
  if (typeof v === "string") return <span className="text-ds-mark-green">{JSON.stringify(v)}</span>;
  if (typeof v === "number") return <span className="text-ds-mark-blue">{String(v)}</span>;
  if (typeof v === "boolean") return <span className="text-ds-mark-purple">{String(v)}</span>;
  return <span>{String(v)}</span>;
}

function Node({ k, v, depth }: { k?: string; v: unknown; depth: number }) {
  const [open, setOpen] = React.useState(depth < 2);
  const label = k !== undefined ? <span className="text-ds-text-muted">{k}: </span> : null;
  if (isEncoded(v)) {
    return (
      <div style={{ paddingLeft: depth * 12 }}>
        {label}
        <Encrypted b64={v.$encoded} />
      </div>
    );
  }
  if (v === null || typeof v !== "object") {
    return (
      <div style={{ paddingLeft: depth * 12 }}>
        {label}
        <Leaf v={v} />
      </div>
    );
  }
  const entries = Array.isArray(v) ? v.map((x, i) => [String(i), x] as const) : Object.entries(v as object);
  const [o, c] = Array.isArray(v) ? ["[", "]"] : ["{", "}"];
  if (entries.length === 0) {
    return (
      <div style={{ paddingLeft: depth * 12 }}>
        {label}
        {o}
        {c}
      </div>
    );
  }
  return (
    <div>
      <button
        type="button"
        className="flex items-center gap-0.5 rounded-sm text-left hover:bg-ds-hover"
        style={{ paddingLeft: depth * 12 }}
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <ChevronRight className={cn("size-3 shrink-0 text-ds-text-subtle transition-transform", open && "rotate-90")} aria-hidden="true" />
        {label}
        {o}
        {!open ? <span className="text-ds-text-subtle"> {entries.length} … {c}</span> : null}
      </button>
      {open ? (
        <>
          {entries.map(([ek, ev]) => (
            <Node key={ek} k={ek} v={ev} depth={depth + 1} />
          ))}
          <div style={{ paddingLeft: depth * 12 + 14 }}>{c}</div>
        </>
      ) : null}
    </div>
  );
}

export function JsonView({ value, className }: { value: unknown; className?: string }) {
  if (value === undefined) return <p className="text-ui-body text-ds-text-subtle">Nothing recorded.</p>;
  return (
    <div className={cn("overflow-x-auto rounded-md border border-ds-border-subtle bg-ds-surface-sunken px-2 py-1.5 font-mono text-ui-code", className)}>
      <Node v={value} depth={0} />
    </div>
  );
}
