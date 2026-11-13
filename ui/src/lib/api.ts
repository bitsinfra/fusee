// The HTTP API (fusee::http), from the page it serves.

export interface RunSummary {
  id: string;
  type: string;
  version: number;
  status: string;
  shard: number;
  run: number;
  startedMs: number;
  finishedMs: number | null;
  error: string | null;
  attributes: Record<string, unknown> | null;
  object?: boolean;
}

export interface Failure {
  message: string;
  type?: string;
  code?: string;
  details?: unknown;
  retryable?: boolean;
  stack?: string;
  cause?: Failure;
}

/** One journal record, normalized (times and run IDs left out). */
export type JournalRecord = { kind: string; seq?: number; name?: string; [k: string]: unknown };

export interface RunDetail extends RunSummary {
  journal: JournalRecord[];
  /**
   * When each step began (Unix ms) and how long it took (ms), by sequence
   * number: recorded under the dev profile only, absent elsewhere.
   */
  stepTimes?: Record<string, [number, number]>;
  objectDetail?: { state: Record<string, unknown>; version?: string; running: unknown[]; waiting: unknown[] } | null;
}

export interface TypeInfo {
  name: string;
  versions: number[];
  object: boolean;
}

export interface LimitInfo {
  name: string;
  /** Permits held at once; 0 means no concurrency limit. */
  concurrent: number;
  /** Permits started per second; 0 means no rate limit. */
  perSecond: number;
  inUse: number;
  waiting: number;
}

export interface Info {
  engine: string;
  version: string;
  sdk: string;
  durability: string;
  dir: string | null;
  shards: number;
  profile?: string | null;
  http?: { url: string; token: boolean };
  /** The operator's codec endpoint, for payloads a codec encrypted. */
  codecUrl?: string | null;
  /** The namespace this page's token reaches, when it is a namespace's. */
  namespace?: string | null;
  /** Who this page acts as: the proxy's signed-in user, or the kind of token (`token`, `http`, ...). */
  viewer?: string;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly docs?: string,
    readonly failure?: Failure,
  ) {
    super(message);
  }
}

const TOKEN_KEY = "fusee.token";

/** A token from the page's address (`?token=`) is kept for the session. */
export function token(): string | null {
  const fromUrl = new URLSearchParams(location.search).get("token");
  if (fromUrl) {
    try {
      sessionStorage.setItem(TOKEN_KEY, fromUrl);
    } catch {
      /* private mode */
    }
    history.replaceState(null, "", location.pathname + location.hash);
    return fromUrl;
  }
  try {
    return sessionStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

/** Dispatched on `window` when the API refuses the page's token. */
export const UNAUTHORIZED = "fusee:unauthorized";

export function setToken(t: string): void {
  try {
    sessionStorage.setItem(TOKEN_KEY, t);
  } catch {
    /* private mode */
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  const t = token();
  if (t) headers.Authorization = `Bearer ${t}`;
  if (method !== "GET") headers["Content-Type"] = "application/json";
  const r = await fetch(path, { method, headers, body: method === "GET" ? undefined : JSON.stringify(body ?? null) });
  const text = await r.text();
  const v = text ? JSON.parse(text) : null;
  if (!r.ok) {
    if (r.status === 401) window.dispatchEvent(new Event(UNAUTHORIZED));
    const e = v?.error ?? {};
    throw new ApiError(r.status, e.code ?? "Io", e.message ?? `${r.status} ${r.statusText}`, e.docs, e.failure);
  }
  return v as T;
}

const enc = encodeURIComponent;

export const api = {
  info: () => request<Info>("GET", "/v1/info"),
  types: () => request<TypeInfo[]>("GET", "/v1/types"),
  limits: () => request<LimitInfo[]>("GET", "/v1/limits"),
  metrics: () => request<{ total: Record<string, number>; shards: Record<string, number>[] }>("GET", "/v1/metrics"),
  runs: (q: { type?: string; status?: string; limit?: number; where?: Record<string, unknown> } = {}) => {
    const p = new URLSearchParams();
    if (q.type) p.set("type", q.type);
    if (q.status) p.set("status", q.status);
    if (q.where) p.set("where", JSON.stringify(q.where));
    p.set("limit", String(q.limit ?? 1000));
    return request<RunSummary[]>("GET", `/v1/runs?${p}`).then(queuedRuns);
  },
  run: (id: string) => request<RunDetail>("GET", `/v1/runs/${enc(id)}`).then(decodePayloads),
  result: (id: string) =>
    request<{ status: string; value?: unknown; error?: Failure }>("GET", `/v1/runs/${enc(id)}/result`).then(decodePayloads),
  signal: (id: string, name: string, value: unknown) => request<null>("POST", `/v1/runs/${enc(id)}/signals/${enc(name)}`, value),
  query: (id: string, name: string, arg: unknown) => request<unknown>("POST", `/v1/runs/${enc(id)}/queries/${enc(name)}`, arg),
  cancel: (id: string, reason: string) => request<null>("POST", `/v1/runs/${enc(id)}/cancel`, { reason }),
  terminate: (id: string, reason: string) => request<null>("POST", `/v1/runs/${enc(id)}/terminate`, { reason }),
  reset: (id: string, beforeStep: string) => request<null>("POST", `/v1/runs/${enc(id)}/reset`, { beforeStep, reapplySignals: true }),
  retry: (id: string) => request<{ resetBefore: string }>("POST", `/v1/runs/${enc(id)}/retry`, null),
  resume: (id: string) => request<null>("POST", `/v1/runs/${enc(id)}/resume`, null),
  schedules: () => request<{ name: string; summary: RunSummary }[]>("GET", "/v1/schedules"),
  schedule: (name: string) => request<Record<string, unknown>>("GET", `/v1/schedules/${enc(name)}`),
  scheduleOp: (name: string, op: "pause" | "resume" | "trigger") => request<null>("POST", `/v1/schedules/${enc(name)}/${op}`, null),
  object: (type: string, key: string) =>
    request<RunDetail & { state?: Record<string, unknown> }>("GET", `/v1/objects/${enc(type)}/${key.split("/").map(enc).join("/")}`).then(
      decodePayloads,
    ),
  /** Edits a key's state: `ifVersion` refuses it (412) if the state changed meanwhile. */
  editState: (type: string, key: string, edit: Record<string, unknown>, ifVersion?: string) =>
    request<{ version: string; changed: string[] }>("POST", `/v1/objects/${enc(type)}/${key.split("/").map(enc).join("/")}/state`,
      ifVersion ? { ...edit, ifVersion } : edit),
  exportUrl: (id: string) => `/v1/runs/${enc(id)}/export`,
};

/**
 * Runs waiting for their turn (a type with runs at once) are slot runs,
 * `fusee.queued/<id>`, until their run starts: show each as its run,
 * queued, and hide the slots whose run exists.
 */
function queuedRuns(rows: RunSummary[]): RunSummary[] {
  const SLOT = "fusee.queued/";
  const ids = new Set(rows.map((r) => r.id));
  const out: RunSummary[] = [];
  for (const r of rows) {
    if (r.type !== "fusee.queued") {
      out.push(r);
      continue;
    }
    const id = r.id.slice(SLOT.length);
    if (r.finishedMs == null && !ids.has(id)) out.push({ ...r, id: r.id, type: "queued", status: "queued" });
  }
  return out;
}

// --------------------------------------------------------------- payloads

/** How the API shows a payload a codec encrypted: `{"$encoded": base64}`. */
export function isEncoded(v: unknown): v is { $encoded: string } {
  if (v === null || typeof v !== "object" || Array.isArray(v)) return false;
  const keys = Object.keys(v);
  return keys.length === 1 && keys[0] === "$encoded" && typeof (v as { $encoded: unknown }).$encoded === "string";
}

let codecUrl: Promise<string | null> | null = null;
// Decoded payloads by their encoded form, and those the codec could not
// decode (asked once): a polled run does not ask again.
const decoded = new Map<string, unknown>();
const undecodable = new Set<string>();

function plain(b64: string): unknown {
  const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
  const text = new TextDecoder().decode(bytes);
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function collect(v: unknown, out: Set<string>): void {
  if (isEncoded(v)) {
    if (!decoded.has(v.$encoded) && !undecodable.has(v.$encoded)) out.add(v.$encoded);
  } else if (Array.isArray(v)) {
    for (const x of v) collect(x, out);
  } else if (v !== null && typeof v === "object") {
    for (const x of Object.values(v)) collect(x, out);
  }
}

function swap(v: unknown): unknown {
  if (isEncoded(v)) return decoded.has(v.$encoded) ? decoded.get(v.$encoded) : v;
  if (Array.isArray(v)) return v.map(swap);
  if (v !== null && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, swap(x)]));
  return v;
}

/**
 * Decodes the encrypted payloads in `v` through the operator's codec
 * endpoint (`codecUrl` in `/v1/info`): the browser calls it directly, with
 * its own sign-in, so the keys never reach this API. Without one, or when
 * it refuses, the payloads stay encoded and show as encrypted.
 */
export async function decodePayloads<T>(v: T): Promise<T> {
  const wanted = new Set<string>();
  collect(v, wanted);
  if (wanted.size > 0) {
    codecUrl ??= api.info().then(
      (i) => i.codecUrl ?? null,
      () => null,
    );
    const url = await codecUrl;
    const payloads = [...wanted];
    let answer: unknown[] | null = null;
    if (url) {
      try {
        const r = await fetch(`${url}/decode`, {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ payloads }),
        });
        if (r.ok) answer = ((await r.json()) as { payloads?: unknown[] }).payloads ?? null;
      } catch {
        answer = null;
      }
    }
    payloads.forEach((p, i) => {
      const out = answer?.[i];
      if (typeof out === "string") decoded.set(p, plain(out));
      else undecodable.add(p);
    });
  }
  return (wanted.size > 0 || decoded.size > 0 ? swap(v) : v) as T;
}

/** Server-sent events, with the token in the address (EventSource sends no headers). */
export function events(path: string): EventSource {
  const t = token();
  const sep = path.includes("?") ? "&" : "?";
  return new EventSource(t ? `${path}${sep}token=${enc(t)}` : path);
}

/** The message of a run summary's error, which holds the failure envelope as JSON. */
export function errorMessage(e: string | null | undefined): string | null {
  if (!e) return null;
  if (e.startsWith("{")) {
    try {
      const f = JSON.parse(e) as Failure;
      if (f && typeof f.message === "string") return f.message;
    } catch {
      /* plain text */
    }
  }
  return e;
}

/** The failure of a recorded step or run, as the journal holds it. */
export function failureOf(v: unknown): Failure | null {
  if (v && typeof v === "object" && "message" in (v as object)) return v as Failure;
  if (typeof v === "string") return { message: v };
  return null;
}
