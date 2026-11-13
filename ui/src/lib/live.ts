import { useEffect, useRef, useState } from "react";
import { api, events, type RunSummary } from "./api";

/** Truth: loading, live, offline, and how stale what is on screen is. */
export interface Liveness {
  state: "loading" | "live" | "offline";
  /** When the data on screen was last confirmed, if it is stale now. */
  staleSince: number | null;
  error: string | null;
}

/** Every run, kept current from the event stream (rows update in place). */
export function useRuns(): { runs: RunSummary[]; live: Liveness; reload: () => void } {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [live, setLive] = useState<Liveness>({ state: "loading", staleSince: null, error: null });
  const byId = useRef(new Map<string, RunSummary>());
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let closed = false;
    let es: EventSource | null = null;
    let lastOk = Date.now();
    const publish = () => setRuns([...byId.current.values()].sort((a, b) => b.startedMs - a.startedMs));
    api
      .runs({ limit: 2000 })
      .then((rows) => {
        if (closed) return;
        byId.current = new Map(rows.map((r) => [r.id, r]));
        publish();
        setLive({ state: "live", staleSince: null, error: null });
      })
      .catch((e: Error) => !closed && setLive({ state: "offline", staleSince: null, error: e.message }));
    const connect = () => {
      es = events("/v1/events");
      es.addEventListener("run", (ev) => {
        const r = JSON.parse((ev as MessageEvent).data) as RunSummary;
        byId.current.set(r.id, r);
        lastOk = Date.now();
        publish();
      });
      es.addEventListener("ready", () => {
        lastOk = Date.now();
        setLive({ state: "live", staleSince: null, error: null });
      });
      es.onmessage = () => (lastOk = Date.now());
      es.onerror = () => {
        if (closed) return;
        setLive((l) => ({ state: "offline", staleSince: l.staleSince ?? lastOk, error: "the engine stopped answering" }));
      };
      es.onopen = () => setLive({ state: "live", staleSince: null, error: null });
    };
    connect();
    return () => {
      closed = true;
      es?.close();
    };
  }, [tick]);
  return { runs, live, reload: () => setTick((t) => t + 1) };
}

/** A value polled every `ms` while `active`. */
export function usePoll<T>(load: () => Promise<T>, ms: number, deps: unknown[], active = true): { data: T | null; error: string | null; loading: boolean; reload: () => void } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let closed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const run = () =>
      load()
        .then((v) => {
          if (closed) return;
          setData(v);
          setError(null);
        })
        .catch((e: Error) => !closed && setError(e.message))
        .finally(() => {
          if (closed) return;
          setLoading(false);
          if (active) timer = setTimeout(run, ms);
        });
    run();
    return () => {
      closed = true;
      clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick, active]);
  return { data, error, loading, reload: () => setTick((t) => t + 1) };
}

export const FINISHED = new Set(["ok", "failed", "cancelled", "timed_out", "terminated"]);
export const ATTENTION = new Set(["stuck", "paused", "waiting_for_code", "failed"]);
