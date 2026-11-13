import { useEffect, useState } from "react";

/** The page's place: `#/runs?view=failed`, `#/run/<id>`. */
export interface Route {
  path: string[];
  query: URLSearchParams;
}

function parse(): Route {
  const h = location.hash.replace(/^#/, "") || "/runs";
  const [p = "", q = ""] = h.split("?");
  return { path: p.split("/").filter(Boolean).map(decodeURIComponent), query: new URLSearchParams(q) };
}

export function useRoute(): Route {
  const [r, set] = useState(parse);
  useEffect(() => {
    const on = () => set(parse());
    addEventListener("hashchange", on);
    return () => removeEventListener("hashchange", on);
  }, []);
  return r;
}

export function href(path: (string | number)[], query?: Record<string, string | undefined>): string {
  const p = path.map((s) => encodeURIComponent(String(s))).join("/");
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(query ?? {})) if (v) q.set(k, v);
  const qs = q.toString();
  return `#/${p}${qs ? `?${qs}` : ""}`;
}

export function go(path: (string | number)[], query?: Record<string, string | undefined>): void {
  location.hash = href(path, query);
}
