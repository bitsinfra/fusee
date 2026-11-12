// `fusee/rich`: a serializer that keeps what JSON loses.
import type { Serializer } from "./index.js";

/**
 * Keeps `Date`, `Map`, `Set` and `bigint` values through steps, inputs,
 * results, signals and object state, as tagged JSON (`{"$date": "…"}`,
 * `{"$map": [[k, v], …]}`, `{"$set": […]}`, `{"$bigint": "…"}`):
 *
 *     import { rich } from "fusee/rich";
 *     const engine = await Engine.open({ dir, serializer: rich });
 *
 * Other languages read the tagged form as plain JSON objects.
 */
export const rich: Serializer = {
  stringify(value: unknown): string {
    return (
      JSON.stringify(value, function (this: Record<string, unknown>, key: string, v: unknown) {
        // `this[key]` is the value before toJSON (a Date's is a string by now).
        const raw = this[key];
        if (raw instanceof Date) return { $date: raw.toISOString() };
        if (raw instanceof Map) return { $map: [...raw.entries()] };
        if (raw instanceof Set) return { $set: [...raw] };
        if (typeof raw === "bigint") return { $bigint: raw.toString() };
        return v;
      }) ?? "null"
    );
  },
  parse(text: string): unknown {
    return JSON.parse(text, (_key, v: unknown) => {
      if (v === null || typeof v !== "object" || Array.isArray(v)) return v;
      const keys = Object.keys(v);
      if (keys.length !== 1) return v;
      const o = v as Record<string, unknown>;
      switch (keys[0]) {
        case "$date":
          return new Date(o.$date as string);
        case "$map":
          return new Map(o.$map as [unknown, unknown][]);
        case "$set":
          return new Set(o.$set as unknown[]);
        case "$bigint":
          return BigInt(o.$bigint as string);
        default:
          return v;
      }
    });
  },
};
