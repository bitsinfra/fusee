import type { Serializer } from "./index.js";
/**
 * Keeps `Date`, `Map`, `Set` and `bigint` values through steps, inputs,
 * results, signals and object state, as tagged JSON (`{"$date": "…"}`,
 * `{"$map": [[k, v], …]}`, `{"$set": […]}`, `{"$bigint": "…"}`):
 *
 *     import { rich } from "@bitsinfra/fusee/rich";
 *     const engine = await Engine.open({ dir, serializer: rich });
 *
 * Other languages read the tagged form as plain JSON objects.
 */
export declare const rich: Serializer;
