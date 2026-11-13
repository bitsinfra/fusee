// Payloads: JSON (or a serializer) and raw bytes, durations, payload encryption.
import { FuseeError, RejectedError, codecError, failureOf, failureText } from "./errors.js";
import { Wire } from "./native.js";

// ---------------------------------------------------------------- codec

const enc = new TextEncoder();
export const dec = new TextDecoder();

/**
 * How payloads become JSON text and back: `Engine.open({ serializer })`.
 * The default is JSON as it is; `@bitsinfra/fusee/rich` keeps dates, maps, sets and
 * bigints. One serializer per process: every engine in it reads the
 * others' payloads.
 */
export interface Serializer {
  stringify(value: unknown): string;
  parse(text: string): unknown;
}
let serializer: Serializer | undefined;
/** Open engines that use `serializer`: it ends with the last of them. */
let serializerUsers = 0;

/**
 * @internal Sets the process's serializer for an engine that opens with
 * one (`Engine.open({ serializer })`); the returned function gives it back
 * when that engine closes.
 */
export function useSerializer(s: Serializer | undefined): (() => void) | undefined {
  if (s === undefined) return undefined;
  if (serializer !== undefined && s !== serializer)
    throw new RejectedError("this process already uses another serializer: open every engine with the same one");
  serializer = s;
  serializerUsers++;
  let given = false;
  return () => {
    if (given) return;
    given = true;
    if (--serializerUsers === 0) serializer = undefined;
  };
}

/** JSON text for a value; a value JSON leaves out (a function) is empty. */
export const json = (v: unknown): string =>
  serializer === undefined ? (JSON.stringify(v === undefined ? null : v) ?? "") : serializer.stringify(v === undefined ? null : v);
/** A value from JSON text, through the serializer when there is one. */
export const parseJson = (t: string): unknown => (serializer === undefined ? JSON.parse(t) : serializer.parse(t));
export const encode = (v: unknown): Buffer => Buffer.from(json(v));
/** A payload for the engine: a `Uint8Array` is recorded as bytes, anything else as JSON. */
export const wire = (v: unknown): string | Uint8Array => (v instanceof Uint8Array ? v : json(v));
/** A recorded payload in its tagged form (raw bytes follow a zero byte; anything else is JSON). */
export const decode = <T>(b: Buffer | undefined | null): T =>
  (b && b.length ? (b[0] === 0 ? b.subarray(1) : parseJson(dec.decode(b))) : null) as T;
/** A payload from the typed boundary: a string is JSON, a Buffer raw bytes. */
export const fromWire = <T>(p: Wire): T =>
  (p == null ? null : typeof p === "string" ? (p.length ? parseJson(p) : null) : p) as T;
export const wireText = (p: Wire): string => (p == null ? "" : typeof p === "string" ? p : p.toString("utf8"));
/** A tagged payload as it would cross the typed boundary. */
export const toWire = (b: Buffer | undefined | null): Wire =>
  !b || !b.length ? null : b[0] === 0 ? b.subarray(1) : dec.decode(b);
export const text = (b: Buffer | undefined): string => (b ? dec.decode(b) : "");
export const utf8 = (s: string): Buffer => Buffer.from(enc.encode(s));
/** A payload in its recorded, tagged form. */
export const tagged = (v: string | Uint8Array): Buffer => {
  if (typeof v === "string") return Buffer.from(v);
  const b = Buffer.allocUnsafe(v.length + 1);
  b[0] = 0;
  b.set(v, 1);
  return b;
};
/** A workflow or update result as [status, payload]: one JSON cannot encode is a failure. */
export const encoded = (v: unknown, what: string): [number, string | Uint8Array] => {
  try {
    return [St.Ok, wire(v)];
  } catch (e) {
    return [St.Failed, failureText(failureOf(codecError(what, v, e), false))];
  }
};

export const enum St {
  Ok = 0,
  Failed = 1,
  Cancelled = 2,
  TimedOut = 3,
  Terminated = 4,
  ContinuedAsNew = 5,
  Reset = 6,
}

export function ms(d: number | string | null | undefined): number | undefined {
  if (d == null) return undefined;
  if (typeof d === "number") return d;
  const m = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/.exec(d.trim());
  if (!m) throw new RejectedError(`bad duration '${d}' (use 500ms, 10s, 5m, 2h, 1d)`);
  const n = Number(m[1]);
  return n * { ms: 1, s: 1e3, m: 6e4, h: 36e5, d: 864e5 }[m[2] as "ms" | "s" | "m" | "h" | "d"];
}

/** Payload encryption: AES-256-GCM. The first key encrypts; every key decrypts. */
export interface Codec {
  kind: "aes-gcm";
  keys: { id: string; key: Buffer }[];
}

/** `aesGcm({ k2: newKey, k1: oldKey })`: 32-byte keys (Buffer, or hex / base64 text). */
export function aesGcm(keys: Record<string, Buffer | Uint8Array | string>): Codec {
  const list = Object.entries(keys).map(([id, k]) => {
    const key =
      typeof k === "string" ? Buffer.from(k, /^[0-9a-f]{64}$/i.test(k) ? "hex" : "base64") : Buffer.from(k);
    if (key.length !== 32) throw new RejectedError(`key '${id}' is not 32 bytes`);
    return { id, key };
  });
  if (!list.length) throw new FuseeError("a codec needs at least one key");
  return { kind: "aes-gcm", keys: list };
}
