// The API's token, asked for when a call is refused (a server started with
// --token-file, or an engine served off loopback). It is kept for the
// browser session only; `?token=` in the address does the same.
import * as React from "react";
import { api, ApiError, setToken, UNAUTHORIZED } from "../lib/api";
import { Button } from "./button";
import { Modal } from "./overlay";
import { Mono } from "./primitives";

export function TokenPrompt() {
  const [open, setOpen] = React.useState(false);
  const [value, setValue] = React.useState("");
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  React.useEffect(() => {
    const on = () => setOpen(true);
    window.addEventListener(UNAUTHORIZED, on);
    return () => window.removeEventListener(UNAUTHORIZED, on);
  }, []);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const t = value.trim();
    if (!t) return;
    setBusy(true);
    setError(null);
    setToken(t);
    try {
      await api.info();
      location.reload();
    } catch (err) {
      setBusy(false);
      setError(err instanceof ApiError && err.status === 401 ? "The API did not accept that token." : String((err as Error).message));
    }
  };

  return (
    <Modal
      open={open}
      onOpenChange={setOpen}
      title="This API needs its token"
      description={
        <>
          It was started with one: the file given to <Mono>--token-file</Mono>, or the <Mono>token</Mono> passed to{" "}
          <Mono>engine.serve()</Mono>. This browser keeps it until the tab closes.
        </>
      }
    >
      <form onSubmit={submit} className="flex flex-col gap-3">
        <label className="flex flex-col gap-1 text-ui-caption text-ds-text-muted">
          Token
          <input
            type="password"
            autoComplete="off"
            autoFocus
            value={value}
            onChange={(e) => setValue(e.target.value)}
            className="h-8 rounded-md border border-ds-border bg-ds-surface px-2 font-mono text-ui-code text-ds-text-strong outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-ds-focus"
          />
        </label>
        {error ? <p className="text-ui-caption text-ds-error">{error}</p> : null}
        <div className="flex justify-end gap-2">
          <Button type="button" variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button type="submit" disabled={busy || !value.trim()}>
            {busy ? "Checking…" : "Use token"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
