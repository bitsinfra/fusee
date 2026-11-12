// The command palette (mod+K): screens, runs by ID, and actions.
import * as Dialog from "@radix-ui/react-dialog";
import { Search } from "lucide-react";
import * as React from "react";
import { cn } from "../lib/cn";
import { Kbd } from "./primitives";

export interface Command {
  id: string;
  label: string;
  hint?: string;
  group: string;
  run: () => void;
}

export function Palette({ open, onOpenChange, commands }: { open: boolean; onOpenChange: (o: boolean) => void; commands: Command[] }) {
  const [q, setQ] = React.useState("");
  const [at, setAt] = React.useState(0);
  React.useEffect(() => {
    if (open) {
      setQ("");
      setAt(0);
    }
  }, [open]);
  const shown = React.useMemo(() => {
    const words = q.toLowerCase().split(/\s+/).filter(Boolean);
    return commands.filter((c) => words.every((w) => `${c.label} ${c.hint ?? ""} ${c.group}`.toLowerCase().includes(w))).slice(0, 50);
  }, [q, commands]);
  const choose = (c: Command | undefined) => {
    if (!c) return;
    onOpenChange(false);
    c.run();
  };
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-black/30 dark:bg-black/50" />
        <Dialog.Content
          aria-label="Command palette"
          className="fixed left-1/2 top-[14vh] z-50 w-[min(36rem,calc(100vw-2rem))] -translate-x-1/2 overflow-hidden rounded-xl border border-ds-border bg-ds-surface shadow-lg"
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") {
              e.preventDefault();
              setAt((a) => Math.min(a + 1, shown.length - 1));
            } else if (e.key === "ArrowUp") {
              e.preventDefault();
              setAt((a) => Math.max(a - 1, 0));
            } else if (e.key === "Enter") {
              e.preventDefault();
              choose(shown[at]);
            }
          }}
        >
          <Dialog.Title className="sr-only">Command palette</Dialog.Title>
          <div className="flex h-11 items-center gap-2 border-b border-ds-border-subtle px-3">
            <Search className="size-4 text-ds-text-subtle" aria-hidden="true" />
            <input
              autoFocus
              value={q}
              onChange={(e) => {
                setQ(e.target.value);
                setAt(0);
              }}
              placeholder="Go to a screen, open a run by ID, or run an action"
              className="h-full flex-1 bg-transparent text-ui-body text-ds-text-strong outline-none placeholder:text-ds-text-subtle"
            />
            <Kbd>Esc</Kbd>
          </div>
          <div role="listbox" className="max-h-80 overflow-y-auto p-1">
            {shown.length === 0 ? <p className="px-2 py-6 text-center text-ui-body text-ds-text-subtle">No match.</p> : null}
            {shown.map((c, i) => (
              <div
                key={c.id}
                role="option"
                aria-selected={i === at}
                onMouseEnter={() => setAt(i)}
                onClick={() => choose(c)}
                className={cn("flex h-8 cursor-pointer items-center gap-2 rounded-md px-2 text-ui-body", i === at && "bg-ds-hover")}
              >
                <span className="flex-1 truncate">{c.label}</span>
                {c.hint ? <span className="truncate font-mono text-ui-code text-ds-text-subtle">{c.hint}</span> : null}
                <span className="text-ui-caption text-ds-text-subtle">{c.group}</span>
              </div>
            ))}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
