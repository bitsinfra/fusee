// Dialogs, confirmations, menus, tooltips and tabs on Radix: 8px radius for
// menus and popovers, 12px for dialogs; only floating layers have depth;
// menus open without delay.
import * as AlertDialog from "@radix-ui/react-alert-dialog";
import * as Dialog from "@radix-ui/react-dialog";
import * as Menu from "@radix-ui/react-dropdown-menu";
import * as Tabs from "@radix-ui/react-tabs";
import * as Tip from "@radix-ui/react-tooltip";
import { X } from "lucide-react";
import * as React from "react";
import { cn } from "../lib/cn";
import { Button, IconButton } from "./button";
import { Kbd } from "./primitives";

const overlay = "fixed inset-0 z-40 bg-black/30 dark:bg-black/50";
const panel = "fixed left-1/2 top-[12vh] z-50 w-[min(32rem,calc(100vw-2rem))] -translate-x-1/2 rounded-xl border border-ds-border bg-ds-surface p-4 text-ds-text-strong shadow-lg focus:outline-none";

export function Modal({
  open,
  onOpenChange,
  title,
  description,
  children,
  footer,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  title: string;
  description?: React.ReactNode;
  children?: React.ReactNode;
  footer?: React.ReactNode;
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className={overlay} />
        <Dialog.Content className={panel}>
          <div className="flex items-start justify-between gap-3">
            <Dialog.Title className="text-ui-heading">{title}</Dialog.Title>
            <Dialog.Close asChild>
              <IconButton label="Close" size="icon-sm">
                <X />
              </IconButton>
            </Dialog.Close>
          </div>
          {description ? <Dialog.Description className="mt-1 text-ui-body text-ds-text-muted">{description}</Dialog.Description> : null}
          {children ? <div className="mt-3">{children}</div> : null}
          {footer ? <div className="mt-4 flex justify-end gap-2">{footer}</div> : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/**
 * A confirmation for what cannot be undone: it names the object and what
 * follows, focus starts on Cancel, and with `typeToConfirm` the action waits
 * for that text (a bulk terminate asks for the count).
 */
export function Confirm({
  open,
  onOpenChange,
  title,
  children,
  action,
  onConfirm,
  typeToConfirm,
  destructive = true,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  title: string;
  children: React.ReactNode;
  action: string;
  onConfirm: () => Promise<unknown> | void;
  typeToConfirm?: string;
  destructive?: boolean;
}) {
  const [typed, setTyped] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  React.useEffect(() => {
    if (!open) {
      setTyped("");
      setError(null);
    }
  }, [open]);
  const ok = !typeToConfirm || typed.trim() === typeToConfirm;
  return (
    <AlertDialog.Root open={open} onOpenChange={onOpenChange}>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className={overlay} />
        <AlertDialog.Content className={panel}>
          <AlertDialog.Title className="text-ui-heading">{title}</AlertDialog.Title>
          <AlertDialog.Description asChild>
            <div className="mt-1 text-ui-body text-ds-text-muted">{children}</div>
          </AlertDialog.Description>
          {typeToConfirm ? (
            <label className="mt-3 block text-ui-label text-ds-text-muted">
              Type <span className="font-mono text-ds-text-strong">{typeToConfirm}</span> to confirm
              <input
                className="mt-1 block h-7 w-full rounded-md border border-ds-border bg-ds-surface px-2 text-ui-body text-ds-text-strong outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-ds-focus"
                value={typed}
                onChange={(e) => setTyped(e.target.value)}
              />
            </label>
          ) : null}
          {error ? <p className="mt-2 text-ui-body text-ds-error">{error}</p> : null}
          <div className="mt-4 flex justify-end gap-2">
            <AlertDialog.Cancel asChild>
              <Button variant="outline" autoFocus>
                Cancel
              </Button>
            </AlertDialog.Cancel>
            <Button
              variant={destructive ? "destructive" : "default"}
              disabled={!ok}
              loading={busy}
              onClick={async () => {
                setBusy(true);
                setError(null);
                try {
                  await onConfirm();
                  onOpenChange(false);
                } catch (e) {
                  setError((e as Error).message);
                } finally {
                  setBusy(false);
                }
              }}
            >
              {action}
            </Button>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

export const MenuRoot = Menu.Root;
export const MenuTrigger = Menu.Trigger;

export function MenuContent({ children, align = "end" }: { children: React.ReactNode; align?: "start" | "end" }) {
  return (
    <Menu.Portal>
      <Menu.Content
        align={align}
        sideOffset={4}
        className="z-50 min-w-44 rounded-lg border border-ds-border bg-ds-surface p-1 text-ds-text-strong shadow-md"
      >
        {children}
      </Menu.Content>
    </Menu.Portal>
  );
}

export function MenuItem({
  children,
  onSelect,
  shortcut,
  destructive,
  disabled,
}: {
  children: React.ReactNode;
  onSelect: () => void;
  shortcut?: string;
  destructive?: boolean;
  disabled?: boolean;
}) {
  return (
    <Menu.Item
      disabled={disabled}
      onSelect={onSelect}
      className={cn(
        "flex h-8 cursor-pointer select-none items-center gap-2 rounded-md px-2 text-ui-body outline-none data-[disabled]:pointer-events-none data-[highlighted]:bg-ds-hover data-[disabled]:opacity-50 [&_svg]:size-3.5 [&_svg]:text-ds-text-muted",
        destructive && "text-ds-error [&_svg]:text-ds-error",
      )}
    >
      <span className="flex flex-1 items-center gap-2">{children}</span>
      {shortcut ? <Kbd>{shortcut}</Kbd> : null}
    </Menu.Item>
  );
}

export function MenuSeparator() {
  return <Menu.Separator className="my-1 h-px bg-ds-border-subtle" />;
}

export function Tooltip({ content, children, keys }: { content: React.ReactNode; children: React.ReactElement; keys?: string }) {
  return (
    <Tip.Root delayDuration={300}>
      <Tip.Trigger asChild>{children}</Tip.Trigger>
      <Tip.Portal>
        <Tip.Content
          sideOffset={6}
          className="z-50 flex items-center gap-1.5 rounded-lg border border-ds-border bg-ds-surface px-2 py-1 text-ui-caption text-ds-text-strong shadow-md"
        >
          {content}
          {keys ? <Kbd>{keys}</Kbd> : null}
        </Tip.Content>
      </Tip.Portal>
    </Tip.Root>
  );
}

export const TooltipProvider = Tip.Provider;

/** View tabs: 6px radius, the active one in strong text over a line. */
export function TabsList({ tabs, className }: { tabs: { value: string; label: React.ReactNode }[]; className?: string }) {
  return (
    <Tabs.List className={cn("flex items-center gap-1 overflow-x-auto", className)}>
      {tabs.map((t) => (
        <Tabs.Trigger
          key={t.value}
          value={t.value}
          className="relative h-7 shrink-0 whitespace-nowrap rounded-md px-2 text-ui-body text-ds-text-muted transition-interactive hover:bg-ds-hover hover:text-ds-text-strong data-[state=active]:font-semibold data-[state=active]:text-ds-text-strong data-[state=active]:after:absolute data-[state=active]:after:inset-x-2 data-[state=active]:after:-bottom-[7px] data-[state=active]:after:h-px data-[state=active]:after:bg-ds-text-strong"
        >
          {t.label}
        </Tabs.Trigger>
      ))}
    </Tabs.List>
  );
}

export const TabsRoot = Tabs.Root;
export const TabsContent = Tabs.Content;
