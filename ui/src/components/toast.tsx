import * as React from "react";
import { cn } from "../lib/cn";

interface Toast {
  id: number;
  text: string;
  tone: "info" | "error";
}

const Ctx = React.createContext<(text: string, tone?: "info" | "error") => void>(() => {});

export function useToast() {
  return React.useContext(Ctx);
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);
  const push = React.useCallback((text: string, tone: "info" | "error" = "info") => {
    const id = Date.now() + Math.random();
    setToasts((t) => [...t, { id, text, tone }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), tone === "error" ? 8000 : 3500);
  }, []);
  return (
    <Ctx.Provider value={push}>
      {children}
      <div aria-live="polite" className="pointer-events-none fixed bottom-4 right-4 z-50 flex flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            className={cn(
              "pointer-events-auto max-w-sm rounded-lg border bg-ds-surface px-3 py-2 text-ui-body shadow-md",
              t.tone === "error" ? "border-ds-error/40 text-ds-error" : "border-ds-border text-ds-text-strong",
            )}
          >
            {t.text}
          </div>
        ))}
      </div>
    </Ctx.Provider>
  );
}
