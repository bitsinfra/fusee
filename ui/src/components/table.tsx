// A list on the 32–36px row grid: no stripes, no gridlines; hover and
// selection carry the structure. Virtualized, with the keyboard contract:
// j/k and arrows move, Enter opens, x selects, Shift extends, Escape clears
// the selection (and then the cursor).
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import * as React from "react";
import { cn } from "../lib/cn";

export interface TableProps<T> {
  rows: T[];
  columns: ColumnDef<T, unknown>[];
  rowId: (r: T) => string;
  onOpen?: (r: T) => void;
  selectable?: boolean;
  selected?: Set<string>;
  onSelectedChange?: (s: Set<string>) => void;
  empty?: React.ReactNode;
  className?: string;
}

const ROW = 34;

/** Column options: `right` aligns the cell; `minTable` hides the column when
 * the table is narrower than that many pixels. A column with a `size` has
 * that width, except the first one on a narrow table, which takes the rest. */
export interface ColumnMeta {
  right?: boolean;
  minTable?: number;
}

const metaOf = (c: { meta?: unknown }) => c.meta as ColumnMeta | undefined;
const NARROW = 560;

function useWidth(ref: React.RefObject<HTMLElement | null>): number {
  const [w, setW] = React.useState(() => (typeof window === "undefined" ? 1280 : window.innerWidth));
  React.useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    setW(el.clientWidth);
    const ro = new ResizeObserver(([e]) => e && setW(e.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, [ref]);
  return w;
}

export function DataTable<T>({ rows, columns: all, rowId, onOpen, selectable, selected, onSelectedChange, empty, className }: TableProps<T>) {
  const root = React.useRef<HTMLDivElement>(null);
  const width = useWidth(root);
  const columns = React.useMemo(() => all.filter((c) => !(width < (metaOf(c)?.minTable ?? 0))), [all, width]);
  const table = useReactTable({ data: rows, columns, getCoreRowModel: getCoreRowModel(), getRowId: rowId });
  const scroller = React.useRef<HTMLDivElement>(null);
  const virt = useVirtualizer({ count: rows.length, getScrollElement: () => scroller.current, estimateSize: () => ROW, overscan: 12 });
  const [cursor, setCursor] = React.useState(-1);
  const anchor = React.useRef(-1);
  const sel = selected ?? new Set<string>();

  const toggle = (i: number, range: boolean) => {
    if (!selectable || !onSelectedChange) return;
    const next = new Set(sel);
    if (range && anchor.current >= 0) {
      const [a, b] = [Math.min(anchor.current, i), Math.max(anchor.current, i)];
      for (let j = a; j <= b; j++) {
        const r = rows[j];
        if (r) next.add(rowId(r));
      }
    } else {
      const r = rows[i];
      if (!r) return;
      const id = rowId(r);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      anchor.current = i;
    }
    onSelectedChange(next);
  };

  const onKey = (e: React.KeyboardEvent) => {
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const move = (d: number) => {
      e.preventDefault();
      const n = Math.max(0, Math.min(rows.length - 1, (cursor < 0 ? -1 : cursor) + d));
      setCursor(n);
      virt.scrollToIndex(n);
      if (e.shiftKey && selectable) toggle(n, true);
    };
    if (e.key === "j" || e.key === "ArrowDown") move(1);
    else if (e.key === "k" || e.key === "ArrowUp") move(-1);
    else if (e.key === "Enter" && cursor >= 0) {
      const r = rows[cursor];
      if (r && onOpen) onOpen(r);
    } else if (e.key === "x" && cursor >= 0) {
      e.preventDefault();
      toggle(cursor, e.shiftKey);
    } else if (e.key === "Escape") {
      // The selection first; a second Escape leaves the row.
      if (sel.size > 0) onSelectedChange?.(new Set());
      else setCursor(-1);
    }
  };

  const narrow = width < NARROW;
  const grid = columns.map((c, i) => (c.size && !(narrow && i === 0) ? `${c.size}px` : "minmax(0,1fr)")).join(" ");
  const template = selectable ? `32px ${grid}` : grid;

  return (
    <div ref={root} className={cn("flex min-h-0 flex-1 flex-col", className)}>
      <div
        role="row"
        className="grid h-8 shrink-0 items-center gap-x-3 border-b border-ds-border-subtle bg-ds-surface px-4 text-ui-caption text-ds-text-muted"
        style={{ gridTemplateColumns: template }}
      >
        {selectable ? (
          <input
            type="checkbox"
            aria-label="Select every row"
            className="size-3.5 accent-[hsl(var(--ds-accent))]"
            checked={rows.length > 0 && sel.size === rows.length}
            onChange={(e) => onSelectedChange?.(e.target.checked ? new Set(rows.map(rowId)) : new Set())}
          />
        ) : null}
        {table.getHeaderGroups()[0]?.headers.map((h) => (
          <div key={h.id} role="columnheader" className={cn("truncate", metaOf(h.column.columnDef)?.right && "text-right")}>
            {flexRender(h.column.columnDef.header, h.getContext())}
          </div>
        ))}
      </div>
      {rows.length === 0 ? (
        empty
      ) : (
        <div ref={scroller} tabIndex={0} role="grid" aria-rowcount={rows.length} onKeyDown={onKey} className="min-h-0 flex-1 overflow-y-auto outline-none">
          <div style={{ height: virt.getTotalSize(), position: "relative" }}>
            {virt.getVirtualItems().map((v) => {
              const row = table.getRowModel().rows[v.index];
              if (!row) return null;
              const id = row.id;
              const isSel = sel.has(id);
              return (
                <div
                  key={id}
                  role="row"
                  aria-selected={isSel}
                  data-cursor={v.index === cursor || undefined}
                  onClick={(e) => {
                    setCursor(v.index);
                    if ((e.target as HTMLElement).closest("input,button,a")) return;
                    onOpen?.(row.original);
                  }}
                  className={cn(
                    "absolute inset-x-0 grid cursor-pointer items-center gap-x-3 border-b border-ds-border-subtle px-4 transition-interactive hover:bg-ds-table-row-hover",
                    isSel && "bg-ds-table-row-selected",
                    v.index === cursor && "outline outline-1 -outline-offset-1 outline-ds-focus",
                  )}
                  style={{ top: v.start, height: ROW, gridTemplateColumns: template }}
                >
                  {selectable ? (
                    <input
                      type="checkbox"
                      aria-label={`Select ${id}`}
                      className="size-3.5 accent-[hsl(var(--ds-accent))]"
                      checked={isSel}
                      onChange={(e) => toggle(v.index, (e.nativeEvent as MouseEvent).shiftKey)}
                    />
                  ) : null}
                  {row.getVisibleCells().map((c) => (
                    <div key={c.id} role="gridcell" className={cn("min-w-0 truncate", metaOf(c.column.columnDef)?.right && "text-right")}>
                      {flexRender(c.column.columnDef.cell, c.getContext())}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
