import * as React from "react";
import { api, type Info } from "./lib/api";
import { ATTENTION, useRuns } from "./lib/live";
import { go, useRoute } from "./lib/route";
import { setAppearance } from "./lib/theme";
import { Palette, type Command } from "./components/palette";
import { TokenPrompt } from "./components/token";
import { TooltipProvider } from "./components/overlay";
import { ToastProvider } from "./components/toast";
import { FixturesScreen } from "./screens/fixtures";
import { AttentionScreen, HealthScreen, LimitsScreen, ObjectsScreen, SchedulesScreen, TypesScreen } from "./screens/others";
import { RunScreen } from "./screens/run";
import { RunsScreen } from "./screens/runs";
import { NAV, Shell } from "./screens/shell";

export function App() {
  const route = useRoute();
  const { runs, live, reload } = useRuns();
  const [info, setInfo] = React.useState<Info | null>(null);
  const [palette, setPalette] = React.useState(false);
  React.useEffect(() => {
    api.info().then(setInfo).catch(() => {});
  }, []);
  const screen = route.path[0] ?? "runs";
  const attention = runs.filter((r) => !r.type.startsWith("fusee.") && ATTENTION.has(r.status)).length;

  React.useEffect(() => {
    document.title = screen === "run" ? `${route.path.slice(1).join("/")} · Fusee` : `${NAV.find((n) => n.id === screen)?.label ?? (screen === "fixtures" ? "Fixtures" : "Runs")} · Fusee`;
  }, [screen, route.path]);

  React.useEffect(() => {
    let g = false;
    const on = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPalette((p) => !p);
        return;
      }
      const t = e.target as HTMLElement;
      if (t.closest("input,textarea,select,[contenteditable]")) return;
      // g then a letter: go to a screen (g r runs, g a attention, g t types, g h health).
      if (e.key === "g" && !e.metaKey && !e.ctrlKey) {
        g = true;
        setTimeout(() => (g = false), 800);
        return;
      }
      if (g) {
        const to: Record<string, string> = { r: "runs", a: "attention", t: "types", s: "schedules", o: "objects", l: "limits", h: "health" };
        const s = to[e.key];
        if (s) go([s]);
        g = false;
      }
    };
    addEventListener("keydown", on);
    return () => removeEventListener("keydown", on);
  }, []);

  const commands = React.useMemo<Command[]>(
    () => [
      ...NAV.map((n) => ({ id: `nav-${n.id}`, label: n.label, group: "Go to", run: () => go([n.id]) })),
      { id: "theme-light", label: "Light theme", group: "Display", run: () => setAppearance("light") },
      { id: "theme-dark", label: "Dark theme", group: "Display", run: () => setAppearance("dark") },
      { id: "theme-system", label: "Theme from the system", group: "Display", run: () => setAppearance("system") },
      { id: "fixtures", label: "Component fixtures", group: "Display", run: () => go(["fixtures"]) },
      ...runs
        .filter((r) => !r.type.startsWith("fusee."))
        .slice(0, 400)
        .map((r) => ({ id: `run-${r.id}`, label: r.id, hint: `${r.type} · ${r.status}`, group: "Runs", run: () => go(["run", r.id]) })),
    ],
    [runs],
  );

  let body: React.ReactNode;
  switch (screen) {
    case "run":
      body = <RunScreen key={route.path.slice(1).join("/")} id={route.path.slice(1).join("/")} />;
      break;
    case "attention":
      body = <AttentionScreen runs={runs} live={live} />;
      break;
    case "types":
      body = <TypesScreen runs={runs} />;
      break;
    case "health":
      body = <HealthScreen runs={runs} />;
      break;
    case "schedules":
      body = <SchedulesScreen />;
      break;
    case "objects":
      body = <ObjectsScreen runs={runs} route={route.path} />;
      break;
    case "limits":
      body = <LimitsScreen />;
      break;
    case "fixtures":
      body = <FixturesScreen />;
      break;
    default:
      body = <RunsScreen runs={runs} live={live} view={route.query.get("view") ?? "all"} reload={reload} />;
  }

  return (
    <TooltipProvider>
      <ToastProvider>
        <Shell info={info} current={screen === "run" ? "runs" : screen} attention={attention} onPalette={() => setPalette(true)}>
          {body}
        </Shell>
        <Palette open={palette} onOpenChange={setPalette} commands={commands} />
        <TokenPrompt />
      </ToastProvider>
    </TooltipProvider>
  );
}
