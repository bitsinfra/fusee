// The contrast the theme promises: 4.5:1 for reading text and 3:1 for focus,
// strong borders and marks, on every layer, in light and dark, standard and
// high contrast.
import { build } from "esbuild";
import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const out = join(mkdtempSync(join(tmpdir(), "fusee-ui-theme-")), "theme.mjs");
await build({ entryPoints: [join(root, "src/tokens/theme.ts")], bundle: true, format: "esm", outfile: out, logLevel: "error" });
const { DEFAULT_THEME_INPUTS, HIGH_CONTRAST, THEME_TOKENS, generateTheme, hslChannelsToSrgb, relativeLuminance, themeStylesheet } =
  await import(out);

function contrast(theme, a, b) {
  assert.ok(theme[a], `${a} is generated`);
  assert.ok(theme[b], `${b} is generated`);
  const [lighter, darker] = [relativeLuminance(hslChannelsToSrgb(theme[a])), relativeLuminance(hslChannelsToSrgb(theme[b]))].sort((x, y) => y - x);
  return (lighter + 0.05) / (darker + 0.05);
}

function atLeast(theme, pairs, min) {
  for (const [a, b] of pairs) {
    const c = contrast(theme, a, b);
    assert.ok(c >= min, `${a} on ${b} is ${c.toFixed(2)}:1, below ${min}:1`);
  }
}

const cross = (as, bs) => as.flatMap((a) => bs.map((b) => [a, b]));
const ladders = [
  ["standard", DEFAULT_THEME_INPUTS],
  ["high contrast", { ...DEFAULT_THEME_INPUTS, contrast: HIGH_CONTRAST }],
];

for (const [ladder, inputs] of ladders)
  for (const appearance of ["light", "dark"])
    describe(`${appearance}, ${ladder}`, () => {
      const theme = generateTheme(inputs, appearance);

      it("keeps reading text at 4.5:1 on every resting and selected layer", () => {
        atLeast(
          theme,
          cross(
            ["--ds-text-strong", "--ds-text-muted", "--ds-text-subtle", "--ds-sidebar-text", "--ds-sidebar-text-muted"],
            ["--ds-surface", "--ds-surface-elevated", "--ds-hover", "--ds-selected", "--ds-table-row-hover", "--ds-table-row-selected"],
          ),
          4.5,
        );
      });

      it("keeps rail text at 4.5:1", () => {
        atLeast(
          theme,
          cross(
            ["--ds-sidebar-text", "--ds-sidebar-text-muted", "--ds-sidebar-section-label"],
            ["--ds-sidebar-bg", "--ds-sidebar-item-hover", "--ds-sidebar-item-active", "--ds-shell-bg"],
          ),
          4.5,
        );
      });

      it("keeps status text readable on resting, selected and tinted surfaces", () => {
        for (const s of ["success", "error", "info", "warning", "live", "caution"])
          atLeast(theme, cross([`--ds-${s}`], ["--ds-surface", "--ds-selected", `--ds-${s}-muted`]), 4.5);
      });

      it("keeps labels readable through solid button states", () => {
        atLeast(theme, cross(["--ds-accent-foreground"], ["--ds-accent", "--ds-accent-hover", "--ds-accent-active"]), 4.5);
        atLeast(theme, cross(["--destructive-foreground"], ["--ds-error", "--ds-error-hover", "--ds-error-active"]), 4.5);
        atLeast(theme, [["--ds-warning-foreground", "--ds-warning"]], 4.5);
      });

      it("keeps focus, strong borders and marks at 3:1", () => {
        atLeast(theme, cross(["--ds-focus-ring", "--ds-border-strong"], ["--ds-page", "--ds-surface", "--ds-surface-elevated", "--ds-selected"]), 3);
        atLeast(
          theme,
          THEME_TOKENS.filter((t) => t.startsWith("--ds-mark-")).map((m) => [m, "--ds-surface"]),
          3,
        );
      });

      it("orders the elevation and state ladders", () => {
        const l = (t) => relativeLuminance(hslChannelsToSrgb(theme[t]));
        const toward = appearance === "light" ? (a, b) => l(a) > l(b) : (a, b) => l(a) < l(b);
        for (const [a, b] of [
          ["--ds-surface", "--ds-hover"],
          ["--ds-hover", "--ds-active"],
          ["--ds-border-subtle", "--ds-border"],
          ["--ds-border", "--ds-border-strong"],
          ["--ds-text-subtle", "--ds-text-strong"],
        ])
          assert.ok(toward(a, b), `${a} sits before ${b}`);
      });
    });

describe("tokens", () => {
  it("raises border and secondary text contrast on the high-contrast ladder", () => {
    for (const appearance of ["light", "dark"]) {
      const standard = generateTheme(DEFAULT_THEME_INPUTS, appearance);
      const high = generateTheme({ ...DEFAULT_THEME_INPUTS, contrast: HIGH_CONTRAST }, appearance);
      for (const t of ["--ds-border", "--ds-border-subtle", "--ds-text-muted", "--ds-text-subtle"])
        assert.ok(contrast(high, t, "--ds-surface") > contrast(standard, t, "--ds-surface"), `${appearance} ${t}`);
    }
  });

  it("emits all four blocks into the stylesheet", () => {
    const css = themeStylesheet();
    for (const sel of [":root", ".dark", ':root:not(.dark)[data-contrast="high"]', '.dark[data-contrast="high"]'])
      assert.ok(css.includes(sel), sel);
    for (const t of THEME_TOKENS) assert.equal(css.split(`${t}:`).length - 1, 4, t);
  });

  it("generates every token the code and the Tailwind preset use", () => {
    const known = new Set(THEME_TOKENS);
    // Variables the stylesheet defines by hand rather than by the generator.
    const own = new Set(
      [...readFileSync(join(root, "src/styles.css"), "utf8").matchAll(/(--ds-[a-z0-9-]+)\s*:/g)].map((m) => m[1]),
    );
    const files = [join(root, "tailwind.config.cjs"), join(root, "src/styles.css")];
    const walk = (d) => {
      for (const f of readdirSync(d)) {
        const p = join(d, f);
        if (statSync(p).isDirectory()) walk(p);
        else if (/\.(ts|tsx)$/.test(f) && !p.endsWith("theme.ts")) files.push(p);
      }
    };
    walk(join(root, "src"));
    const missing = [];
    for (const f of files)
      for (const m of readFileSync(f, "utf8").matchAll(/var\((--ds-[a-z0-9-]+)\)/g))
        if (!known.has(m[1]) && !own.has(m[1])) missing.push(`${m[1]} in ${f.slice(root.length + 1)}`);
    assert.deepEqual(missing, []);
  });
});
