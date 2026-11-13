/**
 * Fusee's theme: an OKLCH theme generator, with Fusee's own mark and open
 * fonts (Inter, Geist Mono).
 *
 * One OKLCH generator produces every surface, border, text, state, rail,
 * accent, status and mark colour for light, dark and high contrast from three
 * inputs: a base tint, an accent tint (ink in light, paper in dark) and a
 * contrast amount. OKLCH keeps equal lightness looking equally bright, so the
 * elevation and state ladders below read the same in both themes.
 *
 * `build.mjs` writes the output into the stylesheet; `test/theme.test.mjs`
 * checks the contrast it promises. Values are HSL channel triples, used as
 * `hsl(var(--ds-*))`.
 */

interface OklchTint {
  /** Hue angle in degrees. */
  readonly hue: number;
  /** OKLCH chroma; neutrals stay legible up to about 0.03. */
  readonly chroma: number;
}

export interface ThemeInputs {
  /** Tint shared by every neutral surface, border, state and text token. */
  readonly base: OklchTint;
  /** Tint of primary actions, links and the rail's active mark. */
  readonly accent: OklchTint;
  /** 0 is the standard ladder; 1 is the high-contrast ladder. */
  readonly contrast: number;
}

export type ThemeAppearance = "light" | "dark";

/** Graphite: untinted ink and paper. Monochrome stays. */
export const DEFAULT_THEME_INPUTS: ThemeInputs = {
  base: { hue: 0, chroma: 0 },
  accent: { hue: 0, chroma: 0 },
  contrast: 0,
};

/** The contrast amount the high-contrast appearance uses. */
export const HIGH_CONTRAST = 1;

/**
 * How a neutral token responds to contrast. Distances from the work surface
 * grow by the role's factor at full contrast; surfaces keep their place, and
 * strong text and focus move toward pure ink or paper.
 */
type NeutralRole =
  "surface" | "state" | "border" | "emphasis" | "text" | "strong" | "focus";

const ROLE_CONTRAST_FACTOR: Record<
  Exclude<NeutralRole, "strong" | "focus">,
  number
> = {
  surface: 1,
  state: 1.4,
  border: 2.3,
  emphasis: 1.6,
  text: 1.22,
};

interface NeutralRecipe {
  readonly role: NeutralRole;
  /** OKLCH lightness on the standard ladder, light then dark. */
  readonly light: number;
  readonly dark: number;
  /** Tinted with the accent instead of the base. */
  readonly accent?: boolean;
  /** Fixed colour: no tint and no contrast response. */
  readonly fixed?: boolean;
}

function neutral(
  role: NeutralRole,
  light: number,
  dark: number,
  options: { accent?: boolean; fixed?: boolean } = {},
): NeutralRecipe {
  return { role, light, dark, ...options };
}

/** The work plane every distance is measured from. */
const WORK_SURFACE = { light: 1, dark: 0.182 } as const;

const NEUTRAL_TOKENS = {
  // Surfaces: the chrome recedes behind a crisp working plane.
  "--ds-page": neutral("surface", 0.97, 0.145),
  "--ds-shell-bg": neutral("surface", 0.955, 0.145),
  "--ds-surface": neutral("surface", 1, 0.182),
  "--ds-surface-elevated": neutral("surface", 0.985, 0.222),
  "--ds-surface-sunken": neutral("surface", 0.944, 0.168),
  // Borders: hairlines first, strong borders only for focus and selection.
  "--ds-border": neutral("border", 0.9, 0.289),
  "--ds-border-strong": neutral("emphasis", 0.608, 0.565),
  "--ds-border-subtle": neutral("border", 0.932, 0.244),
  // Interactive states step toward ink from the layer beneath them.
  "--ds-hover": neutral("state", 0.954, 0.256),
  "--ds-hover-strong": neutral("state", 0.931, 0.285),
  "--ds-active": neutral("state", 0.912, 0.309),
  "--ds-selected": neutral("state", 0.927, 0.285),
  "--ds-text-selection-bg": neutral("state", 0.79, 0.439),
  // Text: subtle copy keeps 4.5:1 on elevated and selected layers.
  "--ds-text-strong": neutral("strong", 0.205, 0.946),
  "--ds-text-muted": neutral("text", 0.496, 0.738),
  "--ds-text-subtle": neutral("text", 0.51, 0.676),
  "--ds-text-disabled": neutral("text", 0.708, 0.51),
  // Accent: ink in light and paper in dark for primary actions and links.
  "--ds-accent": neutral("surface", 0.205, 0.946, { accent: true }),
  "--ds-accent-hover": neutral("surface", 0.301, 0.9, { accent: true }),
  "--ds-accent-active": neutral("surface", 0.371, 0.837, { accent: true }),
  "--ds-accent-muted": neutral("state", 0.954, 0.285, { accent: true }),
  "--ds-accent-text": neutral("strong", 0.205, 0.946, { accent: true }),
  "--ds-accent-foreground": neutral("surface", 1, 0, { fixed: true }),
  // Identity: the neutral workspace and product mark.
  "--ds-identity": neutral("strong", 0.205, 0.946),
  "--ds-identity-muted": neutral("state", 0.954, 0.285),
  // Rail and context navigation: dimmer than the work plane so the task owns
  // the hierarchy; selection is a graphite layer, not a brand colour.
  "--ds-sidebar-bg": neutral("surface", 0.979, 0.145),
  "--ds-sidebar-border": neutral("border", 0.928, 0.244),
  "--ds-sidebar-item-active": neutral("state", 0.943, 0.256),
  "--ds-sidebar-item-hover": neutral("state", 0.955, 0.209),
  "--ds-sidebar-accent": neutral("surface", 0.205, 0.946, { accent: true }),
  "--ds-sidebar-text": neutral("text", 0.387, 0.738),
  "--ds-sidebar-text-muted": neutral("text", 0.51, 0.657),
  "--ds-sidebar-text-active": neutral("strong", 0.205, 0.946),
  "--ds-sidebar-section-label": neutral("text", 0.51, 0.657),
  // Tables: no stripes or gridlines carry structure; hover and selection do.
  "--ds-table-header-bg": neutral("surface", 0.979, 0.2),
  "--ds-table-row-hover": neutral("state", 0.962, 0.256),
  "--ds-table-row-selected": neutral("state", 0.935, 0.285),
  "--ds-table-row-stripe": neutral("surface", 0.989, 0.196),
  // Panels and the agent conversation.
  "--ds-panel-bg": neutral("surface", 1, 0.182),
  "--ds-panel-header": neutral("surface", 0.985, 0.2),
  "--ds-panel-divider": neutral("border", 0.954, 0.244),
  // Focus: a mid graphite that keeps 3:1 against every layer.
  "--ds-focus-ring": neutral("focus", 0.528, 0.613),
  "--ds-warning-foreground": neutral("surface", 1, 0, { fixed: true }),
  "--destructive-foreground": neutral("surface", 1, 0, { fixed: true }),
  "--shadow-color": neutral("surface", 0.146, 0, { fixed: true }),
} as const satisfies Record<string, NeutralRecipe>;

type OklchValue = readonly [lightness: number, chroma: number, hue: number];

interface ChromaticRecipe {
  readonly light: OklchValue;
  readonly dark: OklchValue;
}

/**
 * Status, live, authority and meaning-mark colours are chosen for meaning, so
 * they are not derived from the inputs. Each group shares one lightness per
 * theme; yellow hues sit higher because a dark yellow reads as olive.
 */
const CHROMATIC_TOKENS = {
  "--ds-success": { light: [0.482, 0.1, 162], dark: [0.69, 0.14, 158] },
  "--ds-success-muted": {
    light: [0.958, 0.02, 166],
    dark: [0.275, 0.035, 164],
  },
  "--ds-warning": { light: [0.493, 0.107, 93], dark: [0.88, 0.17, 104] },
  "--ds-warning-muted": { light: [0.962, 0.04, 103], dark: [0.29, 0.04, 104] },
  "--ds-error": { light: [0.508, 0.15, 28], dark: [0.715, 0.15, 23] },
  "--ds-error-hover": { light: [0.47, 0.15, 28], dark: [0.75, 0.15, 23] },
  "--ds-error-active": { light: [0.43, 0.15, 28], dark: [0.79, 0.15, 23] },
  "--ds-error-muted": { light: [0.952, 0.018, 27], dark: [0.275, 0.045, 20] },
  "--ds-info": { light: [0.492, 0.055, 257], dark: [0.71, 0.12, 257] },
  "--ds-info-muted": { light: [0.955, 0.012, 258], dark: [0.275, 0.035, 257] },
  "--ds-caution": { light: [0.488, 0.13, 113], dark: [0.88, 0.18, 115] },
  "--ds-caution-muted": { light: [0.962, 0.035, 110], dark: [0.29, 0.04, 113] },
  "--ds-live": { light: [0.482, 0.11, 161], dark: [0.74, 0.16, 156] },
  "--ds-live-muted": { light: [0.958, 0.02, 166], dark: [0.3, 0.05, 161] },
  "--ds-mark-gray": { light: [0.6, 0, 0], dark: [0.68, 0, 0] },
  "--ds-mark-blue": { light: [0.57, 0.15, 257], dark: [0.72, 0.12, 256] },
  "--ds-mark-teal": { light: [0.57, 0.09, 189], dark: [0.72, 0.105, 186] },
  "--ds-mark-green": { light: [0.57, 0.11, 158], dark: [0.72, 0.13, 158] },
  "--ds-mark-yellow": { light: [0.66, 0.131, 81], dark: [0.81, 0.145, 88] },
  "--ds-mark-orange": { light: [0.6, 0.155, 48], dark: [0.74, 0.135, 56] },
  "--ds-mark-red": { light: [0.57, 0.18, 28], dark: [0.72, 0.15, 26] },
  "--ds-mark-purple": { light: [0.57, 0.145, 302], dark: [0.72, 0.12, 303] },
  "--ds-mark-pink": { light: [0.6, 0.17, 353], dark: [0.73, 0.13, 349] },
} as const satisfies Record<string, ChromaticRecipe>;

export type ThemeToken =
  keyof typeof NEUTRAL_TOKENS | keyof typeof CHROMATIC_TOKENS;

/** Every generated token, in stylesheet order. */
export const THEME_TOKENS: readonly ThemeToken[] = [
  ...(Object.keys(NEUTRAL_TOKENS) as (keyof typeof NEUTRAL_TOKENS)[]),
  ...(Object.keys(CHROMATIC_TOKENS) as (keyof typeof CHROMATIC_TOKENS)[]),
];

export type GeneratedTheme = Readonly<Record<ThemeToken, string>>;

/** The token values of one appearance as HSL channel triples. */
export function generateTheme(
  inputs: ThemeInputs,
  appearance: ThemeAppearance,
): GeneratedTheme {
  const contrast = clamp(inputs.contrast, 0, 1);
  const values: Partial<Record<ThemeToken, string>> = {};
  const surface = WORK_SURFACE[appearance];
  for (const [token, recipe] of Object.entries(NEUTRAL_TOKENS) as [
    keyof typeof NEUTRAL_TOKENS,
    NeutralRecipe,
  ][]) {
    const standard = recipe[appearance];
    if (recipe.fixed) {
      values[token] = hslChannels([standard, 0, 0]);
      continue;
    }
    const lightness = contrastLightness(
      recipe.role,
      standard,
      surface,
      appearance,
      contrast,
    );
    const tint = recipe.accent ? inputs.accent : inputs.base;
    values[token] = hslChannels([lightness, tint.chroma, tint.hue]);
  }
  for (const [token, recipe] of Object.entries(CHROMATIC_TOKENS) as [
    keyof typeof CHROMATIC_TOKENS,
    ChromaticRecipe,
  ][]) {
    values[token] = hslChannels(recipe[appearance]);
  }
  return values as GeneratedTheme;
}

function contrastLightness(
  role: NeutralRole,
  standard: number,
  surface: number,
  appearance: ThemeAppearance,
  contrast: number,
): number {
  if (role === "strong" || role === "focus") {
    const extreme = appearance === "light" ? 0 : 1;
    const reach = role === "focus" ? 1 : 0.55;
    return standard + (extreme - standard) * reach * contrast;
  }
  const factor = 1 + (ROLE_CONTRAST_FACTOR[role] - 1) * contrast;
  return clamp(surface + (standard - surface) * factor, 0, 1);
}

/** The rule selectors each generated block applies to. */
export const THEME_SELECTORS = {
  light: [":root"],
  dark: [".dark"],
  lightHighContrast: [':root:not(.dark)[data-contrast="high"]'],
  darkHighContrast: ['.dark[data-contrast="high"]'],
} as const;

/** The four generated blocks as CSS, indented for runtime.css's base layer. */
export function themeStylesheet(
  inputs: ThemeInputs = DEFAULT_THEME_INPUTS,
  indent = "  ",
): string {
  const highContrast = {
    ...inputs,
    contrast: Math.max(inputs.contrast, HIGH_CONTRAST),
  };
  const blocks: [readonly string[], GeneratedTheme][] = [
    [THEME_SELECTORS.light, generateTheme(inputs, "light")],
    [THEME_SELECTORS.dark, generateTheme(inputs, "dark")],
    [THEME_SELECTORS.lightHighContrast, generateTheme(highContrast, "light")],
    [THEME_SELECTORS.darkHighContrast, generateTheme(highContrast, "dark")],
  ];
  return blocks
    .map(([selectors, theme]) =>
      [
        `${indent}${selectors.join(`,\n${indent}`)} {`,
        ...THEME_TOKENS.map((token) => `${indent}  ${token}: ${theme[token]};`),
        `${indent}}`,
      ].join("\n"),
    )
    .join("\n\n");
}

/** Inputs as the JSON design files import. */
export function themeInputsJson(inputs: ThemeInputs): string {
  return `${JSON.stringify(
    {
      base: inputs.base,
      accent: inputs.accent,
      contrast: inputs.contrast,
      light: generateTheme(inputs, "light"),
      dark: generateTheme(inputs, "dark"),
    },
    null,
    2,
  )}\n`;
}

// ---------------------------------------------------------------------------
// Colour math: OKLCH -> OKLab -> linear sRGB -> sRGB -> HSL.

type Rgb = readonly [number, number, number];

/** sRGB channels (0–1) of an OKLCH colour, with chroma reduced into gamut. */
function oklchToSrgb([lightness, chroma, hue]: OklchValue): Rgb {
  const inGamut = (rgb: Rgb) =>
    rgb.every((channel) => channel >= -0.0005 && channel <= 1.0005);
  let rgb = oklchToSrgbUnclamped(lightness, chroma, hue);
  if (!inGamut(rgb)) {
    let low = 0;
    let high = chroma;
    for (let step = 0; step < 24; step += 1) {
      const middle = (low + high) / 2;
      if (inGamut(oklchToSrgbUnclamped(lightness, middle, hue))) low = middle;
      else high = middle;
    }
    rgb = oklchToSrgbUnclamped(lightness, low, hue);
  }
  return [clamp(rgb[0], 0, 1), clamp(rgb[1], 0, 1), clamp(rgb[2], 0, 1)];
}

function oklchToSrgbUnclamped(
  lightness: number,
  chroma: number,
  hue: number,
): Rgb {
  const radians = (hue * Math.PI) / 180;
  const a = chroma * Math.cos(radians);
  const b = chroma * Math.sin(radians);
  const l = (lightness + 0.3963377774 * a + 0.2158037573 * b) ** 3;
  const m = (lightness - 0.1055613458 * a - 0.0638541728 * b) ** 3;
  const s = (lightness - 0.0894841775 * a - 1.291485548 * b) ** 3;
  return [
    gamma(4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s),
    gamma(-1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s),
    gamma(-0.0041960863 * l - 0.7034186147 * m + 1.707614701 * s),
  ];
}

function gamma(channel: number): number {
  const sign = channel < 0 ? -1 : 1;
  const value = Math.abs(channel);
  return (
    sign *
    (value <= 0.0031308 ? 12.92 * value : 1.055 * value ** (1 / 2.4) - 0.055)
  );
}

/** WCAG relative luminance of sRGB channels. */
export function relativeLuminance([red, green, blue]: Rgb): number {
  const linear = (channel: number) =>
    channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
  return 0.2126 * linear(red) + 0.7152 * linear(green) + 0.0722 * linear(blue);
}

/** sRGB channels of an HSL channel triple such as `0 0% 96.08%`. */
export function hslChannelsToSrgb(value: string): Rgb {
  const match = /^([\d.]+) ([\d.]+)% ([\d.]+)%$/u.exec(value);
  if (!match) throw new Error(`Not an HSL channel triple: ${value}`);
  const hue = Number(match[1]);
  const saturation = Number(match[2]) / 100;
  const lightness = Number(match[3]) / 100;
  const k = (n: number) => (n + hue / 30) % 12;
  const a = saturation * Math.min(lightness, 1 - lightness);
  const f = (n: number) =>
    lightness - a * Math.max(-1, Math.min(k(n) - 3, Math.min(9 - k(n), 1)));
  return [f(0), f(8), f(4)];
}

function hslChannels(value: OklchValue): string {
  const [red, green, blue] = oklchToSrgb(value);
  const max = Math.max(red, green, blue);
  const min = Math.min(red, green, blue);
  const lightness = (max + min) / 2;
  const delta = max - min;
  let hue = 0;
  let saturation = 0;
  if (delta > 0.0005) {
    saturation = delta / (1 - Math.abs(2 * lightness - 1));
    if (max === red) hue = ((green - blue) / delta) % 6;
    else if (max === green) hue = (blue - red) / delta + 2;
    else hue = (red - green) / delta + 4;
    hue *= 60;
    if (hue < 0) hue += 360;
  }
  return `${format(hue, 0)} ${format(saturation * 100, 1)}% ${format(lightness * 100, 2)}%`;
}

function format(value: number, digits: number): string {
  return String(Number(value.toFixed(digits)));
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
