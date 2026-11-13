// Light by default, dark, and high contrast with either.

export type Appearance = "light" | "dark" | "system";

const KEY = "fusee.appearance";
const CONTRAST = "fusee.contrast";

function read(k: string): string | null {
  try {
    return localStorage.getItem(k);
  } catch {
    return null;
  }
}

function write(k: string, v: string): void {
  try {
    localStorage.setItem(k, v);
  } catch {
    /* private mode */
  }
}

export function appearance(): Appearance {
  const v = read(KEY);
  return v === "light" || v === "dark" ? v : "system";
}

export function highContrast(): boolean {
  return read(CONTRAST) === "high";
}

export function apply(a: Appearance = appearance(), contrast = highContrast()): void {
  const dark = a === "dark" || (a === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.classList.toggle("dark", dark);
  if (contrast) document.documentElement.dataset.contrast = "high";
  else delete document.documentElement.dataset.contrast;
}

export function setAppearance(a: Appearance): void {
  write(KEY, a);
  apply(a);
}

export function setHighContrast(on: boolean): void {
  write(CONTRAST, on ? "high" : "normal");
  apply(appearance(), on);
}

export function watchSystem(): void {
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => apply());
}
