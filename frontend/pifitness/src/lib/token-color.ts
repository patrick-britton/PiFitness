/**
 * Single source of truth for design-token → CSS color reads (009-009 T18).
 *
 * Why this exists: FOUR component-local copies of this reader existed
 * (`chart-theme.ts`, `LeaderboardVisuals.tsx`, `ActivityReportCharts.tsx`,
 * `TriTipChart.tsx`) plus a `getTokenRgb` pair in `DbSizeView.tsx`, and only one
 * of them normalised token forms — so converting the tokens to hex silently
 * broke the other three (Bug 009-009-T14-2). One implementation means a
 * token-format decision can never break a subset of surfaces again;
 * `scripts/verify-bars.mjs` asserts there is exactly one implementation.
 *
 * CONTRACT: chart/categorical tokens (`--chart-N`, `--lb-*`) hold BARE RGB
 * channels per `memory-bank/references/design-system.md`, and the channel form
 * is REQUIRED while any consumer wraps a computed value in `rgb()`. Every value
 * returned here is a valid CSS color: bare channels are wrapped, `#rrggbb` and
 * existing `rgb(...)` pass through.
 *
 * Note: `--text`/`--muted`/`--grid`/`--ok`/`--warn`/`--danger`/`--primary`/
 * `--bg`/`--surface`/`--border` are not (yet) declared in this app's
 * `globals.css` — only the chart/lb palette is — so for those tokens the
 * theme-aware fallbacks in this module ARE the palette.
 *
 * Framework-free (no React, no DOM beyond the guarded token read) so the harness
 * can exercise it directly under plain `node`.
 */

/** Theme-aware fallback channels, `[light, dark]` — bare RGB, never hex. */
const FALLBACK_CHANNELS: Record<string, [string, string]> = {
  '--text': ['15 23 42', '241 245 249'],
  '--muted': ['107 114 128', '148 163 184'],
  '--grid': ['226 232 240', '51 65 85'],
  '--bg': ['250 250 250', '15 23 42'],
  '--surface': ['255 255 255', '30 41 59'],
  '--border': ['226 232 240', '51 65 85'],
  '--primary': ['37 99 235', '96 165 250'],
  '--accent': ['217 70 239', '232 121 249'],
  '--ok': ['22 163 74', '74 222 128'],
  '--warn': ['202 138 4', '250 204 21'],
  '--danger': ['220 38 38', '248 113 113'],
  // Categorical chart palette — must equal globals.css (harness-enforced).
  '--chart-1': ['0 114 178', '96 165 250'],
  '--chart-2': ['213 94 0', '251 146 60'],
  '--chart-3': ['0 158 115', '52 211 153'],
  '--chart-4': ['204 121 167', '240 171 252'],
  '--chart-5': ['230 159 0', '253 224 71'],
  '--chart-6': ['86 180 233', '125 211 252'],
  // Lollipop recency buckets (must equal globals.css — harness-enforced).
  '--lb-recent': ['220 38 38', '255 0 0'],
  '--lb-cycle': ['30 58 138', '20 64 240'],
  '--lb-365': ['125 211 252', '56 189 248'],
  '--lb-alltime': ['156 163 175', '107 114 128'],
};

/** Token used when a caller asks for a name we have no fallback for. */
const DEFAULT_TOKEN = '--muted';

/** Theme-aware fallback channels for a token (channels, never a color literal). */
export function tokenFallbackChannels(name: string, isDark: boolean): string {
  const entry = FALLBACK_CHANNELS[name] ?? FALLBACK_CHANNELS[DEFAULT_TOKEN];
  return isDark ? entry[1] : entry[0];
}

/** Normalize a raw CSS custom-property value to a valid CSS color. */
function normalize(value: string, fallback: string): string {
  const v = value.trim();
  if (!v) return `rgb(${fallback})`; // bare channel fallback → wrap
  if (v.startsWith('#') || v.startsWith('rgb')) return v; // real color already
  return `rgb(${v})`; // bare channels → wrap
}

/**
 * SSR-safe token read returning a Chart.js/MapLibre-ready CSS color.
 * Falls back to the theme-aware channels above when the token is missing
 * (SSR, or a token this app does not declare).
 */
export function tokenRgb(name: string, isDark: boolean): string {
  const fallback = tokenFallbackChannels(name, isDark);
  if (typeof window === 'undefined') return normalize('', fallback);
  const value = window
    .getComputedStyle(document.documentElement)
    .getPropertyValue(name);
  return normalize(value, fallback);
}

/**
 * CSS-side equivalent of {@link tokenRgb} for inline styles / class-free CSS:
 * the channel-backed token MUST be wrapped, so use `tokenVar('--chart-N)` and
 * never a bare unwrapped token reference (Bug 009-009-T14-1).
 */
export function tokenVar(name: string): string {
  return `rgb(var(${name}))`;
}
