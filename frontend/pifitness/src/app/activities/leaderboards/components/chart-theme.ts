'use client';

/**
 * Shared chart-theme helpers for the Leaderboards components (009-009 T04):
 * token reads and theme-cycle tracking used by Chart.js configs (Chart.js
 * colors are baked into configs, so themes must rebuild explicitly).
 */

/**
 * Token → color reads live in ONE place: `@/lib/token-color` (009-009 T18).
 * Re-exported here so existing leaderboard imports (EffortLollipop) stay valid;
 * every other surface imports the module directly — the harness asserts there is
 * exactly one implementation.
 *
 * CONTRACT: chart/categorical tokens (`--chart-N`, `--lb-*`) hold BARE RGB
 * channels (`design-system.md`; enforced by `scripts/verify-bars.mjs`), and the
 * channel form is required while any reader wraps a computed value in `rgb()`.
 */
export { tokenRgb } from '@/lib/token-color';

/**
 * Theme-cycle tracking now lives in ONE place for the module:
 * `@/lib/theme-version` (009-003 T08). Re-exported here so existing leaderboard
 * imports stay valid, mirroring the `tokenRgb` re-export above.
 */
export { useThemeVersion } from '@/lib/theme-version';
