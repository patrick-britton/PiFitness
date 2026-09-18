'use client';

import { useEffect, useState } from 'react';

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

/** Increment a version when the `dark` class on <html> toggles (theme change). */
export function useThemeVersion(): number {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const observer = new MutationObserver(() => setVersion((v) => v + 1));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);
  return version;
}
