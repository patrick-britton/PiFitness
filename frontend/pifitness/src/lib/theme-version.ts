'use client';

import { useEffect, useState } from 'react';

/**
 * Theme-change tracking for JS-colored surfaces (Chart.js / MapLibre), 009-003 T08.
 *
 * Chart.js and MapLibre bake colors into configs/paint properties, so a theme
 * toggle must rebuild them. This hook increments a version whenever the `dark`
 * class on `<html>` changes; consumers put the version in their effect deps.
 *
 * One implementation for the module: the Leaderboards copy in
 * `app/activities/leaderboards/components/chart-theme.ts` re-exports this, the
 * same way that module already re-exports `tokenRgb` from `@/lib/token-color`.
 * Copies elsewhere (Beach, Volleyball, Tri-tip, DbSize) are consolidated by T14.
 */
export function useThemeVersion(): number {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const observer = new MutationObserver(() => setVersion((v) => v + 1));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);
  return version;
}

/** True when the `dark` class is on `<html>` right now (SSR-safe). */
export function isDarkTheme(): boolean {
  if (typeof window === 'undefined') return false;
  return document.documentElement.classList.contains('dark');
}