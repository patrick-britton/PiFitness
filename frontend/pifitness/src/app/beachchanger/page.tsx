/**
 * Unlisted Beach Volleyball Viewer — fixed URL /beachchanger (006-002)
 * ('gamechanger', but for beach volleyball). Intentionally NOT added to any
 * navigation, menu, or sub-page config (AC-3): the only way here is typing
 * or bookmarking the URL.
 *
 * Read-only by construction (AC-2): this route imports only the viewer
 * component below — no mutation hooks, no scorekeeper components — so no
 * control capable of mutating volleyball.games / volleyball.points is
 * shipped to this route.
 *
 * Wake lock (006-003, AC-8): while this kiosk page is mounted, request a
 * screen wake lock (mirrors RecipeCook); re-acquired when the tab becomes
 * visible again. Silent no-op where unsupported or insecure (http dev) —
 * no console errors.
 */
'use client';

import { useEffect } from 'react';
import BeachViewer from './components/BeachViewer';

type WakeLockSentinel = { release?: () => Promise<void> };

export default function BeachViewerPage() {
  useEffect(() => {
    if (typeof window === 'undefined') return;
    let lock: WakeLockSentinel | null = null;
    let cancelled = false;

    const acquire = async () => {
      try {
        const nav = navigator as Navigator & {
          wakeLock?: { request: (kind: string) => Promise<WakeLockSentinel> };
        };
        if (typeof navigator === 'undefined' || nav.wakeLock == null) return;
        const got = await nav.wakeLock.request('screen');
        if (!cancelled) {
          lock = got;
        } else {
          await got.release?.();
        }
      } catch {
        // Unsupported, insecure, or denied — viewer works without the lock.
      }
    };

    const onVisibility = () => {
      if (document.visibilityState === 'visible') {
        void acquire();
      }
    };

    void acquire();
    document.addEventListener('visibilitychange', onVisibility);
    return () => {
      cancelled = true;
      document.removeEventListener('visibilitychange', onVisibility);
      lock?.release?.().catch(() => {});
    };
  }, []);

  return <BeachViewer />;
}
