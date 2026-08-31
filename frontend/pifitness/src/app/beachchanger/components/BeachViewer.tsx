/**
 * Beach Volleyball Viewer (006-002)
 * Read-only scoreboard for the unlisted /beachchanger route: partner line,
 * non-interactive score cards, and the shared event-marked score chart.
 *
 * Data source: `useVolleyballViewerActive` — a GET-only query polling every
 * 1 s (unconditionally, Bug T08-1, so a newly started game auto-switches the
 * display). Background refetches never clear cached data, and the render
 * gates on data presence rather than in-flight flags (Bug T08-7), so polling
 * is invisible. This component must never import a mutation hook or the
 * scorekeeper component.
 *
 * Kiosk shell (OQ-1 B1): Layout.tsx hides the app chrome (header/sidebar/
 * nav) for /beachchanger only; DebugPanel intentionally stays (dev-only
 * toggle).
 */
'use client';

import { useEffect, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { useVolleyballViewerActive } from '../../../hooks/useVolleyball';
import VolleyballScoreChart from '../../activities/beach/components/VolleyballScoreChart';

export default function BeachViewer() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';

  const activeQuery = useVolleyballViewerActive();
  const [isDark, setIsDark] = useState(false);

  useEffect(() => {
    const update = () => setIsDark(document.documentElement.classList.contains('dark'));
    update();
    const observer = new MutationObserver(update);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });
    return () => observer.disconnect();
  }, []);

  const game = activeQuery.data?.game ?? null;

  // Skeleton on FIRST load only (Bug T08-7): the 1 s background poll runs
  // even when no game is active, and gating the render on `isFetching` made
  // every poll cycle flash the skeleton over the empty state. Once any data
  // exists (a game, or `{game: null}`), the screen holds steady: React
  // Query's structural sharing passes updates through only when the payload
  // actually changes, so the empty state remains until a game starts and the
  // scoreboard swaps in automatically — no refresh, no flicker.
  if (activeQuery.isPending) {
    return (
      <div className="animate-pulse space-y-4 p-4">
        <div className="h-10 bg-gray-200 dark:bg-gray-700 rounded-md w-56" />
        <div className="h-32 bg-gray-200 dark:bg-gray-700 rounded-lg" />
      </div>
    );
  }

  // Error screen only when there is nothing to show: a transient poll error
  // keeps the last-known state on screen (the poll self-recovers).
  if (activeQuery.isError && !activeQuery.data) {
    const msg =
      activeQuery.error instanceof Error
        ? activeQuery.error.message
        : 'Could not load the live score.';
    return (
      <div className="p-4">
        <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 text-red-700 dark:text-red-300">
          <p className="font-medium">Error loading the live score</p>
          <p className="text-sm mt-1">{msg}</p>
        </div>
      </div>
    );
  }

  if (!game) {
    return (
      <div className="p-4">
        <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-6 text-center">
          <p className="font-semibold text-gray-900 dark:text-white">Welcome to Beachchanger</p>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            The live scoreboard appears here once a game starts.
          </p>
        </div>
      </div>
    );
  }

  const { game: g, score } = game;

  // Match context (006-002): "Playing with #<number> <name>" — name omitted
  // when absent; the whole line omitted for pre-006-002 rows without a
  // partner number.
  const partnerLine =
    g.partner_number === null
      ? null
      : `Playing with #${g.partner_number}${g.partner_name ? ` ${g.partner_name}` : ''}`;

  // Non-interactive score card: same look as the scorekeeper's scoreboard
  // (including the 006-002 light-mode SR color) but with no tap targets.
  const teamDisplay = (team: 'SR' | 'OPPONENT') => {
    const isSR = team === 'SR';
    const teamScore = isSR ? score.sr : score.opponent;
    const name = isSR ? 'Scripps Ranch' : g.team_b_name;
    const bgColor = isSR
      ? isDark ? '#750530' : '#050C46'
      : isDark ? '#374151' : '#9ca3af';
    const scoreColor = isSR ? '#ffffff' : isDark ? '#e5e7eb' : '#111827';
    const sizeCls = isLandscape ? 'text-5xl' : isDesktop ? 'text-7xl' : 'text-6xl';
    return (
      <div
        className="flex-1 rounded-lg p-4 flex flex-col items-center justify-center gap-2 text-center"
        style={{ backgroundColor: bgColor }}
      >
        <p className="text-sm font-semibold text-gray-100 uppercase tracking-wide truncate max-w-full">
          {name}
        </p>
        <p
          className={['w-full text-center font-bold tabular-nums leading-none', sizeCls].join(' ')}
          style={{ color: scoreColor }}
          aria-label={'Current score for ' + name + ': ' + teamScore}
        >
          {teamScore}
        </p>
      </div>
    );
  };

  return (
    <div className={'space-y-4 p-4 ' + (isLandscape || isDesktop ? 'max-w-4xl mx-auto' : '')}>
      {partnerLine && (
        <p className="text-sm italic text-gray-600 dark:text-gray-300">{partnerLine}</p>
      )}

      {/* Scoreboard (display only — no increment/decrement controls) */}
      <div className="flex items-stretch gap-3">
        {teamDisplay('SR')}
        <div className="flex items-center justify-center px-1">
          <span className="text-gray-400 dark:text-gray-500 font-bold text-xl">–</span>
        </div>
        {teamDisplay('OPPONENT')}
      </div>

      {/* Event-marked running-score chart (presentational, shared) */}
      <VolleyballScoreChart detail={game} />
    </div>
  );
}
