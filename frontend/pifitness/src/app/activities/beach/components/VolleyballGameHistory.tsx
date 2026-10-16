/**
 * Game History (volleyball completed games)
 * Shared "Game History" panel used by both the scorekeeper (during setup)
 * and the spectator /beachchanger viewer. Self-contained: fetches via the
 * GET-only `useVolleyballHistory` hook (shared React Query cache) and renders
 * loading / error / empty / list states identically on both surfaces.
 *
 * Cards expand (006-003, AC-7): click toggles one open card at a time
 * (collapsed by default); the open card fetches GET /{id} once via the
 * GET-only `useVolleyballGameDetail` hook and renders the shared
 * `VolleyballScoreChart` + `VolleyballGameLog`. GET-only so the viewer
 * stays read-only — no mutation hook may enter this file's import tree.
 */
'use client';

import { useState } from 'react';
import { useVolleyballHistory, useVolleyballGameDetail } from '../../../../hooks/useVolleyball';
import VolleyballScoreChart from './VolleyballScoreChart';
import VolleyballGameLog from './VolleyballGameLog';

function fmtTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

/** On-demand detail for one expanded card (mounted only while open). */
function ExpandedGameDetail({ gameId }: { gameId: number }) {
  const detailQuery = useVolleyballGameDetail({ id: gameId, enabled: true });
  const detail = detailQuery.data ?? null;

  if (detailQuery.isLoading) {
    return (
      <div className="animate-pulse space-y-2" aria-label="Loading game detail">
        <div className="h-32 bg-gray-200 dark:bg-gray-700 rounded-md" />
        <div className="h-12 bg-gray-200 dark:bg-gray-700 rounded-md" />
      </div>
    );
  }

  if (detailQuery.isError || !detail) {
    return (
      <div className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-sm text-red-700 dark:text-red-300">
        Could not load this game&apos;s chart and log.
      </div>
    );
  }

  return (
    <div className="grid gap-3 md:grid-cols-2">
      <div className="min-w-0">
        <VolleyballScoreChart detail={detail} />
      </div>
      <div className="min-w-0">
        <VolleyballGameLog detail={detail} />
      </div>
    </div>
  );
}

export default function VolleyballGameHistory() {
  const historyQuery = useVolleyballHistory();
  const history = historyQuery.data?.games ?? [];
  const [expandedId, setExpandedId] = useState<number | null>(null);

  return (
    <div>
      <p className="font-semibold text-gray-900 dark:text-white">Game History</p>
      {historyQuery.isLoading ? (
        <div className="animate-pulse mt-2 space-y-2">
          <div className="h-12 bg-gray-200 dark:bg-gray-700 rounded-md" />
          <div className="h-12 bg-gray-200 dark:bg-gray-700 rounded-md" />
        </div>
      ) : historyQuery.isError ? (
        <div className="mt-2 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-sm text-red-700 dark:text-red-300">
          Could not load game history.
        </div>
      ) : history.length === 0 ? (
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-2">No completed games yet.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {history.map(({ game: hg, score }) => {
            const expanded = expandedId === hg.game_id;
            return (
              <li
                key={hg.game_id}
                className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800"
              >
                <button
                  type="button"
                  aria-expanded={expanded}
                  aria-controls={`history-detail-${hg.game_id}`}
                  onClick={() => setExpandedId(expanded ? null : hg.game_id)}
                  className="flex w-full min-h-[44px] items-center justify-between gap-3 px-4 py-3 text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-blue-500 rounded-md"
                >
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-gray-900 dark:text-white">
                      Scripps Ranch vs {hg.team_b_name}
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      Finished {fmtTime(hg.completed_at)}
                    </p>
                  </div>
                  <span className="flex shrink-0 items-center gap-2">
                    <span className="text-lg font-bold text-gray-900 dark:text-white tabular-nums">
                      {score.sr} – {score.opponent}
                    </span>
                    <span
                      aria-hidden="true"
                      className="text-xs text-gray-500 dark:text-gray-400"
                    >
                      {expanded ? '▾' : '▸'}
                    </span>
                  </span>
                </button>
                {expanded && (
                  <div
                    id={`history-detail-${hg.game_id}`}
                    className="border-t border-gray-200 dark:border-gray-700 px-4 py-3"
                  >
                    <ExpandedGameDetail gameId={hg.game_id} />
                  </div>
                )}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}