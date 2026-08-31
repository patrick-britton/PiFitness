/**
 * Game History (volleyball completed games)
 * Shared "Game History" panel used by both the scorekeeper (during setup)
 * and the spectator /beachchanger viewer. Self-contained: fetches via the
 * GET-only `useVolleyballHistory` hook (shared React Query cache) and renders
 * loading / error / empty / list states identically on both surfaces.
 */
'use client';

import { useVolleyballHistory } from '../../../../hooks/useVolleyball';

function fmtTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

export default function VolleyballGameHistory() {
  const historyQuery = useVolleyballHistory();
  const history = historyQuery.data?.games ?? [];

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
          {history.map(({ game: hg, score }) => (
            <li
              key={hg.game_id}
              className="flex items-center justify-between rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-4 py-3"
            >
              <div>
                <p className="text-sm font-medium text-gray-900 dark:text-white">
                  Scripps Ranch vs {hg.team_b_name}
                </p>
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  Finished {fmtTime(hg.completed_at)}
                </p>
              </div>
              <p className="text-lg font-bold text-gray-900 dark:text-white tabular-nums">
                {score.sr} – {score.opponent}
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}