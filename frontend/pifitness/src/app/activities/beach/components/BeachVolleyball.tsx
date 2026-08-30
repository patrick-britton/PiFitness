/**
 * Beach Volleyball Scorekeeping Component
 * Activities -> Beach. Branches on backend state:
 *   - no active game -> start form + game history
 *   - active game    -> scoreboard (click score = +point, "-" below = undo),
 *     chart, then End Game / Abandon buttons below the chart.
 * Colors: Scripps Ranch = #750530 (dark) / #05C460 (light); opponent = neutral
 * gray. Same colors feed the chart lines.
 *
 * Auto-detect: the active query refreshes on every mount; while the first
 * fetch or a background refresh is in flight and no game is known, the
 * skeleton renders instead of a decisive UI (prevents showing the start form
 * off a stale cached `{game:null}` when a game is actually active).
 */
'use client';

import { useEffect, useMemo, useState } from 'react';
import { useViewportStore } from '../../../../stores/viewportStore';
import {
  useVolleyballActive,
  useVolleyballHistory,
  useCreateVolleyballGame,
  useAddVolleyballPoint,
  useRemoveVolleyballPoint,
  useEndVolleyballGame,
  useAbandonVolleyballGame,
} from '../../../../hooks/useVolleyball';
import {
  VolleyballBlockedResponse,
  VolleyballScoringTeam,
} from '../../../../lib/types/volleyball';
import VolleyballScoreChart from './VolleyballScoreChart';

/** Extract a 409 blocked_by payload from a fetchAPI error message. */
function parseBlocked(error: unknown): VolleyballBlockedResponse | null {
  if (!(error instanceof Error)) return null;
  try {
    const parsed = JSON.parse(error.message);
    const detail = parsed?.detail;
    if (detail && typeof detail === 'object' && detail.blocked_by) {
      return detail as VolleyballBlockedResponse;
    }
  } catch {
    // not a JSON error body
  }
  return null;
}

function fmtTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

export default function BeachVolleyball() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';

  const activeQuery = useVolleyballActive();
  const historyQuery = useVolleyballHistory();
  const createGame = useCreateVolleyballGame();
  const addPoint = useAddVolleyballPoint();
  const removePoint = useRemoveVolleyballPoint();
  const endGame = useEndVolleyballGame();
  const abandonGame = useAbandonVolleyballGame();

  const [opponentName, setOpponentName] = useState('');
  const [confirmAbandon, setConfirmAbandon] = useState(false);
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
  const blocked = useMemo(() => parseBlocked(createGame.error), [createGame.error]);

  // Auto-detect: never render a decisive state while a fetch/refresh is in
  // flight and no game is known, so stale cached `{game:null}` cannot strand
  // the user on the start form.
  if (activeQuery.isLoading || (activeQuery.isFetching && !game)) {
    return (
      <div className="animate-pulse space-y-4">
        <div className="h-10 bg-gray-200 dark:bg-gray-700 rounded-md w-56" />
        <div className="h-32 bg-gray-200 dark:bg-gray-700 rounded-lg" />
      </div>
    );
  }

  if (activeQuery.isError) {
    const msg =
      activeQuery.error instanceof Error
        ? activeQuery.error.message
        : 'Could not load volleyball state.';
    return (
      <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 text-red-700 dark:text-red-300">
        <p className="font-medium">Error loading volleyball scorekeeping</p>
        <p className="text-sm mt-1">{msg}</p>
      </div>
    );
  }

  // ------------------------------------------------------------------ active
  if (game) {
    const { game: g, score } = game;
    const srCount = game.points.filter((p) => p.scoring_team === 'SR').length;
    const oppCount = game.points.filter((p) => p.scoring_team === 'OPPONENT').length;

    const handleAdd = (team: VolleyballScoringTeam) => {
      addPoint.mutate({ id: g.game_id, scoringTeam: team });
    };
    const handleRemove = (team: VolleyballScoringTeam, count: number) => {
      if (count <= 0) return;
      removePoint.mutate({ id: g.game_id, scoringTeam: team });
    };
    const teamCard = (team: VolleyballScoringTeam) => {
      const isSR = team === 'SR';
      const teamScore = isSR ? score.sr : score.opponent;
      const teamCount = isSR ? srCount : oppCount;
      const name = isSR ? 'Scripps Ranch' : g.team_b_name;
      const bgColor = isSR
        ? isDark ? '#750530' : '#05C460'
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
          <button
            type="button"
            onClick={() => handleAdd(team)}
            disabled={addPoint.isPending}
            className={[
              'w-full text-center font-bold tabular-nums leading-none cursor-pointer',
              'hover:opacity-80 transition-opacity disabled:opacity-50 disabled:cursor-default',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 rounded',
              sizeCls,
            ].join(' ')}
            style={{ color: scoreColor }}
            aria-label={'Add point for ' + name + ', current score ' + teamScore}
          >
            {teamScore}
          </button>
          <button
            type="button"
            onClick={() => handleRemove(team, teamCount)}
            disabled={removePoint.isPending || teamCount <= 0}
            className={[
              'mt-2 min-h-[44px] w-12 rounded-full text-xl font-bold',
              'text-gray-900 dark:text-white bg-white dark:bg-gray-800',
              'border border-gray-300 dark:border-gray-600',
              'hover:bg-gray-100 dark:hover:bg-gray-700',
              'disabled:opacity-40 disabled:cursor-not-allowed',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2',
            ].join(' ')}
            aria-label={'Remove last point for ' + name}
          >
            −
          </button>
        </div>
      );
    };

    return (
      <div className={'space-y-4 ' + (isLandscape || isDesktop ? 'max-w-4xl mx-auto' : '')}>
        {/* Prominent scoreboard */}
        <div className="flex items-stretch gap-3">
          {teamCard('SR')}
          <div className="flex items-center justify-center px-1">
            <span className="text-gray-400 dark:text-gray-500 font-bold text-xl">–</span>
          </div>
          {teamCard('OPPONENT')}
        </div>

        <p className="text-xs text-gray-500 dark:text-gray-400">
          Started {fmtTime(g.started_at)} · Live updates every 10 s
        </p>

        {/* Score chart */}
        <VolleyballScoreChart detail={game} />

        {(addPoint.isError ||
          removePoint.isError ||
          endGame.isError ||
          abandonGame.isError) && (
          <div className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-sm text-red-700 dark:text-red-300">
            An action failed — the scoreboard will refresh automatically.
          </div>
        )}

        {/* End Game / Abandon (below the chart) */}
        <div className="flex gap-2 flex-wrap">
          <button
            onClick={() => endGame.mutate(g.game_id)}
            disabled={endGame.isPending}
            className="min-h-[44px] px-4 rounded-md bg-green-600 text-white text-sm font-medium hover:bg-green-700 dark:bg-green-500 dark:hover:bg-green-600 disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-green-500"
          >
            End Game
          </button>
          {!confirmAbandon ? (
            <button
              onClick={() => setConfirmAbandon(true)}
              className="min-h-[44px] px-4 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 text-sm font-medium hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-500"
            >
              Abandon
            </button>
          ) : (
            <span className="flex gap-2 items-center">
              <button
                onClick={() => {
                  abandonGame.mutate(g.game_id);
                  setConfirmAbandon(false);
                }}
                disabled={abandonGame.isPending}
                className="min-h-[44px] px-4 rounded-md bg-red-600 text-white text-sm font-medium hover:bg-red-700 dark:bg-red-500 dark:hover:bg-red-600 disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-500"
              >
                Confirm abandon (drops the match)
              </button>
              <button
                onClick={() => setConfirmAbandon(false)}
                className="min-h-[44px] px-3 rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 text-sm"
              >
                Cancel
              </button>
            </span>
          )}
        </div>
      </div>
    );
  }
// ---------------------------------------------------------------- pre-game
  const history = historyQuery.data?.games ?? [];

  return (
    <div className={'space-y-6 ' + (isDesktop ? 'max-w-2xl mx-auto' : '')}>
      {/* Start form */}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (!opponentName.trim() || createGame.isPending) return;
          createGame.mutate(
            { team_b_name: opponentName.trim() },
            { onSuccess: () => setOpponentName('') }
          );
        }}
        className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4"
      >
        <p className="font-semibold text-gray-900 dark:text-white">Start a new game</p>

        <div className="flex gap-2 mt-3">
          <input
            value={opponentName}
            onChange={(e) => setOpponentName(e.target.value)}
            placeholder="Opponent team name"
            maxLength={120}
            className="flex-1 min-h-[44px] rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 text-gray-900 dark:text-white px-3 text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
            aria-label="Opponent team name"
          />
          <button
            type="submit"
            disabled={!opponentName.trim() || createGame.isPending}
            className="min-h-[44px] px-4 rounded-md bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 dark:bg-blue-500 dark:hover:bg-blue-600 disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
          >
            {createGame.isPending ? 'Starting…' : 'Start Game'}
          </button>
        </div>
        {blocked && (
          <div className="mt-3 rounded-md border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-900/20 p-3 text-sm text-amber-800 dark:text-amber-200">
            Cannot start: a game is already active ({blocked.blocked_by.team_b_name}, started{' '}
            {fmtTime(blocked.blocked_by.started_at)}). End or abandon it first.
          </div>
        )}
        {createGame.isError && !blocked && (
          <div className="mt-3 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-sm text-red-700 dark:text-red-300">
            Failed to start the game. Please try again.
          </div>
        )}
      </form>

      {/* History */}
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
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-2">
            No completed games yet.
          </p>
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
    </div>
  );
}