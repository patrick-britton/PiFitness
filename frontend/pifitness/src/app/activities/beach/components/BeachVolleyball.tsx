/**
 * Beach Volleyball Scorekeeping Component
 * Activities -> Beach. Branches on backend state:
 *   - no active game -> start form (partner # + name, opponent) + game history
 *   - active game    -> italic "Playing with #N <name>" line, scoreboard
 *     (click score = +point, "-" below = undo), Ace/Block/Spike/Dive tag row
 *     (annotates the most recent point, either team's), chart, then End Game /
 *     Abandon buttons below the chart.
 * Colors: Scripps Ranch = #750530 (dark) / #050C46 (light, corrected 006-002);
 * opponent = neutral gray. Same colors feed the chart lines.
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
  useCreateVolleyballGame,
  useAddVolleyballPoint,
  useRemoveVolleyballPoint,
  useEndVolleyballGame,
  useAbandonVolleyballGame,
} from '../../../../hooks/useVolleyball';
import {
  VolleyballBlockedResponse,
  VolleyballEventType,
  VolleyballScoringTeam,
} from '../../../../lib/types/volleyball';
import VolleyballScoreChart from './VolleyballScoreChart';
import VolleyballGameHistory from './VolleyballGameHistory';

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
  const createGame = useCreateVolleyballGame();
  const addPoint = useAddVolleyballPoint();
  const removePoint = useRemoveVolleyballPoint();
  const endGame = useEndVolleyballGame();
  const abandonGame = useAbandonVolleyballGame();

  const [opponentName, setOpponentName] = useState('');
  const [partnerNumber, setPartnerNumber] = useState('');
  const [partnerName, setPartnerName] = useState('');
  const [pendingEvent, setPendingEvent] = useState<VolleyballEventType | null>(null);
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
    // Match context line (006-002): "Playing with #<number> <name>", name
    // omitted when absent; the whole line is omitted for pre-006-002 rows
    // that have no partner number.
    const partnerLine =
      g.partner_number === null
        ? null
        : `Playing with #${g.partner_number}${g.partner_name ? ` ${g.partner_name}` : ''}`;

    const handleAdd = (team: VolleyballScoringTeam) => {
      // Bug T08-3: the held event is written atomically with this point;
      // the highlight clears once the point (and its event) is recorded.
      addPoint.mutate(
        { id: g.game_id, scoringTeam: team, eventType: pendingEvent },
        { onSuccess: () => setPendingEvent(null) }
      );
    };
    const handleRemove = (team: VolleyballScoringTeam, count: number) => {
      if (count <= 0) return;
      removePoint.mutate({ id: g.game_id, scoringTeam: team });
    };
    const teamCard = (team: VolleyballScoringTeam) => {
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
        </div>
      );
    };

    return (
      <div className={'space-y-4 ' + (isLandscape || isDesktop ? 'max-w-4xl mx-auto' : '')}>
        {/* Match context: SR's partner for this game (006-002), positioned
            above the score display. Omitted for pre-006-002 rows without a
            partner number. */}
        {partnerLine && (
          <p className="text-sm italic text-gray-600 dark:text-gray-300">{partnerLine}</p>
        )}
        {/* Prominent scoreboard */}
        <div className="flex items-stretch gap-3">
          {teamCard('SR')}
          <div className="flex items-center justify-center px-1">
            <span className="text-gray-400 dark:text-gray-500 font-bold text-xl">–</span>
          </div>
          {teamCard('OPPONENT')}
        </div>

        {/* Per-team undo row — below the score display (Bug T08-2): removes
            that team's most recent point together with any event on it. */}
        <div className="flex items-stretch gap-3">
          <div className="flex-1 flex justify-center">
            <button
              type="button"
              onClick={() => handleRemove('SR', srCount)}
              disabled={removePoint.isPending || srCount <= 0}
              className={[
                'min-h-[44px] w-12 rounded-full text-xl font-bold',
                'text-gray-900 dark:text-white bg-white dark:bg-gray-800',
                'border border-gray-300 dark:border-gray-600',
                'hover:bg-gray-100 dark:hover:bg-gray-700',
                'disabled:opacity-40 disabled:cursor-not-allowed',
                'focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2',
              ].join(' ')}
              aria-label="Remove last point for Scripps Ranch"
            >
              −
            </button>
          </div>
          <div className="flex items-center justify-center px-1">
            <span className="text-xl font-bold opacity-0">–</span>
          </div>
          <div className="flex-1 flex justify-center">
            <button
              type="button"
              onClick={() => handleRemove('OPPONENT', oppCount)}
              disabled={removePoint.isPending || oppCount <= 0}
              className={[
                'min-h-[44px] w-12 rounded-full text-xl font-bold',
                'text-gray-900 dark:text-white bg-white dark:bg-gray-800',
                'border border-gray-300 dark:border-gray-600',
                'hover:bg-gray-100 dark:hover:bg-gray-700',
                'disabled:opacity-40 disabled:cursor-not-allowed',
                'focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2',
              ].join(' ')}
              aria-label={'Remove last point for ' + g.team_b_name}
            >
              −
            </button>
          </div>
        </div>

        {/* Notable-play selector (006-002, Bug T08-3): pressing holds the
            event (highlighted) and it is written atomically with the NEXT
            recorded point, whichever team scores it. Press the highlighted
            button again to clear; press another to switch. Full-width row. */}
        <div className="grid grid-cols-4 gap-2 w-full">
          {(['Ace', 'Block', 'Spike', 'Dive'] as VolleyballEventType[]).map((evt) => {
            const selected = pendingEvent === evt;
            return (
              <button
                key={evt}
                type="button"
                onClick={() => setPendingEvent((cur) => (cur === evt ? null : evt))}
                aria-pressed={selected}
                className={[
                  'min-h-[44px] rounded-md border text-sm font-medium',
                  'focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500',
                  selected
                    ? 'bg-blue-600 border-blue-600 text-white hover:bg-blue-700 dark:bg-blue-500 dark:border-blue-500 dark:hover:bg-blue-600'
                    : 'border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white hover:bg-gray-100 dark:hover:bg-gray-700',
                ].join(' ')}
              >
                {evt}
              </button>
            );
          })}
        </div>

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
  return (
    <div className={'space-y-6 ' + (isDesktop ? 'max-w-2xl mx-auto' : '')}>
      {/* Start form */}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          const partnerNum = Number(partnerNumber);
          if (
            !opponentName.trim() ||
            partnerNumber.trim() === '' ||
            !Number.isInteger(partnerNum) ||
            partnerNum < 0 ||
            createGame.isPending
          ) {
            return;
          }
          createGame.mutate(
            {
              team_b_name: opponentName.trim(),
              partner_number: partnerNum,
              partner_name: partnerName.trim() || null,
            },
            {
              onSuccess: () => {
                setOpponentName('');
                setPartnerNumber('');
                setPartnerName('');
              },
            }
          );
        }}
        className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4"
      >
        <p className="font-semibold text-gray-900 dark:text-white">Start a new game</p>

        <div className="flex gap-2 mt-3">
          <input
            value={partnerNumber}
            onChange={(e) => setPartnerNumber(e.target.value)}
            placeholder="Partner #"
            type="number"
            min={0}
            step={1}
            inputMode="numeric"
            className="w-24 min-h-[44px] rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 text-gray-900 dark:text-white px-3 text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
            aria-label="Partner jersey number"
          />
          <input
            value={partnerName}
            onChange={(e) => setPartnerName(e.target.value)}
            placeholder="Partner name (optional)"
            maxLength={120}
            className="flex-1 min-h-[44px] rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 text-gray-900 dark:text-white px-3 text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
            aria-label="Partner name"
          />
        </div>
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
            disabled={!opponentName.trim() || partnerNumber.trim() === '' || createGame.isPending}
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

      {/* History (shared panel — also used on the spectator /beachchanger) */}
      <VolleyballGameHistory />
    </div>
  );
}