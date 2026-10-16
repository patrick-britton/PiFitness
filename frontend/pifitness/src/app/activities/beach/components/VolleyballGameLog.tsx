/**
 * VolleyballGameLog
 * Shared headerless game log merging points + non-scoring events,
 * rendered newest first beneath the viewer chart (006-003, AC-4).
 *
 * Rows:
 *   - "In Warmups" at game.created_at while first_entry_at is null
 *   - "Point Scripps" / "Point {opponent}" in bold team color
 *   - Tagged point as ONE row: "Point {team} - {play description}" (OQ-1/OQ-3)
 *   - "Time Out"
 *   - Italic "Scorekeeper: {message}"
 * Time of day via toLocaleTimeString h:mm AM/PM.
 */

'use client';

import { VolleyballGameDetail } from '../../../../lib/types/volleyball';

/** Play phrasings per OQ-1: fixed 'Andy' string constant for every tag. */
const PLAY_PHRASES: Record<string, string> = {
  Ace: 'Service Ace by Andy',
  Block: 'Andy with a block at the net',
  Spike: 'Andy spikes',
  Dive: 'Diving save by Andy',
};

function fmtTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
}

/** Team text color, theme-conditioned (matches VolleyballScoreChart colors). */
function teamColor(scoringTeam: 'SR' | 'OPPONENT', isDark: boolean): string {
  if (scoringTeam === 'SR') return isDark ? '#750530' : '#050C46';
  return isDark ? '#374151' : '#9ca3af';
}

interface VolleyballGameLogProps {
  /** Active game detail (points + events in recorded order, derived score). */
  detail: VolleyballGameDetail | null;
}

export default function VolleyballGameLog({ detail }: VolleyballGameLogProps) {
  const game = detail?.game;
  const points = detail?.points ?? [];
  const events = detail?.events ?? [];

  const isDark =
    typeof window !== 'undefined' &&
    window.document.documentElement.classList.contains('dark');

  const opponentName = game?.team_b_name ?? 'Opponent';

  // ------------------------------------------------------------------ build rows
  const rows: {
    id: string;
    time: string;
    text: string;
    style: React.CSSProperties;
  }[] = [];

  if (game?.first_entry_at === null) {
    // Game created, no entry recorded yet — "In Warmups" takes precedence.
    rows.push({
      id: 'warmups',
      time: game.created_at,
      text: 'In Warmups',
      style: { fontStyle: 'italic' },
    });
  } else {
    points.forEach((p) => {
      const base = p.scoring_team === 'SR' ? 'Point Scripps' : `Point ${opponentName}`;
      const text = p.event_type
        ? `${base} - ${PLAY_PHRASES[p.event_type]}`
        : base;
      rows.push({
        id: `point-${p.point_id}`,
        time: p.recorded_at,
        text,
        style: {
          color: teamColor(p.scoring_team, isDark),
          fontWeight: '600',
        },
      });
    });

    events.forEach((e) => {
      if (e.kind === 'timeout') {
        rows.push({
          id: `event-${e.event_id}`,
          time: e.recorded_at,
          text: 'Time Out',
          style: {},
        });
      } else if (e.kind === 'message' && e.message) {
        rows.push({
          id: `event-${e.event_id}`,
          time: e.recorded_at,
          text: `Scorekeeper: ${e.message}`,
          style: { fontStyle: 'italic' },
        });
      }
    });
  }

  // Newest first by timestamp
  rows.sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime());

  if (!detail || rows.length === 0) {
    return (
      <div className="mt-2 space-y-2">
        <p className="text-sm text-gray-500 dark:text-gray-400">
          No game entries yet.
        </p>
      </div>
    );
  }

  return (
    <div className="mt-2 space-y-1">
      {rows.map((row) => (
        <p
          key={row.id}
          className="text-sm text-gray-900 dark:text-white"
          style={row.style}
        >
          {fmtTime(row.time)} {row.text}
        </p>
      ))}
    </div>
  );
}
