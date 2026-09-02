/**
 * RecentlyHeardCompact - compact Recently Heard summary embedded in the
 * Now Playing view (resolved OQ-3: default 20 rows, NO count control).
 *
 * Read-only (FR-8): no editing, no actions, no mutations.
 *
 * Rating bar scaling: the bar reflects each row's ELO rating on a fixed
 * 1300-1700 range (floor 1300, ceiling 1700). The ELO value is shown as a
 * label at the right end of the bar.
 */

'use client';

import { useViewportStore } from '@/stores/viewportStore';
import { useRecentPlays } from '@/hooks/useMusic';
import { RecentPlayRow } from '@/lib/types/music';
import { CircularProgress } from '@mui/material';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Fixed rating-bar floor (ELO). */
const RATING_FLOOR = 1300;

/** Fixed rating-bar ceiling (ELO). */
const RATING_CEILING = 1700;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Relative % width for the rating bar (0..100) on the fixed 1300-1700 range. */
function ratingPct(rating: number): number {
  const range = RATING_CEILING - RATING_FLOOR;
  if (range <= 0) return 100;
  return Math.max(0, Math.min(100, ((rating - RATING_FLOOR) / range) * 100));
}

/** Short "last heard" stamp, e.g. "Jan 1 1:23 PM". */
function lastHeardLabel(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '\u2014';
  return d.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

// ---------------------------------------------------------------------------
// Rating bar
// ---------------------------------------------------------------------------

/**
 * Rating bar - width = row ELO relative to the fixed 1300-1700 range.
 * The ELO value is shown as a label at the right end of the bar.
 */
function RatingBar({ rating }: { rating: number }) {
  const pct = ratingPct(rating);
  return (
    <div className="flex items-center gap-2">
      <div
        role="progressbar"
        aria-valuenow={Math.round(rating)}
        aria-valuemin={RATING_FLOOR}
        aria-valuemax={RATING_CEILING}
        aria-label={`Rating ${Math.round(rating)}`}
        className="relative h-2 flex-1 rounded-full bg-gray-200 dark:bg-gray-700"
      >
        <div
          className="absolute inset-y-0 left-0 rounded-full bg-blue-500 dark:bg-blue-400"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums w-10 text-right">
        {Math.round(rating)}
      </span>
    </div>
  );
}
// ---------------------------------------------------------------------------
// Shared row renderer (three layouts)
// ---------------------------------------------------------------------------

/**
 * Renders the play rows. Uses the layout variant from the viewport store to
 * choose desktop (table), portrait (stacked cards), or landscape (compact
 * horizontal rows). Read-only.
 */
function RenderRows({ plays }: { plays: RecentPlayRow[] }) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';

  if (isDesktop) {
    return (
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
              <th className="py-2 pr-4 font-medium">Track</th>
              <th className="py-2 pr-4 font-medium w-32">Rating</th>
              <th className="py-2 pr-4 font-medium">Plays</th>
              <th className="py-2 font-medium">Last Heard</th>
            </tr>
          </thead>
          <tbody>
            {plays.map((play) => (
              <tr key={play.isrc} className="border-b border-gray-100 dark:border-gray-800">
                <td className="py-2 pr-4">
                  <div className="font-bold text-gray-900 dark:text-white">
                    {play.trackName}
                  </div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">
                    {play.artistName}
                    {play.artistName && play.playlistName && ' '}
                    {play.playlistName && (<span className="italic">{play.playlistName}</span>)}
                  </div>
                </td>
                <td className="py-2 pr-4"><RatingBar rating={play.rating} /></td>
                <td className="py-2 pr-4">
                  <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
                    {play.playcountLast30}·{play.playcountTotal}
                  </span>
                </td>
                <td className="py-2">
                  <span className="text-xs text-gray-400 dark:text-gray-500">
                    {lastHeardLabel(play.lastPlayedAtUtc)}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  if (isPortrait) {
    return (
      <div className="space-y-3">
        {plays.map((play) => (
          <div key={play.isrc} className="rounded-lg border border-gray-200 dark:border-gray-700 p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="font-bold text-gray-900 dark:text-white truncate">
                  {play.trackName}
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400 truncate">
                  {play.artistName}
                  {play.artistName && play.playlistName && ' '}
                  {play.playlistName && (<span className="italic">{play.playlistName}</span>)}
                </div>
              </div>
              <span className="text-xs text-gray-400 dark:text-gray-500 whitespace-nowrap">
                {lastHeardLabel(play.lastPlayedAtUtc)}
              </span>
            </div>
            <div className="mt-2 flex items-center gap-3">
              <div className="flex-1"><RatingBar rating={play.rating} /></div>
              <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
                {play.playcountLast30}·{play.playcountTotal}
              </span>
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {plays.map((play) => (
        <div key={play.isrc} className="flex items-center gap-3 rounded-lg border border-gray-200 dark:border-gray-700 p-2">
          <div className="flex-1 min-w-0">
            <div className="font-bold text-gray-900 dark:text-white truncate">
              {play.trackName}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400 truncate">
              {play.artistName}
              {play.artistName && play.playlistName && ' '}
              {play.playlistName && (<span className="italic">{play.playlistName}</span>)}
            </div>
          </div>
          <div className="w-24"><RatingBar rating={play.rating} /></div>
          <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
            {play.playcountLast30}·{play.playcountTotal}
          </span>
          <span className="text-xs text-gray-400 dark:text-gray-500">
            {lastHeardLabel(play.lastPlayedAtUtc)}
          </span>
        </div>
      ))}
    </div>
  );
}
// ---------------------------------------------------------------------------
// Shared list body
// ---------------------------------------------------------------------------

/** Shared loading / empty / error body. */
function ListBody({ limit }: { limit: number }) {
  const { data, isLoading, isError } = useRecentPlays(limit);

  if (isLoading || !data) {
    return (
      <div className="flex items-center justify-center py-12">
        <CircularProgress />
      </div>
    );
  }

  if (isError) {
    return (
      <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-4 text-red-800 dark:text-red-200 text-sm">
        Could not load Recently Heard. Please try again.
      </div>
    );
  }

  if (!data.plays || data.plays.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500 dark:text-gray-400">
        Nothing recently heard yet.
      </div>
    );
  }

  return <RenderRows plays={data.plays} />;
}

// ---------------------------------------------------------------------------
// Compact variant (embedded in Now Playing; OQ-3)
// ---------------------------------------------------------------------------

/**
 * Compact Recently Heard for embedding in the Now Playing view: fixes the
 * row count at 20 (default) and renders NO count control (resolved OQ-3).
 * Purely presentational.
 */
export function RecentlyHeardCompact() {
  return <ListBody limit={20} />;
}

