/**
 * RatingsView — Music module: Ratings tab content (008-004)
 *
 * Shows a backlog indicator ("N Track(s) to rate") and the head-to-head matchup
 * for ELO-based rating. When nothing is rateable, shows the empty state.
 *
 * Scoring buttons render beneath each card (Bug 008-004-05):
 *   Primary: 5-4-3-2-1 (positive margins)
 *   Challenger: 1-2-3-4-5 (displayed positive, scored as negative margins)
 * No question text or "wins" labels. Width constrained to button row.
 *
 * Reads:
 *   - GET /api/music/ratings/eligible-count (backlog count, FR-1)
 *   - GET /api/music/ratings/matchup (matchup, FR-3) — via useMatchup
 *   - POST /api/music/ratings/matchup/score (scoring, FR-6) — via useScoreMatchup
 *   - GET /api/music/album-art/{albumId} (album art, FR-5)
 */

'use client';

import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { API } from '@/lib/api-client';
import { MatchupTrack } from '@/lib/types/music';
import { useViewportStore } from '@/stores/viewportStore';
import { useScoreMatchup } from '@/hooks/useMusic';

const POSITIVE_MARGINS = [1, 2, 3, 4, 5];

function createScoreButton(margin: number, onClick: (margin: number) => void, disabled: boolean) {
  const displayValue = Math.abs(margin);
  return (
    <button
      key={margin}
      disabled={disabled}
      onClick={() => onClick(margin)}
      className="inline-flex items-center justify-center rounded-md bg-gray-100 dark:bg-gray-700 w-12 h-12 text-lg font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
    >
      {displayValue}
    </button>
  );
}

function MatchupCard({ track, label, scoreButtons }: { track: MatchupTrack; label: string; scoreButtons?: React.ReactNode }) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const artSize = isDesktop ? 128 : 96;
  return (
    <div className="flex flex-col items-center gap-3 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4 w-[18rem] shrink-0 h-full flex flex-col">
      <span className="inline-block rounded-full bg-blue-50 dark:bg-blue-900/30 px-3 py-0.5 text-xs font-semibold text-blue-700 dark:text-blue-300">
        {label}
  </span>
      {track.albumArtUrl ? (
        <img src={track.albumArtUrl} alt={`Cover for ${track.trackName}`} className="object-cover bg-gray-200 dark:bg-gray-700 rounded-md" style={{ width: artSize, height: artSize }} loading="eager" />
      ) : (
        <div className="bg-gray-200 dark:bg-gray-700 rounded-md flex items-center justify-center text-gray-400 dark:text-gray-500" style={{ width: artSize, height: artSize }}>No art</div>
      )}
      <div className="text-center min-w-0 w-full overflow-hidden flex-1">
        <p className="font-semibold text-gray-900 dark:text-white break-words overflow-wrap-anywhere line-clamp-2">{track.trackName}</p>
        <p className="text-sm text-gray-600 dark:text-gray-300 break-words overflow-wrap-anywhere line-clamp-2">{track.artistName}</p>
        <p className="text-xs text-gray-500 dark:text-gray-400 mt-1 font-mono truncate">{track.isrc}</p>
        <p className="text-sm font-semibold text-gray-900 dark:text-white mt-2">Score: {track.score}</p>
      </div>
      {scoreButtons && <div className="flex justify-center gap-1 mt-auto">{scoreButtons}</div>}
    </div>
  );
}

export function useRatingsEligibleCount() {
  return useQuery({
    queryKey: ['music', 'ratings', 'eligible-count'],
    queryFn: () => API.music.getRatingsEligibleCount(),
    staleTime: 30_000,
  });
}

export function useRatingEligiblePlaylists() {
  return useQuery({
    queryKey: ['music', 'ratings', 'eligible-playlists'],
    queryFn: () => API.music.getRatings(),
    staleTime: 30_000,
  });
}

export function useMatchup(playlistId?: string, enabled: boolean = true) {
  return useQuery({
    queryKey: ['music', 'ratings', 'matchup', playlistId ?? null],
    queryFn: () => API.music.getMatchup(playlistId),
    enabled,
    staleTime: 0,
  });
}

export default function RatingsView() {
  const { data: countData, isLoading: countLoading } = useRatingsEligibleCount();
  const { data: playlistsData, isLoading: playlistsLoading } =
    useRatingEligiblePlaylists();
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';

  const [selectedPlaylistId, setSelectedPlaylistId] = useState<string | undefined>(undefined);
  const [matchupRequested, setMatchupRequested] = useState(false);
  const count = countData?.count ?? 0;

  // Auto-select first playlist on initial load
  useEffect(() => {
    if (playlistsData?.data && !selectedPlaylistId) {
      const keys = Object.keys(playlistsData.data);
      if (keys.length > 0) {
        setSelectedPlaylistId(playlistsData.data[keys[0]]);
        setMatchupRequested(true);
      }
    }
  }, [playlistsData, selectedPlaylistId]);

  // Reset selection to "All" if current playlist no longer has rateable tracks
  useEffect(() => {
    if (playlistsData?.data && selectedPlaylistId) {
      const ids = Object.values(playlistsData.data);
      if (!ids.includes(selectedPlaylistId)) {
        setSelectedPlaylistId(undefined);
        setMatchupRequested(true);
      }
    }
  }, [playlistsData, selectedPlaylistId]);

  const { data: matchup, isLoading: matchupLoading } = useMatchup(
    matchupRequested ? selectedPlaylistId : undefined,
    matchupRequested
  );
  const scoreMatchupMutation = useScoreMatchup();
  const scoring = !!matchup?.primary && !!matchup?.challenger;

  const handleScore = (margin: number) => {
    if (!matchup?.primary || !matchup?.challenger) return;
    scoreMatchupMutation.mutate({
      playlist_id: matchup.primary.playlistId,
      isrc: matchup.primary.isrc,
      isrc_vs: matchup.challenger.isrc,
      margin,
    });
  };

  const playlistOptions: { label: string; value: string | undefined }[] = [
    { label: 'All', value: undefined },
  ];
  if (playlistsData?.data) {
    Object.entries(playlistsData.data).forEach(([name, id]) => {
      playlistOptions.push({ label: name, value: id });
    });
  }

  const handlePlaylistChange = (value: string | undefined) => {
    setSelectedPlaylistId(value);
    setMatchupRequested(true);
  };

  if (countLoading || playlistsLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-gray-500 dark:text-gray-400">Loading…</p>
      </div>
    );
  }
  if (count === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-gray-500 dark:text-gray-400 text-lg">
          No songs to rate at this time
        </p>
      </div>
    );
  }

  const backlogLabel = count === 1 ? '1 Track to rate' : `${count} Tracks to rate`;

  return (
    <div className="space-y-6">
      <div className="rounded-md bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 px-4 py-3">
        <p className="text-sm font-medium text-blue-800 dark:text-blue-200">
          {backlogLabel}
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
          Playlist:
        </label>
        <select
          value={selectedPlaylistId ?? ''}
          onChange={(e) => handlePlaylistChange(e.target.value || undefined)}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm text-gray-900 dark:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
        >
          {playlistOptions.map((opt) => (
            <option key={opt.label} value={opt.value ?? ''}>
              {opt.label}
            </option>
          ))}
        </select>
      </div>

      {matchupLoading && (
        <div className="p-8 text-center">
          <p className="text-gray-500 dark:text-gray-400">Loading matchup…</p>
        </div>
      )}

      {matchup && !matchupLoading && (
        <div className="space-y-4">
          {/* No more matchups for this playlist */}
          {!matchup.primary && (
            <div className="rounded-md bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 px-4 py-3">
              <p className="text-sm text-blue-800 dark:text-blue-200">
                No more songs to rate in this playlist.
              </p>
            </div>
          )}

          {!matchup.challenger && matchup.primary && (
            <div className="rounded-md bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 px-4 py-3">
              <p className="text-sm text-yellow-800 dark:text-yellow-200">
                This playlist has only one rateable track — no challenger available.
              </p>
            </div>
          )}

          {matchup.primary && (
            <div className={isPortrait ? 'space-y-4' : 'grid grid-cols-[18rem_18rem] justify-center gap-4'}>
              <MatchupCard
                track={matchup.primary}
                label="Primary"
                scoreButtons={
                  scoring
                    ? POSITIVE_MARGINS.slice().reverse().map((m) =>
                        createScoreButton(m, handleScore, scoreMatchupMutation.isPending)
                      )
                    : undefined
                }
              />
              {matchup.challenger ? (
                <MatchupCard
                  track={matchup.challenger}
                  label="Challenger"
                  scoreButtons={
                    scoring
                      ? POSITIVE_MARGINS.map((m) =>
                          createScoreButton(-m, handleScore, scoreMatchupMutation.isPending)
                        )
                      : undefined
                  }
                />
              ) : (
                <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-gray-300 dark:border-gray-600 p-4 text-center text-gray-500 dark:text-gray-400">
                  <p className="text-sm font-medium">No challenger</p>
                </div>
              )}
            </div>
          )}

          {scoreMatchupMutation.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to submit rating. Please try again.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
