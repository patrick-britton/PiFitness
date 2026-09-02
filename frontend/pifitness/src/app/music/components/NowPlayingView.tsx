/**
 * NowPlayingView — Music module: Now Playing tab content
 *
 * Renders the current track card with context-aware controls, rate-limit
 * banner, loading/empty/error states, and an embedded compact Recently Heard
 * summary (OQ-3) for the idle / no-songs state.
 *
 * Control matrix follows Design Notes — Control × context (FR-6):
 *   - recommendation  → Promote / Soft Reject / Hard Reject / Skip
 *   - regular playlist → Remove / Rank Up / Rank Down / Skip
 *   - not from playlist → Add-to-Playlist picker + Skip
 *
 * Uses MUI Snackbar+Alert for confirmation toasts (MUI already a dependency).
 * Album art is served from the app's /api/music/album-art/{albumId} endpoint (OQ-1).
 */

'use client';

import { useState } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import {
  useNowPlaying,
  useSkipTrack,
  usePromoteTrack,
  useSoftRejectTrack,
  useHardRejectTrack,
  useRemoveTrack,
  useRankUpTrack,
  useRankDownTrack,
  useAddToPlaylist,
  useAddTargets,
} from '@/hooks/useMusic';
import { NowPlayingTrack } from '@/lib/types/music';
import { Snackbar, Alert, CircularProgress } from '@mui/material';
import * as Icons from '@mui/icons-material';
import { RecentlyHeardCompact } from './RecentlyHeardCompact';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Context mode derived from the now-playing track's playback context. */
type ContextMode = 'recommendation' | 'regular' | 'no-playlist';

/**
 * Resolve which control set to render based on the track context.
 * Mirrors the Control × context matrix in Design Notes (FR-6).
 */
function getContextMode(track: NowPlayingTrack): ContextMode {
  const rel = track.context.relationshipType;
  if (rel === 'recommendation') return 'recommendation';
  if (rel === 'regular') return 'regular';
  return 'no-playlist';
}

/** Format elapsed seconds -> mm:ss. */
function formatDuration(totalSeconds: number): string {
  if (!totalSeconds || totalSeconds < 0) return '0:00';
  const m = Math.floor(totalSeconds / 60);
  const s = Math.floor(totalSeconds % 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

/** Friendly label for the playback context line (FR-2). */
function contextLabel(track: NowPlayingTrack): string {
  const ctx = track.context;
  if (!ctx.isPlaylist || !ctx.playlistId) {
    return 'not from a playlist';
  }
  if (ctx.relationshipType === 'recommendation' && ctx.parentPlaylistId && ctx.parentPlaylistName) {
    return `from recommendations — ${ctx.parentPlaylistName}`;
  }
  return `from playlist ${ctx.playlistName ?? ctx.playlistId}`;
}

/**
 * Rating badge: regular -> ratings value, recommendation -> predicted,
 * baseline -> 1500 (shown as plain value, not bar, on Now Playing).
 */
function ratingBadge(track: NowPlayingTrack) {
  const { value, source } = track.rating;
  const label: Record<string, string> = {
    ratings: 'Rated',
    predicted: 'Predicted',
    baseline: 'Baseline',
  };
  return (
    <span className="inline-flex items-baseline gap-1 text-sm">
      <span className="font-semibold text-gray-900 dark:text-white">{value}</span>
      <span className="text-xs text-gray-500 dark:text-gray-400">
        ({label[source] ?? source})
      </span>
    </span>
  );
}


// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

/** Track card: album art, labels, context line, rating. */
function TrackCard({ track }: { track: NowPlayingTrack }) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  return (
    <div className="flex items-center gap-4 px-2">
      {/* Album art via /api/music/album-art/{albumId} (OQ-1) */}
      <img
        src={track.albumArtUrl}
        alt={`Cover for ${track.albumName}`}
        className="object-cover bg-gray-200 dark:bg-gray-700 rounded-md"
        style={{ width: isDesktop ? 96 : 64, height: isDesktop ? 96 : 64 }}
        loading="eager"
      />

      <div className="min-w-0 flex-1">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white truncate">
          {track.trackName}
        </h2>
        <p className="text-sm text-gray-600 dark:text-gray-300 truncate">
          {track.artistName}
        </p>
        <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
          {track.albumName}
        </p>
        {/* Context line */}
        <p className="text-xs text-gray-500 dark:text-gray-400 mt-1 truncate">
          {contextLabel(track)}
        </p>
        {/* Rating badge */}
        <div className="mt-1">{ratingBadge(track)}</div>
      </div>

      {/* Track duration placeholder (FR-4 notes optional) */}
      {track.completion && (
        <span className="text-xs text-gray-500 dark:text-gray-400 tabular-nums">
          {formatDuration(track.completion.doneInS)}
        </span>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Control buttons
// ---------------------------------------------------------------------------

/** Control buttons that vary by context mode.
 * - recommendation: Promote / Soft Reject / Hard Reject
 * - regular: Rank Up / Rank Down (Remove is a separate action above)
 * All modes get Skip.
 */
function ContextControls({
  track,
  onAction,
}: {
  track: NowPlayingTrack;
  onAction: (label: string) => void;
}) {
  const mode = getContextMode(track);

  const handleRemoveClick = () => onAction('remove');
  const handleRank = (label: string) => () => onAction(label);
  const handlePromote = () => onAction('promote');
  const handleSoftReject = () => onAction('soft-reject');
  const handleHardReject = () => onAction('hard-reject');

  return (
    <div className="flex flex-col sm:flex-row flex-wrap items-center gap-2 mt-4">
      {/* Skip — always available */}
      <button
        onClick={() => onAction('skip')}
        className="inline-flex items-center justify-center rounded-md bg-gray-100 dark:bg-gray-800 px-4 py-2 text-sm font-medium text-gray-900 dark:text-white hover:bg-gray-200 dark:hover:bg-gray-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
      >
        <Icons.SkipNext className="mr-1" />
        Skip
      </button>

      {/* Recommendation-mode controls (FR-6 matrix) */}
      {mode === 'recommendation' && (
        <>
          <button
            onClick={handlePromote}
            className="inline-flex items-center justify-center rounded-md bg-green-100 dark:bg-green-900/30 px-4 py-2 text-sm font-medium text-green-800 dark:text-green-300 hover:bg-green-200 dark:hover:bg-green-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-green-500"
          >
            <Icons.ThumbUp className="mr-1" />
            Promote
          </button>
          <button
            onClick={handleSoftReject}
            className="inline-flex items-center justify-center rounded-md bg-yellow-100 dark:bg-yellow-900/30 px-4 py-2 text-sm font-medium text-yellow-800 dark:text-yellow-300 hover:bg-yellow-200 dark:hover:bg-yellow-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-yellow-500"
          >
            <Icons.ThumbDown className="mr-1" />
            Soft Reject
          </button>
          <button
            onClick={handleHardReject}
            className="inline-flex items-center justify-center rounded-md bg-orange-100 dark:bg-orange-900/30 px-4 py-2 text-sm font-medium text-orange-800 dark:text-orange-300 hover:bg-orange-200 dark:hover:bg-orange-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500"
          >
            <Icons.Delete className="mr-1" />
            Hard Reject
          </button>
        </>
      )}

      {/* Regular-playlist controls (FR-6 matrix) */}
      {mode === 'regular' && (
        <>
          <button
            onClick={handleRemoveClick}
            className="inline-flex items-center justify-center rounded-md bg-red-100 dark:bg-red-900/30 px-4 py-2 text-sm font-medium text-red-800 dark:text-red-300 hover:bg-red-200 dark:hover:bg-red-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-500"
          >
            <Icons.RemoveCircle className="mr-1" />
            Remove
          </button>
          <button
            onClick={handleRank('rank-up')}
            className="inline-flex items-center justify-center rounded-md bg-blue-100 dark:bg-blue-900/30 px-4 py-2 text-sm font-medium text-blue-800 dark:text-blue-300 hover:bg-blue-200 dark:hover:bg-blue-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
          >
            <Icons.ArrowUpward className="mr-1" />
            Rank Up
          </button>
          <button
            onClick={handleRank('rank-down')}
            className="inline-flex items-center justify-center rounded-md bg-indigo-100 dark:bg-indigo-900/30 px-4 py-2 text-sm font-medium text-indigo-800 dark:text-indigo-300 hover:bg-indigo-200 dark:hover:bg-indigo-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500"
          >
            <Icons.ArrowDownward className="mr-1" />
            Rank Down
          </button>
        </>
            )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Add-to-Playlist picker (not-from-playlist mode)
// ---------------------------------------------------------------------------


/** Add-to-Playlist picker with confirm + no-eligibles error (OQ-3 / FR-6). */
function AddToPlaylistControl({ onAction }: { onAction: (label: string) => void }) {
  const { data: targets, isLoading: targetsLoading, isError: targetsError } = useAddTargets();
  const [selectedId, setSelectedId] = useState<string>('');
  const [showPicker, setShowPicker] = useState(false);

  const handleSubmit = () => {
    if (!selectedId) return;
    setShowPicker(false);
    onAction(`add-to-playlist:${selectedId}`);
  };

  // No-eligibles error (OQ-3: show error state instead of empty picker)
  const noEligibles = !targetsLoading && targets && !targets.eligible && (!targets.playlists || targets.playlists.length === 0);

  return (
    <div className="mt-4">
      {!showPicker ? (
        <button
          onClick={() => setShowPicker(true)}
          className="inline-flex items-center justify-center rounded-md bg-purple-100 dark:bg-purple-900/30 px-4 py-2 text-sm font-medium text-purple-800 dark:text-purple-300 hover:bg-purple-200 dark:hover:bg-purple-900/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-purple-500"
        >
          <Icons.PlaylistAdd className="mr-1" />
          Add to Playlist
        </button>
      ) : (
        <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2 mt-2">
          {targetsLoading && <CircularProgress size={20} className="self-center" />}
          {targetsError && (
            <p className="text-xs text-red-600 dark:text-red-400">
              Could not load playlist targets. Please try again.
            </p>
          )}
          {noEligibles && (
            <p className="text-xs text-red-600 dark:text-red-400">
              No eligible playlists found to add to.
            </p>
          )}
          {targets && targets.eligible && targets.playlists.length > 0 && (
            <>
              <select
                value={selectedId}
                onChange={(e) => setSelectedId(e.target.value)}
                className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-2 py-1 text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
              >
                <option value="">Select a playlist…</option>
                {targets.playlists.map((p) => (
                  <option key={p.playlist_id} value={p.playlist_id}>
                    {p.playlist_name}
                  </option>
                ))}
              </select>
              <div className="flex gap-2">
                <button
                  onClick={handleSubmit}
                  disabled={!selectedId}
                  className="inline-flex items-center justify-center rounded-md bg-purple-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-purple-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-purple-500 disabled:opacity-50"
                >
                  Confirm
                </button>
                <button
                  onClick={() => setShowPicker(false)}
                  className="inline-flex items-center justify-center rounded-md bg-gray-100 dark:bg-gray-800 px-3 py-1.5 text-sm font-medium text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
                >
                  Cancel
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function NowPlayingView() {
  const { data: npData, isLoading, isError, error } = useNowPlaying();

  const skipTrack = useSkipTrack();
  const promoteTrack = usePromoteTrack();
  const softRejectTrack = useSoftRejectTrack();
  const hardRejectTrack = useHardRejectTrack();
  const removeTrack = useRemoveTrack();
  const rankUpTrack = useRankUpTrack();
  const rankDownTrack = useRankDownTrack();
  const addToPlaylist = useAddToPlaylist();

  // Toast state
  const [toastOpen, setToastOpen] = useState(false);
  const [toastMessage, setToastMessage] = useState('');
  const [toastSeverity, setToastSeverity] = useState<'success' | 'error'>('success');

  const showToast = (message: string, severity: 'success' | 'error' = 'success') => {
    setToastMessage(message);
    setToastSeverity(severity);
    setToastOpen(true);
  };

    const handleCloseToast = () => setToastOpen(false);

  /**
   * Dispatch an action mutation and show a confirmation toast.
   * Each mutation invalidates the now-playing query (via the hook), so the
   * view auto-refreshes after completion (FR-1/FR-6).
   */
  const handleAction = async (label: string) => {
    const okMsg: Record<string, string> = {
      'skip': 'Skipped track.',
      'promote': 'Track promoted to parent playlist.',
      'soft-reject': 'Track soft-rejected.',
      'hard-reject': 'Track hard-rejected and skipped.',
      'remove': 'Track removed from playlist.',
      'rank-up': 'Track ranked up.',
      'rank-down': 'Track ranked down.',
    };

    try {
      let result: { ok?: boolean; message?: string } | void;

      switch (label) {
        case 'skip':
          result = await skipTrack.mutateAsync();
          showToast(okMsg['skip']);
          break;
        case 'promote':
          result = await promoteTrack.mutateAsync();
          result?.ok ? showToast(okMsg['promote']) : showToast(result?.message ?? 'Promote failed.', 'error');
          break;
        case 'soft-reject':
          result = await softRejectTrack.mutateAsync();
          result?.ok ? showToast(okMsg['soft-reject']) : showToast(result?.message ?? 'Soft reject failed.', 'error');
          break;
        case 'hard-reject':
          result = await hardRejectTrack.mutateAsync();
          result?.ok ? showToast(okMsg['hard-reject']) : showToast(result?.message ?? 'Hard reject failed.', 'error');
          break;
        case 'remove':
          result = await removeTrack.mutateAsync();
          if (result?.ok) showToast(okMsg['remove']);
          else showToast(result?.message ?? 'Remove failed.', 'error');
          break;
        case 'rank-up':
          result = await rankUpTrack.mutateAsync();
          result?.ok ? showToast(okMsg['rank-up']) : showToast(result?.message ?? 'Rank up failed.', 'error');
          break;
        case 'rank-down':
          result = await rankDownTrack.mutateAsync();
          result?.ok ? showToast(okMsg['rank-down']) : showToast(result?.message ?? 'Rank down failed.', 'error');
          break;
        default:
          if (label.startsWith('add-to-playlist:')) {
            const playlistId = label.split(':')[1];
            result = await addToPlaylist.mutateAsync(playlistId);
            result?.ok ? showToast('Track added to playlist.') : showToast(result?.message ?? 'Add to playlist failed.', 'error');
          }
      }
        } catch (e: any) {
      showToast(e?.message ?? 'Action failed. Please try again.', 'error');
    }
  };

  // ---------------------------------------------------------------------------
  // Rate-limit banner suppresses content (FR-9)
  // ---------------------------------------------------------------------------

  if (npData?.rateLimited) {
    return (
      <div className="p-4 sm:p-6">
        <div className="max-w-3xl mx-auto">
          <div className="rounded-md bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 p-4">
            <div className="flex items-center gap-3">
              <Icons.Warning className="text-yellow-600 dark:text-yellow-400 flex-shrink-0" />
              <div>
                <p className="font-medium text-yellow-800 dark:text-yellow-200">
                  Spotify rate limited
                </p>
                <p className="text-sm text-yellow-700 dark:text-yellow-300 mt-0.5">
                  Now Playing is temporarily unavailable. New observations will
                  resume automatically when the rate limit clears.
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Loading
  if (isLoading || !npData) {
    return (
      <div className="p-4 sm:p-6">
        <div className="max-w-3xl mx-auto flex items-center justify-center py-12">
          <CircularProgress />
        </div>
      </div>
    );
  }

  // Error
  if (isError) {
    return (
      <div className="p-4 sm:p-6">
        <div className="max-w-3xl mx-auto">
          <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-4">
            <p className="text-red-800 dark:text-red-200">
              Could not load Now Playing: {(error as Error)?.message ?? 'Unknown error'}
            </p>
          </div>
        </div>
      </div>
    );
  }

  // ---------------------------------------------------------------------------
  // Main content — Recently Heard always renders below (FR-7 / OQ-3)
  // ---------------------------------------------------------------------------

  const track = npData.track;
  const playing = npData.playing;
  const mode = track ? getContextMode(track) : null;
  const showAddToPlaylist = mode === 'no-playlist';

  return (
    <div className="p-4 sm:p-6">
      <div className="max-w-3xl mx-auto">
        {!playing || !track ? (
          <div className="text-center py-12">
            <Icons.MusicNote className="w-12 h-12 text-gray-400 dark:text-gray-500 mx-auto mb-3" />
            <h3 className="text-lg font-medium text-gray-700 dark:text-gray-300">
              No songs currently playing
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Start a track on Spotify to see it here.
            </p>
          </div>
        ) : (
          <>
            {/* Track card */}
            <div className="pb-4 border-b border-gray-200 dark:border-gray-700">
              <TrackCard track={track} />
            </div>

            {/* Controls */}
            <div className="pt-4">
              <ContextControls track={track} onAction={handleAction} />
              {showAddToPlaylist && <AddToPlaylistControl onAction={handleAction} />}
            </div>
          </>
        )}

        {/* Recently Heard — always below Now Playing (FR-7 / OQ-3) */}
        <div className="mt-8 text-left">
          <h4 className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">
            Recently Heard
          </h4>
          <RecentlyHeardCompact />
        </div>
      </div>

      {/* Confirmation toast */}
      <Snackbar
        open={toastOpen}
        autoHideDuration={4000}
        onClose={handleCloseToast}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={handleCloseToast} severity={toastSeverity} variant="filled">
          {toastMessage}
        </Alert>
      </Snackbar>
    </div>
  );
}

