/**
 * Music Module React Query Hooks
 * Provides React Query hooks for the Music module endpoints (feature 008-001):
 *  - Now Playing query + action mutations that invalidate the now-playing query
 *  - Recent plays query (read-only)
 *  - Service-status query (rate-limit signal)
 *  - Add-targets query
 *
 * Query keys follow the established `[module]` prefix pattern (see useExercises).
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { API } from "@/lib/api-client";
import {
  NowPlayingResponse,
  MusicActionResponse,
  RecentPlaysResponse,
  ServiceStatus,
  MusicAddTargetsResponse,
  MusicAddToPlaylistRequest,
} from "@/lib/types/music";

// ---------------------------------------------------------------------------
// Query Key Factories
// ---------------------------------------------------------------------------

export const musicKeys = {
  all: ["music"] as const,
  nowPlaying: () => [...musicKeys.all, "now-playing"] as const,
  recentPlays: () => [...musicKeys.all, "recent-plays"] as const,
  serviceStatus: () => [...musicKeys.all, "service-status"] as const,
  addTargets: () => [...musicKeys.all, "add-targets"] as const,
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/**
 * Current Spotify playback state.
 * Refetches on window focus and on action mutation invalidation so the
 * view reflects the latest track after every user action (FR-6).
 */
export function useNowPlaying() {
  return useQuery({
    queryKey: musicKeys.nowPlaying(),
    queryFn: () => API.music.getNowPlaying(),
    staleTime: 0, // always re-fetch on focus/invalidation
    refetchOnWindowFocus: true,
  });
}

/**
 * Recent play history (read-only). Default 20 rows, 10–100 step 10.
 */
export function useRecentPlays(limit?: number) {
  return useQuery({
    queryKey: [...musicKeys.recentPlays(), limit],
    queryFn: () => API.music.getRecentPlays(limit),
    staleTime: 30_000,
  });
}

/**
 * Spotify service status — mirrors the DB rate-limit signal (FR-9).
 * Polls every 30 s while active so the rate-limit banner clears promptly.
 */
export function useServiceStatus() {
  return useQuery({
    queryKey: musicKeys.serviceStatus(),
    queryFn: () => API.music.getServiceStatus(),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });
}

/**
 * Eligible playlists for adding the current track to (AC-6).
 */
export function useAddTargets() {
  return useQuery({
    queryKey: musicKeys.addTargets(),
    queryFn: () => API.music.getAddTargets(),
    staleTime: 30_000,
  });
}

// ---------------------------------------------------------------------------
// Mutations — each invalidates the now-playing query so the view re-fetches
// ---------------------------------------------------------------------------

/**
 * Invalidate now-playing so the view re-fetches after an action (FR-6).
 */
function invalidateNowPlaying(queryClient: ReturnType<typeof useQueryClient>) {
  queryClient.invalidateQueries({ queryKey: musicKeys.nowPlaying() });
}

export function useSkipTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.skipTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function usePromoteTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.promoteTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function useSoftRejectTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.softRejectTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function useHardRejectTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.hardRejectTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function useRemoveTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.removeTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function useRankUpTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.rankUpTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function useRankDownTrack() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => API.music.rankDownTrack(),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}

export function useAddToPlaylist() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (playlistId: string) => API.music.addToPlaylist(playlistId),
    onSuccess: () => invalidateNowPlaying(queryClient),
  });
}
