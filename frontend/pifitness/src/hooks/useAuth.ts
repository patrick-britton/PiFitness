/**
 * Auth React Query Hooks
 * Provides React Query hooks for auth API endpoints.
 * No auto-refetch - user must explicitly trigger status checks.
 */

import { useQuery, useMutation } from '@tanstack/react-query';
import { API } from '../lib/api-client';

// ---------------------------------------------------------------------------
// Query Keys
// ---------------------------------------------------------------------------

export const authKeys = {
  all: ['auth'] as const,
  status: () => [...authKeys.all, 'status'] as const,
};

// ---------------------------------------------------------------------------
// Auth Status Hook - On-Demand Only (No Auto-Refetch)
// ---------------------------------------------------------------------------

/**
 * Hook to fetch auth status.
 * NO auto-refetch interval — user must explicitly refresh.
 * The UI will only call this when the user is actively viewing the Services tab.
 */
export function useAuthStatus(enabled: boolean = false) {
  return useQuery({
    queryKey: authKeys.status(),
    queryFn: () => API.auth.getStatus(),
    enabled,
    staleTime: 0, // Always consider stale, but don't auto-fetch
  });
}

// ---------------------------------------------------------------------------
// Auth Test Mutatons - Triggered by User Click Only
// ---------------------------------------------------------------------------

/**
 * Mutation to test Spotify authentication.
 * Called when user clicks the Spotify status indicator.
 */
export function useTestSpotifyAuth() {
  return useMutation({
    mutationFn: () => API.auth.testSpotify(),
  });
}

/**
 * Mutation to test Garmin authentication.
 * Called when user clicks the Garmin status indicator.
 */
export function useTestGarminAuth() {
  return useMutation({
    mutationFn: () => API.auth.testGarmin(),
  });
}

/**
 * Mutation to refresh Spotify token.
 * Called when user needs to force a token refresh.
 */
export function useRefreshSpotifyToken() {
  return useMutation({
    mutationFn: () => API.auth.refreshSpotify(),
  });
}

// ---------------------------------------------------------------------------
// Auth URL Hooks
// ---------------------------------------------------------------------------

/**
 * Hook to fetch Spotify auth URL (enabled false - only on demand).
 */
export function useSpotifyAuthUrl(enabled: boolean = false) {
  return useQuery({
    queryKey: [...authKeys.all, 'spotify', 'auth-url'] as const,
    queryFn: () => API.auth.getSpotifyAuthUrl(),
    enabled,
  });
}

/**
 * Mutation to handle Spotify OAuth callback.
 */
export function useSpotifyCallback() {
  return useMutation({
    mutationFn: (redirectUrl: string) => API.auth.spotifyCallback(redirectUrl),
  });
}

// ---------------------------------------------------------------------------
// Health Check Hook
// ---------------------------------------------------------------------------

/**
 * Hook to fetch proactive auth health status.
 */
export function useAuthHealth(enabled: boolean = false) {
  return useQuery({
    queryKey: [...authKeys.all, 'health'] as const,
    queryFn: () => API.auth.getHealth(),
    enabled,
  });
}