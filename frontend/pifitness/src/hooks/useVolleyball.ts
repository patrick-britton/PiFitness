/**
 * Volleyball Scorekeeping React Query Hooks
 * Provides React Query hooks for volleyball endpoints with automatic caching,
 * 10 s live polling on the active scoreboard (OQ-2), and mutation-driven
 * invalidation (matching the tri-tip hook patterns).
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { API } from "../lib/api-client";
import {
  VolleyballCreateGameRequest,
  VolleyballScoringTeam,
} from "../lib/types/volleyball";

// ---------------------------------------------------------------------------
// Query Key Factories
// ---------------------------------------------------------------------------

export const volleyballKeys = {
  all: ["volleyball"] as const,
  history: () => [...volleyballKeys.all, "history"] as const,
  active: () => [...volleyballKeys.all, "active"] as const,
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/** Completed games with final scores, sorted by completion time descending. */
export function useVolleyballHistory() {
  return useQuery({
    queryKey: volleyballKeys.history(),
    queryFn: () => API.volleyball.getHistory(),
    staleTime: 30_000,
  });
}

/**
 * The current active game with its points and derived score.
 * Polls every 10 s while a game is active (OQ-2) so other viewers see
 * live score updates without any websocket infrastructure.
 * refetchOnMount: 'always' ensures navigating to the Beach page always
 * re-syncs with the backend (never renders a stale cached `{game:null}`
 * when a game is actually active).
 */
export function useVolleyballActive() {
  return useQuery({
    queryKey: volleyballKeys.active(),
    queryFn: () => API.volleyball.getActive(),
    refetchOnMount: 'always',
    refetchInterval: (query) => (query.state.data?.game ? 10_000 : false),
    staleTime: 5_000,
  });
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/** Create a new game (opponent name only). Throws on 409 (one active). */
export function useCreateVolleyballGame() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (req: VolleyballCreateGameRequest) => API.volleyball.createGame(req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: volleyballKeys.all });
    },
    onError: () => {
      // 409 guard: a game is already active — immediately re-sync the active
      // query so the scoreboard replaces the start form automatically.
      queryClient.invalidateQueries({ queryKey: volleyballKeys.active() });
    },
  });
}

/** Add a point for a team on the active game. */
export function useAddVolleyballPoint() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, scoringTeam }: { id: number; scoringTeam: VolleyballScoringTeam }) =>
      API.volleyball.addPoint(id, { scoring_team: scoringTeam }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: volleyballKeys.all });
    },
  });
}

/** Undo the most recent point of ONE team (per-team undo). */
export function useRemoveVolleyballPoint() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, scoringTeam }: { id: number; scoringTeam: VolleyballScoringTeam }) =>
      API.volleyball.removeLastPoint(id, scoringTeam),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: volleyballKeys.all });
    },
  });
}

/** End the game (completed_at = MAX(recorded_at)). */
export function useEndVolleyballGame() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => API.volleyball.endGame(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: volleyballKeys.all });
    },
  });
}

/** Abandon the active game (deletes game + points via FK cascade). */
export function useAbandonVolleyballGame() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => API.volleyball.abandonGame(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: volleyballKeys.all });
    },
  });
}
