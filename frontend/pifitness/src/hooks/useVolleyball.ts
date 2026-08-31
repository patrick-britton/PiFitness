/**
 * Volleyball Scorekeeping React Query Hooks
 * Provides React Query hooks for volleyball endpoints with automatic caching,
 * a 10 s live poll on the active scoreboard for the scorekeeper (OQ-2),
 * a 1 s live poll on the read-only /beachchanger viewer query (006-002,
 * separate key so the scorekeeper cadence stays untouched), and mutation-driven
 * invalidation (matching the tri-tip hook patterns).
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { API } from "../lib/api-client";
import {
  VolleyballCreateGameRequest,
  VolleyballEventType,
  VolleyballScoringTeam,
} from "../lib/types/volleyball";

// ---------------------------------------------------------------------------
// Query Key Factories
// ---------------------------------------------------------------------------

export const volleyballKeys = {
  all: ["volleyball"] as const,
  history: () => [...volleyballKeys.all, "history"] as const,
  active: () => [...volleyballKeys.all, "active"] as const,
  viewerActive: () => [...volleyballKeys.all, "viewer-active"] as const,
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

/**
 * Read-only active-game query for the unlisted /beachchanger viewer (006-002).
 * Same GET endpoint as the scorekeeper but its own query key, so the
 * scorekeeper's 10 s cadence (OQ-2) stays untouched. Polls every 1 s
 * UNCONDITIONALLY (Bug T08-1) so a standing viewer display picks up a newly
 * started game without a page refresh; background refetches never clear
 * cached data, so polling is invisible (no flicker after the first load).
 * This hook ships no mutation and must stay that way — the viewer route is
 * read-only by design (AC-2).
 */
export function useVolleyballViewerActive() {
  return useQuery({
    queryKey: volleyballKeys.viewerActive(),
    queryFn: () => API.volleyball.getActive(),
    refetchOnMount: 'always',
    refetchInterval: 1_000,
    staleTime: 0,
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

/**
 * Add a point for a team on the active game, optionally carrying a
 * notable-play event written onto the point at creation (006-002, Bug
 * T08-3: the UI holds a selected event and writes it with the next point).
 */
export function useAddVolleyballPoint() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      scoringTeam,
      eventType,
    }: {
      id: number;
      scoringTeam: VolleyballScoringTeam;
      eventType?: VolleyballEventType | null;
    }) =>
      API.volleyball.addPoint(id, {
        scoring_team: scoringTeam,
        event_type: eventType ?? null,
      }),
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

/**
 * Tag the most recently recorded point (either team) with a notable-play
 * event (006-002). Annotates an existing row; never creates a point.
 * Invalidating the whole `volleyball` family also refreshes the viewer's
 * `viewer-active` query, so a tagged point appears on /beachchanger immediately.
 */
export function useTagVolleyballEvent() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, eventType }: { id: number; eventType: VolleyballEventType }) =>
      API.volleyball.tagLastEvent(id, { event_type: eventType }),
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
