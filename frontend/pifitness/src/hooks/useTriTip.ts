/**
 * Tri-tip Timer React Query Hooks
 * Provides React Query hooks for tri-tip endpoints with automatic caching
 * and mutation-driven invalidation (matching the admin hook patterns).
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { API } from "../lib/api-client";
import {
  TriTipInitiateRequest,
  TriTipPlaceRequest,
  TriTipReadingRequest,
} from "../lib/types/tri-tip";

// ---------------------------------------------------------------------------
// Query Key Factories
// ---------------------------------------------------------------------------

export const triTipKeys = {
  all: ["tri-tip"] as const,
  events: () => [...triTipKeys.all, "events"] as const,
  active: () => [...triTipKeys.all, "active"] as const,
  event: (id: number) => [...triTipKeys.all, "events", id] as const,
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/** All tri-tip events (most recent first). */
export function useTriTipEvents() {
  return useQuery({
    queryKey: triTipKeys.events(),
    queryFn: () => API.triTip.getEvents(),
    staleTime: 30_000,
  });
}

/**
 * The current in-progress event (initiated or active) with its readings,
 * prediction, prior-event references, and any single-active blocker (OQ-3).
 * Fetch on mount so returning to the page restores live backend state.
 */
export function useTriTipActive() {
  return useQuery({
    queryKey: triTipKeys.active(),
    queryFn: () => API.triTip.getActive(),
    staleTime: 15_000,
  });
}

/** A single event with its readings. */
export function useTriTipEvent(eventId: number | null) {
  return useQuery({
    queryKey: triTipKeys.event(eventId as number),
    queryFn: () => API.triTip.getEvent(eventId as number),
    enabled: !!eventId,
  });
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/** Create a new initiated event (weight + shape). */
export function useInitiateTriTip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (req: TriTipInitiateRequest) => API.triTip.initiate(req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: triTipKeys.all });
    },
  });
}

/** Place the meat (activate): record first reading @ 38F. */
export function usePlaceTriTip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, req }: { id: number; req: TriTipPlaceRequest }) =>
      API.triTip.placeMeat(id, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: triTipKeys.all });
    },
  });
}

/** Record a temperature reading while active. */
export function useAddTriTipReading() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, req }: { id: number; req: TriTipReadingRequest }) =>
      API.triTip.addReading(id, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: triTipKeys.all });
    },
  });
}

/** Pull the meat: complete the event. */
export function useCompleteTriTip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => API.triTip.complete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: triTipKeys.all });
    },
  });
}

/** Abandon the tri-tip and all readings (FK cascade). */
export function useAbandonTriTip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => API.triTip.abandon(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: triTipKeys.all });
    },
  });
}