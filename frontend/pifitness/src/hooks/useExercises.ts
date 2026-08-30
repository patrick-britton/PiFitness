/**
 * Exercise Timer React Query Hooks
 * Provides React Query hooks for the Exercises module endpoints with automatic
 * caching and mutation-driven invalidation (matching the tri-tip/admin patterns).
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { API } from "../lib/api-client";
import {
  ExerciseCreateRequest,
  ExerciseUpdateRequest,
  ExerciseAttemptCreateRequest,
} from "../lib/types/exercises";

// ---------------------------------------------------------------------------
// Query Key Factories
// ---------------------------------------------------------------------------

export const exerciseKeys = {
  all: ["exercises"] as const,
  summaries: () => [...exerciseKeys.all, "summaries"] as const,
  detail: (id: number) => [...exerciseKeys.all, "detail", id] as const,
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/** All exercise timers with per-timer attempt aggregates (selection screen). */
export function useExerciseSummaries() {
  return useQuery({
    queryKey: exerciseKeys.summaries(),
    queryFn: () => API.exercises.listSummaries(),
    staleTime: 30_000,
  });
}

/**
 * A single timer with its most recent attempt (calibrates the live progress
 * bar on the start/run screen). Fetch on mount so re-entering the page
 * restores the current last-attempt stat.
 */
export function useExerciseDetail(exerciseId: number | null) {
  return useQuery({
    queryKey: exerciseKeys.detail(exerciseId as number),
    queryFn: () => API.exercises.getDetail(exerciseId as number),
    enabled: !!exerciseId,
  });
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/** Create a new exercise timer (master data). */
export function useCreateExercise() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (req: ExerciseCreateRequest) => API.exercises.create(req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: exerciseKeys.all });
    },
  });
}

/** Edit an existing timer (partial update). */
export function useUpdateExercise() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, req }: { id: number; req: ExerciseUpdateRequest }) =>
      API.exercises.update(id, req),
    onSuccess: (_data, vars) => {
      queryClient.invalidateQueries({ queryKey: exerciseKeys.all });
      queryClient.invalidateQueries({ queryKey: exerciseKeys.detail(vars.id) });
    },
  });
}

/**
 * Permanently delete a timer and ALL of its attempt history (OQ-1 cascade).
 */
export function useDeleteExercise() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => API.exercises.remove(id),
    onSuccess: (_data, id) => {
      queryClient.invalidateQueries({ queryKey: exerciseKeys.all });
      queryClient.removeQueries({ queryKey: exerciseKeys.detail(id) });
    },
  });
}

/** Save one confirmed attempt (snapshots the interval; only confirmed counts). */
export function useSaveExerciseAttempt() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, req }: { id: number; req: ExerciseAttemptCreateRequest }) =>
      API.exercises.saveAttempt(id, req),
    onSuccess: (_data, vars) => {
      queryClient.invalidateQueries({ queryKey: exerciseKeys.all });
      queryClient.invalidateQueries({ queryKey: exerciseKeys.detail(vars.id) });
    },
  });
}