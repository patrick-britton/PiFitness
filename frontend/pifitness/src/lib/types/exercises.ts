/**
 * Exercise Timer Types
 *
 * Cross-surface data contract for the Exercise Timer feature.
 * Shared between frontend (React) and backend (FastAPI Pydantic models).
 *
 * Maps directly to the pre-built backend tables (no migrations; design 007-001):
 *   exercises.exercise_timers    (master data — one row per named exercise)
 *   exercises.exercise_attempts  (one row per saved Start/Stop cycle)
 *
 * Endpoints (backend/api/exercises.py, prefix /api/exercises):
 *   GET    /api/exercises               -> { data: ExerciseTimerSummary[], count }
 *   GET    /api/exercises/{id}          -> { exercise, last_attempt }
 *   POST   /api/exercises               -> created ExerciseTimer; 409 on duplicate name
 *   PUT    /api/exercises/{id}          -> updated ExerciseTimer; 409 on duplicate name
 *   DELETE /api/exercises/{id}          -> cascades the timer and ALL of its attempts
 *                                          (attempts deleted first, then the timer row —
 *                                          OQ-1 app-layer transaction honoring ON DELETE RESTRICT)
 *   POST   /api/exercises/{id}/attempts -> saved ExerciseAttempt
 *
 * A run-in-progress lives entirely in the client (no "in-progress" row); only
 * confirmed values from the save prompt are persisted.
 */

/**
 * A named exercise timer (master data).
 * Maps to exercises.exercise_timers.
 */
export interface ExerciseTimer {
  /** Primary key (SERIAL). */
  exercise_id: number;
  /** Display name, unique case-insensitively (idx_exercise_timers_name). */
  name: string;
  /**
   * Pacing interval in seconds. NUMERIC(5,2) CHECK > 0.
   * UI input restricts to one decimal of precision; the DB accepts up to two.
   */
  interval_seconds: number;
  /** Optional notes (unused in v1). */
  notes?: string | null;
  /** Row creation time. */
  created_at: string;
  /** Last update time (server now() on UPDATE). */
  updated_at: string;
}

/**
 * A saved attempt (one Start/Stop cycle).
 * Maps to exercises.exercise_attempts.
 */
export interface ExerciseAttempt {
  /** Primary key (SERIAL). */
  attempt_id: number;
  /** Parent timer id (ON DELETE RESTRICT — app deletes attempts first on timer delete). */
  exercise_id: number;
  /**
   * Snapshot of the timer's interval in effect during this attempt,
   * NUMERIC(5,2) NOT NULL. Editing the timer later must not re-pace history.
   */
  interval_seconds_used: number;
  /** ISO timestamp when the run started (after the 5→1 countdown). */
  started_at: string;
  /** ISO timestamp when Stop was pressed. */
  ended_at: string;
  /** Reps completed while keeping pace. CHECK >= 0. */
  paced_count: number;
  /** Reps completed overall. CHECK >= paced_count. */
  total_count: number;
  /** Optional notes (unused in v1). */
  notes?: string | null;
  /** Row creation time. */
  created_at: string;
}

/**
 * A timer enriched with per-timer attempt aggregates for the selection screen.
 * Derived fields (not columns):
 *   - last_attempt_total_count: total_count of the most recent attempt
 *     (per `(exercise_id, started_at DESC)` index lookup)
 *   - last_attempt_paced_count: paced_count of that same attempt
 *   - highest_score: MAX(total_count) across attempts (OQ-2 — the "highest-ever" score)
 *   - highest_paced_count: MAX(paced_count) across attempts (OQ-2 — both efforts tracked)
 */
export interface ExerciseTimerSummary extends ExerciseTimer {
  /** Total reps of the most recent attempt, if any. */
  last_attempt_total_count: number | null;
  /** On-pace reps of the most recent attempt, if any. */
  last_attempt_paced_count: number | null;
  /** All-time-highest total reps (OQ-2). */
  highest_score: number | null;
  /** All-time-highest on-pace reps (OQ-2). */
  highest_paced_count: number | null;
}

/** Response for GET /api/exercises (timer summaries with attempt aggregates). */
export interface ExerciseListResponse {
  data: ExerciseTimerSummary[];
  count: number;
}

/** Response for GET /api/exercises/{id}. */
export interface ExerciseDetailResponse {
  /** The timer record. */
  exercise: ExerciseTimer;
  /** The most recent attempt (used to calibrate the live progress bar), or null. */
  last_attempt: ExerciseAttempt | null;
}

/** Request body for POST /api/exercises (create a timer). */
export interface ExerciseCreateRequest {
  /** Display name (unique case-insensitively). */
  name: string;
  /** Pacing interval in seconds, > 0, one-decimal UI precision. */
  interval_seconds: number;
  /** Optional notes (unused in v1). */
  notes?: string | null;
}

/** Request body for PUT /api/exercises/{id} (edit a timer). */
export interface ExerciseUpdateRequest {
  /** Display name (unique case-insensitively). */
  name?: string;
  /** Pacing interval in seconds, > 0, one-decimal UI precision. */
  interval_seconds?: number;
  /** Optional notes (unused in v1). */
  notes?: string | null;
}

/**
 * Request body for POST /api/exercises/{id}/attempts (save a confirmed attempt).
 * The client-side countdown/count-up defaults are NEVER sent here — only the
 * user-confirmed paced_count and total_count, and the interval snapshot.
 */
export interface ExerciseAttemptCreateRequest {
  /** ISO timestamp when the run started (after the 5→1 countdown). */
  started_at: string;
  /** ISO timestamp when Stop was pressed. */
  ended_at: string;
  /** Snapshot of the timer's interval in effect during the run. */
  interval_seconds_used: number;
  /** User-confirmed on-pace reps. CHECK >= 0. */
  paced_count: number;
  /** User-confirmed total reps. CHECK >= paced_count. */
  total_count: number;
  /** Optional notes (unused in v1). */
  notes?: string | null;
}