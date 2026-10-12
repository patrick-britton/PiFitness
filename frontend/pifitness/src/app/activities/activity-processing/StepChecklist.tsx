'use client';

import { useState, useEffect } from 'react';
import {
  PLAYLIST_STEPS,
  ProcessStepResult,
  STEP_LABELS,
  STEP_ORDER,
  type ProcessStepId,
} from '@/lib/types/activity-processing';
import { estimateStepMs } from '@/lib/step-estimates';

interface StepChecklistProps {
  steps: ProcessStepResult[];
  /** If provided, used to compute a live elapsed timer for the currently running step */
  loadingStart?: number | null;
  /** Target activity distance (m) captured from the resolve_activity event; null until resolved */
  distanceM?: number | null;
  /** When false, the six playlist-shuffle rows are not rendered at all
   *  (Bug 004-007-7-4) — mirrors the backend's do_shuffle gate. */
  showPlaylistSteps?: boolean;
}

const SYNC_STEPS: ReadonlySet<ProcessStepId> = new Set(['sync_activities', 'sync_details']);

/** Estimate for one row: sync steps use their static constants from t=0 (OQ-6);
 *  every other step needs distanceM; without it there is no bar yet. */
export function estimateForStep(stepId: ProcessStepId, distanceM: number | null | undefined): number | null {
  if (SYNC_STEPS.has(stepId)) return estimateStepMs(stepId, 0);
  if (distanceM == null || !Number.isFinite(distanceM)) return null;
  try {
    return estimateStepMs(stepId, distanceM);
  } catch {
    return null;
  }
}

/** Bar fill fraction (0–1) and over-estimate flag for a row. */
export function barFraction(elapsedMs: number, estimateMs: number | null): { frac: number; over: boolean } {
  if (estimateMs == null || estimateMs <= 0) return { frac: 0, over: false };
  return { frac: Math.min(elapsedMs / estimateMs, 1), over: elapsedMs > estimateMs };
}

export default function StepChecklist({ steps, loadingStart, distanceM, showPlaylistSteps = true }: StepChecklistProps) {
  const [, setTick] = useState(0);

  useEffect(() => {
    // 250 ms tick: smooth enough for the bars, cheaper than 100 ms on the Pi.
    const id = setInterval(() => setTick((t) => t + 1), 250);
    return () => clearInterval(id);
  }, []);

  // AC-4: every STEP_ORDER row renders in execution order — no re-sorting,
  // so all 31 rows fit a 375×667 viewport without scrolling. The six
  // playlist rows are omitted entirely when the run won't shuffle
  // (Bug 004-007-7-4), regardless of the skipped events the backend emits.
  const byId = new Map(steps.map((s) => [s.step_id, s]));
  const displaySteps: ProcessStepResult[] = STEP_ORDER.filter(
    (id) => showPlaylistSteps || !PLAYLIST_STEPS.has(id)
  ).map(
    (id) => byId.get(id) ?? { step_id: id, status: 'pending' as const, elapsed_ms: 0 },
  );

  return (
    <div className="space-y-0.5">
      {displaySteps.map((step) => {
        const label = STEP_LABELS[step.step_id];
        const isRunning = step.status === 'running';
        const isComplete = step.status === 'complete';
        const isError = step.status === 'error';

        // Compute elapsed time — live for the running step, stored for terminal steps
        let elapsedMs = step.elapsed_ms;
        if (isRunning) {
          const stepStart = step.started_at
            ? new Date(step.started_at).getTime()
            : (loadingStart ?? Date.now());
          elapsedMs = Date.now() - stepStart;
        }

        const estimateMs = estimateForStep(step.step_id, distanceM);
        const { frac, over } = barFraction(elapsedMs, estimateMs);
        const showBar = estimateMs != null && (isRunning || isComplete);

        return (
          <div
            key={step.step_id}
            data-testid="step-row"
            data-step-id={step.step_id}
            data-status={step.status}
            className={`px-2 py-1 rounded border text-xs ${
              isRunning
                ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
                : isError
                ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'
                : 'bg-gray-50 dark:bg-gray-800/50 border-gray-200 dark:border-gray-700'
            }`}
          >
            {/* Two-column row (Bug 004-007-7-5): name/timing on the left,
                a dedicated always-rendered bar track on the right so the
                status bar is never squeezed into the text line. */}
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1.5 min-w-0 flex-1">
                <span
                  className={`shrink-0 inline-block w-1.5 h-1.5 rounded-full ${
                    isComplete
                      ? 'bg-green-600 dark:bg-green-400'
                      : isRunning
                      ? 'bg-gray-400 dark:bg-gray-500'
                      : isError
                      ? 'bg-red-600 dark:bg-red-400'
                      : 'bg-gray-300 dark:bg-gray-600'
                  }`}
                  aria-hidden="true"
                />
                <p className={`flex-1 truncate font-medium ${
                  isError
                    ? 'text-red-900 dark:text-red-100'
                    : 'text-gray-700 dark:text-gray-200'
                }`}>
                  {label}
                  {isError && step.error && (
                    <span className="font-normal text-red-700 dark:text-red-300"> — {step.error}</span>
                  )}
                </p>
                <span className="shrink-0 font-mono tabular-nums text-gray-500 dark:text-gray-400">
                  {isComplete || isError || isRunning ? `${Math.round(elapsedMs).toLocaleString()} ms` : '–'}
                </span>
              </div>

              {/* Dedicated bar track (AC-5): gray while running, red past the
                  estimate (capped at full width), green on completion. The
                  track itself is always visible; the progressbar fill only
                  renders when there is something to show. */}
              <div
                data-testid="step-bar-track"
                className="shrink-0 w-20 sm:w-28 h-1.5 overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700"
              >
                {showBar && (
                  <div
                    data-testid="step-bar"
                    data-over={over && isRunning ? 'true' : 'false'}
                    className="h-full"
                    role="progressbar"
                    aria-label={`${label} progress`}
                    aria-valuenow={Math.round(frac * 100)}
                    aria-valuemin={0}
                    aria-valuemax={100}
                  >
                    <div
                      className={`h-full rounded-full ${
                        isComplete
                          ? 'bg-green-600 dark:bg-green-400'
                          : over
                          ? 'bg-red-600 dark:bg-red-400'
                          : 'bg-gray-400 dark:bg-gray-500'
                      }`}
                      style={{ width: `${Math.round(frac * 100)}%` }}
                    />
                  </div>
                )}
              </div>
            </div>

            {isComplete && step.result && (
              <div className="mt-0.5 text-gray-600 dark:text-gray-300">
                {/* lookup_playlist: songs heard */}
                {step.result.song_count != null && (
                  <span>{step.result.song_count} songs heard. </span>
                )}
                {/* report_shuffle: songs sent to Spotify */}
                {step.result.songs_sent != null && (
                  <span>{step.result.songs_sent} songs sent. </span>
                )}
                {step.result.first_song && (
                  <span>First: {step.result.first_song}. </span>
                )}
                {step.result.last_song && (
                  <span>Last: {step.result.last_song}. </span>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
