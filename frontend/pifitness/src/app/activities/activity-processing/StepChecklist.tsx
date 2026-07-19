'use client';

import { useState, useEffect } from 'react';
import { ProcessStepResult, STEP_LABELS } from '@/lib/types/activity-processing';

interface StepChecklistProps {
  steps: ProcessStepResult[];
  /** If provided, used to compute a live elapsed timer for the currently running step */
  loadingStart?: number | null;
}

export default function StepChecklist({ steps, loadingStart }: StepChecklistProps) {
  const [, setTick] = useState(0);

  useEffect(() => {
    // Use 100ms interval for smooth timer updates
    const id = setInterval(() => setTick((t) => t + 1), 100);
    return () => clearInterval(id);
  }, []);

  // Find the first pending step — that's the "currently running" one
  const firstPendingIdx = steps.findIndex((s) => s.status === 'pending');

  return (
    <div className="space-y-3">
      {steps.map((step, idx) => {
        const label = STEP_LABELS[step.step_id];
        const isComplete = step.status === 'complete';
        const isError = step.status === 'error';
        const isSkipped = step.status === 'skipped';
        const isRunning = step.status === 'pending' && idx === firstPendingIdx && loadingStart != null;
        const isPending = step.status === 'pending' && idx !== firstPendingIdx;

        // Compute elapsed time for the running step
        let elapsedMs = step.elapsed_ms;
        if (isRunning && loadingStart != null) {
          elapsedMs = Date.now() - loadingStart;
        }

        return (
          <div
            key={step.step_id}
            className={`flex items-start gap-3 p-4 rounded-md border ${
              isComplete
                ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
                : isError
                ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'
                : isSkipped
                ? 'bg-gray-50 dark:bg-gray-800/50 border-gray-200 dark:border-gray-700 opacity-70'
                : isRunning
                ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800'
                : 'bg-gray-50 dark:bg-gray-800/50 border-gray-200 dark:border-gray-700'
            }`}
          >
            <div className="flex-shrink-0 mt-0.5">
              {isComplete && (
                <svg className="w-5 h-5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              )}
              {isError && (
                <svg className="w-5 h-5 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              )}
              {isSkipped && (
                <svg className="w-5 h-5 text-gray-400 dark:text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 12H4" />
                </svg>
              )}
              {isRunning && (
                <svg className="w-5 h-5 text-blue-600 dark:text-blue-400 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
              )}
              {isPending && (
                <svg className="w-5 h-5 text-gray-400 dark:text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <circle cx="12" cy="12" r="10" strokeWidth={2} />
                </svg>
              )}
            </div>

            <div className="flex-1 min-w-0">
              <div className="flex items-center justify-between gap-2">
                <p className={`text-sm font-medium ${
                  isComplete
                    ? 'text-green-900 dark:text-green-100'
                    : isError
                    ? 'text-red-900 dark:text-red-100'
                    : isSkipped
                    ? 'text-gray-500 dark:text-gray-400'
                    : isRunning
                    ? 'text-blue-900 dark:text-blue-100'
                    : 'text-gray-500 dark:text-gray-400'
                }`}>
                  {label}
                </p>
                <span className={`text-xs font-mono ${
                  isComplete
                    ? 'text-green-700 dark:text-green-300'
                    : isError
                    ? 'text-red-700 dark:text-red-300'
                    : isSkipped
                    ? 'text-gray-500 dark:text-gray-400'
                    : isRunning
                    ? 'text-blue-700 dark:text-blue-300'
                    : 'text-gray-500 dark:text-gray-400'
                }`}>
                  {isComplete || isError ? `${elapsedMs.toLocaleString()} ms` : isRunning ? `${elapsedMs.toLocaleString()} ms` : '-'}
                </span>
              </div>

              {/* Progress bar for running step */}
              {isRunning && (
                <div className="mt-2 w-full bg-blue-200 dark:bg-blue-800 rounded-full h-1.5 overflow-hidden">
                  <div
                    className="bg-blue-600 dark:bg-blue-400 h-full rounded-full transition-[width] duration-300 ease-out"
                    style={{ width: `${Math.min(95, (elapsedMs / 30000) * 100)}%` }}
                  />
                </div>
              )}

              {isError && step.error && (
                <p className="mt-1 text-sm text-red-700 dark:text-red-300 break-words">
                  {step.error}
                </p>
              )}

              {isComplete && step.result && (
                <div className="mt-1 text-sm text-green-700 dark:text-green-300">
                  {step.result.song_count !== undefined && (
                    <span>{step.result.song_count} songs heard. </span>
                  )}
                  {step.result.first_song && (
                    <span>First: {step.result.first_song}. </span>
                  )}
                  {step.result.last_song && (
                    <span>Last: {step.result.last_song}. </span>
                  )}
                  {step.result.playlist_shuffled !== undefined && (
                    <span>
                      Playlist {step.result.playlist_shuffled ? 'shuffled' : 'not shuffled'}.
                    </span>
                  )}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}