'use client';

import { useEffect, useState } from 'react';

/**
 * Step-by-step progress for the find-matches pipeline (009-003 T20, FR-9).
 *
 * Mirrors the Activity Processing StepChecklist pattern: one running step with
 * a live elapsed timer and progress bar, completed steps beneath with their
 * stored elapsed time, pending steps dimmed, and a failed step carrying the
 * server error detail. Dependency-free; Tailwind tokens for both themes.
 */

export interface MatchStepState {
  step: number;
  label: string;
  status: 'pending' | 'running' | 'complete' | 'error';
  /** Elapsed time, stored once the step reaches a terminal state. */
  elapsedMs: number | null;
  /** Wall-clock start of the running step (live timer input). */
  startedAtMs?: number | null;
  error?: string | null;
}

export default function MatchProgressSteps({ steps }: { steps: MatchStepState[] }) {
  const [, setTick] = useState(0);

  useEffect(() => {
    // 100 ms tick keeps the running step's timer smooth (StepChecklist pattern).
    const id = setInterval(() => setTick((t) => t + 1), 100);
    return () => clearInterval(id);
  }, []);

  return (
    <ol className="space-y-2" aria-label="Match pipeline progress">
      {steps.map((s) => {
        const isRunning = s.status === 'running';
        const elapsed =
          s.status === 'complete' || s.status === 'error'
            ? (s.elapsedMs ?? 0)
            : isRunning && s.startedAtMs
              ? Date.now() - s.startedAtMs
              : 0;
        return (
          <li
            key={s.step}
            className={`flex items-start gap-3 px-3 py-2 rounded-md border ${
              isRunning
                ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
                : s.status === 'error'
                  ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'
                  : 'bg-gray-50 dark:bg-gray-800/50 border-gray-200 dark:border-gray-700'
            }`}
          >
            <span className="mt-0.5 flex-shrink-0" aria-hidden="true">
              {isRunning && (
                <svg className="w-4 h-4 text-green-600 dark:text-green-400 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
              )}
              {s.status === 'complete' && (
                <svg className="w-4 h-4 text-gray-500 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              )}
              {s.status === 'error' && (
                <svg className="w-4 h-4 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              )}
              {s.status === 'pending' && (
                <svg className="w-4 h-4 text-gray-400 dark:text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <circle cx="12" cy="12" r="10" strokeWidth={2} />
                </svg>
              )}
            </span>
            <span className="flex-1 min-w-0">
              <span className="flex items-center justify-between gap-2">
                <span
                  className={`text-sm font-medium ${
                    isRunning
                      ? 'text-green-900 dark:text-green-100'
                      : s.status === 'error'
                        ? 'text-red-900 dark:text-red-100'
                        : s.status === 'pending'
                          ? 'text-gray-500 dark:text-gray-400'
                          : 'text-gray-700 dark:text-gray-200'
                  }`}
                >
                  Step {s.step}: {s.label}
                </span>
                <span
                  className={`text-xs font-mono ${
                    isRunning ? 'text-green-700 dark:text-green-300' : 'text-gray-500 dark:text-gray-400'
                  }`}
                >
                  {isRunning || s.status === 'complete' || s.status === 'error'
                    ? `${elapsed.toLocaleString()} ms`
                    : '-'}
                </span>
              </span>
              {isRunning && (
                <span className="mt-1 block w-full bg-green-200 dark:bg-green-800 rounded-full h-1.5 overflow-hidden">
                  <span
                    className="block bg-green-600 dark:bg-green-400 h-full rounded-full transition-[width] duration-300 ease-out"
                    style={{ width: `${Math.min(95, (elapsed / 30000) * 100)}%` }}
                  />
                </span>
              )}
              {s.status === 'error' && s.error && (
                <span className="mt-1 block text-xs text-red-700 dark:text-red-300 break-words">{s.error}</span>
              )}
            </span>
          </li>
        );
      })}
    </ol>
  );
}