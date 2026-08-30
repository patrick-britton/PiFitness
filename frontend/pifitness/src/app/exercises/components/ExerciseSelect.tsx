/**
 * Exercise Select
 *
 * The selection state of Timer Activation: list every exercise timer with its
 * last-attempt total and highest-ever total count (OQ-2). Clicking the timer
 * name itself (no separate "select" button) selects it and shows the start
 * screen.
 */

'use client';

import type { ExerciseTimerSummary } from '../../../lib/types/exercises';

interface ExerciseSelectProps {
  timers: ExerciseTimerSummary[];
  onSelect: (timer: ExerciseTimerSummary) => void;
}

export default function ExerciseSelect({ timers, onSelect }: ExerciseSelectProps) {
  const cardCls =
    'bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm overflow-hidden';

  return (
    <div className={cardCls}>
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Select an Exercise</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Tap an exercise to start its pace timer.
        </p>
      </div>

      {timers.length === 0 ? (
        <div className="p-8 text-center">
          <p className="text-gray-500 dark:text-gray-400 text-lg">No timers yet.</p>
          <p className="text-gray-400 dark:text-gray-500 mt-2">
            Create a timer on the Timer Creation page first.
          </p>
        </div>
      ) : (
        <ul className="divide-y divide-gray-200 dark:divide-gray-700">
          {timers.map((t) => (
            <li key={t.exercise_id}>
              {/* The whole row is the click target — clicking the name selects. */}
              <button
                type="button"
                onClick={() => onSelect(t)}
                className="flex w-full items-center justify-between gap-4 px-6 py-4 text-left hover:bg-gray-50 dark:hover:bg-gray-700/40 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
                aria-label={`Start ${t.name}`}
              >
                <span className="min-w-0">
                  <span className="block font-medium text-gray-900 dark:text-white truncate">
                    {t.name}
                  </span>
                  <span className="block text-sm text-gray-500 dark:text-gray-400">
                    {t.interval_seconds} sec/rep
                  </span>
                </span>
                <span className="shrink-0 text-right">
                  <span className="block text-sm text-gray-500 dark:text-gray-400">
                    Last: {t.last_attempt_total_count ?? '—'}
                  </span>
                  <span className="block text-sm text-gray-500 dark:text-gray-400">
                    Best: {t.highest_score ?? '—'}
                  </span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}