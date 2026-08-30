/**
 * Timer Activation
 *
 * Exercises -> Timer Activation sub-module: choose an exercise timer, then land
 * on a start screen with a large Start button. The start screen hands off to the
 * run engine (`TimerRun`, T07) once Start is pressed.
 *
 * Selection: `ExerciseSelect` lists every timer with last-attempt + highest-ever
 * stats (OQ-2). Clicking the timer name selects it (no separate button).
 */

'use client';

import { useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { useExerciseSummaries } from '../../../hooks/useExercises';
import type { ExerciseTimerSummary } from '../../../lib/types/exercises';
import ExerciseSelect from './ExerciseSelect';
import TimerRun from './TimerRun';
import SavePrompt, { type StoppedRun } from './SavePrompt';

export default function TimerActivation() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  const summaries = useExerciseSummaries();
  const [selected, setSelected] = useState<ExerciseTimerSummary | null>(null);
  const [running, setRunning] = useState(false);
  const [run, setRun] = useState<StoppedRun | null>(null);

  if (summaries.isLoading) {
    return (
      <div className="animate-pulse space-y-4">
        <div className="h-8 bg-gray-200 dark:bg-gray-700 rounded-md w-48" />
        <div className="h-24 bg-gray-200 dark:bg-gray-700 rounded-lg" />
      </div>
    );
  }

  if (summaries.isError) {
    const msg =
      summaries.error instanceof Error
        ? summaries.error.message
        : 'Could not load exercise timers.';
    return (
      <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 text-red-700 dark:text-red-300">
        <p className="font-medium">Error loading exercise timers</p>
        <p className="text-sm mt-1">{msg}</p>
      </div>
    );
  }

  const timers = summaries.data?.data ?? [];

  // Flow transitions.
  const handleBack = () => {
    // Covers back-from-start-screen and back-from-run (discard); resets flow state.
    setRunning(false);
    setRun(null);
    setSelected(null);
  };
  const handleStop = (r: StoppedRun) => setRun(r); // TimerRun cleans itself up first.
  const handleDone = () => {
    // Save hook has already invalidated queries — the refreshed selection
    // list (new last-attempt + highest) is waiting when we land there.
    setRun(null);
    setRunning(false);
    setSelected(null);
  };

  const startBtnCls =
    'inline-flex items-center justify-center rounded-full bg-blue-600 text-white px-10 py-4 text-2xl font-semibold hover:bg-blue-700 focus:outline-none focus-visible:ring-4 focus-visible:ring-blue-500';

  return (
    <div className={isDesktop ? 'max-w-3xl mx-auto' : ''}>
      {selected === null ? (
        <>
          <ExerciseSelect timers={timers} onSelect={setSelected} />
          {timers.length > 0 && (
            <p className="mt-4 text-center text-sm text-gray-500 dark:text-gray-400">
              Highest-ever is the max total count across your saved attempts.
            </p>
          )}
        </>
      ) : run !== null ? (
        <SavePrompt timer={selected} run={run} onDone={handleDone} />
      ) : running ? (
        <TimerRun timer={selected} onStop={handleStop} onBack={handleBack} />
      ) : (
        <div className="flex flex-col items-center gap-6 py-8">
          <div className="text-center">
            <h2 className="text-2xl font-bold text-gray-900 dark:text-white">{selected.name}</h2>
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              {selected.interval_seconds} sec/rep · Best: {selected.highest_score ?? '—'}
            </p>
          </div>
          {/* Hand off to the run engine */}
          <button
            type="button"
            className={startBtnCls}
            onClick={() => setRunning(true)}
          >
            Start
          </button>
          <button
            type="button"
            onClick={handleBack}
            className="text-sm font-medium text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 rounded-md px-3 py-2"
          >
            ← Back
          </button>
        </div>
      )}
    </div>
  );
}