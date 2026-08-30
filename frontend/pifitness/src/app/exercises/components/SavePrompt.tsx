/**
 * Save Prompt
 *
 * Exercises -> Timer Activation: shown after Stop. Displays client-side
 * defaults (paced = round(elapsed / interval), total = paced) that the user
 * confirms or adjusts with +/- buttons or manual entry — only the confirmed
 * values are persisted (FR-8); the interval is snapshotted (FR-10).
 *
 * On successful save the Celebration fires (standard vs all-time-high variant,
 * OQ-2), then Done returns to the selection screen (queries already
 * invalidated by the save hook, so refreshed stats await there).
 */

'use client';

import { useState } from 'react';
import { useSaveExerciseAttempt } from '../../../hooks/useExercises';
import type { ExerciseTimerSummary } from '../../../lib/types/exercises';
import Celebration from './Celebration';

/** The stopped run handed from TimerRun via onStop. */
export interface StoppedRun {
  startedAtIso: string;
  endedAtIso: string;
  elapsedMs: number;
}

interface SavePromptProps {
  timer: ExerciseTimerSummary;
  run: StoppedRun;
  /** Leave the save flow (after save or discard) back to the selection list. */
  onDone: () => void;
}

const stepBtnCls =
  'inline-flex h-11 w-11 shrink-0 items-center justify-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-lg font-semibold text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500';

export default function SavePrompt({ timer, run, onDone }: SavePromptProps) {
  const interval = timer.interval_seconds;

  // Client-side defaults (FR-8): paced = round(elapsed / interval), total = paced.
  const [pacedCount, setPacedCount] = useState(() =>
    Math.max(0, Math.round(run.elapsedMs / 1000 / interval)),
  );
  const [totalCount, setTotalCount] = useState(() =>
    Math.max(0, Math.round(run.elapsedMs / 1000 / interval)),
  );
  const [saved, setSaved] = useState(false);

  const save = useSaveExerciseAttempt();

  // Clamp: paced >= 0; total >= paced (DB CHECK total_count >= paced_count).
  const applyPaced = (n: number) => {
    const p = Math.max(0, Math.floor(n) || 0);
    setPacedCount(p);
    setTotalCount((t) => Math.max(t, p));
  };
  const applyTotal = (n: number) => {
    const t = Math.max(0, Math.floor(n) || 0);
    setTotalCount(Math.max(t, pacedCount));
  };

  const handleSave = () => {
    save.mutate(
      {
        id: timer.exercise_id,
        req: {
          started_at: run.startedAtIso,
          ended_at: run.endedAtIso,
          interval_seconds_used: interval,
          paced_count: pacedCount,
          total_count: totalCount,
        },
      },
      { onSuccess: () => setSaved(true) },
    );
  };

  const totalSec = Math.floor(run.elapsedMs / 1000);
  const durationLabel = `${Math.floor(totalSec / 60)}:${String(totalSec % 60).padStart(2, '0')}`;


  if (saved) {
    return (
      <div className="flex w-full flex-col items-center gap-6 py-8">
        <Celebration
          pacedCount={pacedCount}
          totalCount={totalCount}
          priorPacedCount={timer.last_attempt_paced_count}
          priorTotalCount={timer.last_attempt_total_count}
          highestScore={timer.highest_score}
        />
        <div className="text-center">
          <p className="text-3xl font-bold tabular-nums text-gray-900 dark:text-white">
            {totalCount} <span className="text-base font-medium text-gray-500 dark:text-gray-400">total</span>
            {' · '}
            {pacedCount} <span className="text-base font-medium text-gray-500 dark:text-gray-400">on-pace</span>
          </p>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Attempt saved to {timer.name}
          </p>
        </div>
        <button
          type="button"
          onClick={onDone}
          className="inline-flex items-center justify-center rounded-full bg-blue-600 px-10 py-4 text-xl font-semibold text-white hover:bg-blue-700 focus:outline-none focus-visible:ring-4 focus-visible:ring-blue-500"
        >
          Done
        </button>
      </div>
    );
  }

  return (
    <div className="w-full max-w-sm mx-auto">
      <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-6 shadow-sm">
        <div className="text-center">
          <h2 className="text-xl font-bold text-gray-900 dark:text-white">Save attempt</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            {timer.name} · {durationLabel} elapsed · {interval} sec/rep
          </p>
        </div>

        {save.isError && (
          <div className="mt-4 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-sm text-red-700 dark:text-red-300">
            {save.error instanceof Error ? save.error.message : 'Could not save the attempt.'}
          </div>
        )}

        {/* Paced count */}
        <div className="mt-6">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300" htmlFor="save-paced">
            On-pace reps
          </label>
          <div className="mt-2 flex items-center gap-3">
            <button
              type="button"
              aria-label="Decrease on-pace reps"
              onClick={() => applyPaced(pacedCount - 1)}
              className={stepBtnCls}
            >
              −
            </button>
            <input
              id="save-paced"
              type="number"
              min={0}
              inputMode="numeric"
              value={pacedCount}
              onChange={(e) => applyPaced(e.target.valueAsNumber)}
              className="h-11 w-full min-w-0 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 text-center text-xl font-bold tabular-nums text-gray-900 dark:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
            />
            <button
              type="button"
              aria-label="Increase on-pace reps"
              onClick={() => applyPaced(pacedCount + 1)}
              className={stepBtnCls}
            >
              +
            </button>
          </div>
        </div>

        {/* Total count */}
        <div className="mt-4">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300" htmlFor="save-total">
            Total reps
          </label>
          <div className="mt-2 flex items-center gap-3">
            <button
              type="button"
              aria-label="Decrease total reps"
              onClick={() => setTotalCount((t) => Math.max(t - 1, pacedCount))}
              className={stepBtnCls}
            >
              −
            </button>
            <input
              id="save-total"
              type="number"
              min={pacedCount}
              inputMode="numeric"
              value={totalCount}
              onChange={(e) => applyTotal(e.target.valueAsNumber)}
              className="h-11 w-full min-w-0 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 text-center text-xl font-bold tabular-nums text-gray-900 dark:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
            />
            <button
              type="button"
              aria-label="Increase total reps"
              onClick={() => setTotalCount((t) => t + 1)}
              className={stepBtnCls}
            >
              +
            </button>
          </div>
        </div>

        <div className="mt-6 flex flex-col gap-3">
          <button
            type="button"
            onClick={handleSave}
            disabled={save.isPending}
            className="inline-flex h-12 items-center justify-center rounded-full bg-blue-600 px-6 text-lg font-semibold text-white hover:bg-blue-700 focus:outline-none focus-visible:ring-4 focus-visible:ring-blue-500 disabled:opacity-60"
          >
            {save.isPending ? 'Saving…' : 'Save attempt'}
          </button>
          <button
            type="button"
            onClick={onDone}
            disabled={save.isPending}
            className="text-sm font-medium text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 rounded-md px-3 py-2 disabled:opacity-60"
          >
            Discard without saving
          </button>
        </div>
      </div>
    </div>
  );
}
