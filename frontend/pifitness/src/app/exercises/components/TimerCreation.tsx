/**
 * Timer Creation
 *
 * Exercises -> Timer Creation sub-module: master-data management for exercise
 * timers. Create, edit (inline name + interval), and delete (with confirmation)
 * timer rows, all against /api/exercises.
 *
 * - interval input accepts one decimal of precision (step 0.1).
 * - duplicate names surface the backend 409 as an inline error (no data change).
 * - delete permanently removes the timer AND its attempt history (OQ-1).
 *
 * Three layouts via useViewportStore.layoutVariant; loading/empty/error states;
 * light/dark tokens via Tailwind theme classes.
 */

'use client';

import { useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import {
  useExerciseSummaries,
  useCreateExercise,
  useUpdateExercise,
  useDeleteExercise,
} from '../../../hooks/useExercises';
import type {
  ExerciseTimerSummary,
  ExerciseCreateRequest,
  ExerciseUpdateRequest,
} from '../../../lib/types/exercises';

export default function TimerCreation() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  const summaries = useExerciseSummaries();
  const createTimer = useCreateExercise();
  const updateTimer = useUpdateExercise();
  const deleteTimer = useDeleteExercise();

  const [newName, setNewName] = useState('');
  const [newInterval, setNewInterval] = useState('');
  const [formError, setFormError] = useState<string | null>(null);

  // Editing state: exercise_id of the row being edited, and its draft values.
  const [editId, setEditId] = useState<number | null>(null);
  const [editName, setEditName] = useState('');
  const [editInterval, setEditInterval] = useState('');
  const [editError, setEditError] = useState<string | null>(null);

  // Confirm-dialog state for delete (OQ-1 destructive).
  const [confirmTarget, setConfirmTarget] = useState<ExerciseTimerSummary | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const inputCls =
    'mt-1 block w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 px-3 py-2 text-base text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500';
  const labelCls = 'block text-sm font-medium text-gray-700 dark:text-gray-300';
  const cardCls =
    'bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm';

  // -------------------------------------------------------------------------
  // Handlers
  // -------------------------------------------------------------------------

  const roundInterval = (n: number) => Math.round(n * 10) / 10;

  const handleCreate = () => {
    setFormError(null);
    const intervalNum = Number.parseFloat(newInterval);
    if (newName.trim().length === 0 || !Number.isFinite(intervalNum) || intervalNum <= 0) {
      setFormError('Enter a name and an interval greater than 0.');
      return;
    }
    const req: ExerciseCreateRequest = {
      name: newName.trim(),
      interval_seconds: roundInterval(intervalNum),
    };
    createTimer.mutate(req, {
      onSuccess: () => {
        setNewName('');
        setNewInterval('');
      },
      onError: (err) => {
        const msg = err instanceof Error ? err.message : String(err);
        setFormError(
          msg.includes('already exists')
            ? `An exercise named "${newName.trim()}" already exists.`
            : `Could not create the timer: ${msg}`,
        );
      },
    });
  };

  const startEdit = (t: ExerciseTimerSummary) => {
    setEditId(t.exercise_id);
    setEditName(t.name);
    setEditInterval(String(t.interval_seconds));
    setEditError(null);
  };

  const cancelEdit = () => {
    setEditId(null);
    setEditName('');
    setEditInterval('');
    setEditError(null);
  };

  const saveEdit = () => {
    if (editId === null) return;
    setEditError(null);
    const intervalNum = Number.parseFloat(editInterval);
    if (editName.trim().length === 0 || !Number.isFinite(intervalNum) || intervalNum <= 0) {
      setEditError('Enter a name and an interval greater than 0.');
      return;
    }
    const req: ExerciseUpdateRequest = {
      name: editName.trim(),
      interval_seconds: roundInterval(intervalNum),
    };
    updateTimer.mutate(
      { id: editId, req },
      {
        onSuccess: () => cancelEdit(),
        onError: (err) => {
          const msg = err instanceof Error ? err.message : String(err);
          setEditError(
            msg.includes('already exists')
              ? `An exercise named "${editName.trim()}" already exists.`
              : `Could not save changes: ${msg}`,
          );
        },
      },
    );
  };

  const handleDelete = () => {
    if (!confirmTarget) return;
    setDeleteError(null);
    deleteTimer.mutate(confirmTarget.exercise_id, {
      onSuccess: () => setConfirmTarget(null),
      onError: (err) => {
        const msg = err instanceof Error ? err.message : String(err);
        setDeleteError(`Could not delete timer: ${msg}`);
      },
    });
  };
// -------------------------------------------------------------------------
  // States
  // -------------------------------------------------------------------------

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

  return (
    <div className={isDesktop ? 'max-w-3xl mx-auto' : ''}>
      <div className={`${cardCls} p-6`}>
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Create a Timer</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Define a paced exercise (name + interval in seconds) that you can run later.
        </p>

        <div className="mt-5 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <label htmlFor="tc-name" className={labelCls}>Name</label>
            <input
              id="tc-name"
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="e.g. Push ups"
              className={inputCls}
            />
          </div>
          <div>
            <label htmlFor="tc-interval" className={labelCls}>
              Interval (seconds)
            </label>
            <input
              id="tc-interval"
              type="number"
              inputMode="decimal"
              min={0.1}
              step={0.1}
              value={newInterval}
              onChange={(e) => setNewInterval(e.target.value)}
              placeholder="e.g. 2"
              className={inputCls}
            />
          </div>
        </div>

        {formError && (
          <div className="mt-4 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2 text-sm text-red-700 dark:text-red-300">
            {formError}
          </div>
        )}

        <button
          type="button"
          onClick={handleCreate}
          disabled={createTimer.isPending}
          className="mt-5 inline-flex w-full sm:w-auto items-center justify-center px-4 py-2.5 text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {createTimer.isPending ? 'Creating…' : 'Create Timer'}
        </button>
      </div>

      {/* Timer list */}
      <div className={`${cardCls} mt-6 overflow-hidden`}>
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white">My Timers</h2>
        </div>

        {timers.length === 0 ? (
          <div className="p-8 text-center">
            <p className="text-gray-500 dark:text-gray-400 text-lg">No timers yet.</p>
            <p className="text-gray-400 dark:text-gray-500 mt-2">Create one above to get started.</p>
          </div>
        ) : (
          <ul className="divide-y divide-gray-200 dark:divide-gray-700">
            {timers.map((t) => {
              const isEditing = editId === t.exercise_id;
              return (
                <li key={t.exercise_id} className="px-6 py-4">
                  {isEditing ? (
<div className="grid grid-cols-1 sm:grid-cols-[1fr_auto] gap-3 items-end">
                      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                        <div>
                          <label htmlFor={`edit-name-${t.exercise_id}`} className={labelCls}>Name</label>
                          <input
                            id={`edit-name-${t.exercise_id}`}
                            type="text"
                            value={editName}
                            onChange={(e) => setEditName(e.target.value)}
                            className={inputCls}
                          />
                        </div>
                        <div>
                          <label htmlFor={`edit-interval-${t.exercise_id}`} className={labelCls}>
                            Interval (s)
                          </label>
                          <input
                            id={`edit-interval-${t.exercise_id}`}
                            type="number"
                            inputMode="decimal"
                            min={0.1}
                            step={0.1}
                            value={editInterval}
                            onChange={(e) => setEditInterval(e.target.value)}
                            className={inputCls}
                          />
                        </div>
                      </div>
                      <div className="flex gap-2">
                        <button
                          type="button"
                          onClick={saveEdit}
                          disabled={updateTimer.isPending}
                          className="inline-flex items-center justify-center px-3 py-2 text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          Save
                        </button>
                        <button
                          type="button"
                          onClick={cancelEdit}
                          className="inline-flex items-center justify-center px-3 py-2 text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700/50"
                        >
                          Cancel
                        </button>
                      </div>
                      {editError && (
                        <div className="sm:col-span-2 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2 text-sm text-red-700 dark:text-red-300">
                          {editError}
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="flex items-center justify-between gap-4">
                      <div className="min-w-0">
                        <p className="font-medium text-gray-900 dark:text-white truncate">
                          {t.name}
                        </p>
                        <p className="text-sm text-gray-500 dark:text-gray-400">
                          {t.interval_seconds} sec/rep
                        </p>
                      </div>
                      <div className="flex shrink-0 gap-2">
                        <button
                          type="button"
                          onClick={() => startEdit(t)}
                          className="inline-flex items-center justify-center px-3 py-2 text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700/50"
                          aria-label={`Edit ${t.name}`}
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setConfirmTarget(t);
                            setDeleteError(null);
                          }}
                          className="inline-flex items-center justify-center px-3 py-2 text-sm font-medium rounded-md text-red-700 dark:text-red-300 border border-red-300 dark:border-red-700 hover:bg-red-50 dark:hover:bg-red-900/20"
                          aria-label={`Delete ${t.name}`}
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        )}
      </div>
{/* Delete confirmation dialog (OQ-1: permanently removes timer + history) */}
      {confirmTarget && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50"
          role="dialog"
          aria-modal="true"
          aria-labelledby="delete-dialog-title"
        >
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg max-w-md w-full p-6">
            <h2 id="delete-dialog-title" className="text-lg font-semibold text-gray-900 dark:text-white">
              Delete “{confirmTarget.name}”?
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-2">
              This permanently removes the timer and all of its saved attempts. This cannot be undone.
            </p>
            {deleteError && (
              <div className="mt-4 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2 text-sm text-red-700 dark:text-red-300">
                {deleteError}
              </div>
            )}
            <div className="mt-6 flex justify-end gap-3">
              <button
                type="button"
                onClick={() => setConfirmTarget(null)}
                className="inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700/50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleDelete}
                disabled={deleteTimer.isPending}
                className="inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {deleteTimer.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}