/**
 * Initiation Form
 *
 * The no-active-event state of the Tri-tip Timer: capture a weight (lbs) and
 * exactly one meat shape, then create a new 'initiated' event.
 *
 * OQ-3 (block-and-guide): if the backend refuses initiation because another
 * event is in progress, the returned message is surfaced so the user knows to
 * complete/abandon that event first.
 */

'use client';

import { useState } from 'react';
import { useInitiateTriTip } from '../../../hooks/useTriTip';
import type { TriTipShape } from '../../../lib/types/tri-tip';

const SHAPES: { value: TriTipShape; label: string; hint: string }[] = [
  { value: 'Short+Fat', label: 'Short + Fat', hint: 'Wide, short roast' },
  { value: 'Long+Skinny', label: 'Long + Skinny', hint: 'Long, thin roast' },
  { value: 'Typical', label: 'Typical', hint: 'Standard tri-tip' },
];

export default function InitiationForm() {
  const initiate = useInitiateTriTip();
  const [weight, setWeight] = useState<string>('');
  const [shape, setShape] = useState<TriTipShape | null>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const weightNum = Number.parseFloat(weight);
  const weightValid = Number.isFinite(weightNum) && weightNum > 0;

  const handleSubmit = () => {
    setErrorMsg(null);
    if (!weightValid || !shape) return;
    initiate.mutate(
      { weight_lbs: weightNum, shape },
      {
        onError: (err) => {
          const raw = err instanceof Error ? err.message : String(err);
          // FastAPI 409 detail may be JSON: {"message":..., "blocked_by":...}
          if (raw.includes('already in progress')) {
            setErrorMsg('A tri-tip event is already in progress. Complete or abandon it first.');
          } else {
            setErrorMsg(`Could not start the event: ${raw}`);
          }
        },
      },
    );
  };

  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm p-6">
      <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Start a Tri-tip</h2>
      <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
        Enter the weight and shape to begin a new grilling event.
      </p>

      <div className="mt-5 space-y-5">
        {/* Weight */}
        <div>
          <label
            htmlFor="tritip-weight"
            className="block text-sm font-medium text-gray-700 dark:text-gray-300"
          >
            Weight (lbs)
          </label>
          <input
            id="tritip-weight"
            type="number"
            inputMode="decimal"
            min={0.1}
            step="any"
            value={weight}
            onChange={(e) => setWeight(e.target.value)}
            placeholder="e.g. 4.2"
            className="mt-1 block w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 px-3 py-2 text-base text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          {weight && !weightValid && (
            <p className="mt-1 text-sm text-red-600 dark:text-red-400">
              Weight must be a number greater than 0.
            </p>
          )}
        </div>

        {/* Shape */}
        <fieldset>
          <legend className="block text-sm font-medium text-gray-700 dark:text-gray-300">
            Shape
          </legend>
          <div className="mt-2 grid grid-cols-1 sm:grid-cols-3 gap-2">
            {SHAPES.map((s) => {
              const selected = shape === s.value;
              return (
                <button
                  key={s.value}
                  type="button"
                  onClick={() => setShape(s.value)}
                  aria-pressed={selected}
                  className={`flex flex-col items-start justify-center px-4 py-3 rounded-md border text-left transition-colors ${
                    selected
                      ? 'border-blue-500 bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'
                      : 'border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50'
                  }`}
                >
                  <span className="text-sm font-medium">{s.label}</span>
                  <span className="text-xs text-gray-500 dark:text-gray-400">{s.hint}</span>
                </button>
              );
            })}
          </div>
        </fieldset>

        {errorMsg && (
          <div className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2 text-sm text-red-700 dark:text-red-300">
            {errorMsg}
          </div>
        )}

        <button
          type="button"
          onClick={handleSubmit}
          disabled={!weightValid || !shape || initiate.isPending}
          className="inline-flex w-full items-center justify-center px-4 py-2.5 text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {initiate.isPending ? 'Starting…' : 'Start Grilling'}
        </button>
      </div>
    </div>
  );
}