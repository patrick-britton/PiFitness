/**
 * Active Event View (T07 — active grilling dashboard)
 *
 * Rendered when an in-progress event exists:
 *   - 'initiated' -> "Place meat" prompt (grill temp); placing flips to active.
 *   - 'active'    -> reading form (grill + internal temp), prominent ETA with
 *                    minutes ticking locally from projected_done_at (no
 *                    refetch), plus Pull Meat (complete) and Abandon Meat
 *                    (delete event + readings) with confirmation.
 * Revisiting the page restores live state from GET /active (OQ-3).
 */

'use client';

import { useEffect, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import {
  useTriTipActive,
  usePlaceTriTip,
  useAddTriTipReading,
  useCompleteTriTip,
  useAbandonTriTip,
} from '../../../hooks/useTriTip';
import type { TriTipEvent } from '../../../lib/types/tri-tip';
import TriTipChart from './TriTipChart';

function formatTimeOfDay(etaIso: string): string {
  try {
    return new Date(etaIso).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
  } catch {
    return etaIso;
  }
}

/** Prominent ETA panel; minutes tick locally from projected_done_at. */
function EtaPanel({ projectedDoneAt }: { projectedDoneAt: string }) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 15_000);
    return () => clearInterval(t);
  }, []);

  const etaMs = new Date(projectedDoneAt).getTime();
  const minutesRemaining = Math.round((etaMs - now) / 60_000);
  const minutesLabel =
    minutesRemaining > 0 ? `~${minutesRemaining} min remaining` : 'Target temp reached';

  return (
    <div className="rounded-lg border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/30 p-4 text-center">
      <p className="text-xs uppercase tracking-wide text-blue-700 dark:text-blue-300">
        Estimated done
      </p>
      <p className="text-3xl font-bold text-blue-900 dark:text-blue-100 mt-1">
        {formatTimeOfDay(projectedDoneAt)}
      </p>
      <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">{minutesLabel}</p>
    </div>
  );
}

function parseTemp(v: string): number | null {
  const n = Number.parseFloat(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/** Numeric input with validation. */
function TempInput({
  id,
  label,
  value,
  onChange,
}: {
  id: string;
  label: string;
  value: string;
  onChange: (v: string) => void;
}) {
  const valid = value === '' || Number.isFinite(Number.parseFloat(value));
  return (
    <div>
      <label htmlFor={id} className="block text-sm font-medium text-gray-700 dark:text-gray-300">
        {label}
      </label>
      <input
        id={id}
        type="number"
        inputMode="decimal"
        step="any"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={`mt-1 block w-full rounded-md border bg-white dark:bg-gray-900 px-3 py-2 text-base text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500 ${
          valid
            ? 'border-gray-300 dark:border-gray-600'
            : 'border-red-400 dark:border-red-600'
        }`}
      />
    </div>
  );
}

export default function ActiveEventView({ event }: { event: TriTipEvent }) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  const { data } = useTriTipActive();
  const place = usePlaceTriTip();
  const addReading = useAddTriTipReading();
  const complete = useCompleteTriTip();
  const abandon = useAbandonTriTip();

  const [grillTemp, setGrillTemp] = useState('');
  const [internalTemp, setInternalTemp] = useState('');
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [confirmAction, setConfirmAction] = useState<'complete' | 'abandon' | null>(null);

  const prediction = data?.prediction ?? null;
  const readings = data?.readings ?? [];

  const onPlace = () => {
    setErrorMsg(null);
    const grill = parseTemp(grillTemp);
    if (grill === null) return;
    place.mutate(
      { id: event.tri_tip_id, req: { grill_temp_f: grill } },
      { onError: (err) => setErrorMsg(err instanceof Error ? err.message : String(err)) },
    );
  };

  const onAddReading = () => {
    setErrorMsg(null);
    const grill = parseTemp(grillTemp);
    const internal = parseTemp(internalTemp);
    if (grill === null || internal === null) return;
    addReading.mutate(
      { id: event.tri_tip_id, req: { grill_temp_f: grill, internal_temp_f: internal } },
      {
        onSuccess: () => {
          setGrillTemp('');
          setInternalTemp('');
        },
        onError: (err) => setErrorMsg(err instanceof Error ? err.message : String(err)),
      },
    );
  };

  const onComplete = () => {
    setErrorMsg(null);
    complete.mutate(event.tri_tip_id, {
      onError: (err) => setErrorMsg(err instanceof Error ? err.message : String(err)),
    });
    setConfirmAction(null);
  };

  const onAbandon = () => {
    setErrorMsg(null);
    abandon.mutate(event.tri_tip_id, {
      onError: (err) => setErrorMsg(err instanceof Error ? err.message : String(err)),
    });
    setConfirmAction(null);
  };

  const cardCls =
    'bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm p-6 space-y-5';

  return (
    <div className={isDesktop ? 'max-w-2xl mx-auto space-y-4' : 'space-y-4'}>
      {/* Event summary */}
      <div className={cardCls}>
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm text-gray-500 dark:text-gray-400">Tri-tip on the grill</p>
            <p className="text-lg font-semibold text-gray-900 dark:text-white">
              {event.weight_lbs.toFixed(1)} lbs · {event.shape.replace('+', ' + ')}
            </p>
          </div>
          <span
            className={`text-xs font-medium px-2 py-1 rounded-full ${
              event.status === 'active'
                ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
                : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300'
            }`}
          >
            {event.status}
          </span>
        </div>

      </div>

      {/* ETA */}
      {prediction?.projected_done_at && (
        <EtaPanel projectedDoneAt={prediction.projected_done_at} />
      )}
      {/* Place meat (initiated) or reading form (active) */}
      <div className={cardCls}>
        {event.status === 'initiated' ? (
          <>
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">
              Place the meat
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Enter the grill temperature as you place the roast on the grill. The internal
              temperature is recorded at 38°F automatically.
            </p>
            <TempInput
              id="tritip-place-grill"
              label="Grill temperature (°F)"
              value={grillTemp}
              onChange={setGrillTemp}
            />
            <button
              type="button"
              onClick={onPlace}
              disabled={parseTemp(grillTemp) === null || place.isPending}
              className="inline-flex w-full items-center justify-center px-4 py-2.5 text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {place.isPending ? 'Placing…' : 'Place Meat on Grill'}
            </button>
          </>
        ) : (
          <>
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">
              Record a reading
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <TempInput
                id="tritip-grill"
                label="Grill temperature (°F)"
                value={grillTemp}
                onChange={setGrillTemp}
              />
              <TempInput
                id="tritip-internal"
                label="Internal temperature (°F)"
                value={internalTemp}
                onChange={setInternalTemp}
              />
            </div>
            <button
              type="button"
              onClick={onAddReading}
              disabled={
                parseTemp(grillTemp) === null ||
                parseTemp(internalTemp) === null ||
                addReading.isPending
              }
              className="inline-flex w-full items-center justify-center px-4 py-2.5 text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {addReading.isPending ? 'Saving…' : 'Add Reading'}
            </button>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Adding a reading refines the prediction automatically.
            </p>
          </>
        )}
      </div>

      {/* Pull / Abandon actions with confirm */}
      <div className={cardCls}>
        {confirmAction === null ? (
          <div className="flex flex-col sm:flex-row gap-3">
            <button
              type="button"
              onClick={() => setConfirmAction('complete')}
              className="flex-1 inline-flex items-center justify-center px-4 py-2.5 text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500"
            >
              Pull Meat
            </button>
            <button
              type="button"
              onClick={() => setConfirmAction('abandon')}
              className="flex-1 inline-flex items-center justify-center px-4 py-2.5 text-sm font-medium rounded-md text-red-700 dark:text-red-300 border border-red-300 dark:border-red-700 hover:bg-red-50 dark:hover:bg-red-900/30 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
            >
              Abandon Meat
            </button>
          </div>
        ) : (
          <div className="rounded-md border border-gray-300 dark:border-gray-600 p-4 space-y-3">
            <p className="text-sm text-gray-700 dark:text-gray-300">
              {confirmAction === 'complete'
                ? 'Pull the meat and complete this event? The event will be marked complete with the last reading time.'
                : 'Abandon this tri-tip? The event and all of its readings will be permanently deleted.'}
            </p>
            <div className="flex gap-3">
              <button
                type="button"
                onClick={confirmAction === 'complete' ? onComplete : onAbandon}
                disabled={complete.isPending || abandon.isPending}
                className={`flex-1 inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md text-white ${
                  confirmAction === 'complete'
                    ? 'bg-green-600 hover:bg-green-700'
                    : 'bg-red-600 hover:bg-red-700'
                } disabled:opacity-50 disabled:cursor-not-allowed`}
              >
                {complete.isPending || abandon.isPending
                  ? 'Working…'
                  : confirmAction === 'complete'
                    ? 'Yes, pull it'
                    : 'Yes, abandon'}
              </button>
              <button
                type="button"
                onClick={() => setConfirmAction(null)}
                className="flex-1 inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Temperature + prediction chart (T08) */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm p-4">
        <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-3">
          Temperature curve
        </h3>
        <TriTipChart
          event={event}
          readings={readings}
          prediction={prediction}
          references={data?.references ?? []}
        />
      </div>

      {errorMsg && (
        <div className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2 text-sm text-red-700 dark:text-red-300">
          {errorMsg}
        </div>
      )}

    </div>
  );
}
