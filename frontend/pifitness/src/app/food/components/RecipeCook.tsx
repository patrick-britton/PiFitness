/**
 * RecipeCook - 010-005 T08 cook mode (part 1: helpers + shell).
 * All steps + timers at once; reorder checklist; independent countdowns
 * from server absolute deadlines; red flash until Done; wake-lock;
 * hydrate/persist via cook-state endpoints. Token-only classes, 44px.
 */
'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { API } from '../../../lib/api-client';
import type {
  CookFoodCheck,
  CookState,
  CookTimer,
  RecipeDetail,
} from '../../../lib/types/food-contract';
import { useViewportStore } from '../../../stores/viewportStore';
import { EmptyBox, ErrorBox, LoadingSkeleton, Section } from './food-ui';

/** Identity of one step-ingredient checkbox — must invert CookFoodCheck exactly. */
export function foodCheckKey(check: Pick<CookFoodCheck, 'step_no' | 'food_id' | 'unit'>): string {
  return `${check.step_no}|${check.food_id}|${check.unit}`;
}

/** Remaining whole seconds from an absolute deadline (clamped at 0). */
export function remainingSeconds(endsAt: string, nowMs: number): number {
  const t = Date.parse(endsAt);
  if (!Number.isFinite(t)) return 0;
  return Math.max(0, Math.ceil((t - nowMs) / 1000));
}

/** Human countdown label: whole minutes + seconds (never negative). */
export function formatCountdown(totalSeconds: number): string {
  const t = Math.max(0, Math.floor(totalSeconds));
  return `${Math.floor(t / 60)}:${String(t % 60).padStart(2, '0')}`;
}

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export interface RecipeCookProps {
  recipeId: number;
  onBack: () => void;
  /** T08 reports completion; T09 owns the edit re-entry + re-sort. */
  onCompleted: () => void;
  onEdit: () => void;
}
export default function RecipeCook({ recipeId, onBack, onCompleted, onEdit }: RecipeCookProps) {
  const { layoutVariant } = useViewportStore();
  const twoCol = layoutVariant === 'desktop' || layoutVariant === 'landscape';

  const [detail, setDetail] = useState<RecipeDetail | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [checkedSteps, setCheckedSteps] = useState<number[]>([]);
  const [checkedFoods, setCheckedFoods] = useState<CookFoodCheck[]>([]);
  const [timers, setTimers] = useState<CookTimer[]>([]);
  /** Steps whose timer the user cleared with Done — never auto-restarted. */
  const [cleared, setCleared] = useState<number[]>([]);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [completing, setCompleting] = useState(false);
  /** T09: confirm-no-undo — completing bumps times_prepared and clears cook state. */
  const [confirmComplete, setConfirmComplete] = useState(false);
  const [nowMs, setNowMs] = useState(() => Date.now());

  const stateRef = useRef({ checkedSteps, checkedFoods, timers });
  stateRef.current = { checkedSteps, checkedFoods, timers };
  const persistTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Hydrate: recipe detail + persisted session (empty lists = no session).
  useEffect(() => {
    let live = true;
    setLoading(true);
    setLoadError(null);
    (async () => {
      try {
        const [d, s]: [RecipeDetail, CookState] = await Promise.all([
          API.food.recipeDetail(recipeId),
          API.food.getCookState(recipeId),
        ]);
        if (!live) return;
        setDetail(d);
        setCheckedSteps([...s.checked_steps]);
        setCheckedFoods([...s.checked_foods]);
        setTimers([...s.timers]);
      } catch (error) {
        if (!live) return;
        setLoadError(errMsg(error));
      } finally {
        if (live) setLoading(false);
      }
    })();
    return () => { live = false; };
  }, [recipeId]);

  // Debounced persist of the full session (server is full-replace).
  const persist = useCallback((next: {
    checkedSteps: number[]; checkedFoods: CookFoodCheck[]; timers: CookTimer[];
  }) => {
    if (persistTimer.current) clearTimeout(persistTimer.current);
    persistTimer.current = setTimeout(() => {
      API.food.saveCookState(recipeId, {
        checked_steps: next.checkedSteps,
        checked_foods: next.checkedFoods,
        timers: next.timers,
      }).catch((error: unknown) => setSaveError(errMsg(error)));
    }, 300);
  }, [recipeId]);

  useEffect(() => () => {
    if (persistTimer.current) clearTimeout(persistTimer.current);
  }, []);

  const update = useCallback((next: {
    checkedSteps: number[]; checkedFoods: CookFoodCheck[]; timers: CookTimer[];
  }) => {
    setCheckedSteps(next.checkedSteps);
    setCheckedFoods(next.checkedFoods);
    setTimers(next.timers);
    persist(next);
  }, [persist]);

  // 1s tick — countdowns derive from absolute deadlines, never a local tick.
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  // Wake lock while cooking (graceful fallback where unsupported/denied).
  useEffect(() => {
    let lock: { release?: () => Promise<void> } | null = null;
    let cancelled = false;
    (async () => {
      try {
        const nav = navigator as Navigator & {
          wakeLock?: { request: (kind: string) => Promise<{ release?: () => Promise<void> }> };
        };
        if (typeof navigator === 'undefined' || nav.wakeLock == null) return;
        const got = await nav.wakeLock.request('screen');
        if (!cancelled) lock = got;
        else await got.release?.();
      } catch {
        // Unsupported or denied — cooking continues without the lock.
      }
    })();
    return () => {
      cancelled = true;
      lock?.release?.().catch(() => {});
    };
  }, []);

  const toggleStep = useCallback((stepNo: number) => {
    const cur = stateRef.current;
    const on = cur.checkedSteps.includes(stepNo);
    update({
      ...cur,
      checkedSteps: on
        ? cur.checkedSteps.filter((s) => s !== stepNo)
        : [...cur.checkedSteps, stepNo],
    });
  }, [update]);

  const toggleFood = useCallback((check: CookFoodCheck) => {
    const cur = stateRef.current;
    const key = foodCheckKey(check);
    const on = cur.checkedFoods.some((c) => foodCheckKey(c) === key);
    update({
      ...cur,
      checkedFoods: on
        ? cur.checkedFoods.filter((c) => foodCheckKey(c) !== key)
        : [...cur.checkedFoods, check],
    });
  }, [update]);

  const startTimer = useCallback((stepNo: number, seconds: number) => {
    const cur = stateRef.current;
    const endsAt = new Date(Date.now() + Math.max(1, seconds) * 1000).toISOString();
    setCleared((prev) => prev.filter((s) => s !== stepNo));
    update({
      ...cur,
      timers: [
        ...cur.timers.filter((t) => t.step_no !== stepNo),
        { step_no: stepNo, ends_at: endsAt },
      ],
    });
  }, [update]);

  const clearTimer = useCallback((stepNo: number) => {
    const cur = stateRef.current;
    setCleared((prev) => (prev.includes(stepNo) ? prev : [...prev, stepNo]));
    update({ ...cur, timers: cur.timers.filter((t) => t.step_no !== stepNo) });
  }, [update]);
  const orderedSteps = useMemo(() => {
    if (!detail) return [];
    const done = new Set(checkedSteps);
    // Completed sink to the bottom (relative order kept in each group);
    // unchecking restores the original position by re-sorting.
    return [...detail.steps].sort((a, b) => {
      const da = done.has(a.step_no) ? 1 : 0;
      const db = done.has(b.step_no) ? 1 : 0;
      return da - db || a.step_no - b.step_no;
    });
  }, [detail, checkedSteps]);

  const expired = useMemo(() => timers.filter((t) =>
    remainingSeconds(t.ends_at, nowMs) <= 0 && !cleared.includes(t.step_no)),
  [timers, cleared, nowMs]);

  const onComplete = useCallback(async () => {
    setCompleting(true);
    setSaveError(null);
    try {
      await API.food.completeRecipe(recipeId);
      onCompleted();
    } catch (error) {
      setSaveError(errMsg(error));
    } finally {
      setCompleting(false);
      setConfirmComplete(false);
    }
  }, [recipeId, onCompleted]);

  if (loading) return <LoadingSkeleton rows={5} testId="recipe-cook" />;
  if (loadError != null || detail == null) {
    return (
      <div className="space-y-4" data-testid="recipe-cook">
        <button type="button" onClick={onBack}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          ← Back to recipes
        </button>
        <ErrorBox label="Recipe" message={loadError ?? 'Recipe not found.'} />
      </div>
    );
  }
  const expiredFlash = expired.length > 0;
  return (
    <div className="space-y-4" data-testid="recipe-cook">
      {expiredFlash && (
        <div role="alert" data-testid="cook-timer-expired"
          className="rounded-lg border border-red-600 bg-red-600 p-4 text-sm font-semibold text-white">
          {expired.length === 1
            ? `Step ${expired[0].step_no} timer is done.`
            : `${expired.length} step timers are done.`}{' '}
          Clear each with its Done button.
        </div>
      )}
      <div className="flex flex-wrap items-center gap-2">
        <button type="button" onClick={onBack}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          ← Back to recipes
        </button>
        <h2 className="text-base font-semibold text-gray-900 dark:text-white">{detail.title}</h2>
      </div>
      {saveError != null && <ErrorBox label="Cook state" message={saveError} />}
      {detail.steps.length === 0 ? (
        <EmptyBox title="No steps" message="This recipe has no instructions — Complete when you're done cooking." />
      ) : (
        <ol aria-label="Cook steps"
          className={twoCol ? 'grid grid-cols-2 items-start gap-3' : 'space-y-3'}>
          {orderedSteps.map((step) => {
            const done = checkedSteps.includes(step.step_no);
            const timer = timers.find((t) => t.step_no === step.step_no) ?? null;
            const remaining = timer ? remainingSeconds(timer.ends_at, nowMs) : null;
            const isExpired = timer != null && remaining != null && remaining <= 0
              && !cleared.includes(step.step_no);
            const stepLabel = done
              ? 'block text-sm italic text-gray-500 dark:text-gray-400 line-through'
              : 'block text-sm text-gray-900 dark:text-white';
            return (
              <li key={step.step_no} data-testid={`cook-step-${step.step_no}`}
                className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-3">
                <label className="flex cursor-pointer items-start gap-2 min-h-[44px]">
                  <input type="checkbox" checked={done}
                    onChange={() => toggleStep(step.step_no)}
                    aria-label={`Step ${step.step_no} done`}
                    className="mt-1 h-5 w-5 accent-blue-600" />
                  <span className="flex-1">
                    <span className="text-xs font-medium text-gray-500 dark:text-gray-400">
                      Step {step.step_no}
                    </span>
                    <span className={stepLabel}>{step.instruction}</span>
                  </span>
                </label>
                <StepFoods stepNo={step.step_no} foods={step.foods}
                  checkedFoods={checkedFoods} onToggle={toggleFood} />
                <StepTimer stepNo={step.step_no} seconds={step.timer_seconds}
                  timer={timer} remaining={remaining} isExpired={isExpired}
                  isCleared={cleared.includes(step.step_no)}
                  onStart={startTimer} onDone={clearTimer} />
              </li>
            );
          })}
        </ol>
      )}
      <Section title="Finish">
        <div className="flex flex-wrap gap-2">
          <button type="button" onClick={() => setConfirmComplete(true)} disabled={completing}
            className="rounded-md bg-blue-600 hover:bg-blue-700 disabled:opacity-50 px-4 py-2 min-h-[44px] text-sm font-medium text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
            {completing ? 'Completing…' : 'Complete recipe'}
          </button>
          <button type="button" onClick={onEdit}
            className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
            Edit recipe
          </button>
        </div>
      </Section>
      {confirmComplete && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" role="dialog" aria-modal="true" aria-label="Complete recipe">
          <div className="w-full max-w-md space-y-3 rounded-lg bg-white dark:bg-gray-800 p-4 shadow-xl">
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">
              Mark {detail.title} complete?
            </h3>
            <p className="text-sm text-gray-700 dark:text-gray-200">
              This counts one more preparation and clears the cook state. There is no undo.
            </p>
            <div className="flex gap-2">
              <button type="button" disabled={completing}
                onClick={() => setConfirmComplete(false)}
                className="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 disabled:opacity-50">
                Cancel
              </button>
              <button type="button" disabled={completing} onClick={() => void onComplete()}
                className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white disabled:opacity-50">
                {completing ? 'Completing…' : 'Yes, complete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function StepFoods({ stepNo, foods, checkedFoods, onToggle }: {
  stepNo: number;
  foods: RecipeDetail['steps'][number]['foods'];
  checkedFoods: CookFoodCheck[];
  onToggle: (check: CookFoodCheck) => void;
}) {
  if (foods.length === 0) return null;
  return (
    <ul aria-label={`Step ${stepNo} ingredients`} className="mt-2 space-y-1 pl-7">
      {foods.map((f, i) => {
        const key = foodCheckKey({ step_no: stepNo, food_id: f.food_id, unit: f.unit });
        const foodOn = checkedFoods.some((c) => foodCheckKey(c) === key);
        return (
          <li key={`${f.food_id}-${f.unit}-${i}`}>
            <label className="flex cursor-pointer items-center gap-2 min-h-[44px] text-sm">
              <input type="checkbox" checked={foodOn}
                onChange={() => onToggle({ step_no: stepNo, food_id: f.food_id, unit: f.unit })}
                aria-label={`Step ${stepNo} ingredient ${f.name ?? f.food_id} ${f.qty} ${f.unit}`}
                className="h-5 w-5 accent-blue-600" />
              <span className={foodOn
                ? 'italic text-gray-500 dark:text-gray-400 line-through'
                : 'text-gray-700 dark:text-gray-200'}>
                {f.name ?? `Food #${f.food_id}`} — {f.qty} {f.unit}
              </span>
            </label>
          </li>
        );
      })}
    </ul>
  );
}

function StepTimer({ stepNo, seconds, timer, remaining, isExpired, isCleared, onStart, onDone }: {
  stepNo: number;
  seconds: number | null;
  timer: CookTimer | null;
  remaining: number | null;
  isExpired: boolean;
  isCleared: boolean;
  onStart: (stepNo: number, seconds: number) => void;
  onDone: (stepNo: number) => void;
}) {
  if (seconds == null) return null;
  if (timer == null || isCleared) {
    return (
      <div className="mt-2 flex flex-wrap items-center gap-2 pl-7">
        <button type="button" onClick={() => onStart(stepNo, seconds)}
          aria-label={`Start step ${stepNo} timer`}
          className="rounded-md bg-blue-600 hover:bg-blue-700 px-3 py-1.5 min-h-[44px] text-sm font-medium text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          Start {formatCountdown(seconds)} timer
        </button>
      </div>
    );
  }
  return (
    <div className="mt-2 flex flex-wrap items-center gap-2 pl-7">
      <span data-testid={`cook-timer-${stepNo}`} aria-live="polite"
        className={isExpired
          ? 'rounded-md bg-red-600 px-2 py-1 text-sm font-bold text-white'
          : 'rounded-md bg-gray-100 dark:bg-gray-700 px-2 py-1 text-sm font-medium text-gray-900 dark:text-white'}>
        {formatCountdown(remaining ?? 0)}
      </span>
      <button type="button" onClick={() => onDone(stepNo)}
        aria-label={`Timer done for step ${stepNo}`}
        className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
        Done
      </button>
    </div>
  );
}
