/**
 * RecipeSteps - 010-005 T07 instructions builder (standalone).
 * One step at a time: instruction text, timer (minutes/seconds inputs,
 * default 1:00, persisted as integer seconds), 0..n ingredient portions
 * drawn from the recipe's own ingredient list (full/partial qty, same
 * ingredient reusable across steps — no exclusivity tracking).
 * Controlled: steps live in the parent (create/edit share it); this owns
 * only the draft form. Token-only classes, 44px targets.
 */
'use client';

import { useCallback, useState } from 'react';
import type { RecipeStep } from '../../../lib/types/food-contract';
import type { SessionIngredient } from './RecipeCreate';
import { EmptyBox, ErrorBox, Section } from './food-ui';

export const DEFAULT_STEP_SECONDS = 60;

/** Split integer seconds into minute/second inputs (clamped, finite). */
export function secondsToParts(total: number): { minutes: number; seconds: number } {
  const t = Number.isFinite(total) && total > 0 ? Math.floor(total) : 0;
  return { minutes: Math.floor(t / 60), seconds: t % 60 };
}

/** Combine minute/second inputs into integer seconds (null when disabled). */
export function partsToSeconds(enabled: boolean, minutes: unknown, seconds: unknown): number | null {
  if (!enabled) return null;
  const m = Number(minutes);
  const s = Number(seconds);
  const mm = Number.isFinite(m) && m > 0 ? Math.floor(m) : 0;
  const ss = Number.isFinite(s) && s > 0 ? Math.floor(s) : 0;
  return mm * 60 + ss;
}

/** One draft portion inside the step form. */
export interface DraftPortion {
  key: number;
  food_id: number;
  qty: string;
  unit: string;
}

/** A step draft as held by the form (timer as parts + enable flag). */
export interface DraftStep {
  instruction: string;
  timerEnabled: boolean;
  minutes: string;
  seconds: string;
  portions: DraftPortion[];
}

export const EMPTY_DRAFT: DraftStep = {
  instruction: '', timerEnabled: true, minutes: '1', seconds: '0', portions: [],
};

let portionKey = 1;

/** Validate + convert a draft into a RecipeStep (step_no assigned by caller). */
export function draftToStep(draft: DraftStep, stepNo: number): { step?: RecipeStep; error?: string } {
  const instruction = draft.instruction.trim();
  if (!instruction) return { error: 'Write the instruction for this step.' };
  const timer_seconds = partsToSeconds(draft.timerEnabled, draft.minutes, draft.seconds);
  const foods: RecipeStep['foods'] = [];
  for (const p of draft.portions) {
    const qty = Number(p.qty);
    if (!Number.isFinite(qty) || qty <= 0) return { error: 'Each portion needs a quantity above zero.' };
    if (!p.unit.trim()) return { error: 'Each portion needs a unit.' };
    foods.push({ food_id: p.food_id, qty, unit: p.unit.trim() });
  }
  return { step: { step_no: stepNo, instruction, timer_seconds, foods } };
}

export default function RecipeSteps({ lines, steps, onChange }: {
  /** The recipe's own ingredient list — portions are drawn from here. */
  lines: SessionIngredient[];
  steps: RecipeStep[];
  onChange: (steps: RecipeStep[]) => void;
}) {
  const [draft, setDraft] = useState<DraftStep>(EMPTY_DRAFT);
  const [editing, setEditing] = useState<number | null>(null);
  const [formErr, setFormErr] = useState<string | null>(null);
  const [attachId, setAttachId] = useState('');
  const [attachQty, setAttachQty] = useState('1');
  const [attachUnit, setAttachUnit] = useState('');

  const resetDraft = useCallback(() => {
    setDraft(EMPTY_DRAFT);
    setEditing(null);
    setFormErr(null);
    setAttachId('');
    setAttachQty('1');
    setAttachUnit('');
  }, []);

  const addPortion = useCallback(() => {
    const line = lines.find((l) => String(l.food_id) === attachId);
    if (!line) {
      setFormErr(lines.length === 0
        ? 'Add ingredients to the recipe first.'
        : 'Pick an ingredient to attach.');
      return;
    }
    const qty = Number(attachQty);
    if (!Number.isFinite(qty) || qty <= 0) {
      setFormErr('Portion quantity must be above zero.');
      return;
    }
    const unit = (attachUnit.trim() || line.unit).trim();
    if (!unit) {
      setFormErr('Portion needs a unit.');
      return;
    }
    setFormErr(null);
    setDraft((d) => ({
      ...d,
      portions: [...d.portions, { key: portionKey++, food_id: line.food_id, qty: String(qty), unit }],
    }));
  }, [attachId, attachQty, attachUnit, lines]);

  const removePortion = useCallback((key: number) => {
    setDraft((d) => ({ ...d, portions: d.portions.filter((p) => p.key !== key) }));
  }, []);

  const submitStep = useCallback(() => {
    const { step, error } = draftToStep(draft, editing ?? steps.length + 1);
    if (!step) {
      setFormErr(error ?? 'This step is not ready yet.');
      return;
    }
    if (editing != null) {
      onChange(steps.map((s) => (s.step_no === editing ? step : s)));
    } else {
      onChange([...steps, step]);
    }
    resetDraft();
  }, [draft, editing, steps, onChange, resetDraft]);

  const startEdit = useCallback((s: RecipeStep) => {
    const parts = s.timer_seconds != null ? secondsToParts(s.timer_seconds) : { minutes: 1, seconds: 0 };
    setEditing(s.step_no);
    setFormErr(null);
    setDraft({
      instruction: s.instruction,
      timerEnabled: s.timer_seconds != null,
      minutes: String(parts.minutes),
      seconds: String(parts.seconds),
      portions: s.foods.map((f) => ({
        key: portionKey++, food_id: f.food_id ?? 0, qty: String(f.qty), unit: f.unit,
      })),
    });
    setAttachId('');
    setAttachQty('1');
    setAttachUnit('');
  }, []);

  const removeStep = useCallback((stepNo: number) => {
    onChange(steps.filter((s) => s.step_no !== stepNo)
      .map((s, i) => ({ ...s, step_no: i + 1 })));
    if (editing === stepNo) resetDraft();
  }, [steps, onChange, editing, resetDraft]);

  const nameOf = (foodId: number) =>
    lines.find((l) => l.food_id === foodId)?.name ?? `Ingredient #${foodId}`;

  const timerLabel = (t: number | null) =>
    t == null ? 'No timer' : t >= 60 ? `${Math.floor(t / 60)}m ${t % 60}s` : `${t}s`;

  return (
    <div className="space-y-4" data-testid="recipe-steps">
      <Section title={`Steps (${steps.length})`}>
        {steps.length === 0 ? (
          <EmptyBox title="Steps" message="No steps yet - add the first one below. Saving without steps stays available." />
        ) : (
          <ol className="space-y-1.5" aria-label="Recipe steps">
            {steps.map((s) => (
              <li key={s.step_no}
                className="flex w-full items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2.5 min-h-[44px] text-sm">
                <span className="min-w-0">
                  <span className="block font-medium text-gray-900 dark:text-white">Step {s.step_no}: {s.instruction}</span>
                  <span className="text-xs text-gray-500 dark:text-gray-400">
                    {timerLabel(s.timer_seconds)}
                    {s.foods.length > 0 && ` - ${s.foods.map((f) => `${f.qty} ${f.unit} ${nameOf(f.food_id ?? 0)}`).join(', ')}`}
                  </span>
                </span>
                <span className="flex shrink-0 gap-1">
                  <button type="button" onClick={() => startEdit(s)} aria-label={`Edit step ${s.step_no}`}
                    className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
                    Edit
                  </button>
                  <button type="button" onClick={() => removeStep(s.step_no)} aria-label={`Remove step ${s.step_no}`}
                    className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
                    Remove
                  </button>
                </span>
              </li>
            ))}
          </ol>
        )}
      </Section>
      <Section title={editing != null ? `Edit step ${editing}` : `Add step ${steps.length + 1}`}>
        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-step-text">Instruction</label>
            <textarea id="recipe-step-text" value={draft.instruction} rows={2}
              onChange={(e) => setDraft((d) => ({ ...d, instruction: e.target.value }))}
              placeholder="e.g. Whisk the oats into the milk."
              className="mt-1 w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
          </div>
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-200 min-h-[44px]">
              <input type="checkbox" checked={draft.timerEnabled}
                onChange={(e) => setDraft((d) => ({ ...d, timerEnabled: e.target.checked }))}
                className="h-5 w-5" />
              Timer
            </label>
            {draft.timerEnabled && (
              <>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-step-min">Minutes</label>
                  <input id="recipe-step-min" type="number" min={0} step={1} value={draft.minutes}
                    onChange={(e) => setDraft((d) => ({ ...d, minutes: e.target.value }))}
                    className="mt-1 w-20 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-step-sec">Seconds</label>
                  <input id="recipe-step-sec" type="number" min={0} max={59} step={1} value={draft.seconds}
                    onChange={(e) => setDraft((d) => ({ ...d, seconds: e.target.value }))}
                    className="mt-1 w-20 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
                </div>
              </>
            )}
          </div>
          <div>
            <span className="block text-sm font-medium text-gray-700 dark:text-gray-200" id="recipe-step-attach">Ingredient portions</span>
            {draft.portions.length > 0 && (
              <ul className="mt-1 space-y-1" aria-label="Attached portions">
                {draft.portions.map((p) => (
                  <li key={p.key} className="flex items-center justify-between gap-2 text-sm">
                    <span className="text-gray-700 dark:text-gray-200">{p.qty} {p.unit} {nameOf(p.food_id)}</span>
                    <button type="button" onClick={() => removePortion(p.key)} aria-label={`Remove ${nameOf(p.food_id)} portion`}
                      className="rounded-md border border-gray-300 dark:border-gray-600 px-2 py-1 min-h-[44px] text-xs text-gray-700 dark:text-gray-300">
                      Remove
                    </button>
                  </li>
                ))}
              </ul>
            )}
            <div className="mt-1 flex flex-wrap items-end gap-2" role="group" aria-labelledby="recipe-step-attach">
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400" htmlFor="recipe-attach-pick">Ingredient</label>
                <select id="recipe-attach-pick" value={attachId}
                  onChange={(e) => {
                    const id = e.target.value;
                    setAttachId(id);
                    const line = lines.find((l) => String(l.food_id) === id);
                    if (line) setAttachUnit(line.unit);
                  }}
                  className="mt-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white">
                  <option value="">Pick...</option>
                  {lines.map((l) => <option key={l.food_id} value={String(l.food_id)}>{l.name} ({l.qty} {l.unit})</option>)}
                </select>
              </div>
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400" htmlFor="recipe-attach-qty">Qty</label>
                <input id="recipe-attach-qty" type="number" min={0} step="any" value={attachQty}
                  onChange={(e) => setAttachQty(e.target.value)}
                  className="mt-1 w-20 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
              </div>
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400" htmlFor="recipe-attach-unit">Unit</label>
                <input id="recipe-attach-unit" value={attachUnit} onChange={(e) => setAttachUnit(e.target.value)}
                  placeholder="g" autoComplete="off"
                  className="mt-1 w-20 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
              </div>
              <button type="button" onClick={addPortion}
                className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
                Attach
              </button>
            </div>
          </div>
          {formErr != null && <ErrorBox label={editing != null ? `Edit step ${editing}` : 'Add step'} message={formErr} />}
          <div className="flex gap-2">
            {editing != null && (
              <button type="button" onClick={resetDraft}
                className="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300">
                Cancel
              </button>
            )}
            <button type="button" onClick={submitStep}
              className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
              {editing != null ? `Save step ${editing}` : `Add step ${steps.length + 1}`}
            </button>
          </div>
        </div>
      </Section>
    </div>
  );
}
