/**
 * RecipeCreate - 010-005 T06 create-recipe workflow (part 1: helpers).
 * Title -> ingredients (shared FoodSearchPicker) -> servings + unit +
 * category (OQ-1 set) -> save via POST /api/food/recipes, steps: [].
 * Persist-at-pick (OQ-3, T12) via POST /api/food/persist; abandonment
 * writes nothing to recipe tables. Token-only classes, 44px targets.
 */
'use client';

import { useCallback, useMemo, useState } from 'react';
import { API } from '../../../lib/api-client';
import { previewKcal } from '../../../lib/food-units';
import type {
  RecipeCategory,
  RecipeIngredient,
  RecipeStep,
  SaveRecipeRequest,
  RecipeSummary,
} from '../../../lib/types/food-contract';
import { useViewportStore } from '../../../stores/viewportStore';
import AmountPicker, { type AmountUnit } from './AmountPicker';
import FoodSearchPicker, { defaultAmount, errMsg, type FoodPick } from './FoodSearchPicker';
import RecipeSteps from './RecipeSteps';
import { EmptyBox, ErrorBox, Section } from './food-ui';

export const RECIPE_CATEGORIES: RecipeCategory[] = [
  'Breakfast', 'Lunch', 'Dinner', 'Side', 'Dessert', 'Drink',
];

/** One session ingredient line - every line carries a real food_id. */
export interface SessionIngredient {
  food_id: number;
  name: string;
  qty: number;
  unit: string;
  kcalPer100g: number | null;
}

export function previewTotals(
  lines: Pick<SessionIngredient, 'qty' | 'unit' | 'kcalPer100g'>[],
): { perLines: (number | null)[]; total: number | null } {
  const perLines = lines.map((l) => previewKcal(l.kcalPer100g, l.qty, l.unit));
  const known = perLines.filter((v): v is number => v != null);
  return { perLines, total: known.length > 0 ? known.reduce((a, b) => a + b, 0) : null };
}

export function buildSavePayload(args: {
  title: string;
  category: RecipeCategory;
  servingsCount: number;
  servingUnit: string;
  lines: SessionIngredient[];
  steps?: RecipeStep[];
}): SaveRecipeRequest {
  const ingredients: RecipeIngredient[] = args.lines.map((l) => ({
    food_id: l.food_id, qty: l.qty, unit: l.unit,
  }));
  return {
    title: args.title.trim(),
    category: args.category,
    ingredients,
    servings_count: args.servingsCount,
    serving_unit: args.servingUnit.trim(),
    steps: args.steps ?? [],
  };
}

export interface RecipeEditInitial {
  recipeId: number;
  title: string;
  category: RecipeCategory;
  servingsCount: number;
  servingUnit: string;
  lines: SessionIngredient[];
  steps: RecipeStep[];
}

/** Convert a RecipeDetail into edit-mode initial values (names may be null → fallback). */
export function detailToEditInitial(
  detail: Pick<
    import('../../../lib/types/food-contract').RecipeDetail,
    'recipe_id' | 'title' | 'category' | 'servings_count' | 'serving_unit'
      | 'ingredients' | 'steps'
  >,
): RecipeEditInitial {
  return {
    recipeId: detail.recipe_id,
    title: detail.title,
    category: detail.category,
    servingsCount: detail.servings_count ?? 1,
    servingUnit: detail.serving_unit ?? 'serving',
    lines: detail.ingredients.map((l) => ({
      food_id: l.food_id,
      name: l.name ?? `Food #${l.food_id}`,
      qty: l.qty,
      unit: l.unit,
      kcalPer100g: null,
    })),
    steps: detail.steps.map((s) => ({
      step_no: s.step_no,
      instruction: s.instruction,
      timer_seconds: s.timer_seconds,
      foods: s.foods.map((f) => ({ food_id: f.food_id, qty: f.qty, unit: f.unit })),
    })),
  };
}

export default function RecipeCreate({ onBack, onSaved, edit }: {
  onBack: () => void;
  onSaved: (saved: RecipeSummary) => void;
  /** T09 edit mode: prefill from detail, save via PUT, delete behind confirm. */
  edit?: RecipeEditInitial;
}) {
  const { layoutVariant } = useViewportStore();
  const twoCol = layoutVariant === 'desktop' || layoutVariant === 'landscape';

  const [title, setTitle] = useState(edit?.title ?? '');
  const [category, setCategory] = useState<RecipeCategory>(edit?.category ?? 'Dinner');
  const [servingsCount, setServingsCount] = useState(String(edit?.servingsCount ?? 4));
  const [servingUnit, setServingUnit] = useState(edit?.servingUnit ?? 'serving');
  const [lines, setLines] = useState<SessionIngredient[]>(edit?.lines ?? []);
  const [steps, setSteps] = useState<RecipeStep[]>(edit?.steps ?? []);
  const [confirm, setConfirm] = useState<FoodPick | null>(null);
  const [confirmName, setConfirmName] = useState('');
  const [confirmQty, setConfirmQty] = useState(100);
  const [confirmUnit, setConfirmUnit] = useState<AmountUnit>('g');
  const [persisting, setPersisting] = useState(false);
  const [persistErr, setPersistErr] = useState<string | null>(null);
  const [saveErr, setSaveErr] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const openConfirm = useCallback((pick: FoodPick) => {
    const d = defaultAmount(pick);
    setConfirm(pick);
    setConfirmName(pick.name);
    setConfirmQty(d.qty);
    setConfirmUnit((d.unit as AmountUnit) ?? 'g');
    setPersistErr(null);
  }, []);

  const addLine = useCallback(async () => {
    if (!confirm || persisting) return;
    const name = confirmName.trim();
    const qty = Number(confirmQty);
    if (!name || !Number.isFinite(qty) || qty <= 0) {
      setPersistErr(!name ? 'Name is required.' : 'Quantity must be positive.');
      return;
    }
    setPersisting(true);
    setPersistErr(null);
    try {
      let foodId = confirm.food_id;
      if (foodId == null) {
        if (!confirm.payload) {
          setPersistErr('This pick cannot be saved - pick another result.');
          return;
        }
        const key = confirm.payload.provider === 'usda' ? 'usda_payload' : 'off_payload';
        const res = await API.food.persistApiFood({
          [key]: { ...confirm.payload, name },
        });
        foodId = res.food_id;
      }
      const line: SessionIngredient = {
        food_id: foodId as number, name, qty, unit: confirmUnit,
        kcalPer100g: confirm.kcalPer100g,
      };
      setLines((prev) => [...prev, line]);
      setConfirm(null);
    } catch (e) {
      setPersistErr(errMsg(e));
    } finally {
      setPersisting(false);
    }
  }, [confirm, confirmName, confirmQty, confirmUnit, persisting]);

  const removeLine = useCallback((idx: number) => {
    setLines((prev) => prev.filter((_, i) => i !== idx));
  }, []);

  const servingsNum = Number(servingsCount);
  const titleOk = title.trim().length > 0;
  const servingsOk = Number.isFinite(servingsNum) && servingsNum > 0
    && Number.isInteger(servingsNum);
  const unitOk = servingUnit.trim().length > 0;
  const canSave = titleOk && lines.length > 0 && servingsOk && unitOk && !saving;
  const gateMsg = !titleOk ? 'Enter a title.'
    : lines.length === 0 ? 'Add at least one ingredient.'
    : !servingsOk ? 'Servings must be a whole number above zero.'
    : !unitOk ? 'Enter a serving unit label (e.g. slice).'
    : null;

  const { perLines, total } = useMemo(() => previewTotals(lines), [lines]);
  const perServing = total != null && servingsOk ? total / servingsNum : null;

  const save = useCallback(async () => {
    if (!canSave) return;
    setSaving(true);
    setSaveErr(null);
    try {
      const payload = buildSavePayload({
        title, category, servingsCount: servingsNum,
        servingUnit, lines, steps,
      });
      const saved = edit
        ? await API.food.updateRecipe(edit.recipeId, payload)
        : await API.food.saveRecipe(payload);
      onSaved(saved);
    } catch (e) {
      setSaveErr(errMsg(e));
    } finally {
      setSaving(false);
    }
  }, [canSave, title, category, servingsNum, servingUnit, lines, steps, edit, onSaved]);

  const [deleting, setDeleting] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const remove = useCallback(async () => {
    if (!edit || deleting) return;
    setDeleting(true);
    setSaveErr(null);
    try {
      await API.food.deleteRecipe(edit.recipeId);
      onBack();
    } catch (e) {
      setSaveErr(errMsg(e));
    } finally {
      setDeleting(false);
      setConfirmDelete(false);
    }
  }, [edit, deleting, onBack]);
  const previewLine = (i: number) => perLines[i] != null
    ? ` - approx ${Math.round(perLines[i] as number)} kcal` : ' - kcal n/a';
  const previewText = total == null
    ? 'Nutrition preview unavailable for these portions.'
    : `approx ${Math.round(total)} kcal total`;

  return (
    <div className="space-y-4" data-testid="recipe-create">
      <div className="flex flex-wrap items-center gap-2">
        <button type="button" onClick={onBack}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          Back to recipes
        </button>
        <h2 className="text-base font-semibold text-gray-900 dark:text-white">{edit ? 'Edit recipe' : 'Create recipe'}</h2>
      </div>
      <div className={twoCol ? 'grid grid-cols-2 gap-4 items-start' : 'space-y-4'}>
        <div className="space-y-4">
          <Section title="Details">
            <div className="space-y-3">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-title">Title</label>
                <input id="recipe-title" value={title} onChange={(e) => setTitle(e.target.value)}
                  placeholder="e.g. Pancakes" autoComplete="off"
                  className="mt-1 w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
              </div>
              <div className="flex flex-wrap gap-3">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-servings">Servings</label>
                  <input id="recipe-servings" type="number" min={1} step={1} value={servingsCount}
                    onChange={(e) => setServingsCount(e.target.value)}
                    className="mt-1 w-24 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
                </div>
                <div className="flex-1 min-w-32">
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-unit">Serving unit</label>
                  <input id="recipe-unit" value={servingUnit} onChange={(e) => setServingUnit(e.target.value)}
                    placeholder="e.g. slice" autoComplete="off"
                    className="mt-1 w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-category">Category</label>
                  <select id="recipe-category" value={category}
                    onChange={(e) => setCategory(e.target.value as RecipeCategory)}
                    className="mt-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white">
                    {RECIPE_CATEGORIES.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
              </div>
              <p className="text-xs text-gray-500 dark:text-gray-400" aria-live="polite">{previewText}</p>
            </div>
          </Section>
          <Section title="Find ingredients">
            <FoodSearchPicker onPick={openConfirm}
              leading={<span className="text-xs text-gray-500 dark:text-gray-400">Add ingredients</span>} />
          </Section>
        </div>
        <div className="space-y-4">
          <Section title={`Ingredients (${lines.length})`}>
            {lines.length === 0 ? (
              <EmptyBox title="Ingredients" message="No ingredients yet - search above to add some." />
            ) : (
              <ul className="space-y-1.5" aria-label="Ingredient lines">
                {lines.map((l, i) => (
                  <li key={`${l.food_id}-${i}`}
                    className="flex w-full items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2.5 min-h-[44px] text-sm">
                    <span className="min-w-0">
                      <span className="block truncate font-medium text-gray-900 dark:text-white">{l.name}</span>
                      <span className="text-xs text-gray-500 dark:text-gray-400">{l.qty} {l.unit}{previewLine(i)}</span>
                    </span>
                    <button type="button" onClick={() => removeLine(i)} aria-label={`Remove ${l.name}`}
                      className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
                      Remove
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </Section>
          <RecipeSteps lines={lines} steps={steps} onChange={setSteps} />
          <div className="space-y-2">
            {gateMsg != null && <p className="text-xs text-gray-500 dark:text-gray-400" role="status">{gateMsg}</p>}
            {saveErr != null && <ErrorBox label="Save recipe" message={saveErr} />}
            <button type="button" onClick={() => void save()} disabled={!canSave}
              className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
              {saving ? 'Saving...' : edit ? 'Save changes' : 'Save recipe'}
            </button>
          </div>
          {edit && (
            <div className="space-y-2 border-t border-gray-200 dark:border-gray-700 pt-4">
              <button type="button" onClick={() => setConfirmDelete(true)} disabled={deleting}
                className="rounded-md border border-red-300 dark:border-red-700 px-4 py-2 min-h-[44px] text-sm font-medium text-red-700 dark:text-red-300 hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-500">
                Delete recipe
              </button>
            </div>
          )}
        </div>
      </div>
      {confirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" role="dialog" aria-modal="true" aria-label="Add ingredient">
          <div className="w-full max-w-md space-y-3 rounded-lg bg-white dark:bg-gray-800 p-4 shadow-xl">
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">
              {confirm.payload ? 'Confirm new ingredient' : 'Add ingredient'}
            </h3>
            {confirm.payload ? (
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-200" htmlFor="recipe-pick-name">Name</label>
                <input id="recipe-pick-name" value={confirmName}
                  onChange={(e) => setConfirmName(e.target.value)} autoComplete="off"
                  className="mt-1 w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
              </div>
            ) : (
              <p className="text-sm text-gray-700 dark:text-gray-200">{confirm.name}</p>
            )}
            <AmountPicker value={{ qty: confirmQty, unit: confirmUnit }}
              extraUnit={confirm.recipe_serving_unit ?? null}
              onChange={(a) => { setConfirmQty(a.qty); setConfirmUnit(a.unit); }} />
            {persistErr != null && <ErrorBox label="Add ingredient" message={persistErr} />}
            <div className="flex gap-2">
              <button type="button" disabled={persisting}
                onClick={() => { if (!persisting) setConfirm(null); }}
                className="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 disabled:opacity-50">
                Cancel
              </button>
              <button type="button" disabled={persisting} onClick={() => void addLine()}
                className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white disabled:opacity-50">
                {persisting ? 'Adding...' : 'Add'}
              </button>
            </div>
          </div>
        </div>
      )}
      {confirmDelete && edit && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" role="dialog" aria-modal="true" aria-label="Delete recipe">
          <div className="w-full max-w-md space-y-3 rounded-lg bg-white dark:bg-gray-800 p-4 shadow-xl">
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">
              Delete {edit.title || 'recipe'}?
            </h3>
            <p className="text-sm text-gray-700 dark:text-gray-200">
              This hides the recipe from the box and diary search. There is no undo.
            </p>
            <div className="flex gap-2">
              <button type="button" disabled={deleting}
                onClick={() => setConfirmDelete(false)}
                className="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 disabled:opacity-50">
                Cancel
              </button>
              <button type="button" disabled={deleting} onClick={() => void remove()}
                className="rounded-md bg-red-600 hover:bg-red-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white disabled:opacity-50">
                {deleting ? 'Deleting...' : 'Yes, delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}



