/**
 * Add-entry mode for the Food Diary (010-003 T06; 010-005 T06 extracted the
 * shared search/scan/suggest picker into FoodSearchPicker — Diary and the
 * Recipe Box create flow consume the same component; AddEntry keeps the
 * diary-specific confirm popup + save).
 */
'use client';
import { useCallback, useState } from 'react';
import { API } from '../../../lib/api-client';
import { previewKcal } from '../../../lib/food-units';
import type { CombinedSearchResult, NutrientSnapshot } from '../../../lib/types/food-contract';
import AmountPicker, { type AmountUnit } from './AmountPicker';
import FoodSearchPicker, {
  errMsg,
  SOURCE_ICON,
  SOURCE_STYLE,
  defaultAmount,
  type FoodPick,
} from './FoodSearchPicker';
/** Back-compat alias — the pick shape now lives in FoodSearchPicker (010-005 T06). */
export type AddEntryPick = FoodPick;

/** 010-004 T04 (OQ-7): bar references for the confirm panel. Column 1 —
 * kcal/2000; carbs/protein/fat against their group max (10 g floor when all
 * are zero/missing). Column 2 — fixed Daily-Value style references. */
export const NUTRIENT_BAR_REFS = { kcal: 2000, fiber: 28, sugars: 50, sodium: 2300, caffeine: 400 } as const;
export function macroBarRef(n: { carbs_g?: number | null; protein_g?: number | null; fat_g?: number | null }): number {
  const vals = [n.carbs_g, n.protein_g, n.fat_g].filter((v): v is number => v != null && Number.isFinite(v));
  const max = vals.length ? Math.max(...vals) : 0;
  return max > 0 ? max : 10;
}
/** Bar fill percent (0-100) for a value against its reference; null value → 0 (em-dash shown, no fill). */
export function barPct(value: number | null | undefined, ref: number): number {
  if (value == null || !Number.isFinite(value) || ref <= 0) return 0;
  return Math.min(100, (value / ref) * 100);
}
/** One label+value row with a token-only CSS bar (mirrors Diary StatBars: track + fill, meaning never color-alone). */
export function NutrientBar({ label, value, unit, pct }: {
  label: string; value: number | null | undefined; unit: string; pct: number;
}) {
  const shown = value == null || !Number.isFinite(value) ? '—' : String(Math.round(value * 10) / 10);
  const unitTxt = unit ? ` ${unit}` : '';
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2 text-xs text-gray-600 dark:text-gray-300">
        <span>{label}</span>
        <span className="font-medium text-gray-900 dark:text-white">{shown}{unitTxt}</span>
      </div>
      <div className="mt-0.5 h-1.5 overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700"
        role="img" aria-label={`${label}: ${shown}${unitTxt}`}>
        <div className="h-full rounded-full bg-blue-600 dark:bg-blue-400" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

/** Confirm popup: editable name for API picks, editable qty + unit, live kcal, Confirm/Cancel.
 *
 * T08 (OQ-6): `editableName` is true only for API/Barcode picks (payload
 * present). DB picks keep the static title — no rename. Empty name blocks
 * save with inline validation.
 * T10 (OQ-8): opens on `defaultAmount` (mode → feed serving → 100 g), shows
 * the "Saved to your diary" panel (per-100g nutrients + serving + source),
 * and closes ONLY via Cancel/Confirm — the backdrop is inert.
 * 010-005 T10 (AC-10): recipe picks open at qty 1 in their own serving
 * label (`extraUnit` offers it in the unit set) and the live kcal preview
 * resolves that label through the row's grams-per-unit (`ownServing`).
 * 010-004 T04 (OQ-7): panel is two columns of horizontal bars (kcal/carbs/
 * protein/fat | fiber/sugars/sodium/caffeine) + serving line + source;
 * fdcId and the High/Medium/Low label are gone (AC-3, AC-6).
 */
export function ConfirmPopup({
  title, kcalPer100g, initial, saving, error, onConfirm, onCancel,
  editableName, nutrients, serving, sourceKind, keys, ownServing, extraUnit,
}: {
  title: string;
  kcalPer100g: number | null | undefined;
  initial: { qty: number; unit: string };
  saving: boolean;
  error: string | null;
  onConfirm: (name: string, qty: number, unit: string) => void;
  onCancel: () => void;
  editableName?: boolean;
  nutrients?: NutrientSnapshot | null;
  serving?: { size: number; unit: string; text?: string | null } | null;
  sourceKind?: CombinedSearchResult['kind'];
  keys?: { fdcId?: number | null; barcode?: string | null };
  /** 010-005 T10: the row's own serving (recipe grams-per-unit) — the live
   * kcal preview resolves a matching unit label through it (mirrors the
   * server's `_kcal_for`); null = preview falls back to the FDA table. */
  ownServing?: { size: number; unit: string } | null;
  /** 010-005 T10: one caller-supplied unit label (a recipe's serving label)
   * offered beside the closed unit set in AmountPicker. */
  extraUnit?: string | null;
}) {
  const [name, setName] = useState(title);
  const [qty, setQty] = useState(initial.qty);
  const [unit, setUnit] = useState<AmountUnit>((initial.unit as AmountUnit) ?? 'g');
  const [nameErr, setNameErr] = useState<string | null>(null);
  const preview = previewKcal(kcalPer100g, qty, unit, ownServing);
  const macroRef = macroBarRef(nutrients ?? {});
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/40 p-4 sm:items-center"
      role="presentation">
      <div role="dialog" aria-modal="true" aria-label={`Confirm ${title}`}
        className="w-full max-w-md rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4 shadow-xl"
        onClick={(e) => e.stopPropagation()}>
        {editableName ? (
          <div className="space-y-1">
            <label className="text-xs font-medium text-gray-700 dark:text-gray-300" htmlFor="confirm-name">
              Name for your diary
            </label>
            <input id="confirm-name" value={name} onChange={(e) => { setName(e.target.value); setNameErr(null); }}
              autoComplete="off"
              className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-base font-semibold text-gray-900 dark:text-white" />
            {nameErr && <p role="alert" className="text-xs text-red-700 dark:text-red-300">{nameErr}</p>}
          </div>
        ) : (
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">{title}</h2>
        )}
        <div className="mt-3">
          <AmountPicker value={{ qty: initial.qty, unit: (initial.unit as AmountUnit) ?? 'g' }}
            extraUnit={extraUnit}
            onChange={(a) => { setQty(a.qty); setUnit(a.unit); }} />
        </div>
        <p role="status" className="mt-3 text-sm text-gray-700 dark:text-gray-300">
          {preview != null ? `≈ ${Math.round(preview)} kcal` : 'kcal unavailable'}
        </p>
        {(nutrients || serving || sourceKind) && (
          <section aria-label="Saved to your diary"
            className="mt-3 space-y-1 rounded-md border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/40 p-3 text-xs text-gray-700 dark:text-gray-300">
            <h3 className="text-xs font-semibold text-gray-900 dark:text-white">Saved to your diary</h3>
            <div className="grid grid-cols-2 gap-x-4 gap-y-2" data-testid="panel-nutrients">
              <div className="space-y-2">
                <NutrientBar label="kcal" unit="" pct={barPct(nutrients?.kcal_per_100g ?? kcalPer100g, NUTRIENT_BAR_REFS.kcal)}
                  value={nutrients?.kcal_per_100g ?? kcalPer100g} />
                <NutrientBar label="carbs" unit="g" pct={barPct(nutrients?.carbs_g, macroRef)} value={nutrients?.carbs_g} />
                <NutrientBar label="protein" unit="g" pct={barPct(nutrients?.protein_g, macroRef)} value={nutrients?.protein_g} />
                <NutrientBar label="fat" unit="g" pct={barPct(nutrients?.fat_g, macroRef)} value={nutrients?.fat_g} />
              </div>
              <div className="space-y-2">
                <NutrientBar label="fiber" unit="g" pct={barPct(nutrients?.fiber_g, NUTRIENT_BAR_REFS.fiber)} value={nutrients?.fiber_g} />
                <NutrientBar label="sugars" unit="g" pct={barPct(nutrients?.sugars_g, NUTRIENT_BAR_REFS.sugars)} value={nutrients?.sugars_g} />
                <NutrientBar label="sodium" unit="mg" pct={barPct(nutrients?.sodium_mg, NUTRIENT_BAR_REFS.sodium)} value={nutrients?.sodium_mg} />
                <NutrientBar label="caffeine" unit="mg" pct={barPct(nutrients?.caffeine_mg, NUTRIENT_BAR_REFS.caffeine)} value={nutrients?.caffeine_mg} />
              </div>
            </div>
            <p data-testid="panel-serving">
              Serving: {serving
                ? `${serving.size} ${serving.unit}${serving.text ? ` (${serving.text})` : ''}`
                : '—'}
            </p>
            <p className="flex flex-wrap items-center gap-x-2" data-testid="panel-source">
              <span>Source:</span>
              {sourceKind && (
                <span className={`rounded-full px-2 py-0.5 font-medium ${SOURCE_STYLE[sourceKind]}`}>
                  {SOURCE_ICON[sourceKind]}
                </span>
              )}
              {keys?.barcode && <span>barcode {keys.barcode}</span>}
            </p>
          </section>
        )}
        {error && <p role="alert" className="mt-2 text-sm text-red-700 dark:text-red-300">{error}</p>}
        <div className="mt-4 flex items-center justify-end gap-2">
          <button type="button" onClick={onCancel}
            className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300">
            Cancel
          </button>
          <button type="button" onClick={() => {
            if (editableName && !name.trim()) {
              setNameErr('Name is required.');
              return;
            }
            onConfirm(name.trim() || title, qty, unit);
          }} disabled={saving}
            className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white disabled:opacity-60">
            {saving ? 'Saving…' : 'Confirm'}
          </button>
        </div>
      </div>
    </div>
  );
}
export default function AddEntry({ onDone }: { onDone: () => void }) {
  const [confirm, setConfirm] = useState<AddEntryPick | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveErr, setSaveErr] = useState<string | null>(null);
  const [pickMeta, setPickMeta] = useState<{ query?: string; rank?: number; result_source?: string }>({});
  const openConfirm = useCallback((pick: AddEntryPick) => {
    setSaveErr(null);
    setConfirm(pick);
    setPickMeta({ query: undefined, rank: pick.rank, result_source: pick.result_source });
  }, []);
  const confirmPick = useCallback(async (name: string, qty: number, unit: string) => {
    if (!confirm) return;
    setSaving(true);
    setSaveErr(null);
    try {
      if (confirm.payload) {
        const key = confirm.payload.provider === 'usda' ? 'usda_payload' : 'off_payload';
        await API.food.logApiEntry({
          [key]: { ...confirm.payload, name }, qty, unit,
          query: pickMeta.query, rank: pickMeta.rank,
          result_source: pickMeta.result_source,
        } as Parameters<typeof API.food.logApiEntry>[0]);
      } else {
        await API.food.logEntry({
          food_id: confirm.food_id, qty, unit,
          query: pickMeta.query, rank: pickMeta.rank,
          result_source: pickMeta.result_source,
        });
      }
      onDone();
    } catch (e) {
      setSaveErr(errMsg(e));
      setSaving(false);
    }
  }, [confirm, onDone, pickMeta]);
  return (
    <div className="space-y-4">
      <FoodSearchPicker onPick={openConfirm}
        onQueryChange={(qq) => setPickMeta((m) => ({ ...m, query: qq || undefined }))}
        leading={(
          <button type="button" aria-label="Cancel add entry" onClick={onDone}
            className="flex h-11 w-11 items-center justify-center rounded-full bg-red-600 hover:bg-red-700 text-white">
            <span aria-hidden className="text-xl leading-none">X</span>
          </button>
        )} />
      {confirm && (
        <ConfirmPopup title={confirm.name}
          kcalPer100g={confirm.kcalPer100g}
          initial={defaultAmount(confirm)}
          extraUnit={confirm.recipe_serving_unit ?? null}
          ownServing={confirm.row_serving ?? null}
          saving={saving} error={saveErr}
          editableName={confirm.payload != null}
          nutrients={confirm.nutrients}
          serving={confirm.serving}
          sourceKind={confirm.sourceKind}
          keys={confirm.keys}
          onConfirm={(name, qty, unit) => void confirmPick(name, qty, unit)}
          onCancel={() => { if (!saving) setConfirm(null); }} />
      )}
    </div>
  );
}
