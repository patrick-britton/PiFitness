/**
 * Add-entry mode for the Food Diary (010-003 T06, part 1: shell + helpers).
 */
'use client';
import { useCallback, useEffect, useRef, useState } from 'react';
import dynamic from 'next/dynamic';
import { API } from '../../../lib/api-client';
import { feedServing, offNutrientsToSnapshot, previewKcal } from '../../../lib/food-units';
import type {
  ApiFoodPayload,
  CombinedSearchResult,
  NutrientSnapshot,
  UsualFood,
} from '../../../lib/types/food-contract';
import { EmptyBox, ErrorBox, LoadingSkeleton } from './food-ui';
import AmountPicker, { type AmountUnit } from './AmountPicker';
const BarcodeScanner = dynamic(() => import('./BarcodeScanner'), { ssr: false });
export const DEV_BARCODE = process.env.NODE_ENV !== 'production' ? '737628064502' : undefined;
/** How many results show before "Show more" (contract: backend trims to ~30). */
export const VISIBLE_RESULTS = 9;
/** Scanner open + lookup in flight — scanner stays mounted so it owns release. */
export type ScanPhase = 'camera' | 'lookup';
export function errMsg(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}
export const SOURCE_ICON: Record<CombinedSearchResult['kind'], string> = {
  'db-food': 'DB',
  'db-recipe': 'Recipe',
  usda: 'USDA',
  off: 'OFF',
};
/** T09 (OQ-7): source pill styling — legible in light + dark (mirrors QUALITY_STYLE). */
export const SOURCE_STYLE: Record<CombinedSearchResult['kind'], string> = {
  'db-food': 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-200',
  'db-recipe': 'bg-purple-100 text-purple-800 dark:bg-purple-900/40 dark:text-purple-200',
  usda: 'bg-sky-100 text-sky-800 dark:bg-sky-900/40 dark:text-sky-200',
  off: 'bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-200',
};
/** 010-004 T03: percent-pill styling (theme tokens only; numeric cue, never
 * color-alone — the integer is always rendered as text). */
export const PERCENT_STYLE =
  'bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-200';
export function modeLine(item: { mode_qty: number | null; mode_unit: string | null }): string | null {
  if (item.mode_qty == null || !item.mode_unit) return null;
  return `${item.mode_qty} ${item.mode_unit}`;
}

/** 010-004 T03: percent pill for a result — backend quality_percent when
 * present, else null (pill hidden). Saved rows carry null until ranked. */
export function qualityPercentLabel(h: CombinedSearchResult): string | null {
  const p = h.quality_percent;
  if (p == null || !Number.isFinite(p)) return null;
  return `${Math.round(p)}%`;
}

/** 010-004 T03: facet value for a hit — category from food_category, unit
 * from serving_unit_facet ?? serving_unit. Missing facet value reads null
 * (excluded when that facet is actively filtered). */
export function facetOf(h: CombinedSearchResult, which: 'category' | 'unit'): string | null {
  const raw = which === 'category' ? h.food_category : (h.serving_unit_facet ?? h.serving_unit);
  const v = (raw ?? '').trim();
  return v ? v : null;
}

/** 010-004 T03: distinct facet values with counts over a hit list. */
export function facetCounts(hits: CombinedSearchResult[], which: 'category' | 'unit'): { value: string; count: number }[] {
  const counts = new Map<string, number>();
  for (const h of hits) {
    const v = facetOf(h, which);
    if (v != null) counts.set(v, (counts.get(v) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value));
}

/** 010-004 T03: single-select client-side facet filter — a row missing the
 * active facet value is excluded (covers saved DB rows lacking USDA
 * category). Null filter = no filtering on that facet. */
export function applyFacets(hits: CombinedSearchResult[], category: string | null, unit: string | null): CombinedSearchResult[] {
  return hits.filter((h) => {
    if (category != null && facetOf(h, 'category') !== category) return false;
    if (unit != null && facetOf(h, 'unit') !== unit) return false;
    return true;
  });
}
export interface AddEntryPick {
  food_id: number | null;
  name: string;
  mode_qty: number | null;
  mode_unit: string | null;
  kcalPer100g: number | null;
  rank: number;
  result_source: string;
  payload?: ApiFoodPayload;
  /** T10 (OQ-8): panel + default-serving context for the confirm popup. */
  sourceKind: CombinedSearchResult['kind'];
  nutrients: NutrientSnapshot | null;
  serving: { size: number; unit: string; text?: string | null } | null;
  keys?: { fdcId?: number | null; barcode?: string | null };
}

/** T10 (OQ-8): popup default chain — history mode → feed serving → 100 g. */
export function defaultAmount(p: Pick<AddEntryPick, 'mode_qty' | 'mode_unit' | 'serving'>): {
  qty: number;
  unit: string;
} {
  if (p.mode_qty != null && p.mode_unit) return { qty: p.mode_qty, unit: p.mode_unit };
  if (p.serving) return { qty: p.serving.size, unit: p.serving.unit };
  return { qty: 100, unit: 'g' };
}

/** 010-004 T03: one facet filter row — button group at ≤3 values, select
 * dropdown at 4+. Single-select with an All reset; token-only styling,
 * 44px touch targets, labelled for screen readers. */
export function FacetControl({ label, options, value, onChange }: {
  label: string;
  options: { value: string; count: number }[];
  value: string | null;
  onChange: (v: string | null) => void;
}): React.JSX.Element | null {
  if (options.length === 0) return null;
  const id = `facet-${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
  const allActive = value == null;
  const btn = (active: boolean) =>
    `rounded-md border px-3 py-2 min-h-[44px] text-sm ${active
      ? 'border-blue-600 bg-blue-600 text-white'
      : 'border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300'}`;
  return (
    <div className="flex flex-wrap items-center gap-2" aria-label={`${label} filter`}>
      <span className="text-xs font-medium text-gray-500 dark:text-gray-400">{label}:</span>
      {options.length <= 3 ? (
        <>
          <button type="button" aria-pressed={allActive} onClick={() => onChange(null)} className={btn(allActive)}>All</button>
          {options.map((o) => (
            <button key={o.value} type="button" aria-pressed={value === o.value}
              onClick={() => onChange(value === o.value ? null : o.value)} className={btn(value === o.value)}>
              {o.value} ({o.count})
            </button>
          ))}
        </>
      ) : (
        <select id={id} aria-label={`${label} filter`} value={value ?? ''}
          onChange={(e) => onChange(e.target.value || null)}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300">
          <option value="">All ({options.reduce((n, o) => n + o.count, 0)})</option>
          {options.map((o) => (
            <option key={o.value} value={o.value}>{o.value} ({o.count})</option>
          ))}
        </select>
      )}
    </div>
  );
}

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
 * 010-004 T04 (OQ-7): panel is two columns of horizontal bars (kcal/carbs/
 * protein/fat | fiber/sugars/sodium/caffeine) + serving line + source;
 * fdcId and the High/Medium/Low label are gone (AC-3, AC-6).
 */
export function ConfirmPopup({
  title, kcalPer100g, initial, saving, error, onConfirm, onCancel,
  editableName, nutrients, serving, sourceKind, keys,
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
}) {
  const [name, setName] = useState(title);
  const [qty, setQty] = useState(initial.qty);
  const [unit, setUnit] = useState<AmountUnit>((initial.unit as AmountUnit) ?? 'g');
  const [nameErr, setNameErr] = useState<string | null>(null);
  const preview = previewKcal(kcalPer100g, qty, unit);
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
  const [suggest, setSuggest] = useState<UsualFood[] | null>(null);
  const [suggestErr, setSuggestErr] = useState<string | null>(null);
  const [q, setQ] = useState('');
  const [searched, setSearched] = useState(false);
  const [hits, setHits] = useState<CombinedSearchResult[] | null>(null);
  const [searchErr, setSearchErr] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);
  const [expanded, setExpanded] = useState(false);
  /** 010-004 T03: independent single-select facet filters (null = all). */
  const [facetCategory, setFacetCategory] = useState<string | null>(null);
  const [facetUnit, setFacetUnit] = useState<string | null>(null);
  const [scanPhase, setScanPhase] = useState<ScanPhase | null>(null);
  const [scanMsg, setScanMsg] = useState<string | null>(null);
  const [manual, setManual] = useState(false);
  const [code, setCode] = useState('');
  const [confirm, setConfirm] = useState<AddEntryPick | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveErr, setSaveErr] = useState<string | null>(null);
  const scanSeq = useRef(0);
  useEffect(() => {
    let live = true;
    API.food.suggest(new Date().toISOString())
      .then((rows) => { if (live) setSuggest(rows.slice(0, 10)); })
      .catch((e) => { if (live) setSuggestErr(errMsg(e)); });
    return () => { live = false; };
  }, []);
  const runSearch = useCallback(async () => {
    const query = q.trim();
    if (!query) return;
    setSearching(true);
    setSearchErr(null);
    setExpanded(false);
    setFacetCategory(null);
    setFacetUnit(null);
    try {
      setHits(await API.food.combinedSearch(query, 30));
      setSearched(true);
    } catch (e) {
      setSearchErr(errMsg(e));
    } finally {
      setSearching(false);
    }
  }, [q]);
  const openConfirm = useCallback((pick: AddEntryPick) => {
    setSaveErr(null);
    setConfirm(pick);
  }, []);
  const pickSuggestion = useCallback(async (s: UsualFood) => {
    if (s.food_id == null) return;
    // T10 (OQ-8): hydrate the full row so the panel + serving default are
    // exact; on failure the popup still opens with what suggest carried.
    let nutrients: NutrientSnapshot | null = {
      kcal_per_100g: s.kcal_per_100g, protein_g: null, fat_g: null,
      carbs_g: null, fiber_g: null, sugars_g: null, sodium_mg: null,
      caffeine_mg: null,
    };
    let serving: AddEntryPick['serving'] = null;
    try {
      const row = await API.food.foodById(s.food_id);
      nutrients = row.nutrients;
      const sv = feedServing(row.serving_size, row.serving_unit);
      serving = sv ? { size: sv.size, unit: sv.unit, text: row.serving_text } : null;
    } catch {
      // degraded panel (kcal only); never block the pick on the fetch.
    }
    openConfirm({
      food_id: s.food_id, name: s.name,
      mode_qty: s.mode_qty, mode_unit: s.mode_unit,
      kcalPer100g: s.kcal_per_100g, rank: 0, result_source: 'suggest',
      sourceKind: s.source === 'recipe' ? 'db-recipe' : 'db-food',
      nutrients, serving,
    });
  }, [openConfirm]);
  const pickHit = useCallback((h: CombinedSearchResult) => {
    const isApi = h.kind === 'usda' || h.kind === 'off';
    const sv = h.serving_size != null ? feedServing(h.serving_size, h.serving_unit) : null;
    openConfirm({
      food_id: h.food_id, name: h.name,
      mode_qty: h.mode_qty, mode_unit: h.mode_unit,
      kcalPer100g: h.kcal_preview, rank: h.rank, result_source: h.result_source,
      // T08 (OQ-6): hits array is the session store — carry the persist
      // payload for API kinds (fdcId/barcode + raw upstream hit).
      payload: isApi ? {
        provider: h.kind === 'off' ? 'off' : 'usda',
        fdcId: h.fdcId ?? undefined,
        barcode: h.barcode ?? undefined,
        name: h.name,
        nutrients_raw: h.nutrients_raw ?? null,
      } : undefined,
      sourceKind: h.kind,
      nutrients: h.nutrients ?? null,
      serving: sv ? { size: sv.size, unit: sv.unit, text: h.serving_text } : null,
      keys: { fdcId: h.fdcId, barcode: h.barcode },
    });
  }, [openConfirm]);
  /** Barcode scan → DB by barcode first; miss → OFF lookup → confirm. */
  const scan = useCallback(async (value: string) => {
    const seq = ++scanSeq.current;
    setScanPhase('lookup');
    setScanMsg(`Looking up barcode ${value}…`);
    try {
      const db = await API.food.byBarcode(value);
      if (seq !== scanSeq.current) return;
      if (db.found && db.food_id != null) {
        setScanPhase(null);
        setScanMsg(`Found saved food: ${db.name}`);
        const sv = feedServing(db.serving_size, db.serving_unit);
        openConfirm({
          food_id: db.food_id, name: db.name,
          mode_qty: null, mode_unit: null,
          kcalPer100g: db.nutrients?.kcal_per_100g ?? null,
          rank: 0, result_source: 'saved:barcode',
          sourceKind: db.source === 'recipe' ? 'db-recipe' : 'db-food',
          nutrients: db.nutrients ?? null,
          serving: sv ? { size: sv.size, unit: sv.unit, text: db.serving_text } : null,
          keys: { barcode: db.barcode },
        });
        return;
      }
    } catch {
      // DB check failed — fall through to OFF lookup.
      if (seq !== scanSeq.current) return;
    }
    try {
      const r = await API.foodLookup.lookupOff(value);
      if (seq !== scanSeq.current) return;
      const b = r.body as {
        status?: number; product?: { product_name?: string; nutriments?: unknown };
      } | null;
      const name = b?.product?.product_name?.trim();
      if (b?.status === 0 || !name) {
        setScanMsg(`No product found for barcode ${value}.`);
        setScanPhase(null);
        return;
      }
      setScanPhase(null);
      setScanMsg(`Scanned ${value}: ${name}`);
      // T10 (OQ-8): OFF panel from the stashed product (native *_100g —
      // key mapping only) + serving default; kcal preview now works too.
      const product = (b?.product ?? {}) as {
        nutriments?: unknown;
        serving_quantity?: unknown;
        serving_quantity_unit?: unknown;
        serving_size?: unknown;
      };
      const nutrients = offNutrientsToSnapshot(product.nutriments);
      const svOff = feedServing(product.serving_quantity, product.serving_quantity_unit);
      openConfirm({
        food_id: null, name, mode_qty: null, mode_unit: null,
        kcalPer100g: nutrients.kcal_per_100g, rank: 0, result_source: 'off:scan',
        payload: {
          provider: 'off', barcode: value, name,
          nutrients_raw: {
            nutriments: product.nutriments ?? null,
            serving_quantity: product.serving_quantity ?? null,
            serving_quantity_unit: product.serving_quantity_unit ?? null,
            serving_size: product.serving_size ?? null,
          },
        },
        sourceKind: 'off',
        nutrients,
        serving: svOff
          ? { size: svOff.size, unit: svOff.unit,
              text: typeof product.serving_size === 'string' ? product.serving_size : null }
          : null,
        keys: { barcode: value },
      });
    } catch (e) {
      if (seq !== scanSeq.current) return;
      setScanMsg(`Barcode lookup failed: ${errMsg(e)}`);
      setScanPhase(null);
    }
  }, [openConfirm]);
  const closeScanner = useCallback(() => {
    scanSeq.current += 1;
    setScanPhase(null);
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
          query: q.trim() || undefined, rank: confirm.rank,
          result_source: confirm.result_source,
        } as Parameters<typeof API.food.logApiEntry>[0]);
      } else {
        await API.food.logEntry({
          food_id: confirm.food_id, qty, unit,
          query: q.trim() || undefined, rank: confirm.rank,
          result_source: confirm.result_source,
        });
      }
      onDone();
    } catch (e) {
      setSaveErr(errMsg(e));
      setSaving(false);
    }
  }, [confirm, onDone, q]);
  /** 010-004 T03: client-side facet narrowing over the single fetched list
   * (no new fetch; applies to all rows incl. saved). Show-more counts the
   * filtered list. */
  const filtered = hits == null ? null : applyFacets(hits, facetCategory, facetUnit);
  const shown = filtered == null ? [] : (expanded ? filtered : filtered.slice(0, VISIBLE_RESULTS));
  const categoryFacets = hits == null ? [] : facetCounts(hits, 'category');
  const unitFacets = hits == null ? [] : facetCounts(hits, 'unit');
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <button type="button" aria-label="Cancel add entry" onClick={onDone}
          className="flex h-11 w-11 items-center justify-center rounded-full bg-red-600 hover:bg-red-700 text-white">
          <span aria-hidden className="text-xl leading-none">✕</span>
        </button>
        <form onSubmit={(e) => { e.preventDefault(); void runSearch(); }}
          className="flex flex-1 items-center gap-2" aria-label="Search foods">
          <label className="sr-only" htmlFor="add-search">Food name</label>
          <input id="add-search" value={q} onChange={(e) => setQ(e.target.value)}
            placeholder="Search foods" autoComplete="off"
            className="flex-1 min-w-24 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
          <button type="submit" aria-label="Search"
            className="flex h-11 w-11 items-center justify-center rounded-md bg-blue-600 hover:bg-blue-700 text-white">
            <span aria-hidden>⌕</span>
          </button>
          {scanPhase == null && (
            <button type="button" aria-label="Scan barcode" onClick={() => { setScanMsg(null); setScanPhase('camera'); }}
              className="flex h-11 w-11 items-center justify-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300">
              <span aria-hidden>▮⌁</span>
            </button>
          )}
        </form>
      </div>
      {scanPhase != null && (
        <section aria-label="Barcode scanner" className="space-y-2">
          {scanPhase === 'camera' && (
            <BarcodeScanner active={scanPhase === 'camera'}
              onScan={(v) => void scan(v)}
              onError={closeScanner}
              devSimulateValue={DEV_BARCODE} />
          )}
          {scanPhase === 'lookup' && <LoadingSkeleton rows={2} testId="scan" />}
          <button type="button" onClick={closeScanner}
            className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300">
            Close scanner
          </button>
        </section>
      )}
      {scanMsg && <p role="status" className="text-xs text-gray-700 dark:text-gray-300">{scanMsg}</p>}
      {manual
        ? (
          <form onSubmit={(e) => { e.preventDefault(); if (code.trim()) void scan(code.trim()); }}
            className="flex items-center gap-2" aria-label="Type a barcode">
            <label className="sr-only" htmlFor="add-manual-barcode">Barcode</label>
            <input id="add-manual-barcode" value={code} onChange={(e) => setCode(e.target.value)}
              placeholder="Enter barcode" inputMode="numeric" autoComplete="off"
              className="flex-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white" />
            <button type="submit" className="rounded-md bg-blue-600 hover:bg-blue-700 px-3 py-2 min-h-[44px] text-sm text-white">
              Look up
            </button>
          </form>
        )
        : (
          <button type="button" onClick={() => setManual(true)}
            className="text-xs text-gray-500 dark:text-gray-400 underline">
            Enter barcode manually
          </button>
        )}
      {!searched && (
        <section aria-label="Suggested foods" className="space-y-2">
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">Suggested</h2>
          {suggest == null && suggestErr == null && <LoadingSkeleton rows={3} testId="suggest" />}
          {suggestErr != null && <ErrorBox label="Suggested foods" message={suggestErr} />}
          {suggest != null && suggest.length === 0 && (
            <EmptyBox title="Suggested foods" message="No suggestions yet." />
          )}
          {suggest != null && suggest.length > 0 && (
            <ul className="space-y-1.5" aria-label="Suggestion cards">
              {suggest.map((s) => (
                <li key={`${s.source}-${s.food_id ?? s.name}`}>
                  <button type="button" onClick={() => void pickSuggestion(s)}
                    className="flex w-full items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2.5 min-h-[44px] text-left text-sm hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <span className="text-gray-900 dark:text-white">{s.name}</span>
                    <span className="text-xs text-gray-500 dark:text-gray-400">
                      {modeLine(s) ?? (s.kcal_per_100g != null ? `${Math.round(s.kcal_per_100g)} kcal/100g` : 'kcal n/a')}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}
      {(searched || searching || searchErr) && (
        <section aria-label="Search results" className="space-y-2">
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">Results</h2>
          {searching && <LoadingSkeleton rows={4} testId="results" />}
          {searchErr != null && !searching && <ErrorBox label="Search results" message={searchErr} />}
          {!searching && searchErr == null && hits != null && hits.length === 0 && (
            <EmptyBox title="Search results" message={`No matches for “${q.trim()}”.`} />
          )}
          {!searching && searchErr == null && hits != null && hits.length > 0 && (
            <>
              <FacetControl label="Category" options={categoryFacets}
                value={facetCategory} onChange={setFacetCategory} />
              <FacetControl label="Serving unit" options={unitFacets}
                value={facetUnit} onChange={setFacetUnit} />
              {shown.length === 0 ? (
                <EmptyBox title="Search results"
                  message="No matches for the selected filters." />
              ) : (
              <ul className="space-y-1.5" aria-label="Result cards">
                {shown.map((h, i) => (
                  <li key={`${h.kind}-${h.food_id ?? h.name}-${i}`}>
                    <button type="button" onClick={() => pickHit(h)}
                      className="flex w-full items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2.5 min-h-[44px] text-left text-sm hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      <span className="flex min-w-0 items-center gap-2">
                        <span title={h.kind} className={`rounded-full px-2 py-0.5 text-xs font-medium ${SOURCE_STYLE[h.kind]}`}>{SOURCE_ICON[h.kind]}</span>
                        <span className="truncate text-gray-900 dark:text-white">{h.name}</span>
                      </span>
                      <span className="flex shrink-0 items-center gap-2">
                        {qualityPercentLabel(h) != null && (
                          <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${PERCENT_STYLE}`}>
                            {qualityPercentLabel(h)}
                          </span>
                        )}
                        <span className="text-xs text-gray-500 dark:text-gray-400">
                          {modeLine(h) ?? (h.kcal_preview != null ? `≈ ${Math.round(h.kcal_preview)}` : 'kcal n/a')}
                        </span>
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
              )}
              {filtered != null && filtered.length > VISIBLE_RESULTS && !expanded && (
                <button type="button" onClick={() => setExpanded(true)}
                  className="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-300">
                  Show more ({filtered.length - VISIBLE_RESULTS} more)
                </button>
              )}
            </>
          )}
        </section>
      )}
      {confirm && (
        <ConfirmPopup title={confirm.name}
          kcalPer100g={confirm.kcalPer100g}
          initial={defaultAmount(confirm)}
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



