/**
 * FoodSearchPicker — the ONE combined food search + barcode scan entry
 * (extracted from AddEntry by 010-005 T06; consumed by Diary AddEntry and
 * the Recipe Box create flow — memory-bank/designs/food_logging.md §2).
 *
 * Emits a normalized `FoodPick` via `onPick`; the consumer decides what a
 * pick means (diary logs it, the recipe flow adds an ingredient line and
 * persists API hits at pick time per OQ-3). Suggestions, tiered combined
 * search with facets/show-more, camera scan (DB-by-barcode first, OFF miss
 * fallback) and manual barcode entry all live here — exactly once.
 */
'use client';
import { useCallback, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import dynamic from 'next/dynamic';
import { API } from '../../../lib/api-client';
import { feedServing, offNutrientsToSnapshot } from '../../../lib/food-units';
import type {
  ApiFoodPayload,
  CombinedSearchResult,
  NutrientSnapshot,
  UsualFood,
} from '../../../lib/types/food-contract';
import { EmptyBox, ErrorBox, LoadingSkeleton } from './food-ui';

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

/** One pick, normalized across search hits, scans, and suggestions
 * (010-003 T06; renamed from AddEntryPick on extraction — 010-005 T06). */
export interface FoodPick {
  food_id: number | null;
  name: string;
  mode_qty: number | null;
  mode_unit: string | null;
  kcalPer100g: number | null;
  rank: number;
  result_source: string;
  /** Trimmed search query at pick time (OQ-4 telemetry for the diary save). */
  query?: string;
  payload?: ApiFoodPayload;
  /** T10 (OQ-8): panel + default-serving context for the confirm popup. */
  sourceKind: CombinedSearchResult['kind'];
  nutrients: NutrientSnapshot | null;
  serving: { size: number; unit: string; text?: string | null } | null;
  keys?: { fdcId?: number | null; barcode?: string | null };
  /** Raw row/feed serving (any unit) — recipe-flow preview math only; never
   * fed to the diary default chain (that stays fail-closed to g/ml). */
  row_serving?: { size: number; unit: string } | null;
  /** 010-005 T10: the recipe's own serving label (`serving_unit` on the
   * `source='recipe'` food row, e.g. `pancake`). Set only for `db-recipe`
   * picks; the diary default chain turns it into qty 1 + label. */
  recipe_serving_unit?: string | null;
}

/** Default portion chain. 010-005 T10 (AC-10): a recipe pick (`db-recipe`
 * carrying its own serving label) ALWAYS defaults to qty 1 in that label —
 * history mode never overrides it — and kcal resolves server-side via the
 * derived grams-per-unit. Non-recipe picks keep the T10 (OQ-8) chain:
 * history mode → feed serving → 100 g. */
export function defaultAmount(p: Pick<FoodPick, 'mode_qty' | 'mode_unit' | 'serving' | 'sourceKind' | 'recipe_serving_unit'>): {
  qty: number;
  unit: string;
} {
  if (p.sourceKind === 'db-recipe' && p.recipe_serving_unit) {
    return { qty: 1, unit: p.recipe_serving_unit };
  }
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

export interface FoodSearchPickerProps {
  /** A pick is ready: the diary opens its confirm popup, the recipe flow
   * adds an ingredient line (persisting API hits first per OQ-3). */
  onPick: (pick: FoodPick) => void;
  /** Slot at the start of the search row (AddEntry puts its cancel X here). */
  leading?: ReactNode;
  /** Keystroke echo of the search box (diary pick telemetry). */
  onQueryChange?: (query: string) => void;
}

export default function FoodSearchPicker({ onPick, leading, onQueryChange }: FoodSearchPickerProps) {
  // Suggestions (mounted fetch; 010-003 T06).
  const [suggest, setSuggest] = useState<UsualFood[] | null>(null);
  const [suggestErr, setSuggestErr] = useState<string | null>(null);
  // Tiered combined search (010-003 T06).
  const [q, setQ] = useState('');
  const [searched, setSearched] = useState(false);
  const [hits, setHits] = useState<CombinedSearchResult[] | null>(null);
  const [searchErr, setSearchErr] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);
  const [expanded, setExpanded] = useState(false);   // 010-004 T03: show-more
  // 010-004 T03: client-side facet filters over the one fetched result set.
  const [facetCategory, setFacetCategory] = useState<string | null>(null);
  const [facetUnit, setFacetUnit] = useState<string | null>(null);
  // Barcode (T08): camera scan → DB by-barcode first, else OFF lookup.
  const [scanPhase, setScanPhase] = useState<ScanPhase | null>(null);
  const [scanMsg, setScanMsg] = useState<string | null>(null);
  const [manual, setManual] = useState(false);
  const [code, setCode] = useState('');
  const suggestSeq = useRef(0);
  const scanSeq = useRef(0);

  useEffect(() => {
    let live = true;
    API.food.suggest(new Date().toISOString())
      .then((rows) => { if (live) setSuggest(rows.slice(0, 10)); })
      .catch((e) => { if (live) setSuggestErr(errMsg(e)); });
    return () => { live = false; };
  }, []);

  // 010-004 T03: client-side facet narrowing over the single fetched list
  // (no new fetch; applies to all rows incl. saved). Show-more counts the
  // filtered list.
  const filtered = hits == null ? null : applyFacets(hits, facetCategory, facetUnit);
  const shown = filtered == null ? [] : (expanded ? filtered : filtered.slice(0, VISIBLE_RESULTS));
  const categoryFacets = hits == null ? [] : facetCounts(hits, 'category');
  const unitFacets = hits == null ? [] : facetCounts(hits, 'unit');

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

  const pickSuggestion = useCallback(async (s: UsualFood) => {
    if (s.food_id == null) return;
    // T10 (OQ-8): hydrate the full row so the panel + serving default are
    // exact; on failure the pick still goes through with what suggest carried.
    let nutrients: NutrientSnapshot | null = {
      kcal_per_100g: s.kcal_per_100g, protein_g: null, fat_g: null,
      carbs_g: null, fiber_g: null, sugars_g: null, sodium_mg: null,
      caffeine_mg: null,
    };
    let serving: FoodPick['serving'] = null;
    let rowServing: FoodPick['row_serving'] = null;
    let recipeUnit: string | null = null;
    try {
      const row = await API.food.foodById(s.food_id);
      nutrients = row.nutrients;
      const sv = feedServing(row.serving_size, row.serving_unit);
      serving = sv ? { size: sv.size, unit: sv.unit, text: row.serving_text } : null;
      rowServing = row.serving_size != null && row.serving_unit
        ? { size: row.serving_size, unit: row.serving_unit }
        : null;
      // 010-005 T10: recipe rows log by their own serving label.
      recipeUnit = s.source === 'recipe' ? (row.serving_unit?.trim() || null) : null;
    } catch {
      // degraded pick (kcal only); never block the pick on the fetch.
    }
    onPick({
      food_id: s.food_id, name: s.name,
      mode_qty: s.mode_qty, mode_unit: s.mode_unit,
      kcalPer100g: s.kcal_per_100g, rank: 0, result_source: 'suggest',
      query: q.trim(),
      sourceKind: s.source === 'recipe' ? 'db-recipe' : 'db-food',
      nutrients, serving,
      row_serving: rowServing,
      recipe_serving_unit: recipeUnit,
    });
  }, [onPick, q]);
  const pickHit = useCallback((h: CombinedSearchResult) => {
    const isApi = h.kind === 'usda' || h.kind === 'off';
    const sv = h.serving_size != null ? feedServing(h.serving_size, h.serving_unit) : null;
    // 010-005 T10: recipe rows carry their own serving label (e.g. pancake)
    // straight through — feedServing only knows g/ml, so it stays separate.
    const recipeUnit = h.kind === 'db-recipe' ? (h.serving_unit?.trim() || null) : null;
    onPick({
      food_id: h.food_id, name: h.name,
      mode_qty: h.mode_qty, mode_unit: h.mode_unit,
      kcalPer100g: h.kcal_preview, rank: h.rank, result_source: h.result_source,
      query: q.trim(),
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
      row_serving: h.serving_size != null && h.serving_unit
        ? { size: h.serving_size, unit: h.serving_unit }
        : null,
      recipe_serving_unit: recipeUnit,
    });
  }, [onPick, q]);
  /** Barcode scan → DB by barcode first; miss → OFF lookup → emit pick. */
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
        onPick({
          food_id: db.food_id, name: db.name,
          mode_qty: null, mode_unit: null,
          kcalPer100g: db.nutrients?.kcal_per_100g ?? null,
          rank: 0, result_source: 'saved:barcode',
          query: q.trim(),
          sourceKind: db.source === 'recipe' ? 'db-recipe' : 'db-food',
          nutrients: db.nutrients ?? null,
          serving: sv ? { size: sv.size, unit: sv.unit, text: db.serving_text } : null,
          keys: { barcode: db.barcode },
          row_serving: db.serving_size != null && db.serving_unit
            ? { size: db.serving_size, unit: db.serving_unit }
            : null,
          // 010-005 T10: recipe scans log by their own serving label.
          recipe_serving_unit: db.source === 'recipe'
            ? (db.serving_unit?.trim() || null)
            : null,
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
      onPick({
        food_id: null, name, mode_qty: null, mode_unit: null,
        kcalPer100g: nutrients.kcal_per_100g, rank: 0, result_source: 'off:scan',
        query: q.trim(),
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
        row_serving: typeof product.serving_quantity === 'number'
          ? { size: product.serving_quantity,
              unit: String(product.serving_quantity_unit ?? 'g') }
          : null,
      });
    } catch (e) {
      if (seq !== scanSeq.current) return;
      setScanMsg(`Barcode lookup failed: ${errMsg(e)}`);
      setScanPhase(null);
    }
  }, [onPick, q]);
  const closeScanner = useCallback(() => {
    scanSeq.current += 1;
    setScanPhase(null);
  }, []);
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        {leading}
        <form onSubmit={(e) => { e.preventDefault(); void runSearch(); }}
          className="flex flex-1 items-center gap-2" aria-label="Search foods">
          <label className="sr-only" htmlFor="add-search">Food name</label>
          <input id="add-search" value={q}
            onChange={(e) => { setQ(e.target.value); onQueryChange?.(e.target.value); }}
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
    </div>
  );
}



