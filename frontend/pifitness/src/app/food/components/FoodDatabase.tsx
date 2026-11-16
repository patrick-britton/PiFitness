/** Food Database T06: search/sort + edit (T13). */
'use client';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { API } from '../../../lib/api-client';
import type { FoodRow, UpdateFoodRequest } from '../../../lib/types/food-contract';
import { EmptyBox, LoadingSkeleton, Section } from './food-ui';
type SortKey = 'name' | 'kcal';
type NutrientKey =
  | 'kcal_per_100g'
  | 'protein_g'
  | 'fat_g'
  | 'carbs_g'
  | 'fiber_g'
  | 'sugars_g'
  | 'sodium_mg'
  | 'caffeine_mg';
const NUTRIENTS: { key: NutrientKey; label: string }[] = [
  { key: 'kcal_per_100g', label: 'kcal' },
  { key: 'protein_g', label: 'Protein' },
  { key: 'fat_g', label: 'Fat' },
  { key: 'carbs_g', label: 'Carbs' },
  { key: 'fiber_g', label: 'Fiber' },
  { key: 'sugars_g', label: 'Sugars' },
  { key: 'sodium_mg', label: 'Sodium' },
  { key: 'caffeine_mg', label: 'Caffeine' },
];
interface EditDraft {
  name: string;
  kcal_per_100g: number | null;
  protein_g: number | null;
  fat_g: number | null;
  carbs_g: number | null;
  fiber_g: number | null;
  sugars_g: number | null;
  sodium_mg: number | null;
  caffeine_mg: number | null;
  serving_size: number | null;
  serving_unit: string;
  serving_text: string;
}
const inputCls =
  'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 text-sm text-gray-900 dark:text-white';
export default function FoodDatabase() {
  const { layoutVariant } = useViewportStore();
  const desk = layoutVariant === 'desktop';
  const [q, setQ] = useState('');
  const [sort, setSort] = useState<SortKey>('name');
  const [rows, setRows] = useState<FoodRow[] | null>(null);
  const [showDel, setShowDel] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editDraft, setEditDraft] = useState<EditDraft | null>(null);
  const [editErr, setEditErr] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [confirmMsg, setConfirmMsg] = useState<string | null>(null);
  const load = useCallback(async () => {
    setRows(null);
    setErr(null);
    try {
      const r = await API.food.foods(q ? { q, limit: 50 } : undefined);
      setRows(r.data);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }, [q]);
  useEffect(() => { void load(); }, [load]);
  const parseNumber = useCallback((v: string): number | null => {
    const n = Number(v);
    return v === '' || Number.isNaN(n) ? null : n;
  }, []);
  const openEdit = useCallback((row: FoodRow) => {
    setEditingId(row.food_id);
    setEditDraft({
      name: row.name,
      kcal_per_100g: row.nutrients.kcal_per_100g,
      protein_g: row.nutrients.protein_g,
      fat_g: row.nutrients.fat_g,
      carbs_g: row.nutrients.carbs_g,
      fiber_g: row.nutrients.fiber_g,
      sugars_g: row.nutrients.sugars_g,
      sodium_mg: row.nutrients.sodium_mg,
      caffeine_mg: row.nutrients.caffeine_mg,
      serving_size: row.serving_size ?? null,
      serving_unit: row.serving_unit ?? '',
      serving_text: row.serving_text ?? '',
    });
    setEditErr(null);
  }, []);
  const save = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingId || !editDraft) return;
    setEditErr(null);
    try {
      await API.food.updateFood(editingId, editDraft);
      setEditingId(null);
      setEditDraft(null);
      setConfirmMsg('Changes saved');
      setTimeout(() => setConfirmMsg(null), 3000);
      void load();
    } catch (e) {
      setEditErr(e instanceof Error ? e.message : String(e));
    }
  }, [editingId, editDraft, load]);
  const cancel = useCallback(() => {
    setEditingId(null);
    setEditDraft(null);
    setEditErr(null);
  }, []);
  const vis = useMemo(() => {
    const base = (rows ?? []).filter((r) => showDel || !r.is_deleted);
    return [...base].sort((a, b) => sort === 'name' ? a.name.localeCompare(b.name) : (a.nutrients.kcal_per_100g ?? -1) - (b.nutrients.kcal_per_100g ?? -1));
  }, [rows, showDel, sort]);
  let body;
  if (err) body = <EmptyBox title="Food database" message="Food backend not yet available — apply the 010-002 DDL and refresh." />;
  else if (rows === null) body = <LoadingSkeleton rows={5} testId="foods" />;
  else if (vis.length === 0) body = <EmptyBox title="Food database" message={q ? `No foods match “${q}”.` : 'No foods yet — import via search/scan first.'} />;
  else body = (
    <ul className={desk ? 'grid grid-cols-2 gap-3' : 'space-y-2'} aria-label="Foods">
      {vis.map((r) => (
        <li key={r.food_id} className="flex items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-sm">
          <span className="text-gray-900 dark:text-white">{r.name}{r.is_deleted ? ' (deleted)' : ''}</span>
          <span className="flex items-center gap-2">
            <span className="text-xs text-gray-500 dark:text-gray-400">{r.nutrients.kcal_per_100g != null ? `${r.nutrients.kcal_per_100g} kcal/100g` : 'kcal n/a'}</span>
            <button type="button" aria-label={`Edit ${r.name}`} onClick={() => openEdit(r)} className="rounded-md px-2 py-1 text-xs text-gray-500 dark:text-gray-400 underline">Edit</button>
            <button type="button" aria-label={r.is_deleted ? `Restore ${r.name}` : `Delete ${r.name}`} className="rounded-md px-2 py-1 text-xs text-gray-500 dark:text-gray-400 underline">{r.is_deleted ? 'Restore' : 'Delete'}</button>
          </span>
        </li>))}
    </ul>);
  const editor = editingId && editDraft ? (
    <Section title="Edit food">
      <form onSubmit={save} className="space-y-3" noValidate>
        <div className="grid grid-cols-2 gap-2">
          <label htmlFor="edit-name" className="text-xs text-gray-500 dark:text-gray-400">
            Name
          </label>
          <input
            id="edit-name"
            value={editDraft.name}
            onChange={(e) => setEditDraft({ ...editDraft, name: e.target.value })}
            className={inputCls}
          />
        </div>
        <fieldset className="border border-gray-300 dark:border-gray-600 rounded-md p-2">
          <legend className="text-xs text-gray-500 dark:text-gray-400 px-1">Nutrients per 100 g</legend>
          <div className="grid grid-cols-4 gap-2">
            {NUTRIENTS.map(({ key, label }) => (
              <div key={key} className="flex flex-col gap-1">
                <label className="text-[10px] text-gray-500 dark:text-gray-400">{label}</label>
                <input
                  type="number"
                  step="any"
                  value={editDraft[key] == null ? '' : String(editDraft[key])}
                  onChange={(e) => setEditDraft({ ...editDraft, [key]: parseNumber(e.target.value) })}
                  className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-1 py-1 text-[11px] text-gray-900 dark:text-white"
                />
              </div>
            ))}
          </div>
        </fieldset>
        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-500 dark:text-gray-400">Serving size</label>
            <input
              type="number"
              step="any"
              value={editDraft.serving_size == null ? '' : String(editDraft.serving_size)}
              onChange={(e) => setEditDraft({ ...editDraft, serving_size: parseNumber(e.target.value) })}
              className={inputCls}
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-500 dark:text-gray-400">Serving unit</label>
            <input
              id="edit-serving-unit"
              value={editDraft.serving_unit}
              onChange={(e) => setEditDraft({ ...editDraft, serving_unit: e.target.value })}
              className={inputCls}
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-500 dark:text-gray-400">Serving text</label>
            <input
              id="edit-serving-text"
              value={editDraft.serving_text}
              onChange={(e) => setEditDraft({ ...editDraft, serving_text: e.target.value })}
              className={inputCls}
            />
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button type="submit" className="rounded-md bg-blue-600 hover:bg-blue-700 px-3 py-1 text-xs text-white">
            Save
          </button>
          <button type="button" onClick={cancel} className="rounded-md px-3 py-1 text-xs text-gray-500 dark:text-gray-400 underline">
            Cancel
          </button>
        </div>
        {editErr && <p className="text-xs text-red-600 dark:text-red-400">{editErr}</p>}
      </form>
    </Section>
  ) : null;
  return (
    <div className="space-y-4">
      <form onSubmit={(e) => { e.preventDefault(); void load(); }} className="flex flex-wrap items-center gap-2">
        <label className="sr-only" htmlFor="foods-q">Search foods</label>
        <input id="foods-q" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search foods" autoComplete="off"
          className="flex-1 min-w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white" />
        <label className="text-xs text-gray-500 dark:text-gray-400" htmlFor="foods-sort">Sort</label>
        <select id="foods-sort" value={sort} onChange={(e) => setSort(e.target.value as SortKey)}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-2 text-sm text-gray-900 dark:text-white">
          <option value="name">Name</option><option value="kcal">kcal</option>
        </select>
        <button type="submit" className="rounded-md bg-blue-600 hover:bg-blue-700 px-3 py-2 text-sm text-white">Search</button>
        <label className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
          <input type="checkbox" checked={showDel} onChange={(e) => setShowDel(e.target.checked)} /> Show deleted
        </label>
      </form>
      {confirmMsg && <p className="mt-2 text-sm text-green-700 dark:text-green-400">{confirmMsg}</p>}
      <Section title="Foods">{body}</Section>
      {editor}
    </div>
  );
}
