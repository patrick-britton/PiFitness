/** Food Database skeleton T06: search/sort + edit/delete affordances. */
'use client';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { API } from '../../../lib/api-client';
import type { FoodRow } from '../../../lib/types/food-contract';
import { EmptyBox, LoadingSkeleton, Section } from './food-ui';
type SortKey = 'name' | 'kcal';
export default function FoodDatabase() {
  const { layoutVariant } = useViewportStore();
  const desk = layoutVariant === 'desktop';
  const [q, setQ] = useState('');
  const [sort, setSort] = useState<SortKey>('name');
  const [showDel, setShowDel] = useState(false);
  const [rows, setRows] = useState<FoodRow[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const load = useCallback(async () => {
    setRows(null); setErr(null);
    try { const r = await API.food.foods(q ? { q, limit: 50 } : undefined); setRows(r.data); }
    catch (e) { setErr(e instanceof Error ? e.message : String(e)); }
  }, [q]);
  useEffect(() => { void load(); }, [load]);
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
            <button type="button" aria-label={`Edit ${r.name}`} className="rounded-md px-2 py-1 text-xs text-gray-500 dark:text-gray-400 underline">Edit</button>
            <button type="button" aria-label={r.is_deleted ? `Restore ${r.name}` : `Delete ${r.name}`} className="rounded-md px-2 py-1 text-xs text-gray-500 dark:text-gray-400 underline">{r.is_deleted ? 'Restore' : 'Delete'}</button>
          </span>
        </li>))}
    </ul>);
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
      <Section title="Foods">{body}</Section>
    </div>
  );
}
