/**
 * Recipe Box (010-005 T05/T08/T09): list mode + mode machine.
 *
 * List mode: cards sorted most-made first, single-select category chips
 * (OQ-1 set, 'Side' included), a `+ Create recipe` entry point, and explicit
 * loading / empty / error(+retry) regions. The mode machine hosts create
 * (T06), cook (T08) and edit (T09, re-enters the create workflow at
 * ingredient verification). Three layouts via `layoutVariant`;
 * theme-aware Tailwind classes only (no hardcoded colors); 44px targets.
 */

'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { API } from '../../../lib/api-client';
import type { RecipeCategory, RecipeSummary } from '../../../lib/types/food-contract';
import { EmptyBox, ErrorBox, LoadingSkeleton, Section } from './food-ui';
import RecipeCook from './RecipeCook';
import RecipeCreate from './RecipeCreate';
import RecipeEdit from './RecipeEdit';

const CATEGORIES: (RecipeCategory | 'All')[] = [
  'All', 'Breakfast', 'Lunch', 'Dinner', 'Side', 'Dessert', 'Drink',
];

type Region =
  | { status: 'loading' }
  | { status: 'ready'; items: RecipeSummary[] }
  | { status: 'error'; error: string };

/** List → create → cook → edit (edit re-enters the create workflow). */
type Mode =
  | { name: 'list' }
  | { name: 'create' }
  | { name: 'cook'; recipeId: number }
  | { name: 'edit'; recipeId: number };

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export default function RecipeBox() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  const [mode, setMode] = useState<Mode>({ name: 'list' });
  const [region, setRegion] = useState<Region>({ status: 'loading' });
  const [filter, setFilter] = useState<RecipeCategory | 'All'>('All');

  const load = useCallback(async () => {
    setRegion({ status: 'loading' });
    try {
      setRegion({ status: 'ready', items: await API.food.recipes() });
    } catch (error) {
      setRegion({ status: 'error', error: errMsg(error) });
    }
  }, []);
  useEffect(() => { void load(); }, [load]);

  // Most-made first, title asc tiebreak — mirrors the server ORDER BY so
  // AC-1 holds even if a stale or mocked response arrives unsorted.
  const items = useMemo(() => {
    if (region.status !== 'ready') return [];
    const visible =
      filter === 'All'
        ? region.items
        : region.items.filter((r) => r.category === filter);
    return [...visible].sort(
      (a, b) => b.times_prepared - a.times_prepared
        || a.title.localeCompare(b.title),
    );
  }, [region, filter]);

  if (mode.name === 'create') {
    return (
      <RecipeCreate
        onBack={() => setMode({ name: 'list' })}
        onSaved={() => {
          setMode({ name: 'list' });
          void load();
        }}
      />
    );
  }
  if (mode.name === 'cook') {
    return (
      <RecipeCook
        recipeId={mode.recipeId}
        onBack={() => setMode({ name: 'list' })}
        onCompleted={() => {
          setMode({ name: 'list' });
          void load();
        }}
        onEdit={() => setMode({ name: 'edit', recipeId: mode.recipeId })}
      />
    );
  }
  if (mode.name === 'edit') {
    return (
      <RecipeEdit
        recipeId={mode.recipeId}
        onBack={() => setMode({ name: 'cook', recipeId: mode.recipeId })}
        onSaved={() => {
          setMode({ name: 'list' });
          void load();
        }}
      />
    );
  }

  let body;
  if (region.status === 'loading') {
    body = <LoadingSkeleton rows={4} testId="recipe" />;
  } else if (region.status === 'error') {
    body = (
      <div className="space-y-2">
        <ErrorBox label="Recipes" message={region.error} />
        <button type="button" onClick={() => void load()}
          className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          Retry
        </button>
      </div>
    );
  } else if (items.length === 0) {
    body = (
      <EmptyBox title={filter === 'All' ? 'Recipe box' : 'No recipes in this category'}
        message={filter === 'All'
          ? 'No recipes yet — use Create to add one.'
          : 'No recipes in this category — pick another chip or create one.'} />
    );
  } else {
    body = (
      <ul className={isDesktop ? 'grid grid-cols-2 gap-3' : 'space-y-2'} aria-label="Recipes">
        {items.map((r) => (
          <li key={r.recipe_id}>
            <button type="button"
              onClick={() => setMode({ name: 'cook', recipeId: r.recipe_id })}
              className="flex w-full items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2.5 min-h-[44px] text-left text-sm hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
              <span className="font-medium text-gray-900 dark:text-white">{r.title}</span>
              <span className="text-xs text-gray-500 dark:text-gray-400">
                {r.category} · {r.times_prepared}× prepared
              </span>
            </button>
          </li>
        ))}
      </ul>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2" role="group"
        aria-label="Filter by category">
        {CATEGORIES.map((c) => (
          <button key={c} type="button" onClick={() => setFilter(c)}
            aria-pressed={filter === c}
            className={
              filter === c
                ? 'rounded-full bg-blue-600 px-3 py-1.5 min-h-[44px] text-sm text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500'
                : 'rounded-full border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 min-h-[44px] text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500'
            }>
            {c}
          </button>
        ))}
      </div>

      <button type="button" onClick={() => setMode({ name: 'create' })}
        className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 min-h-[44px] text-sm font-medium text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
        + Create recipe
      </button>

      <Section title="Recipes">{body}</Section>
    </div>
  );
}