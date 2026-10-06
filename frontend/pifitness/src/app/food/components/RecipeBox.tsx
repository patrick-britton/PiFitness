/**
 * Recipe Box skeleton (010-002 T06).
 * Category-filter affordance + a Create entry point + recipe list with
 * loading / empty / error states. Until the 010-002 DDL is applied the list
 * reads "Recipe backend not yet available" (AC-4). Recipe creation is a mode
 * inside the box (T01) and cook-mode checklist / per-step timers arrive with
 * the recipe-detail feature — this skeleton only signals the affordances and
 * never duplicates the shared finder/amount-picker.
 */

'use client';

import { useCallback, useEffect, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import { API } from '../../../lib/api-client';
import type { RecipeCategory, RecipeSummary } from '../../../lib/types/food-contract';
import { EmptyBox, LoadingSkeleton, Section } from './food-ui';

const CATEGORIES: (RecipeCategory | 'All')[] = [
  'All', 'Breakfast', 'Lunch', 'Dinner', 'Dessert', 'Drink', 'Appetizer',
];

type Region =
  | { status: 'loading' }
  | { status: 'ready'; items: RecipeSummary[] }
  | { status: 'error'; error: string };

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export default function RecipeBox() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

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

  const items =
    region.status === 'ready'
      ? (filter === 'All'
          ? region.items
          : region.items.filter((r) => r.category === filter))
      : [];

  let body;
  if (region.status === 'loading') {
    body = <LoadingSkeleton rows={4} testId="recipe" />;
  } else if (region.status === 'error') {
    body = (
      <EmptyBox title="Recipe box"
        message="Recipe backend not yet available — apply the 010-002 DDL and refresh." />
    );
  } else if (items.length === 0) {
    body = (
      <EmptyBox title={filter === 'All' ? 'Recipe box' : 'No recipes in this category'}
        message="No recipes yet — use Create to add one." />
    );
  } else {
    body = (
      <ul className={isDesktop ? 'grid grid-cols-2 gap-3' : 'space-y-2'} aria-label="Recipes">
        {items.map((r) => (
          <li key={r.recipe_id}
            className="flex items-center justify-between gap-2 rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-sm">
            <span className="text-gray-900 dark:text-white">{r.title}</span>
            <span className="text-xs text-gray-500 dark:text-gray-400">
              {r.category} · {r.times_prepared}× prepared
            </span>
          </li>
        ))}
      </ul>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2" aria-label="Filter by category">
        {CATEGORIES.map((c) => (
          <button key={c} type="button" onClick={() => setFilter(c)} aria-pressed={filter === c}
            className={
              filter === c
                ? 'rounded-full bg-blue-600 px-3 py-1.5 text-sm text-white'
                : 'rounded-full border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm text-gray-700 dark:text-gray-300'
            }>
            {c}
          </button>
        ))}
      </div>

      <button type="button"
        className="rounded-md bg-blue-600 hover:bg-blue-700 px-4 py-2 text-sm text-white">
        + Create recipe
      </button>

      <Section title="Recipes">{body}</Section>

      <p className="text-xs text-gray-500 dark:text-gray-400">
        Cook-mode checklist &amp; per-step timers arrive with the recipe-detail feature
        (client-only, no writes on check).
      </p>
    </div>
  );
}