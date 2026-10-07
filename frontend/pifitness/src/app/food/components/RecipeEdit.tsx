/**
 * RecipeEdit - 010-005 T09 edit entry point.
 * Loads RecipeDetail, converts to RecipeCreate edit-mode initials
 * (ingredient verify/add/remove/edit first, then per-step edits), and
 * hands save (PUT) + delete (confirm → DELETE) to RecipeCreate.
 * Token-only classes, 44px targets.
 */
'use client';

import { useEffect, useState } from 'react';
import { API } from '../../../lib/api-client';
import type { RecipeDetail, RecipeSummary } from '../../../lib/types/food-contract';
import { EmptyBox, ErrorBox, LoadingSkeleton } from './food-ui';
import RecipeCreate, { detailToEditInitial, type RecipeEditInitial } from './RecipeCreate';

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export default function RecipeEdit({ recipeId, onBack, onSaved }: {
  recipeId: number;
  onBack: () => void;
  onSaved: (saved: RecipeSummary) => void;
}) {
  const [initial, setInitial] = useState<RecipeEditInitial | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    (async () => {
      try {
        const detail: RecipeDetail = await API.food.recipeDetail(recipeId);
        if (live) setInitial(detailToEditInitial(detail));
      } catch (e) {
        if (live) setError(errMsg(e));
      }
    })();
    return () => { live = false; };
  }, [recipeId]);

  if (error != null) {
    return (
      <div className="space-y-4" data-testid="recipe-edit">
        <button type="button" onClick={onBack}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          ← Back to recipes
        </button>
        <ErrorBox label="Recipe" message={error} />
      </div>
    );
  }
  if (initial == null) return <LoadingSkeleton rows={5} testId="recipe-edit" />;
  if (initial.lines.length === 0) {
    return (
      <div className="space-y-4" data-testid="recipe-edit">
        <EmptyBox title="No ingredients"
          message="This recipe has no ingredient list to verify — it cannot be edited." />
        <button type="button" onClick={onBack}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 min-h-[44px] text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500">
          ← Back to recipes
        </button>
      </div>
    );
  }
  return (
    <div data-testid="recipe-edit">
      <RecipeCreate onBack={onBack} onSaved={onSaved} edit={initial} />
    </div>
  );
}
