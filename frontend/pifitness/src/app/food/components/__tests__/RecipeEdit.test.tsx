/** T09: RecipeEdit - edit starts at ingredients, PUT full payload, delete confirm. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RecipeEdit from '../RecipeEdit';
import { detailToEditInitial } from '../RecipeCreate';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import type { RecipeDetail } from '@/lib/types/food-contract';

vi.mock('@/lib/api-client', () => ({
  API: {
    food: {
      recipes: vi.fn(), suggest: vi.fn(), combinedSearch: vi.fn(),
      byBarcode: vi.fn(), foodById: vi.fn(),
      persistApiFood: vi.fn(), saveRecipe: vi.fn(),
      recipeDetail: vi.fn(), getCookState: vi.fn(),
      saveCookState: vi.fn(), completeRecipe: vi.fn(),
      updateRecipe: vi.fn(), deleteRecipe: vi.fn(),
    },
    foodLookup: { lookupOff: vi.fn() },
  },
}));
vi.mock('next/dynamic', () => ({ default: () => () => null }));

const recipeDetail = vi.mocked(API.food.recipeDetail);
const updateRecipe = vi.mocked(API.food.updateRecipe);
const deleteRecipe = vi.mocked(API.food.deleteRecipe);
const suggest = vi.mocked(API.food.suggest);
const combinedSearch = vi.mocked(API.food.combinedSearch);

const DETAIL: RecipeDetail = {
  recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2,
  food_id: 100, servings_count: 4, serving_unit: 'pancake',
  ingredients: [{ food_id: 11, qty: 100, unit: 'g', name: 'Oats' }],
  steps: [
    {
      step_no: 1, instruction: 'Mix it.', timer_seconds: 60,
      foods: [{ food_id: 11, qty: 50, unit: 'g', name: 'Oats' }],
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  suggest.mockResolvedValue([]);
  combinedSearch.mockResolvedValue([]);
  recipeDetail.mockResolvedValue(DETAIL);
  updateRecipe.mockResolvedValue({
    recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2,
  });
  deleteRecipe.mockResolvedValue({ success: true, recipe_id: 7, food_id: 100 });
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

describe('detailToEditInitial', () => {
  it('maps detail into ingredient-first edit initials', () => {
    const init = detailToEditInitial(DETAIL);
    expect(init.recipeId).toBe(7);
    expect(init.title).toBe('Pancakes');
    expect(init.lines).toEqual([{
      food_id: 11, name: 'Oats', qty: 100, unit: 'g', kcalPer100g: null,
    }]);
    expect(init.steps).toEqual([{
      step_no: 1, instruction: 'Mix it.', timer_seconds: 60,
      foods: [{ food_id: 11, qty: 50, unit: 'g' }],
    }]);
  });
});

describe('RecipeEdit', () => {
  it('renders in all three layouts starting at the ingredient list', async () => {
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = render(
        <RecipeEdit recipeId={7} onBack={vi.fn()} onSaved={vi.fn()} />);
      expect(await screen.findByTestId('recipe-edit')).toBeInTheDocument();
      // Ingredient verification comes first: the saved line is listed.
      expect(await screen.findByRole('list', { name: 'Ingredient lines' }))
        .toHaveTextContent('Oats');
      expect(screen.getByDisplayValue('Pancakes')).toBeInTheDocument();
      unmount();
    }
  });

  it('save issues PUT with the full payload (ingredients + steps + servings)', async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    render(<RecipeEdit recipeId={7} onBack={vi.fn()} onSaved={onSaved} />);
    await screen.findByRole('list', { name: 'Ingredient lines' });
    await user.click(screen.getByRole('button', { name: 'Save changes' }));
    await waitFor(() => expect(updateRecipe).toHaveBeenCalledTimes(1));
    expect(updateRecipe).toHaveBeenCalledWith(7, expect.objectContaining({
      title: 'Pancakes', category: 'Breakfast', servings_count: 4,
      serving_unit: 'pancake',
      ingredients: [{ food_id: 11, qty: 100, unit: 'g' }],
      steps: [expect.objectContaining({ step_no: 1, instruction: 'Mix it.' })],
    }));
    expect(onSaved).toHaveBeenCalledTimes(1);
  });

  it('delete asks for confirmation, then deletes and leaves edit', async () => {
    const user = userEvent.setup();
    const onBack = vi.fn();
    render(<RecipeEdit recipeId={7} onBack={onBack} onSaved={vi.fn()} />);
    await screen.findByRole('list', { name: 'Ingredient lines' });
    await user.click(screen.getByRole('button', { name: 'Delete recipe' }));
    // Confirm dialog appears; nothing deleted yet.
    expect(screen.getByRole('dialog', { name: 'Delete recipe' })).toBeInTheDocument();
    expect(deleteRecipe).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Yes, delete' }));
    await waitFor(() => expect(deleteRecipe).toHaveBeenCalledWith(7));
    expect(onBack).toHaveBeenCalledTimes(1);
  });

  it('delete cancel keeps the recipe (no DELETE)', async () => {
    const user = userEvent.setup();
    render(<RecipeEdit recipeId={7} onBack={vi.fn()} onSaved={vi.fn()} />);
    await screen.findByRole('list', { name: 'Ingredient lines' });
    await user.click(screen.getByRole('button', { name: 'Delete recipe' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(deleteRecipe).not.toHaveBeenCalled();
    expect(screen.queryByRole('dialog', { name: 'Delete recipe' })).not.toBeInTheDocument();
  });
});
