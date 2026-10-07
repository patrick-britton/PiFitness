/** T06: RecipeCreate - shared picker, persist-at-pick, save gates, preview. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RecipeCreate, { buildSavePayload, previewTotals } from '../RecipeCreate';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import type { CombinedSearchResult, UsualFood } from '@/lib/types/food-contract';

vi.mock('@/lib/api-client', () => ({
  API: {
    food: {
      suggest: vi.fn(), combinedSearch: vi.fn(), byBarcode: vi.fn(),
      foodById: vi.fn(), persistApiFood: vi.fn(), saveRecipe: vi.fn(),
      recipeDetail: vi.fn(), updateRecipe: vi.fn(), deleteRecipe: vi.fn(),
    },
    foodLookup: { lookupOff: vi.fn() },
  },
}));

vi.mock('next/dynamic', () => ({
  default: () => () => null,
}));

const suggest = vi.mocked(API.food.suggest);
const combinedSearch = vi.mocked(API.food.combinedSearch);
const persistApiFood = vi.mocked(API.food.persistApiFood);
const saveRecipe = vi.mocked(API.food.saveRecipe);
const updateRecipe = vi.mocked(API.food.updateRecipe);
const deleteRecipe = vi.mocked(API.food.deleteRecipe);

const SUGGEST: UsualFood[] = [];
function dbHit(): CombinedSearchResult {
  return {
    kind: 'db-food', food_id: 11, name: 'Oats', mode_qty: null,
    mode_unit: null, kcal_preview: 389, data_quality: 'High',
    rank: 1, result_source: 'saved', usda_score: null,
    nutrients: {
      kcal_per_100g: 389, protein_g: 17, fat_g: 7, carbs_g: 66,
      fiber_g: 10, sugars_g: 1, sodium_mg: 2, caffeine_mg: null,
    },
    serving_size: null, serving_unit: null, serving_text: null,
  };
}
function apiHit(): CombinedSearchResult {
  return {
    kind: 'usda', food_id: null, name: 'Wild rice', mode_qty: null,
    mode_unit: null, kcal_preview: 120, data_quality: 'High',
    rank: 2, result_source: 'usda', usda_score: 900,
    fdcId: 555, barcode: null, nutrients_raw: { foodNutrients: [] },
    nutrients: {
      kcal_per_100g: 120, protein_g: 4, fat_g: 0.5, carbs_g: 26,
      fiber_g: 2, sugars_g: 1, sodium_mg: 5, caffeine_mg: null,
    },
    serving_size: null, serving_unit: null, serving_text: null,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  suggest.mockResolvedValue(SUGGEST);
  combinedSearch.mockResolvedValue([]);
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

describe('RecipeCreate', () => {
  it('renders in all three layouts', async () => {
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = render(<RecipeCreate onBack={vi.fn()} onSaved={vi.fn()} />);
      expect(await screen.findByTestId('recipe-create')).toBeInTheDocument();
      unmount();
    }
  });

  it('blocks save until title + ingredient are set; saved rows skip persist', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([dbHit()]);
    render(<RecipeCreate onBack={vi.fn()} onSaved={vi.fn()} />);
    const save = screen.getByRole('button', { name: /Save recipe/i });
    expect(save).toBeDisabled();
    await user.type(screen.getByLabelText('Title'), 'Porridge');
    expect(screen.getByRole('button', { name: /Save recipe/i })).toBeDisabled();
    await user.type(screen.getByPlaceholderText('Search foods'), 'oats');
    await user.click(screen.getByRole('button', { name: 'Search' }));
    await user.click(await screen.findByRole('button', { name: /Oats/ }));
    await user.click(screen.getByRole('button', { name: 'Add' }));
    await waitFor(() => expect(screen.getByRole('list', { name: 'Ingredient lines' })).toHaveTextContent('Oats'));
    expect(persistApiFood).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.getByRole('button', { name: /Save recipe/i })).toBeEnabled());
  });

  it('API-hit pick resolves to a food_id via persist and saves the payload', async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    combinedSearch.mockResolvedValue([apiHit()]);
    persistApiFood.mockResolvedValue({ food_id: 77 });
    saveRecipe.mockResolvedValue({ recipe_id: 9, title: 'Rice bowl', category: 'Dinner', times_prepared: 0 });
    render(<RecipeCreate onBack={vi.fn()} onSaved={onSaved} />);
    await user.type(screen.getByLabelText('Title'), 'Rice bowl');
    await user.type(screen.getByPlaceholderText('Search foods'), 'rice');
    await user.click(screen.getByRole('button', { name: 'Search' }));
    await user.click(await screen.findByRole('button', { name: /Wild rice/ }));
    await user.clear(screen.getByLabelText('Name'));
    await user.type(screen.getByLabelText('Name'), 'Wild rice mix');
    await user.click(screen.getByRole('button', { name: 'Add' }));
    await waitFor(() => expect(persistApiFood).toHaveBeenCalledTimes(1));
    expect(persistApiFood).toHaveBeenCalledWith({
      usda_payload: expect.objectContaining({ name: 'Wild rice mix' }),
    });
    await user.click(screen.getByRole('button', { name: /Save recipe/i }));
    await waitFor(() => expect(saveRecipe).toHaveBeenCalledTimes(1));
    expect(saveRecipe).toHaveBeenCalledWith(expect.objectContaining({
      title: 'Rice bowl', category: 'Dinner', servings_count: 4,
      serving_unit: 'serving',
      ingredients: [{ food_id: 77, qty: 100, unit: 'g' }],
      steps: [],
    }));
    expect(onSaved).toHaveBeenCalled();
  });

  it('save carries built steps; skip path saves with steps: []', async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    combinedSearch.mockResolvedValue([dbHit()]);
    saveRecipe.mockResolvedValue({ recipe_id: 9, title: 'Porridge', category: 'Dinner', times_prepared: 0 });
    render(<RecipeCreate onBack={vi.fn()} onSaved={onSaved} />);
    await user.type(screen.getByLabelText('Title'), 'Porridge');
    await user.type(screen.getByPlaceholderText('Search foods'), 'oats');
    await user.click(screen.getByRole('button', { name: 'Search' }));
    await user.click(await screen.findByRole('button', { name: /Oats/ }));
    await user.click(screen.getByRole('button', { name: 'Add' }));
    await waitFor(() => expect(screen.getByRole('list', { name: 'Ingredient lines' })).toHaveTextContent('Oats'));
    // Skip path: save with no steps.
    await user.click(screen.getByRole('button', { name: /Save recipe/i }));
    await waitFor(() => expect(saveRecipe).toHaveBeenCalledTimes(1));
    expect(saveRecipe).toHaveBeenCalledWith(expect.objectContaining({ steps: [] }));
    // Build one step (default 60s) and re-save with it attached.
    await user.type(screen.getByLabelText('Instruction'), 'Stir it.');
    await user.click(screen.getByRole('button', { name: 'Add step 1' }));
    await user.click(screen.getByRole('button', { name: /Save recipe/i }));
    await waitFor(() => expect(saveRecipe).toHaveBeenCalledTimes(2));
    expect(saveRecipe).toHaveBeenLastCalledWith(expect.objectContaining({
      steps: [{ step_no: 1, instruction: 'Stir it.', timer_seconds: 60, foods: [] }],
    }));
    expect(onSaved).toHaveBeenCalled();
  });

  it('abandoning the flow writes nothing to recipe tables', async () => {
    const user = userEvent.setup();
    const onBack = vi.fn();
    render(<RecipeCreate onBack={onBack} onSaved={vi.fn()} />);
    await user.click(screen.getByRole('button', { name: /Back to recipes/i }));
    expect(onBack).toHaveBeenCalledTimes(1);
    expect(saveRecipe).not.toHaveBeenCalled();
    expect(persistApiFood).not.toHaveBeenCalled();
  });

  it('buildSavePayload matches SaveRecipeRequest', () => {
    expect(buildSavePayload({
      title: '  Soup ', category: 'Side', servingsCount: 2,
      servingUnit: ' bowl ', lines: [{ food_id: 5, name: 'X', qty: 50, unit: 'g', kcalPer100g: 100 }],
    })).toEqual({
      title: 'Soup', category: 'Side',
      ingredients: [{ food_id: 5, qty: 50, unit: 'g' }],
      servings_count: 2, serving_unit: 'bowl', steps: [],
    });
  });

  it('previewTotals sums convertible lines and nulls when none convert', () => {
    expect(previewTotals([
      { qty: 100, unit: 'g', kcalPer100g: 100 },
      { qty: 1, unit: 'cup', kcalPer100g: 50 },
    ]).total).toBeCloseTo(220);
    expect(previewTotals([{ qty: 1, unit: 'serving', kcalPer100g: 100 }]).total).toBeNull();
  });
});
