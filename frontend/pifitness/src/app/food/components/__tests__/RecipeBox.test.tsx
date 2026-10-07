/** T05: Recipe Box list + mode machine (010-005). */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RecipeBox from '../RecipeBox';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
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
const recipes = vi.mocked(API.food.recipes);
const recipeDetail = vi.mocked(API.food.recipeDetail);
const getCookState = vi.mocked(API.food.getCookState);
const suggest = vi.mocked(API.food.suggest);
const combinedSearch = vi.mocked(API.food.combinedSearch);
beforeEach(() => {
  vi.clearAllMocks();
  suggest.mockResolvedValue([]);
  combinedSearch.mockResolvedValue([]);
  useViewportStore.setState({ layoutVariant: 'desktop' });
});
describe('RecipeBox', () => {
  it('renders in all three layouts', async () => {
    recipes.mockResolvedValue([]);
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = render(<RecipeBox />);
      expect(await screen.findByText(/Create recipe/i)).toBeInTheDocument();
      unmount();
    }
  });
  it('shows loading state', async () => {
    recipes.mockReturnValue(new Promise(() => {}));
    render(<RecipeBox />);
    expect(screen.getByTestId('recipe-loading')).toBeInTheDocument();
  });
  it('shows empty state when the box has no recipes', async () => {
    recipes.mockResolvedValue([]);
    render(<RecipeBox />);
    expect(await screen.findByText(/No recipes yet/i)).toBeInTheDocument();
  });
  it('shows error state with a retry that reloads', async () => {
    const user = userEvent.setup();
    recipes.mockRejectedValueOnce(new Error('down'));
    recipes.mockResolvedValueOnce([]);
    render(<RecipeBox />);
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText(/No recipes yet/i)).toBeInTheDocument();
    expect(recipes).toHaveBeenCalledTimes(2);
  });
  it('sorts cards most-made first (title asc on ties)', async () => {
    recipes.mockResolvedValue([
      { recipe_id: 1, title: 'Stew', category: 'Dinner', times_prepared: 5 },
      { recipe_id: 2, title: 'Pancakes', category: 'Breakfast', times_prepared: 5 },
      { recipe_id: 3, title: 'Salad', category: 'Lunch', times_prepared: 1 },
    ]);
    render(<RecipeBox />);
    const metas = await screen.findAllByText(/× prepared/);
    expect(metas).toHaveLength(3);
    expect(metas[0].closest('li')).toHaveTextContent('Pancakes');
    expect(metas[1].closest('li')).toHaveTextContent('Stew');
    expect(metas[2].closest('li')).toHaveTextContent('Salad');
  });

  it('filters by category with single-select chips', async () => {
    const user = userEvent.setup();
    recipes.mockResolvedValue([
      { recipe_id: 1, title: 'Pancakes', category: 'Breakfast', times_prepared: 2 },
      { recipe_id: 2, title: 'Salad', category: 'Lunch', times_prepared: 1 },
    ]);
    render(<RecipeBox />);
    expect(await screen.findByText('Pancakes')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'All' }))
      .toHaveAttribute('aria-pressed', 'true');
    await user.click(screen.getByRole('button', { name: 'Lunch' }));
    expect(screen.getByRole('button', { name: 'Lunch' }))
      .toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: 'All' }))
      .toHaveAttribute('aria-pressed', 'false');
    expect(screen.queryByText('Pancakes')).not.toBeInTheDocument();
    expect(screen.getByText('Salad')).toBeInTheDocument();
    // a chip with no matches shows the filtered empty state
    await user.click(screen.getByRole('button', { name: 'Dessert' }));
    expect(screen.getByText('No recipes in this category')).toBeInTheDocument();
  });

  it('opens create mode and returns to the list', async () => {
    const user = userEvent.setup();
    recipes.mockResolvedValue([
      { recipe_id: 1, title: 'Pancakes', category: 'Breakfast', times_prepared: 2 },
    ]);
    render(<RecipeBox />);
    expect(await screen.findByText('Pancakes')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Create recipe/i }));
    expect(screen.getByTestId('recipe-create')).toBeInTheDocument();
    expect(screen.getByLabelText('Title')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Back to recipes/i }));
    expect(await screen.findByText('Pancakes')).toBeInTheDocument();
  });

  it('edit from cook mode re-enters at the ingredient list; back returns to cook', async () => {
    const user = userEvent.setup();
    recipes.mockResolvedValue([
      { recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2 },
    ]);
    recipeDetail.mockResolvedValue({
      recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2,
      food_id: 100, servings_count: 4, serving_unit: 'pancake',
      ingredients: [{ food_id: 11, qty: 100, unit: 'g', name: 'Oats' }],
      steps: [],
    });
    getCookState.mockResolvedValue({
      recipe_id: 7, checked_steps: [], checked_foods: [], timers: [], updated_at: null,
    });
    suggest.mockResolvedValue([]);
    combinedSearch.mockResolvedValue([]);
    render(<RecipeBox />);
    expect(await screen.findByText('Pancakes')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Pancakes/ }));
    expect(await screen.findByTestId('recipe-cook')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Edit recipe' }));
    expect(await screen.findByTestId('recipe-edit')).toBeInTheDocument();
    // Edit starts at ingredient verification with the saved line listed.
    expect(await screen.findByRole('list', { name: 'Ingredient lines' }))
      .toHaveTextContent('Oats');
    await user.click(screen.getByRole('button', { name: /Back to recipes/i }));
    expect(await screen.findByTestId('recipe-cook')).toBeInTheDocument();
  });

  it('opens cook mode for the tapped card and returns', async () => {
    const user = userEvent.setup();
    recipes.mockResolvedValue([
      { recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2 },
      { recipe_id: 9, title: 'Salad', category: 'Lunch', times_prepared: 1 },
    ]);
    recipeDetail.mockResolvedValue({
      recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2,
      food_id: 100, servings_count: 4, serving_unit: 'pancake',
      ingredients: [], steps: [],
    });
    getCookState.mockResolvedValue({
      recipe_id: 7, checked_steps: [], checked_foods: [], timers: [], updated_at: null,
    });
    render(<RecipeBox />);
    expect(await screen.findByText('Pancakes')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Pancakes/ }));
    expect(await screen.findByTestId('recipe-cook')).toBeInTheDocument();
    expect(screen.getByText('No steps')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Back to recipes/i }));
    expect(await screen.findByText('Salad')).toBeInTheDocument();
  });

  it('complete bumps the count and re-sorts most-made first', async () => {
    const user = userEvent.setup();
    recipes.mockResolvedValue([
      { recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2 },
      { recipe_id: 9, title: 'Salad', category: 'Lunch', times_prepared: 3 },
    ]);
    recipeDetail.mockResolvedValue({
      recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2,
      food_id: 100, servings_count: 4, serving_unit: 'pancake',
      ingredients: [], steps: [],
    });
    getCookState.mockResolvedValue({
      recipe_id: 7, checked_steps: [], checked_foods: [], timers: [], updated_at: null,
    });
    const completeRecipe = vi.mocked(API.food.completeRecipe);
    completeRecipe.mockResolvedValue({
      recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 4,
    });
    render(<RecipeBox />);
    // Salad (3) outranks Pancakes (2) before completing.
    let metas = await screen.findAllByText(/× prepared/);
    expect(metas[0].closest('li')).toHaveTextContent('Salad');
    await user.click(screen.getByRole('button', { name: /Pancakes/ }));
    expect(await screen.findByTestId('recipe-cook')).toBeInTheDocument();
    // The completion reloads the list — point the re-mock first.
    recipes.mockResolvedValue([
      { recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 4 },
      { recipe_id: 9, title: 'Salad', category: 'Lunch', times_prepared: 3 },
    ]);
    await user.click(screen.getByRole('button', { name: 'Complete recipe' }));
    await user.click(screen.getByRole('button', { name: 'Yes, complete' }));
    // After complete the list reloads with Pancakes (4) on top.
    await waitFor(() => expect(completeRecipe).toHaveBeenCalledWith(7));
    metas = await screen.findAllByText(/× prepared/);
    expect(metas[0].closest('li')).toHaveTextContent('Pancakes');
    expect(metas[0]).toHaveTextContent('4× prepared');
  });
});
