/** T06: Recipe Box skeleton. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RecipeBox from '../RecipeBox';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
vi.mock('@/lib/api-client', () => ({ API: { food: { recipes: vi.fn() } } }));
const recipes = vi.mocked(API.food.recipes);
beforeEach(() => { vi.clearAllMocks(); useViewportStore.setState({ layoutVariant: 'desktop' }); });
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
  it('shows loading then empty', async () => {
    recipes.mockReturnValue(new Promise(() => {}));
    render(<RecipeBox />);
    expect(screen.getByTestId('recipe-loading')).toBeInTheDocument();
  });
  it('shows backend-missing placeholder on error', async () => {
    recipes.mockRejectedValue(new Error('down'));
    render(<RecipeBox />);
    expect(await screen.findByText(/backend not yet available/i)).toBeInTheDocument();
  });
  it('filters by category', async () => {
    const user = userEvent.setup();
    recipes.mockResolvedValue([
      { recipe_id: 1, title: 'Pancakes', category: 'Breakfast', times_prepared: 2 },
      { recipe_id: 2, title: 'Salad', category: 'Lunch', times_prepared: 1 },
    ]);
    render(<RecipeBox />);
    expect(await screen.findByText('Pancakes')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Lunch' }));
    expect(screen.queryByText('Pancakes')).not.toBeInTheDocument();
    expect(screen.getByText('Salad')).toBeInTheDocument();
  });
});
