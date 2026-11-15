/** T06: Food Database skeleton. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import FoodDatabase from '../FoodDatabase';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
vi.mock('@/lib/api-client', () => ({ API: { food: { foods: vi.fn(), updateFood: vi.fn() } } }));
const foods = vi.mocked(API.food.foods);
const updateFood = vi.mocked(API.food.updateFood);
const row = (o: object) => ({ food_id: 1, name: 'Oats', nutrients: { kcal_per_100g: 100, protein_g: null, fat_g: null, carbs_g: null, fiber_g: null, sugars_g: null, sodium_mg: null, caffeine_mg: null }, is_deleted: false, deleted_at: null, ...o });
beforeEach(() => { vi.clearAllMocks(); useViewportStore.setState({ layoutVariant: 'desktop' }); });
describe('FoodDatabase', () => {
  // VERIFIED: Food database view renders heading across desktop, portrait, and landscape viewport layouts
  it('renders in all three layouts', async () => {
    foods.mockResolvedValue({ data: [], next_cursor: null });
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = render(<FoodDatabase />);
      expect(await screen.findByRole('heading', { name: 'Foods' })).toBeInTheDocument();
      unmount();
    }
  });

  // VERIFIED: Network failure displays placeholder notice rather than crashing
  it('shows backend-missing placeholder on error', async () => {
    foods.mockRejectedValue(new Error('down'));
    render(<FoodDatabase />);
    expect(await screen.findByText(/backend not yet available/i)).toBeInTheDocument();
  });

  // VERIFIED: Food list displays edit and delete controls for active items while hiding soft-deleted items
  it('shows edit/delete affordances and hides deleted by default', async () => {
    foods.mockResolvedValue({ data: [row({}), row({ food_id: 2, name: 'Gone', is_deleted: true })], next_cursor: null });
    render(<FoodDatabase />);
    expect(await screen.findByText('Oats')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Edit Oats/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Delete Oats/i })).toBeInTheDocument();
    expect(screen.queryByText('Gone')).not.toBeInTheDocument();
  });

  // VERIFIED: Toggling show-deleted filter reveals soft-deleted items with restore controls
  it('show-deleted reveals + restore appears', async () => {
    const user = userEvent.setup();
    foods.mockResolvedValue({ data: [row({ food_id: 2, name: 'Gone', is_deleted: true })], next_cursor: null });
    render(<FoodDatabase />);
    await user.click(screen.getByLabelText(/Show deleted/i));
    expect(await screen.findByRole('button', { name: /Restore Gone/i })).toBeInTheDocument();
  });

  // VERIFIED: Editing a food pre-fills existing values and persists name changes via updateFood API
  it('opens a pre-filled editor on Edit and persists via updateFood', async () => {
    const user = userEvent.setup();
    foods.mockResolvedValue({ data: [row({ food_id: 1, name: 'Oats' })], next_cursor: null });
    updateFood.mockResolvedValue(row({ food_id: 1, name: 'Rolled Oats' }));
    render(<FoodDatabase />);
    await user.click(await screen.findByRole('button', { name: /Edit Oats/i }));
    expect(screen.getByLabelText(/Name/i)).toHaveValue('Oats');
    await user.clear(screen.getByLabelText(/Name/i));
    await user.type(screen.getByLabelText(/Name/i), 'Rolled Oats');
    await user.click(screen.getByRole('button', { name: /Save/i }));
    await waitFor(() =>
      expect(updateFood).toHaveBeenCalledWith(1, expect.objectContaining({ name: 'Rolled Oats' }))
    );
    await waitFor(() => expect(foods).toHaveBeenCalledTimes(2));
  });
});
