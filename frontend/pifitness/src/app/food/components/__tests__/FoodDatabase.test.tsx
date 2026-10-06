/** T06: Food Database skeleton. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import FoodDatabase from '../FoodDatabase';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
vi.mock('@/lib/api-client', () => ({ API: { food: { foods: vi.fn() } } }));
const foods = vi.mocked(API.food.foods);
const row = (o: object) => ({ food_id: 1, name: 'Oats', nutrients: { kcal_per_100g: 100, protein_g: null, fat_g: null, carbs_g: null, fiber_g: null, sugars_g: null, sodium_mg: null, caffeine_mg: null }, is_deleted: false, deleted_at: null, ...o });
beforeEach(() => { vi.clearAllMocks(); useViewportStore.setState({ layoutVariant: 'desktop' }); });
describe('FoodDatabase', () => {
  it('renders in all three layouts', async () => {
    foods.mockResolvedValue({ data: [], next_cursor: null });
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = render(<FoodDatabase />);
      expect(await screen.findByRole('heading', { name: 'Foods' })).toBeInTheDocument();
      unmount();
    }
  });
  it('shows backend-missing placeholder on error', async () => {
    foods.mockRejectedValue(new Error('down'));
    render(<FoodDatabase />);
    expect(await screen.findByText(/backend not yet available/i)).toBeInTheDocument();
  });
  it('shows edit/delete affordances and hides deleted by default', async () => {
    foods.mockResolvedValue({ data: [row({}), row({ food_id: 2, name: 'Gone', is_deleted: true })], next_cursor: null });
    render(<FoodDatabase />);
    expect(await screen.findByText('Oats')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Edit Oats/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Delete Oats/i })).toBeInTheDocument();
    expect(screen.queryByText('Gone')).not.toBeInTheDocument();
  });
  it('show-deleted reveals + restore appears', async () => {
    const user = userEvent.setup();
    foods.mockResolvedValue({ data: [row({ food_id: 2, name: 'Gone', is_deleted: true })], next_cursor: null });
    render(<FoodDatabase />);
    await user.click(screen.getByLabelText(/Show deleted/i));
    expect(await screen.findByRole('button', { name: /Restore Gone/i })).toBeInTheDocument();
  });
});
