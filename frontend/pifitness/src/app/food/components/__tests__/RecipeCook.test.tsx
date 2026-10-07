/** T08: RecipeCook - reorder checklist, independent timers, persist/restore. */
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import RecipeCook, {
  foodCheckKey, remainingSeconds, formatCountdown,
} from '../RecipeCook';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import type { CookState, RecipeDetail } from '@/lib/types/food-contract';

vi.mock('@/lib/api-client', () => ({
  API: {
    food: {
      recipes: vi.fn(), suggest: vi.fn(), combinedSearch: vi.fn(),
      byBarcode: vi.fn(), foodById: vi.fn(),
      persistApiFood: vi.fn(), saveRecipe: vi.fn(),
      recipeDetail: vi.fn(), getCookState: vi.fn(),
      saveCookState: vi.fn(), completeRecipe: vi.fn(),
    },
    foodLookup: { lookupOff: vi.fn() },
  },
}));

vi.mock('next/dynamic', () => ({ default: () => () => null }));

const recipeDetail = vi.mocked(API.food.recipeDetail);
const getCookState = vi.mocked(API.food.getCookState);
const saveCookState = vi.mocked(API.food.saveCookState);
const completeRecipe = vi.mocked(API.food.completeRecipe);

const DETAIL: RecipeDetail = {
  recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 2,
  food_id: 100, servings_count: 4, serving_unit: 'pancake',
  ingredients: [{ food_id: 11, qty: 100, unit: 'g', name: 'Oats' }],
  steps: [
    {
      step_no: 1, instruction: 'Mix the oats.', timer_seconds: 120,
      foods: [{ food_id: 11, qty: 100, unit: 'g', name: 'Oats' }],
    },
    {
      step_no: 2, instruction: 'Rest the batter.', timer_seconds: 60,
      foods: [],
    },
  ],
};

const EMPTY_COOK: CookState = {
  recipe_id: 7, checked_steps: [], checked_foods: [], timers: [], updated_at: null,
};

function mockHydrate(detail: RecipeDetail = DETAIL, cook: CookState = EMPTY_COOK) {
  recipeDetail.mockResolvedValue(detail);
  getCookState.mockResolvedValue({ ...cook });
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-10-07T12:00:00.000Z'));
  mockHydrate();
  saveCookState.mockResolvedValue({ ...EMPTY_COOK });
  completeRecipe.mockResolvedValue({
    recipe_id: 7, title: 'Pancakes', category: 'Breakfast', times_prepared: 3,
  });
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

afterEach(() => {
  vi.useRealTimers();
});

async function renderCook(props?: Partial<{ onBack: () => void }>) {
  const view = render(<RecipeCook recipeId={7}
    onBack={props?.onBack ?? vi.fn()}
    onCompleted={vi.fn()} onEdit={vi.fn()} />);
  // Flush the hydration microtasks (Promise.all of two mocked GETs);
  // no waitFor — it stalls under fake timers.
  await act(async () => {});
  expect(screen.getByTestId('recipe-cook')).toBeInTheDocument();
  return view;
}
describe('RecipeCook helpers', () => {
  it('foodCheckKey inverts CookFoodCheck exactly', () => {
    expect(foodCheckKey({ step_no: 2, food_id: 11, unit: 'g' })).toBe('2|11|g');
  });

  it('remainingSeconds derives from the absolute deadline (clamped at 0)', () => {
    const endsAt = new Date('2026-10-07T12:02:00.000Z').toISOString();
    expect(remainingSeconds(endsAt, Date.parse('2026-10-07T12:00:30.000Z'))).toBe(90);
    expect(remainingSeconds(endsAt, Date.parse('2026-10-07T12:05:00.000Z'))).toBe(0);
    expect(remainingSeconds('not-a-time', Date.now())).toBe(0);
  });

  it('formatCountdown renders m:ss and never negatives', () => {
    expect(formatCountdown(150)).toBe('2:30');
    expect(formatCountdown(60)).toBe('1:00');
    expect(formatCountdown(0)).toBe('0:00');
    expect(formatCountdown(-5)).toBe('0:00');
  });
});

describe('RecipeCook', () => {
  it('renders all steps + timers at once in all three layouts', async () => {
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = await renderCook();
      expect(screen.getByText('Mix the oats.')).toBeInTheDocument();
      expect(screen.getByText('Rest the batter.')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Start step 1 timer' })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Start step 2 timer' })).toBeInTheDocument();
      unmount();
    }
  });

  it('checking a step sinks it to the bottom; unchecking restores position', async () => {
    await renderCook();
    expect(screen.getByRole('list', { name: 'Cook steps' }).children[0])
      .toHaveAttribute('data-testid', 'cook-step-1');
    fireEvent.click(screen.getByRole('checkbox', { name: 'Step 1 done' }));
    expect(screen.getByRole('list', { name: 'Cook steps' }).children[0])
      .toHaveAttribute('data-testid', 'cook-step-2');
    expect(screen.getByText('Mix the oats.')).toHaveClass('line-through');
    fireEvent.click(screen.getByRole('checkbox', { name: 'Step 1 done' }));
    expect(screen.getByRole('list', { name: 'Cook steps' }).children[0])
      .toHaveAttribute('data-testid', 'cook-step-1');
  });

  it('per-ingredient checkboxes persist via the cook-state endpoint', async () => {
    await renderCook();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Step 1 ingredient Oats 100 g' }));
    // Flush the 300ms debounced persist.
    await act(async () => { vi.advanceTimersByTime(500); });
    expect(saveCookState).toHaveBeenCalled();
    const last = saveCookState.mock.calls[saveCookState.mock.calls.length - 1][1];
    expect(last.checked_foods).toEqual([{ step_no: 1, food_id: 11, unit: 'g' }]);
  });
  it('two timers count down independently from server deadlines', async () => {
    await renderCook();
    fireEvent.click(screen.getByRole('button', { name: 'Start step 1 timer' }));
    fireEvent.click(screen.getByRole('button', { name: 'Start step 2 timer' }));
    expect(screen.getByTestId('cook-timer-1')).toHaveTextContent('2:00');
    expect(screen.getByTestId('cook-timer-2')).toHaveTextContent('1:00');
    await act(async () => { vi.advanceTimersByTime(30_000); });
    expect(screen.getByTestId('cook-timer-1')).toHaveTextContent('1:30');
    expect(screen.getByTestId('cook-timer-2')).toHaveTextContent('0:30');
  });

  it('expiry flash shows until Done clears it (and only Done)', async () => {
    mockHydrate(DETAIL, {
      recipe_id: 7, checked_steps: [], checked_foods: [],
      timers: [{ step_no: 2, ends_at: new Date('2026-10-07T12:00:10.000Z').toISOString() }],
      updated_at: '2026-10-07T12:00:00.000Z',
    });
    await renderCook();
    expect(screen.queryByTestId('cook-timer-expired')).not.toBeInTheDocument();
    await act(async () => { vi.advanceTimersByTime(11_000); });
    expect(screen.getByTestId('cook-timer-expired')).toHaveTextContent('Step 2 timer is done.');
    // Ticking further does not clear it — only Done does.
    await act(async () => { vi.advanceTimersByTime(60_000); });
    expect(screen.getByTestId('cook-timer-expired')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Timer done for step 2' }));
    expect(screen.queryByTestId('cook-timer-expired')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start step 2 timer' })).toBeInTheDocument();
  });

  it('restores persisted checks + deadlines after unmount/remount', async () => {
    const { unmount } = await renderCook();
    expect(screen.getByRole('checkbox', { name: 'Step 1 done' })).not.toBeChecked();
    // A persisted session arrives from the server; remount restores it.
    getCookState.mockResolvedValue({
      recipe_id: 7, checked_steps: [1],
      checked_foods: [{ step_no: 1, food_id: 11, unit: 'g' }],
      timers: [{ step_no: 2, ends_at: new Date('2026-10-07T12:05:00.000Z').toISOString() }],
      updated_at: '2026-10-07T12:00:00.000Z',
    });
    unmount();
    await renderCook();
    // Checked step 1 sinks below step 2; its ingredient stays checked.
    expect(screen.getByRole('list', { name: 'Cook steps' }).children[0])
      .toHaveAttribute('data-testid', 'cook-step-2');
    expect(screen.getByRole('checkbox', { name: 'Step 1 done' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'Step 1 ingredient Oats 100 g' })).toBeChecked();
    expect(screen.getByTestId('cook-timer-2')).toHaveTextContent('5:00');
  });

  it('Complete asks for confirmation (no-undo), then calls the endpoint', async () => {
    const onCompleted = vi.fn();
    render(<RecipeCook recipeId={7} onBack={vi.fn()} onCompleted={onCompleted} onEdit={vi.fn()} />);
    await act(async () => {});
    expect(screen.getByTestId('recipe-cook')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Complete recipe' }));
    // Confirm dialog appears; nothing completed yet.
    expect(screen.getByRole('dialog', { name: 'Complete recipe' })).toBeInTheDocument();
    expect(completeRecipe).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Yes, complete' }));
    await act(async () => {});
    expect(completeRecipe).toHaveBeenCalledWith(7);
    expect(onCompleted).toHaveBeenCalledTimes(1);
  });
});
