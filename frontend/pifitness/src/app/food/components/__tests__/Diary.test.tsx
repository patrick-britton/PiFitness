/**
 * Diary — 010-003 T05 diary-home (stats bars + day nav + entry cards +
 * plus-circle Add) + edit popup (live kcal + Save + Delete + undo banner).
 *
 * Behaviour only (jsdom has no layout engine): day navigation, the three-bar
 * stats summary, the edit popup (live kcal + Save), the undo-vs-confirm delete,
 * and the add-entry mode toggle. The camera scanner is stubbed (camera/wasm is
 * client-only) so the shared FoodFinder still mounts in add mode.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Diary from '../Diary';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';

vi.mock('@/lib/api-client', () => ({
  API: {
    food: {
      diary: vi.fn(),
      diaryStats: vi.fn(),
      updateDiaryEntry: vi.fn(),
      deleteDiaryEntry: vi.fn(),
      suggest: vi.fn(),
      combinedSearch: vi.fn(),
      logEntry: vi.fn(),
      logApiEntry: vi.fn(),
    },
    foodLookup: { lookupOff: vi.fn() },
  },
}));

vi.mock('next/dynamic', () => ({
  default: () => () => null,
}));

const diary = vi.mocked(API.food.diary);
const diaryStats = vi.mocked(API.food.diaryStats);
const updateDiaryEntry = vi.mocked(API.food.updateDiaryEntry);
const deleteDiaryEntry = vi.mocked(API.food.deleteDiaryEntry);

/** Device-local day offset from today (mirrors the component's own rule). */
function isoDay(offset = 0): string {
  const d = new Date();
  const local = new Date(d.getTime() - d.getTimezoneOffset() * 60_000);
  local.setDate(local.getDate() + offset);
  return local.toISOString().slice(0, 10);
}

const MEAL = {
  entry_id: 1,
  food_id: null,
  name_snapshot: 'Banana',
  nutrients_snapshot: {
    kcal_per_100g: 89.0, protein_g: 1.1, fat_g: 0.3, carbs_g: 22.8,
    fiber_g: 2.6, sugars_g: 12.2, sodium_mg: 1.0, caffeine_mg: null,
  },
  is_partial: false,
  qty: 100.0,
  unit: 'g',
  kcal: 89.0,
  logged_at: '2026-10-04T12:00:00+00:00',
};

const STATS = { today_kcal: 1500, last_24h_kcal: 1800, avg_7d_kcal: 2000, scale_max: 2000 };

beforeEach(() => {
  vi.clearAllMocks();
  useViewportStore.setState({ layoutVariant: 'desktop' });
  diary.mockResolvedValue([MEAL]);
  diaryStats.mockResolvedValue(STATS);
});

describe('Diary', () => {
  it('renders stats bars + entry cards in all three layout variants', async () => {
    for (const variant of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: variant });
      const { unmount } = render(<Diary />);
      expect(await screen.findByLabelText(/Calorie bars/i)).toBeInTheDocument();
      expect(await screen.findByText('Banana')).toBeInTheDocument();
      unmount();
    }
  });

  it('shows loading skeletons while stats/entries resolve', () => {
    diaryStats.mockReturnValue(new Promise(() => {}));
    diary.mockReturnValue(new Promise(() => {}));
    render(<Diary />);
    expect(screen.getAllByTestId('diary-stats-loading').length).toBeGreaterThan(0);
    expect(screen.getAllByTestId('diary-entries-loading').length).toBeGreaterThan(0);
  });

  it('shows empty + error states, never a blank screen', async () => {
    diaryStats.mockResolvedValue({ today_kcal: null, last_24h_kcal: null, avg_7d_kcal: null, scale_max: 0 });
    diary.mockResolvedValue([]);
    const { unmount } = render(<Diary />);
    expect(await screen.findByText(/No foods logged for this day/i)).toBeInTheDocument();
    unmount();

    diaryStats.mockRejectedValue(new Error('stats boom'));
    diary.mockRejectedValue(new Error('diary bust'));
    render(<Diary />);
    expect(await screen.findByText(/Calorie summary/i)).toBeInTheDocument();
    expect(await screen.findByText(/Diary entries/i)).toBeInTheDocument();
  });

  it('day arrows load the neighbouring day', async () => {
    const user = userEvent.setup();
    render(<Diary />);
    await screen.findByText('Banana');
    const yesterday = isoDay(-1);
    await user.click(screen.getByRole('button', { name: /Previous day/i }));
    await waitFor(() => expect(diary).toHaveBeenCalledWith(yesterday));
  });

  it('plus-circle Add swaps to add-entry mode; red cancel returns', async () => {
    const user = userEvent.setup();
    vi.mocked(API.food.suggest).mockResolvedValue([]);
    render(<Diary />);
    await screen.findByText('Banana');
    await user.click(screen.getByRole('button', { name: /Add entry/i }));
    expect(await screen.findByLabelText(/Search foods/i)).toBeInTheDocument();
    expect(await screen.findByRole('heading', { name: 'Suggested' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Cancel add entry/i }));
    expect(await screen.findByText('Banana')).toBeInTheDocument();
  });

  it('tapping a card opens the edit popup', async () => {
    const user = userEvent.setup();
    updateDiaryEntry.mockResolvedValue({ ...MEAL, qty: 200 });
    render(<Diary />);
    await user.click(await screen.findByText('Banana'));
    expect(await screen.findByRole('dialog', { name: /Edit Banana/i })).toBeInTheDocument();

    expect(screen.getByText(/≈ 89 kcal/i)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^Save$/i }));
    await waitFor(() => expect(updateDiaryEntry).toHaveBeenCalled());
  });

  it('Delete follows undo-vs-confirm: banner + Undo, no commit', async () => {
    const user = userEvent.setup();
    deleteDiaryEntry.mockResolvedValue({ success: true, entry_id: 1 });
    render(<Diary />);
    await user.click(await screen.findByText('Banana'));
    await user.click(await screen.findByRole('button', { name: /^Delete$/i }));
    expect(await screen.findByText(/1 entry removed/i)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Undo/i }));
    expect(screen.queryByText(/1 entry removed/i)).not.toBeInTheDocument();
  });

  it('T10: edit popup closes only via buttons — backdrop click does nothing', async () => {
    const user = userEvent.setup();
    render(<Diary />);
    await user.click(await screen.findByText('Banana'));
    expect(await screen.findByRole('dialog', { name: /Edit Banana/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('presentation'));
    expect(screen.getByRole('dialog', { name: /Edit Banana/i })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Cancel$/i }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});