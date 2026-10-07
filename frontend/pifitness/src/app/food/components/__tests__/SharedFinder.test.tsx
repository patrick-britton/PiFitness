/** T06: shared finder + amount picker.
 * 010-005 T13: repointed from the retired FoodFinder to the shared
 * FoodSearchPicker — keeps the 010-001 proxy wiring (combined-search
 * USDA pick + OFF scan pick) covered. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import FoodSearchPicker from '../FoodSearchPicker';
import AmountPicker from '../AmountPicker';
import { API } from '@/lib/api-client';
vi.mock('@/lib/api-client', () => ({
  API: {
    food: {
      suggest: vi.fn(),
      combinedSearch: vi.fn(),
      byBarcode: vi.fn(),
      foodById: vi.fn(),
    },
    foodLookup: { lookupOff: vi.fn() },
  },
}));
vi.mock('next/dynamic', () => ({ default: () => (p: { onScan?: (v: string) => void }) => (
  <button type="button" onClick={() => p.onScan?.('737628064502')}>mock-scan</button>) }));
const suggest = vi.mocked(API.food.suggest);
const combinedSearch = vi.mocked(API.food.combinedSearch);
const byBarcode = vi.mocked(API.food.byBarcode);
const lookupOff = vi.mocked(API.foodLookup.lookupOff);
beforeEach(() => {
  vi.clearAllMocks();
  suggest.mockResolvedValue([]);
  combinedSearch.mockResolvedValue([]);
  byBarcode.mockResolvedValue({ found: false, barcode: '737628064502', name: '' });
});
describe('shared finder/amount', () => {
  it('searches the combined index and picks a USDA hit', async () => {
    const user = userEvent.setup();
    const onPick = vi.fn();
    combinedSearch.mockResolvedValue([{
      kind: 'usda', food_id: null, name: 'Banana, raw',
      mode_qty: null, mode_unit: null, kcal_preview: 89,
      data_quality: 'High', rank: 1, result_source: 'usda:SR Legacy',
      usda_score: 90, fdcId: 1169, barcode: null,
      nutrients_raw: { fdcId: 1169, description: 'Banana, raw' },
    }]);
    render(<FoodSearchPicker onPick={onPick} />);
    await screen.findByRole('heading', { name: 'Suggested' });
    await user.type(screen.getByLabelText(/Food name/i), 'banana');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Banana, raw'));
    expect(combinedSearch).toHaveBeenCalled();
    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({
      food_id: null, name: 'Banana, raw', sourceKind: 'usda',
      payload: expect.objectContaining({ provider: 'usda', fdcId: 1169 }),
    }));
  });
  it('scans a barcode → DB miss → OFF lookup and emits an OFF pick', async () => {
    const user = userEvent.setup();
    const onPick = vi.fn();
    lookupOff.mockResolvedValue({ upstreamStatus: 200, body: { status: 1, product: { product_name: 'Oats' } }, rateLimitRemaining: null });
    render(<FoodSearchPicker onPick={onPick} />);
    await screen.findByRole('heading', { name: 'Suggested' });
    await user.click(screen.getByRole('button', { name: /Scan barcode/i }));
    await user.click(screen.getByRole('button', { name: /mock-scan/i }));
    expect(await screen.findByText(/Scanned 737628064502: Oats/i)).toBeInTheDocument();
    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({
      food_id: null, name: 'Oats', sourceKind: 'off',
      payload: expect.objectContaining({ provider: 'off', barcode: '737628064502' }),
    }));
  });
  it('amount picker emits qty+unit', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<AmountPicker onChange={onChange} />);
    await user.clear(screen.getByLabelText(/Quantity/i));
    await user.type(screen.getByLabelText(/Quantity/i), '250');
    await user.selectOptions(screen.getByLabelText(/Unit/i), 'cup');
    expect(onChange).toHaveBeenLastCalledWith({ qty: 250, unit: 'cup' });
  });
  it('010-005 T10: extraUnit offers a recipe serving label beside the closed set', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<AmountPicker value={{ qty: 1, unit: 'pancake' }} extraUnit="pancake" onChange={onChange} />);
    const sel = screen.getByLabelText(/Unit/i) as HTMLSelectElement;
    expect(sel.value).toBe('pancake');
    expect(screen.getByRole('option', { name: 'pancake' })).toBeInTheDocument();
    expect(screen.getAllByRole('option')).toHaveLength(9); // 8 closed units + label
    await user.selectOptions(sel, 'cup');
    expect(onChange).toHaveBeenLastCalledWith({ qty: 1, unit: 'cup' });
  });
});
