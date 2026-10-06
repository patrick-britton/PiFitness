/** T06: shared finder + amount picker. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import FoodFinder from '../FoodFinder';
import AmountPicker from '../AmountPicker';
import { API } from '@/lib/api-client';
vi.mock('@/lib/api-client', () => ({ API: { foodLookup: { searchUsda: vi.fn(), lookupOff: vi.fn() } } }));
vi.mock('next/dynamic', () => ({ default: () => (p: { onScan?: (v: string) => void }) => (
  <button type="button" onClick={() => p.onScan?.('737628064502')}>mock-scan</button>) }));
const searchUsda = vi.mocked(API.foodLookup.searchUsda);
const lookupOff = vi.mocked(API.foodLookup.lookupOff);
beforeEach(() => { vi.clearAllMocks(); });
describe('shared finder/amount', () => {
  it('searches USDA and picks', async () => {
    const user = userEvent.setup();
    const onPick = vi.fn();
    searchUsda.mockResolvedValue({ upstreamStatus: 200, body: { foods: [{ fdcId: 1, description: 'Banana, raw' }] }, rateLimitRemaining: null });
    render(<FoodFinder onPick={onPick} />);
    await user.type(screen.getByLabelText(/Food name/i), 'banana');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText(/Banana, raw/i));
    expect(onPick).toHaveBeenCalledWith({ kind: 'usda', fdcId: 1, name: 'Banana, raw' });
  });
  it('scans OFF and picks', async () => {
    const user = userEvent.setup();
    const onPick = vi.fn();
    lookupOff.mockResolvedValue({ upstreamStatus: 200, body: { status: 1, product: { product_name: 'Oats' } } });
    render(<FoodFinder onPick={onPick} />);
    await user.click(screen.getByRole('button', { name: /Scan barcode/i }));
    await user.click(screen.getByRole('button', { name: /mock-scan/i }));
    expect(await screen.findByText(/Scanned 737628064502: Oats/i)).toBeInTheDocument();
    expect(onPick).toHaveBeenCalledWith({ kind: 'off', barcode: '737628064502', name: 'Oats' });
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
});
