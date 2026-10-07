/**
 * AddEntry — 010-003 T06 add-entry mode.
 *
 * Behaviour only: suggestions load, search shows tiered results with
 * show-more, scan hit opens confirm directly, confirm persists and returns.
 * The camera scanner is stubbed (client-only wasm) via next/dynamic.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import AddEntry from '../AddEntry';
import { defaultAmount } from '../FoodSearchPicker';
import { API } from '@/lib/api-client';
import type { CombinedSearchResult, DayEntry, UsualFood } from '@/lib/types/food-contract';

vi.mock('@/lib/api-client', () => ({
  API: {
    food: {
      suggest: vi.fn(),
      combinedSearch: vi.fn(),
      logEntry: vi.fn(),
      logApiEntry: vi.fn(),
      byBarcode: vi.fn(),
      foodById: vi.fn(),
    },
    foodLookup: { lookupOff: vi.fn() },
  },
}));

vi.mock('next/dynamic', () => ({
  default:
    () =>
    (props: { onScan?: (value: string, format: string) => void }) => (
      <button type="button" onClick={() => props.onScan?.('737628064502', 'ean_13')}>
        mock-scan
      </button>
    ),
}));

const suggest = vi.mocked(API.food.suggest);
const combinedSearch = vi.mocked(API.food.combinedSearch);
const logEntry = vi.mocked(API.food.logEntry);
const logApiEntry = vi.mocked(API.food.logApiEntry);
const byBarcode = vi.mocked(API.food.byBarcode);
const foodById = vi.mocked(API.food.foodById);
const lookupOff = vi.mocked(API.foodLookup.lookupOff);

const SUGGEST: UsualFood[] = [
  {
    food_id: 1, name: 'Oatmeal', source: 'db', kcal_per_100g: 68,
    quality_score: 1, times_eaten: 5, hour_weight: 1,
    mode_qty: 40, mode_unit: 'g',
  },
];

const ENTRY: DayEntry = {
  entry_id: 7, food_id: 1, name_snapshot: 'Oatmeal',
  nutrients_snapshot: {
    kcal_per_100g: 68, protein_g: 2.4, fat_g: 1.4, carbs_g: 12,
    fiber_g: 1.7, sugars_g: null, sodium_mg: 1, caffeine_mg: null,
  },
  is_partial: false, qty: 40, unit: 'g', kcal: 27,
  logged_at: '2026-10-05T12:00:00+00:00',
};

function hit(i: number): CombinedSearchResult {
  return {
    kind: 'usda', food_id: null, name: `Result ${i}`,
    mode_qty: null, mode_unit: null, kcal_preview: 100 + i,
    data_quality: 'High', rank: i, result_source: 'usda:Branded',
    usda_score: 100 - i, fdcId: 1000 + i, barcode: null,
    nutrients_raw: { fdcId: 1000 + i, description: `Result ${i}` },
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  suggest.mockResolvedValue(SUGGEST);
  combinedSearch.mockResolvedValue([hit(0)]);
  byBarcode.mockResolvedValue({ found: false, barcode: 'x', name: '' });
  // T10: suggestion hydration — row serving intentionally differs from the
  // card's mode (15 g vs 40 g) so mode-precedence is observable.
  foodById.mockResolvedValue({
    food_id: 1, name: 'Oatmeal',
    nutrients: {
      kcal_per_100g: 68, protein_g: 2.4, fat_g: 1.4, carbs_g: 12,
      fiber_g: 1.7, sugars_g: null, sodium_mg: null, caffeine_mg: null,
    },
    serving_size: 15, serving_unit: 'g', serving_text: null,
    is_deleted: false, deleted_at: null,
  });
});

describe('AddEntry', () => {
  it('shows suggestion cards with mode qty + unit', async () => {
    render(<AddEntry onDone={() => {}} />);
    expect(await screen.findByText('Oatmeal')).toBeInTheDocument();
    expect(screen.getByText('40 g')).toBeInTheDocument();
  });

  it('search shows top results; Show more reveals the rest, no extra call', async () => {
    const user = userEvent.setup();
    const many = Array.from({ length: 12 }, (_, i) => hit(i));
    combinedSearch.mockResolvedValue(many);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'oats');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    expect(await screen.findByText('Result 0')).toBeInTheDocument();
    expect(screen.queryByText('Result 11')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Show more/i }));
    expect(await screen.findByText('Result 11')).toBeInTheDocument();
    expect(combinedSearch).toHaveBeenCalledTimes(1);
  });

  it('result cards carry source + percent pill (010-004 T03, no High/Medium/Low)', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([{
      ...hit(0), data_quality: 'High', quality_percent: 90,
      food_category: 'Beverages', serving_unit_facet: 'ml',
    }]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'oats');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    const card = await screen.findByText('Result 0');
    expect(card).toBeInTheDocument();
    expect(screen.getByText('90%')).toBeInTheDocument();
    expect(screen.queryByText('High')).not.toBeInTheDocument();
    expect(screen.queryByText('Medium')).not.toBeInTheDocument();
    expect(screen.queryByText('Low')).not.toBeInTheDocument();
    // Single value per facet -> button group with All reset; both facet
    // controls (Category + Serving unit) render their own All reset.
    expect(screen.getByRole('button', { name: /Beverages \(1\)/ })).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /^All$/ })).toHaveLength(2);
  });

  it('facet filter narrows the list client-side with no extra fetch (010-004 T03)', async () => {
    const user = userEvent.setup();
    const mk = (i: number, cat: string | undefined, unit: string | undefined): CombinedSearchResult => ({
      ...hit(i), name: `Food ${i}`, quality_percent: 80 + i,
      food_category: cat ?? null, serving_unit: unit ?? null,
      serving_unit_facet: unit ?? null,
    });
    combinedSearch.mockResolvedValue([
      mk(0, 'Beverages', 'ml'),
      mk(1, 'Snacks', 'g'),
      // Saved row without a category: excluded once the category filter applies.
      { ...mk(2, undefined, 'g'), kind: 'db-food', food_id: 9 },
    ]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'cof');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await screen.findByText('Food 0');
    await user.click(screen.getByRole('button', { name: /Beverages \(1\)/ }));
    expect(screen.getByText('Food 0')).toBeInTheDocument();
    expect(screen.queryByText('Food 1')).not.toBeInTheDocument();
    expect(screen.queryByText('Food 2')).not.toBeInTheDocument();
    expect(combinedSearch).toHaveBeenCalledTimes(1);
  });

  it('facet control renders a dropdown at 4+ values (010-004 T03)', async () => {
    const user = userEvent.setup();
    const mk = (i: number, cat: string): CombinedSearchResult => ({
      ...hit(i), name: `Item ${i}`, quality_percent: 70,
      food_category: cat, serving_unit_facet: 'g',
    });
    combinedSearch.mockResolvedValue([
      mk(0, 'A'), mk(1, 'B'), mk(2, 'C'), mk(3, 'D'),
    ]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'x');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await screen.findByText('Item 0');
    const select = screen.getByRole('combobox', { name: /Category filter/i });
    expect(select).toBeInTheDocument();
    await user.selectOptions(select, 'B');
    expect(screen.queryByText('Item 0')).not.toBeInTheDocument();
    expect(screen.getByText('Item 1')).toBeInTheDocument();
    expect(combinedSearch).toHaveBeenCalledTimes(1);
  });

  it('scan hit opens confirm directly; Confirm persists via from-api', async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    lookupOff.mockResolvedValue({
      upstreamStatus: 200,
      body: { status: 1, product: { product_name: 'Oats', nutriments: {} } },
    });
    logApiEntry.mockResolvedValue({ ...ENTRY, entry_id: 9 });
    render(<AddEntry onDone={onDone} />);
    await screen.findByText('Oatmeal');
    await user.click(screen.getByRole('button', { name: /Scan barcode/i }));
    await user.click(screen.getByRole('button', { name: /mock-scan/i }));
    expect(await screen.findByRole('dialog', { name: /Confirm Oats/i })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^Confirm$/i }));
    await waitFor(() => expect(logApiEntry).toHaveBeenCalled());
    expect(onDone).toHaveBeenCalled();
  });

  it('suggestion pick opens confirm with live kcal; Confirm logs + returns', async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    logEntry.mockResolvedValue({ ...ENTRY, entry_id: 7 });
    render(<AddEntry onDone={onDone} />);
    await user.click(await screen.findByText('Oatmeal'));
    expect(await screen.findByRole('dialog', { name: /Confirm Oatmeal/i })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^Confirm$/i }));
    await waitFor(() => expect(logEntry).toHaveBeenCalledWith(
      expect.objectContaining({ food_id: 1 }),
    ));
    expect(onDone).toHaveBeenCalled();
  });

  it('red cancel returns to diary without logging', async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    render(<AddEntry onDone={onDone} />);
    await screen.findByText('Oatmeal');
    await user.click(screen.getByRole('button', { name: /Cancel add entry/i }));
    expect(onDone).toHaveBeenCalled();
    expect(logEntry).not.toHaveBeenCalled();
  });

  it('T08: usda pick shows editable name; Confirm sends renamed payload', async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    logApiEntry.mockResolvedValue({ ...ENTRY, entry_id: 11 });
    render(<AddEntry onDone={onDone} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'oats');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Result 0'));
    // Name field present for API picks, prefilled with the hit name.
    const nameInput = await screen.findByLabelText('Name for your diary') as HTMLInputElement;
    expect(nameInput.value).toBe('Result 0');
    await user.clear(nameInput);
    await user.type(nameInput, 'Coffee');
    await user.click(screen.getByRole('button', { name: /^Confirm$/i }));
    await waitFor(() => expect(logApiEntry).toHaveBeenCalled());
    expect(logApiEntry).toHaveBeenCalledWith(expect.objectContaining({
      usda_payload: expect.objectContaining({ name: 'Coffee', fdcId: 1000 }),
    }));
    expect(onDone).toHaveBeenCalled();
  });

  it('T08: db suggestion pick has no name field; empty API name blocks save', async () => {
    const user = userEvent.setup();
    logEntry.mockResolvedValue({ ...ENTRY, entry_id: 7 });
    render(<AddEntry onDone={() => {}} />);
    // DB pick: static title, no name input.
    await user.click(await screen.findByText('Oatmeal'));
    expect(await screen.findByRole('dialog', { name: /Confirm Oatmeal/i })).toBeInTheDocument();
    expect(screen.queryByLabelText('Name for your diary')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Cancel$/i }));
    // API pick with cleared name: inline error, no save call.
    await user.type(screen.getByLabelText(/Food name/i), 'oats');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Result 0'));
    const nameInput = await screen.findByLabelText('Name for your diary') as HTMLInputElement;
    await user.clear(nameInput);
    await user.click(screen.getByRole('button', { name: /^Confirm$/i }));
    expect(await screen.findByText(/Name is required/i)).toBeInTheDocument();
    expect(logApiEntry).not.toHaveBeenCalled();
  });

  it('T08: barcode already saved opens DB confirm without name field', async () => {
    const user = userEvent.setup();
    logEntry.mockResolvedValue({ ...ENTRY, entry_id: 12 });
    byBarcode.mockResolvedValue({
      found: true, barcode: '737628064502', food_id: 1, name: 'Oatmeal',
      nutrients: {
        kcal_per_100g: 68, protein_g: 2.4, fat_g: 1.4, carbs_g: 12,
        fiber_g: 1.7, sugars_g: null, sodium_mg: null, caffeine_mg: null,
      },
    });
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.click(screen.getByRole('button', { name: /Scan barcode/i }));
    await user.click(screen.getByRole('button', { name: /mock-scan/i }));
    expect(await screen.findByText(/Found saved food: Oatmeal/i)).toBeInTheDocument();
    expect(lookupOff).not.toHaveBeenCalled();
    expect(screen.queryByLabelText('Name for your diary')).not.toBeInTheDocument();
  });

  it('T09: source pill renders per kind with light + dark tokens', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([
      hit(0),
      { ...hit(1), kind: 'db-food', food_id: 3 },
    ]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'oats');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));

    const usdaPill = await screen.findByTitle('usda');
    expect(usdaPill).toHaveClass('rounded-full', 'bg-sky-100', 'dark:bg-sky-900/40');
    expect(usdaPill).toHaveTextContent('USDA');

    const dbPill = screen.getByTitle('db-food');
    expect(dbPill).toHaveClass('rounded-full', 'bg-blue-100', 'dark:bg-blue-900/40');
    expect(dbPill).toHaveTextContent('DB');
  });

  it('T10: suggestion pick defaults to its mode qty/unit (mode beats row serving)', async () => {
    const user = userEvent.setup();
    render(<AddEntry onDone={() => {}} />);
    await user.click(await screen.findByText('Oatmeal'));
    expect(await screen.findByRole('dialog', { name: /Confirm Oatmeal/i })).toBeInTheDocument();
    // foodById row serving is 15 g; the card's mode (40 g) must win.
    expect((screen.getByLabelText('Quantity') as HTMLInputElement).value).toBe('40');
    expect((screen.getByLabelText('Unit') as HTMLSelectElement).value).toBe('g');
  });

  it('T10: API pick with feed serving opens at that serving (ml), not 100 g', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([
      { ...hit(0), serving_size: 310.5, serving_unit: 'ml', serving_text: '10.5 ONZ' },
    ]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'latte');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Result 0'));
    expect(await screen.findByRole('dialog', { name: /Confirm Result 0/i })).toBeInTheDocument();
    expect((screen.getByLabelText('Quantity') as HTMLInputElement).value).toBe('310.5');
    expect((screen.getByLabelText('Unit') as HTMLSelectElement).value).toBe('ml');
  });

  it('T10: pick without a feed serving falls back to 100 g', async () => {
    const user = userEvent.setup();
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'oats');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Result 0'));
    expect(await screen.findByRole('dialog', { name: /Confirm Result 0/i })).toBeInTheDocument();
    expect((screen.getByLabelText('Quantity') as HTMLInputElement).value).toBe('100');
    expect((screen.getByLabelText('Unit') as HTMLSelectElement).value).toBe('g');
  });

  it('T10: popup shows the saved-data panel (nutrients, serving, source, keys)', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([{
      ...hit(0),
      serving_size: 84, serving_unit: 'g', serving_text: '0.5 cup',
      nutrients: {
        kcal_per_100g: 2, protein_g: 0.1, fat_g: 0, carbs_g: 0.4,
        fiber_g: 0, sugars_g: 0, sodium_mg: 3, caffeine_mg: 80,
      },
    }]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'brew');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Result 0'));
    expect(await screen.findByRole('dialog', { name: /Confirm Result 0/i })).toBeInTheDocument();
    expect(screen.getByLabelText('Saved to your diary')).toBeInTheDocument();
    expect(screen.getByLabelText('caffeine: 80 mg')).toBeInTheDocument();
    expect(screen.getByTestId('panel-serving')).toHaveTextContent('84 g (0.5 cup)');
    expect(screen.getByTestId('panel-source')).toHaveTextContent('USDA');
    // AC-3/AC-6 (T04): High/Medium/Low and fdcId are gone from the panel.
    expect(screen.getByTestId('panel-source')).not.toHaveTextContent('High');
    expect(screen.queryByText(/fdcId/)).not.toBeInTheDocument();
  });

  it('T04: panel shows 2-column nutrient bars with OQ-7 scaling; nulls get em-dash, no fdcId (010-004)', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([{
      ...hit(0),
      nutrients: {
        kcal_per_100g: 100, protein_g: 5, fat_g: 2, carbs_g: 10,
        fiber_g: null, sugars_g: 0, sodium_mg: 2300, caffeine_mg: null,
      },
      serving_size: 100, serving_unit: 'g', serving_text: null,
    }]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'brew');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Result 0'));
    expect(await screen.findByRole('dialog', { name: /Confirm Result 0/i })).toBeInTheDocument();

    // Two columns in the specified order: kcal/carbs/protein/fat | fiber/sugars/sodium/caffeine.
    const cols = screen.getByTestId('panel-nutrients').children;
    expect(cols).toHaveLength(2);
    const colLabels = (col: Element) =>
      Array.from(col.querySelectorAll('[role="img"]')).map((b) => b.getAttribute('aria-label')!.split(':')[0]);
    expect(colLabels(cols[0])).toEqual(['kcal', 'carbs', 'protein', 'fat']);
    expect(colLabels(cols[1])).toEqual(['fiber', 'sugars', 'sodium', 'caffeine']);

    // OQ-7 scaling: kcal/2000 = 5%; carbs vs macro group max (10) = 100%; protein = 50%.
    const widthOf = (name: string) =>
      (screen.getByLabelText(name).firstElementChild as HTMLElement).style.width;
    expect(widthOf('kcal: 100')).toBe('5%');
    expect(widthOf('carbs: 10 g')).toBe('100%');
    expect(widthOf('protein: 5 g')).toBe('50%');
    // Fixed refs: sodium 2300/2300 = 100%; sugars 0/50 = 0%.
    expect(widthOf('sodium: 2300 mg')).toBe('100%');
    expect(widthOf('sugars: 0 g')).toBe('0%');

    // Null nutrients: em-dash value, empty bar.
    expect(screen.getByLabelText('fiber: — g')).toBeInTheDocument();
    expect(screen.getByLabelText('caffeine: — mg')).toBeInTheDocument();
    expect(widthOf('caffeine: — mg')).toBe('0%');

    // Source shown; fdcId absent from the panel.
    expect(screen.getByTestId('panel-source')).toHaveTextContent('USDA');
    expect(screen.queryByText(/fdcId/)).not.toBeInTheDocument();
  });

  it('T10: backdrop click does not close the confirm popup; Cancel does', async () => {
    const user = userEvent.setup();
    render(<AddEntry onDone={() => {}} />);
    await user.click(await screen.findByText('Oatmeal'));
    expect(await screen.findByRole('dialog', { name: /Confirm Oatmeal/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('presentation'));
    expect(screen.getByRole('dialog', { name: /Confirm Oatmeal/i })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Cancel$/i }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('010-005 T10: recipe pick opens at qty 1 in its own serving unit and logs qty 3 pancake (AC-10)', async () => {
    const user = userEvent.setup();
    combinedSearch.mockResolvedValue([{
      ...hit(0),
      kind: 'db-recipe', food_id: 42, name: 'Pancakes',
      mode_qty: null, mode_unit: null,
      kcal_preview: 250,
      serving_size: 40, serving_unit: 'pancake', serving_text: null,
      nutrients: {
        kcal_per_100g: 250, protein_g: 6, fat_g: 10, carbs_g: 30,
        fiber_g: 2, sugars_g: 5, sodium_mg: 400, caffeine_mg: null,
      },
    }]);
    render(<AddEntry onDone={() => {}} />);
    await screen.findByText('Oatmeal');
    await user.type(screen.getByLabelText(/Food name/i), 'pan');
    await user.click(screen.getByRole('button', { name: /^Search$/i }));
    await user.click(await screen.findByText('Pancakes'));
    const dialog = await screen.findByRole('dialog', { name: /Confirm Pancakes/i });

    // AC-10 defaults: qty 1, own label selected AND offered in the unit set.
    expect((within(dialog).getByLabelText('Quantity') as HTMLInputElement).value).toBe('1');
    expect((within(dialog).getByLabelText('Unit') as HTMLSelectElement).value).toBe('pancake');
    expect(within(dialog).getByRole('option', { name: 'pancake' })).toBeInTheDocument();
    // Live kcal preview resolves via the row's grams-per-unit (1 × 40 g).
    expect(within(dialog).getByRole('status')).toHaveTextContent('≈ 100 kcal');

    await user.clear(within(dialog).getByLabelText('Quantity'));
    await user.type(within(dialog).getByLabelText('Quantity'), '3');
    expect(within(dialog).getByRole('status')).toHaveTextContent('≈ 300 kcal');
    await user.click(within(dialog).getByRole('button', { name: /^Confirm$/i }));
    expect(logEntry).toHaveBeenCalledWith(
      expect.objectContaining({ food_id: 42, qty: 3, unit: 'pancake' }));
  });

  it('010-005 T10: recipe default beats history mode; non-recipe defaults unchanged', () => {
    // Recipe with history still opens at qty 1 in its own unit (AC-10).
    expect(defaultAmount({ mode_qty: 2, mode_unit: 'g', serving: null, sourceKind: 'db-recipe', recipe_serving_unit: 'slice' }))
      .toEqual({ qty: 1, unit: 'slice' });
    // Recipe without a serving label falls back to the existing chain.
    expect(defaultAmount({ mode_qty: 2, mode_unit: 'g', serving: null, sourceKind: 'db-recipe', recipe_serving_unit: null }))
      .toEqual({ qty: 2, unit: 'g' });
    // Non-recipe picks keep today's defaults: mode → feed serving → 100 g.
    expect(defaultAmount({ mode_qty: 40, mode_unit: 'g', serving: { size: 15, unit: 'g' }, sourceKind: 'db-food', recipe_serving_unit: null }))
      .toEqual({ qty: 40, unit: 'g' });
    expect(defaultAmount({ mode_qty: null, mode_unit: null, serving: { size: 310.5, unit: 'ml' }, sourceKind: 'off', recipe_serving_unit: null }))
      .toEqual({ qty: 310.5, unit: 'ml' });
    expect(defaultAmount({ mode_qty: null, mode_unit: null, serving: null, sourceKind: 'usda', recipe_serving_unit: null }))
      .toEqual({ qty: 100, unit: 'g' });
  });
});
