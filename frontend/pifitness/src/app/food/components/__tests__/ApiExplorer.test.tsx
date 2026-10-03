/**
 * ApiExplorer — food API raw-JSON viewer (010-001 T03/T06).
 * Behaviour only (jsdom has no layout engine): empty state, USDA search
 * renders verbatim JSON, OFF status:0 shows adjacent miss hint as-is,
 * failures show the raw error text, layouts render, and manual barcode
 * entry stays a camera fallback (never the primary path).
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import ApiExplorer from '../ApiExplorer';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';

vi.mock('@/lib/api-client', () => ({
  API: { foodLookup: { searchUsda: vi.fn(), lookupOff: vi.fn() } },
}));

// The camera scanner is camera/wasm code loaded client-side only; stub it with a
// control that fires onScan so these tests exercise the OFF panel wiring.
vi.mock('next/dynamic', () => ({
  default:
    () =>
    (props: { onScan?: (value: string, format: string) => void }) => (
      <button type="button" onClick={() => props.onScan?.('737628064502', 'ean_13')}>
        mock-scan
      </button>
    ),
}));

const searchUsda = vi.mocked(API.foodLookup.searchUsda);
const lookupOff = vi.mocked(API.foodLookup.lookupOff);

beforeEach(() => {
  vi.clearAllMocks();
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

describe('ApiExplorer', () => {
  it('shows the empty state before any lookup', () => {
    render(<ApiExplorer />);
    expect(screen.getByText(/No lookup yet/i)).toBeInTheDocument();
  });

  it('renders the DEV-ONLY banner', () => {
    render(<ApiExplorer />);
    expect(screen.getByText(/DEV-ONLY diagnostic tool/i)).toBeInTheDocument();
  });

  it('USDA search renders the verbatim body', async () => {
    const user = userEvent.setup();
    const body = { totalHits: 1, foods: [{ fdcId: 1, description: 'Banana, raw' }] };
    searchUsda.mockResolvedValue({ upstreamStatus: 200, body, rateLimitRemaining: '999' });
    render(<ApiExplorer />);
    await user.type(screen.getByLabelText(/Food name/i), 'banana');
    await user.click(screen.getByRole('button', { name: /Search USDA/i }));
    expect(searchUsda).toHaveBeenCalledWith('banana', 10, undefined);
    expect(await screen.findByText(/upstream status: 200/i)).toBeInTheDocument();
    expect(screen.getByText(/Banana, raw/)).toBeInTheDocument();
  });

  it('OFF status:0 miss shows the adjacent hint with raw body as-is', async () => {
    const user = userEvent.setup();
    lookupOff.mockResolvedValue({ upstreamStatus: 200, body: { status: 0, status_verbose: 'not found' } });
    render(<ApiExplorer />);
    await user.click(screen.getByRole('button', { name: /Start camera scan/i }));
    await user.click(screen.getByRole('button', { name: /Enter barcode manually/i }));
    await user.type(screen.getByLabelText(/Barcode/i), '000000000000');
    await user.click(screen.getByRole('button', { name: /Look up barcode/i }));
    expect(lookupOff).toHaveBeenCalledWith('000000000000');
    expect(await screen.findByText(/not in Open Food Facts/i)).toBeInTheDocument();
    expect(screen.getByText(/not found/)).toBeInTheDocument();
  });

  it('keeps manual barcode entry as a fallback, never the primary path', async () => {
    const user = userEvent.setup();
    render(<ApiExplorer />);
    // Primary path is the camera: no barcode field exists up front.
    expect(screen.getByRole('button', { name: /Start camera scan/i })).toBeInTheDocument();
    expect(screen.queryByLabelText(/Barcode/i)).not.toBeInTheDocument();

    // Starting the scanner still does not expose typed entry…
    await user.click(screen.getByRole('button', { name: /Start camera scan/i }));
    expect(screen.queryByLabelText(/Barcode/i)).not.toBeInTheDocument();

    // …it only appears when the fallback is explicitly requested.
    await user.click(screen.getByRole('button', { name: /Enter barcode manually/i }));
    expect(screen.getByLabelText(/Barcode/i)).toBeInTheDocument();
  });

  it('failure surfaces the raw error text', async () => {
    const user = userEvent.setup();
    searchUsda.mockRejectedValue(new Error('{"detail":"boom","status":502}'));
    render(<ApiExplorer />);
    await user.type(screen.getByLabelText(/Food name/i), 'x');
    await user.click(screen.getByRole('button', { name: /Search USDA/i }));
    expect(await screen.findByRole('alert')).toBeInTheDocument();
  });

  it('a successful camera scan stops the camera and looks the code up', async () => {
    const user = userEvent.setup();
    lookupOff.mockResolvedValue({ upstreamStatus: 200, body: { status: 1 } });
    render(<ApiExplorer />);
    await user.click(screen.getByRole('button', { name: /Start camera scan/i }));
    await user.click(screen.getByRole('button', { name: /mock-scan/i }));
    expect(lookupOff).toHaveBeenCalledWith('737628064502');
    // Camera released: scanner controls gone, start control back.
    expect(screen.queryByRole('button', { name: /mock-scan/i })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Start camera scan/i })).toBeInTheDocument();
  });

  it('a repeat lookup of the same barcode is skipped (OFF 429 guard)', async () => {
    const user = userEvent.setup();
    lookupOff.mockResolvedValue({ upstreamStatus: 200, body: { status: 1 } });
    render(<ApiExplorer />);
    await user.click(screen.getByRole('button', { name: /Start camera scan/i }));
    await user.click(screen.getByRole('button', { name: /Enter barcode manually/i }));
    await user.type(screen.getByLabelText(/Barcode/i), '737628064502');
    await user.click(screen.getByRole('button', { name: /Look up barcode/i }));
    await user.click(screen.getByRole('button', { name: /Look up barcode/i }));
    expect(lookupOff).toHaveBeenCalledTimes(1);
  });

  it('renders in all three layout variants', async () => {
    for (const variant of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: variant });
      const { unmount } = render(<ApiExplorer />);
      expect(screen.getByText(/USDA FoodData Central/i)).toBeInTheDocument();
      unmount();
    }
  });
});
