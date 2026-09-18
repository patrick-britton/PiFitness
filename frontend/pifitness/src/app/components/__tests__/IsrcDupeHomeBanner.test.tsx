/**
 * IsrcDupeHomeBanner — home route pending-duplicates banner (008-005 T10, AC-1).
 *
 * FR-7/FR-9: the banner appears only when the shared pair count is > 0.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactElement } from 'react';

import IsrcDupeHomeBanner from '../IsrcDupeHomeBanner';
import { API } from '@/lib/api-client';

vi.mock('@/lib/api-client', () => ({
  API: {
    music: {
      getIsrcDupeCount: vi.fn(),
      getIsrcDupeMatch: vi.fn(),
      decideIsrcDupe: vi.fn(),
      rescanIsrcDupes: vi.fn(),
    },
  },
}));

const getIsrcDupeCount = vi.mocked(API.music.getIsrcDupeCount);

function renderWithQuery(ui: ReactElement) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('IsrcDupeHomeBanner', () => {
  it('shows the pending pair count when duplicates exist', async () => {
    getIsrcDupeCount.mockResolvedValue({ count: 3 });

    renderWithQuery(<IsrcDupeHomeBanner />);

    expect(
      await screen.findByText('3 potential duplicate isrcs found'),
    ).toBeInTheDocument();
  });

  it('renders nothing when the queue is empty', async () => {
    getIsrcDupeCount.mockResolvedValue({ count: 0 });

    const { container } = renderWithQuery(<IsrcDupeHomeBanner />);

    await waitFor(() => expect(getIsrcDupeCount).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing while the count is still loading', () => {
    // Never resolves — the banner must not flash a count it does not have.
    getIsrcDupeCount.mockImplementation(() => new Promise(() => {}));

    const { container } = renderWithQuery(<IsrcDupeHomeBanner />);

    expect(container).toBeEmptyDOMElement();
  });
});