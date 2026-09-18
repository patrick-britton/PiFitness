/**
 * IsrcReviewView — Music module ISRC Review tab (008-005 T10).
 *
 * Verifies behaviour only: which branch renders, what it says, and what a click
 * sends. jsdom has no layout engine, so CSS layout and light/dark appearance
 * stay a human runtime check (AC-1/AC-2/AC-6).
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import IsrcReviewView from '../IsrcReviewView';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import type { IsrcDupeMatch } from '@/lib/types/music';

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

const getCount = vi.mocked(API.music.getIsrcDupeCount);
const getMatch = vi.mocked(API.music.getIsrcDupeMatch);
const decide = vi.mocked(API.music.decideIsrcDupe);
const rescan = vi.mocked(API.music.rescanIsrcDupes);

/** Identical track + artist, differing album + duration (mirrors a real pair). */
const MATCH: IsrcDupeMatch = {
  isrc1: 'ISRC-A',
  isrc2: 'ISRC-B',
  trackName1: 'Same Song',
  trackName2: 'Same Song',
  artistName1: 'Band',
  artistName2: 'Band',
  albumName1: 'Album One',
  albumName2: 'Album Two',
  duration1: 210,
  duration2: 211,
  matchScore: 0.92,
  trackScore: 1,
  artistScore: 1,
  albumScore: 0.75,
  durationScore: 0.99,
  preferredIsrc: 'ISRC-A',
};

const NEXT_MATCH: IsrcDupeMatch = {
  ...MATCH,
  isrc1: 'ISRC-C',
  isrc2: 'ISRC-D',
  trackName1: 'Next Song',
  trackName2: 'Next Song',
  preferredIsrc: 'ISRC-C',
};

function renderView() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={client}>
      <IsrcReviewView />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

describe('IsrcReviewView — matchup rendering', () => {
  it('shows the pair count and one pair: ✓ for identical values, score + both values otherwise', async () => {
    getCount.mockResolvedValue({ count: 3 });
    getMatch.mockResolvedValue({ match: MATCH });

    const { container } = renderView();

    expect(
      await screen.findByText('3 potential duplicate isrcs found'),
    ).toBeInTheDocument();
    await screen.findByText('Same Song');

    const text = container.textContent ?? '';
    expect(text).toContain('Overall:');
    expect(text).toContain('0.92');
    expect(text).toContain('✓'); // track + artist are identical
    expect(text).toContain('75%'); // album differs -> score ...
    expect(text).toContain('Album One vs Album Two'); // ... plus both values
    expect(text).toContain('0.99'); // duration differs -> duration score
    expect(text).toContain('Preferred ISRC:');
    expect(text).toContain('(first)'); // preferredIsrc === isrc1
    expect(text).toContain('ISRC-A vs ISRC-B');
  });

  it('labels the preferred ISRC as (second) when it is isrc2', async () => {
    getCount.mockResolvedValue({ count: 1 });
    getMatch.mockResolvedValue({ match: { ...MATCH, preferredIsrc: 'ISRC-B' } });

    const { container } = renderView();

    await screen.findByText('Same Song');
    expect(container.textContent ?? '').toContain('(second)');
  });

  it('renders the matchup in all three layout variants', async () => {
    getCount.mockResolvedValue({ count: 2 });
    getMatch.mockResolvedValue({ match: MATCH });

    for (const variant of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: variant });
      const { unmount } = renderView();
      expect(await screen.findByText('Same Song')).toBeInTheDocument();
      unmount();
    }
  });
});

describe('IsrcReviewView — decisions', () => {
  it('ACCEPT sends both ISRCs + the preferred target, toasts, and advances to the next pair', async () => {
    getCount.mockResolvedValue({ count: 2 });
    getMatch
      .mockResolvedValueOnce({ match: MATCH })
      .mockResolvedValue({ match: NEXT_MATCH });
    decide.mockResolvedValue({ ok: true, message: 'Match accepted' });

    const user = userEvent.setup();
    renderView();
    await screen.findByText('Same Song');

    await user.click(screen.getByRole('button', { name: 'ACCEPT' }));

    expect(decide).toHaveBeenCalledWith({
      isrc1: 'ISRC-A',
      isrc2: 'ISRC-B',
      preferredIsrc: 'ISRC-A',
      accept: true,
    });
    expect(await screen.findByText('Match accepted')).toBeInTheDocument();
    // Invalidation refetches the queue -> the next pair replaces the old one.
    expect(await screen.findByText('Next Song')).toBeInTheDocument();
  });

  it('REJECT sends accept: false and toasts the rejection', async () => {
    getCount.mockResolvedValue({ count: 2 });
    getMatch.mockResolvedValue({ match: MATCH });
    decide.mockResolvedValue({ ok: true, message: 'Match rejected' });

    const user = userEvent.setup();
    renderView();
    await screen.findByText('Same Song');

    await user.click(screen.getByRole('button', { name: 'REJECT' }));

    expect(decide).toHaveBeenCalledWith(
      expect.objectContaining({ accept: false }),
    );
    expect(await screen.findByText('Match rejected')).toBeInTheDocument();
  });

  it('surfaces a failed decision instead of confirming it (T08 contract)', async () => {
    getCount.mockResolvedValue({ count: 2 });
    getMatch.mockResolvedValue({ match: MATCH });
    decide.mockRejectedValue(new Error('500 Internal Server Error'));

    const user = userEvent.setup();
    renderView();
    await screen.findByText('Same Song');

    await user.click(screen.getByRole('button', { name: 'ACCEPT' }));

    expect(
      await screen.findByText('Decision failed. Please try again.'),
    ).toBeInTheDocument();
    expect(screen.queryByText('Match accepted')).not.toBeInTheDocument();
  });
});

describe('IsrcReviewView — empty queue and errors', () => {
  it('shows the empty state, skips the matchup fetch, and re-scans with a ~25 s indicator', async () => {
    getCount.mockResolvedValue({ count: 0 });
    rescan.mockImplementation(() => new Promise(() => {})); // stays pending

    const user = userEvent.setup();
    renderView();

    expect(
      await screen.findByText('No duplicate isrcs found'),
    ).toBeInTheDocument();
    // An empty queue must not cost a second round-trip.
    expect(getMatch).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Look for new matches' }));

    expect(rescan).toHaveBeenCalledTimes(1);
    expect(
      await screen.findByRole('button', { name: /25 seconds/ }),
    ).toBeDisabled();
  });

  it('surfaces a count failure', async () => {
    getCount.mockRejectedValue(new Error('boom'));

    renderView();

    expect(
      await screen.findByText('Could not load duplicate ISRCs.'),
    ).toBeInTheDocument();
  });
});