/**
 * ActivitySourceList behaviour tests (009-003 T08, AC-1).
 *
 * Verifies which state renders, what the rows say, what the filter sends, and
 * what a selection hands back. jsdom has no layout engine, so the three layout
 * variants and light/dark appearance remain a human runtime check.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import ActivitySourceList from '../ActivitySourceList';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import type { ActivitySummary } from '@/lib/types/segment-management';

vi.mock('@/lib/api-client', () => ({
  API: { segments: { getActivities: vi.fn() } },
}));

const getActivities = vi.mocked(API.segments.getActivities);

const ROW: ActivitySummary = {
  activity_id: 24408723933,
  activity_type_name: 'running',
  start_time_utc: new Date(2026, 8, 18, 7, 5).toISOString(),
  distance_m: 1609.344,
  child_segment_count: 2,
  matched_count: 5,
};

beforeEach(() => {
  getActivities.mockReset();
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

describe('ActivitySourceList', () => {
  it('lists type, start date, distance, child and matched counts (AC-1)', async () => {
    getActivities.mockResolvedValue({ data: [ROW], count: 1 });
    render(<ActivitySourceList onSelect={vi.fn()} />);

    expect(await screen.findByText('running')).toBeInTheDocument();
    expect(screen.getByText('18 Sep 2026')).toBeInTheDocument();
    expect(screen.getByText('1.00 mi')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
  });

  it('sends the activity-type filter to the server and resets paging', async () => {
    getActivities.mockResolvedValue({ data: [ROW], count: 1 });
    render(<ActivitySourceList onSelect={vi.fn()} />);
    await screen.findByText('running');

    await userEvent.click(screen.getByRole('button', { name: 'Trail Run' }));

    await waitFor(() =>
      expect(getActivities).toHaveBeenLastCalledWith(
        expect.objectContaining({ activity_type: 'Trail Run', offset: 0 }),
      ),
    );
    expect(screen.getByRole('button', { name: 'Trail Run' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('hands the chosen activity to the caller', async () => {
    getActivities.mockResolvedValue({ data: [ROW], count: 1 });
    const onSelect = vi.fn();
    render(<ActivitySourceList onSelect={onSelect} />);
    await screen.findByText('running');

    await userEvent.click(screen.getByRole('button', { name: 'Select activity 24408723933' }));

    expect(onSelect).toHaveBeenCalledWith(ROW);
  });

  it('renders stacked cards instead of a table in portrait', async () => {
    useViewportStore.setState({ layoutVariant: 'portrait' });
    getActivities.mockResolvedValue({ data: [ROW], count: 1 });
    render(<ActivitySourceList onSelect={vi.fn()} />);

    const card = await screen.findByRole('button', {
      name: 'Select activity 24408723933 from 18 Sep 2026',
    });
    expect(card).toHaveTextContent('Child segments/courses: 2');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });

  it('shows an empty state when nothing matches', async () => {
    getActivities.mockResolvedValue({ data: [], count: 0 });
    render(<ActivitySourceList onSelect={vi.fn()} />);

    expect(await screen.findByText('No matching activities found.')).toBeInTheDocument();
  });

  it('shows an error state with a retry that refetches', async () => {
    getActivities.mockRejectedValueOnce(new Error('boom'));
    render(<ActivitySourceList onSelect={vi.fn()} />);

    expect(await screen.findByRole('alert')).toHaveTextContent('Could not load activities.');
    getActivities.mockResolvedValueOnce({ data: [ROW], count: 1 });
    await userEvent.click(screen.getByRole('button', { name: 'Retry' }));

    expect(await screen.findByText('running')).toBeInTheDocument();
  });

  it('offers Load more only while a full page came back', async () => {
    const page = Array.from({ length: 50 }, (_, i) => ({ ...ROW, activity_id: 1000 + i }));
    const nextPage = Array.from({ length: 1 }, (_, i) => ({ ...ROW, activity_id: 5000 + i }));
    getActivities
      .mockResolvedValueOnce({ data: page, count: 50 })
      .mockResolvedValueOnce({ data: nextPage, count: 1 });
    render(<ActivitySourceList onSelect={vi.fn()} />);
    await screen.findByText('Showing 50 activities');

    await userEvent.click(screen.getByRole('button', { name: 'Load more' }));
    await waitFor(() =>
      expect(getActivities).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 50 })),
    );
    expect(await screen.findByText('Showing 51 activities')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Load more' })).not.toBeInTheDocument();
  });
});