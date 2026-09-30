/**
 * SegmentTrimEditor behaviour tests (009-003 T08, AC-2/AC-4/AC-5/AC-6).
 *
 * jsdom has no WebGL and no canvas, so the map and the Chart.js profile are
 * stubbed: what is asserted is the editor's own behaviour — the activity meta,
 * the gate-bounded range, the creation call and its confirmation, and the two
 * controls that change activity / start a new segment. T23 moved the gate
 * inputs into TrimRouteViews, so they render together with the maps and appear
 * once the basemap style has loaded. Rendering of the map and profile stays a
 * human runtime check.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import SegmentTrimEditor from '../SegmentTrimEditor';
import { API } from '@/lib/api-client';
import type { ActivitySummary, ActivityRoute } from '@/lib/types/segment-management';

vi.mock('@/lib/api-client', () => ({
  API: {
    segments: { getRoute: vi.fn(), createSegment: vi.fn(), getMapStyles: vi.fn() },
    config: { getMapConfig: vi.fn() },
  },
}));

vi.mock('maplibre-gl', () => {
  class Map {
    addControl() {}
    isStyleLoaded() {
      return false;
    }
    once() {}
    off() {}
    getSource() {
      return undefined;
    }
    addSource() {}
    addLayer() {}
    setPaintProperty() {}
    fitBounds() {}
    setCenter() {}
    setZoom() {}
    remove() {}
  }
  class NavigationControl {}
  return { default: { Map, NavigationControl } };
});

vi.mock('../ElevationProfile', () => ({ default: () => <div data-testid="elevation" /> }));

const getRoute = vi.mocked(API.segments.getRoute);
const createSegment = vi.mocked(API.segments.createSegment);
const getMapStyles = vi.mocked(API.segments.getMapStyles);
const getMapConfig = vi.mocked(API.config.getMapConfig);

/** 0.001° of latitude per point ≈ 111 m, so 11 points ≈ 1.1 km. */
const ROUTE: ActivityRoute = {
  path_coords: Array.from({ length: 11 }, (_, i) => [0, i * 0.001]),
  elevations: Array.from({ length: 11 }, (_, i) => 100 + i),
  elapsed_s: Array.from({ length: 11 }, (_, i) => i * 10),
  distance_m: 1111,
};

const ACTIVITY: ActivitySummary = {
  activity_id: 24408723933,
  activity_type_name: 'running',
  start_time_utc: new Date(2026, 8, 18, 7, 5).toISOString(),
  distance_m: 1609.344,
  child_segment_count: 1,
  matched_count: 2,
};

beforeEach(() => {
  vi.clearAllMocks();
  getRoute.mockResolvedValue({ data: ROUTE });
  getMapStyles.mockResolvedValue({
    data: [
      { id: 'positron', name: 'Positron', requires_satellite_token: false },
      { id: 'mb-satellite', name: 'Satellite', requires_satellite_token: true },
    ],
    satellite_available: false,
  });
  getMapConfig.mockResolvedValue({ mapboxToken: null });
});

describe('SegmentTrimEditor', () => {
  it('shows the activity meta and the selected range (AC-2/AC-4)', async () => {
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={vi.fn()} />);

    expect(await screen.findByText(/24408723933/)).toBeInTheDocument();
    expect(screen.getByText(/1\.00 mi/)).toBeInTheDocument();
    // The range line lives with the maps (T23), so it arrives with the style.
    expect(await screen.findByText(/Selected range: 0–1111 m \(0\.69 mi\)/)).toBeInTheDocument();
  });

  it('re-bounds the displayed range when a gate moves (AC-4)', async () => {
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={vi.fn()} />);
    const startGate = await screen.findByLabelText('Select start gate');

    await userEvent.clear(startGate);
    await userEvent.type(startGate, '500');

    expect(await screen.findByText(/Selected range: 500–1111 m \(0\.38 mi\)/)).toBeInTheDocument();
  });

  it('sends the gate meters and chosen kind, then confirms the new id (AC-5)', async () => {
    createSegment.mockResolvedValue({ segment_id: 555 });
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={vi.fn()} />);

    await userEvent.type(await screen.findByLabelText('Segment Name'), 'Test Course');
    await userEvent.click(screen.getByRole('button', { name: 'Create Course' }));

    await waitFor(() =>
      expect(createSegment).toHaveBeenCalledWith({
        activity_id: 24408723933,
        start_m: 0,
        end_m: 1111,
        name: 'Test Course',
        is_course: true,
        map_style: 'positron',
      }),
    );
    expect(await screen.findByRole('status')).toHaveTextContent(
      'Course “Test Course” created as ID# 555',
    );
  });

  it('refuses to create without a name (AC-5)', async () => {
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={vi.fn()} />);
    await screen.findByLabelText('Segment Name');

    await userEvent.click(screen.getByRole('button', { name: 'Create Segment' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Enter a name');
    expect(createSegment).not.toHaveBeenCalled();
  });

  it('offers both AC-6 controls: choose new activity, or new segment from the same activity', async () => {
    const onChooseNewActivity = vi.fn();
    createSegment.mockResolvedValue({ segment_id: 556 });
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={onChooseNewActivity} />);

    await userEvent.type(await screen.findByLabelText('Segment Name'), 'Repeat');
    await userEvent.click(screen.getByRole('button', { name: 'Create Segment' }));
    expect(await screen.findByRole('status')).toHaveTextContent('created as ID# 556');

    await userEvent.click(screen.getByRole('button', { name: 'New segment from same activity' }));
    expect(screen.getByLabelText('Segment Name')).toHaveValue('');
    expect(screen.queryByText(/created as ID# 556/)).not.toBeInTheDocument();
    expect(getRoute).toHaveBeenCalledTimes(1);

    await userEvent.click(screen.getByRole('button', { name: 'Choose new activity' }));
    expect(onChooseNewActivity).toHaveBeenCalled();
  });

  it('reports a route-load failure with a retry (error state)', async () => {
    getRoute.mockRejectedValueOnce(new Error('boom'));
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={vi.fn()} />);

    expect(await screen.findByRole('alert')).toHaveTextContent('Could not load the activity route.');

    getRoute.mockResolvedValueOnce({ data: ROUTE });
    await userEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText(/Selected range:/)).toBeInTheDocument();
  });

  it('explains when the activity has no GPS route (empty state)', async () => {
    getRoute.mockResolvedValue({ data: { path_coords: [], elevations: [], elapsed_s: [], distance_m: 5000 } });
    render(<SegmentTrimEditor activity={ACTIVITY} onChooseNewActivity={vi.fn()} />);

    expect(
      await screen.findByText(/no GPS route data to create a course or segment from/i),
    ).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Create Segment' })).not.toBeInTheDocument();
  });
});