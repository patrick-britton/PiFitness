/**
 * Auto-reject sweep tests (009-003 T32, AC-20). Part 1: mocks + fixtures.
 * MatchSession is data-heavy (maps, charts, viewport store), so the suite
 * stubs the map/chart/child components and the API client.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

import MatchSession from '../MatchSession';
import { API } from '@/lib/api-client';
import { directionArrows } from '@/lib/route-direction';
import { cumulativeMeters } from '@/lib/segment-trim';
import { useViewportStore } from '@/stores/viewportStore';
import type { CandidateEffort, CandidatesResponse, SegmentListRow } from '@/lib/types/segment-management';

/**
 * `directionArrows` is spied rather than replaced (009-010 T04): the pass itself
 * stays real, while the call count proves it runs once per route load and never
 * again for session state the routes do not carry.
 */
vi.mock('@/lib/route-direction', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/route-direction')>();
  return { ...actual, directionArrows: vi.fn(actual.directionArrows) };
});

/**
 * Distance the `simulate-plot-pick` control reports (009-003 T40). A mutable
 * hoisted holder so each test can pick its own x on the plot.
 */
const plotPick = vi.hoisted(() => ({ targetM: 0 }));

vi.mock('@/lib/api-client', () => ({
  API: {
    segments: {
      getRoute: vi.fn(),
      getCandidates: vi.fn(),
      bulkReject: vi.fn(),
      findMatchesStep: vi.fn(),
      confirmMatch: vi.fn(),
      rejectMatch: vi.fn(),
      bulkConfirm: vi.fn(),
      hausdorffScoring: vi.fn(),
      frechetScoring: vi.fn(),
      deleteSegment: vi.fn(),
    },
    config: { getMapConfig: vi.fn() },
  },
}));

vi.mock('maplibre-gl', () => {
  class Map {
    addControl() {}
    isStyleLoaded() {
      return true;
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

vi.mock('../ElevationProfile', () => ({
  default: ({
    coords,
    elevations,
    ariaLabel,
    label,
    comparison,
    pickDistance,
    onPickDistance,
  }: {
    coords: number[][];
    elevations: number[];
    ariaLabel?: string;
    label?: string;
    comparison?: { coords: number[][]; elevations: number[]; label: string } | null;
    pickDistance?: number | null;
    onPickDistance?: (distanceM: number) => void;
  }) => (
    <div
      data-testid="elevation"
      data-aria-label={ariaLabel}
      data-label={label}
      data-points={`${coords.length}:${elevations.length}`}
      data-pick-distance={pickDistance ?? ''}
      data-comparison={
        comparison
          ? `${comparison.label}:${comparison.coords.length}:${comparison.elevations.length}`
          : ''
      }
    >
      <button type="button" onClick={() => onPickDistance?.(plotPick.targetM)}>
        simulate-plot-pick
      </button>
    </div>
  ),
}));
vi.mock('../SegmentRouteMap', () => ({
  default: ({
    pickMarkers,
    arrows,
    ariaLabel,
  }: {
    pickMarkers?: { series: string; point: number[]; label: string }[];
    arrows?: { series: string; lngLat: number[]; bearing: number }[];
    ariaLabel?: string;
  }) => (
    <div
      data-testid="segment-map"
      data-aria-label={ariaLabel}
      data-pick-markers={pickMarkers ? JSON.stringify(pickMarkers) : ''}
      data-arrows={arrows ? JSON.stringify(arrows) : ''}
    />
  ),
}));
vi.mock('../MapStyleSelector', () => ({ default: () => <div data-testid="style-selector" /> }));
vi.mock('../ConfirmDialog', () => ({
  default: ({ title, onCancel, onConfirm }: { title: string; onCancel: () => void; onConfirm: () => void }) => (
    <div role="dialog" aria-label={title}>
      <button type="button" onClick={onCancel}>
        Cancel
      </button>
      <button type="button" onClick={onConfirm}>
        Confirm
      </button>
    </div>
  ),
}));

/** Props received by the candidate-table stub, and how often it rendered (T42). */
const tableProps = vi.hoisted(() => ({
  renders: 0,
  last: null as null | {
    rows: unknown;
    selectedActivityId: unknown;
    busy: unknown;
    onSelect: unknown;
    onConfirm: unknown;
    onReject: unknown;
  },
}));

vi.mock('../CandidateTable', async () => {
  /**
   * The real table is `memo(CandidateTable)` (009-003 T42, Bug 009-003-41-1), so
   * the stub is wrapped in `memo` the same way. `tableProps.renders` then counts
   * what a production build would render: flat while a pick moves session state
   * whose props the table does not consume, and up the moment a real prop
   * (rows, selection, busy, or a row action) changes identity.
   */
  const { memo } = await import('react');
  function CandidateTableStub({
    rows,
    selectedActivityId,
    busy,
    onSelect,
    onConfirm,
    onReject,
  }: {
    rows: CandidateEffort[];
    selectedActivityId: number | null;
    busy: boolean;
    onSelect: (row: CandidateEffort) => void;
    onConfirm: (row: CandidateEffort) => void;
    onReject: (row: CandidateEffort) => void;
  }) {
    tableProps.renders += 1;
    tableProps.last = { rows, selectedActivityId, busy, onSelect, onConfirm, onReject };
    return (
      <div data-testid="candidate-table">
        {rows.map((r) => r.activity_id).join(',')}
        {rows.length > 0 && (
          <button type="button" onClick={() => onSelect(rows[0])}>
            select-candidate
          </button>
        )}
      </div>
    );
  }
  return { default: memo(CandidateTableStub) };
});
vi.mock('../MatchProgressSteps', () => ({ default: () => <div data-testid="match-steps" /> }));

const getRoute = vi.mocked(API.segments.getRoute);
const getCandidates = vi.mocked(API.segments.getCandidates);
const bulkReject = vi.mocked(API.segments.bulkReject);
const findMatchesStep = vi.mocked(API.segments.findMatchesStep);
const getMapConfig = vi.mocked(API.config.getMapConfig);

const SEGMENT: SegmentListRow = {
  segment_id: 9,
  segment_name: 'Test Loop',
  is_course: false,
  activity_type_name: 'Run',
  distance_mi: 2.0,
  elevation_gain: 10,
  last_effort: '2026-09-01',
  matched_activity_count: 0,
};

function candidate(activity_id: number, confidence: number): CandidateEffort {
  return {
    activity_id,
    best_start_dist: 0,
    best_end_dist: 100,
    confidence,
    deviation: {
      distance_deviation_m: 1,
      polygon_deviation: 2,
      hausdorff_deviation: null,
      frechet_deviation: null,
    },
    start_time_utc: '2026-09-01T10:00:00+00:00',
    distance_m: 3200,
    elevation_gain: 10,
    confidence_scaled: 0.5,
    distance_deviation_scaled: 0.5,
    polygon_deviation_scaled: 0.5,
    hausdorff_deviation_scaled: null,
    frechet_deviation_scaled: null,
  };
}

function candidatesResponse(rows: CandidateEffort[], existingMatchCount = 3): CandidatesResponse {
  return { data: rows, count: rows.length, existing_match_count: existingMatchCount };
}

beforeEach(() => {
  vi.clearAllMocks();
  /**
   * `clearAllMocks()` clears call history but NOT an unconsumed
   * `mockResolvedValueOnce` queue (Bug 009-010-04-1), so a response queued by a
   * test that never rendered was consumed by the next test's mount read and
   * shifted every later response. Reset both queue-carrying route reads (their
   * implementations are re-established here / per test).
   */
  getRoute.mockReset();
  getCandidates.mockReset();
  plotPick.targetM = 0;
  tableProps.renders = 0;
  tableProps.last = null;
  useViewportStore.setState({ layoutVariant: 'desktop' });
  getRoute.mockResolvedValue({ data: { path_coords: [], elevations: [], elapsed_s: [], distance_m: 0 } });
  getMapConfig.mockResolvedValue({ mapboxToken: null } as never);
});

describe('MatchSession auto-reject (T32)', () => {
  it('rejects >200 rows on load, reloads, shows message', async () => {
    const dirty = [candidate(11, 50), candidate(13, 250)];
    const clean = [candidate(11, 50)];
    getCandidates
      .mockResolvedValueOnce(candidatesResponse(dirty))
      .mockResolvedValueOnce(candidatesResponse(clean));
    bulkReject.mockResolvedValue({ rejected: 1 });

    render(<MatchSession segment={SEGMENT} onChooseDifferentSegment={() => {}} />);

    await waitFor(() => expect(bulkReject).toHaveBeenCalledTimes(1));
    expect(bulkReject).toHaveBeenCalledWith(9, { confidence_over: 200 });
    await waitFor(() => expect(getCandidates).toHaveBeenCalledTimes(2));
    expect(await screen.findByTestId('candidate-table')).toHaveTextContent('11');
    expect(await screen.findByText('Auto-rejected 1 candidate with confidence > 200.')).toBeInTheDocument();
  });

  it('makes no write call when the set is clean', async () => {
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50), candidate(12, 200)]));

    render(<MatchSession segment={SEGMENT} onChooseDifferentSegment={() => {}} />);

    await waitFor(() => expect(getCandidates).toHaveBeenCalled());
    expect(bulkReject).not.toHaveBeenCalled();
    expect(screen.queryByText(/Auto-rejected/)).not.toBeInTheDocument();
  });

  it('clears the message when the segment changes', async () => {
    const dirty = [candidate(11, 50), candidate(13, 250)];
    const clean = [candidate(11, 50)];
    getCandidates
      .mockResolvedValueOnce(candidatesResponse(dirty))
      .mockResolvedValueOnce(candidatesResponse(clean))
      .mockResolvedValue(candidatesResponse(clean));
    bulkReject.mockResolvedValue({ rejected: 1 });

    const { rerender } = render(<MatchSession segment={SEGMENT} onChooseDifferentSegment={() => {}} />);
    expect(await screen.findByText('Auto-rejected 1 candidate with confidence > 200.')).toBeInTheDocument();

    rerender(
      <MatchSession segment={{ ...SEGMENT, segment_id: 10 }} onChooseDifferentSegment={() => {}} />,
    );
    await waitFor(() =>
      expect(screen.queryByText('Auto-rejected 1 candidate with confidence > 200.')).not.toBeInTheDocument(),
    );
  });
});

describe('MatchSession reject-all (T33)', () => {
  it('fires bulkReject only after dialog confirm, then reloads', async () => {
    const rows = [candidate(11, 50), candidate(12, 60)];
    getCandidates
      .mockResolvedValueOnce(candidatesResponse(rows))
      .mockResolvedValueOnce(candidatesResponse([]))
      .mockResolvedValue(candidatesResponse([]));
    bulkReject.mockResolvedValue({ rejected: 2 });

    const { user } = await renderSession();
    const button = await screen.findByRole('button', { name: 'Reject all remaining' });
    // Red like the per-row Reject buttons (bg-red-700) and immediately beside confirm-all.
    expect(button).toHaveClass('bg-red-700');
    expect(screen.getByRole('button', { name: 'Confirm all remaining' }).nextElementSibling).toBe(
      button,
    );
    await user.click(button);

    // The dialog gates the write: nothing fires until confirm.
    expect(await screen.findByRole('dialog')).toBeInTheDocument();
    expect(bulkReject).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Confirm' }));

    await waitFor(() => expect(bulkReject).toHaveBeenCalledWith(9, {}));
    expect(await screen.findByText('Rejected 2 candidates.')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('refreshes both counts and clears the selected candidate after confirm', async () => {
    const rows = [candidate(11, 50), candidate(12, 60)];
    const withReference: SegmentListRow = {
      ...SEGMENT,
      activity_reference_id: 22047927643,
      reference_start_point: 0,
      reference_end_point: 100,
    };
    getRoute
      .mockResolvedValueOnce({
        data: { path_coords: [[0, 0], [0.001, 0.001]], elevations: [10, 12], elapsed_s: [0, 10], distance_m: 100 },
      })
      .mockResolvedValue({
        data: { path_coords: [[0, 0], [0.002, 0.002]], elevations: [11, 13], elapsed_s: [0, 12], distance_m: 100 },
      });
    getCandidates
      .mockResolvedValueOnce(candidatesResponse(rows, 3))
      .mockResolvedValue(candidatesResponse([], 4));
    bulkReject.mockResolvedValue({ rejected: 2 });

    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);

    expect(await screen.findByText(/Potential matches:/)).toHaveTextContent('Potential matches: 2');
    expect(screen.getByText(/Existing matches:/)).toHaveTextContent('Existing matches: 3');

    // Select a candidate so the comparison overlay exists and its clearing is observable.
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    expect(await screen.findByText(/orange = candidate 11 effort/)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Reject all remaining' }));
    expect(bulkReject).not.toHaveBeenCalled();
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));

    await waitFor(() => expect(bulkReject).toHaveBeenCalledWith(9, {}));
    // Both counts come from the post-reject read, not from local arithmetic.
    await waitFor(() =>
      expect(screen.getByText(/Existing matches:/)).toHaveTextContent('Existing matches: 4'),
    );
    expect(screen.getByText(/Potential matches:/)).toHaveTextContent('Potential matches: 0');
    // The stale selection and its comparison overlay are gone.
    expect(screen.queryByText(/orange = candidate/)).not.toBeInTheDocument();
  });

  it('stays disabled with an empty candidate set', async () => {
    getCandidates.mockResolvedValue(candidatesResponse([]));

    await renderSession();
    const button = await screen.findByRole('button', { name: 'Reject all remaining' });
    expect(button).toBeDisabled();
  });
});

describe('MatchSession comparison elevation (T34/T35)', () => {
  const withReference: SegmentListRow = {
    ...SEGMENT,
    activity_reference_id: 22047927643,
    reference_start_point: 0,
    reference_end_point: 100,
  };
  const segmentRoute = {
    data: { path_coords: [[0, 0], [0.001, 0.001]], elevations: [10, 12], elapsed_s: [0, 10], distance_m: 100 },
  };
  const candidateRoute = {
    data: {
      path_coords: [[0, 0], [0.001, 0.001], [0.002, 0.002]],
      elevations: [11, 13, 15],
      elapsed_s: [0, 11, 22],
      distance_m: 100,
    },
  };

  it('feeds the selected candidate in as the comparison series of the one profile', async () => {
    getRoute.mockResolvedValueOnce(segmentRoute).mockResolvedValue(candidateRoute);
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50)]));

    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);

    // No selection yet: one profile, primary series only, no comparison.
    const before = await screen.findByTestId('elevation');
    expect(screen.getAllByTestId('elevation')).toHaveLength(1);
    expect(before).toHaveAttribute('data-comparison', '');
    expect(before).toHaveAttribute('data-points', '2:2');
    expect(before).toHaveAttribute('data-label', 'Test Loop');
    expect(before).toHaveAttribute('data-aria-label', 'Elevation profile of Test Loop');

    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));

    const profile = await screen.findByTestId('elevation');
    await waitFor(() =>
      expect(profile).toHaveAttribute('data-comparison', 'Candidate 11:3:3'),
    );
    // Still a single profile: the segment series is untouched and both are named.
    expect(screen.getAllByTestId('elevation')).toHaveLength(1);
    expect(profile).toHaveAttribute('data-points', '2:2');
    expect(profile).toHaveAttribute(
      'data-aria-label',
      'Elevation profile of Test Loop compared with candidate 11',
    );
  });

  it('drops the comparison series when the candidate route fails', async () => {
    getRoute
      .mockResolvedValueOnce(segmentRoute)
      .mockRejectedValue(new Error('candidate route unavailable'));
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50)]));

    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));

    expect(
      await screen.findByText('Could not load the route for candidate 11.'),
    ).toBeInTheDocument();
    expect(screen.getAllByTestId('elevation')).toHaveLength(1);
    expect(screen.getByTestId('elevation')).toHaveAttribute('data-comparison', '');
    expect(screen.getByTestId('elevation')).toHaveAttribute(
      'data-aria-label',
      'Elevation profile of Test Loop',
    );
  });

  it('drops the comparison series when the segment changes', async () => {
    getRoute.mockResolvedValueOnce(segmentRoute).mockResolvedValue(candidateRoute);
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50)]));

    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    const { rerender } = render(
      <MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />,
    );
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() =>
      expect(screen.getByTestId('elevation')).toHaveAttribute(
        'data-comparison',
        'Candidate 11:3:3',
      ),
    );

    rerender(
      <MatchSession
        segment={{ ...withReference, segment_id: 10 }}
        onChooseDifferentSegment={() => {}}
      />,
    );

    await waitFor(() =>
      expect(screen.getByTestId('elevation')).toHaveAttribute('data-comparison', ''),
    );
  });
});

async function renderSession() {
  const userEvent = (await import('@testing-library/user-event')).default;
  const user = userEvent.setup();
  render(<MatchSession segment={SEGMENT} onChooseDifferentSegment={() => {}} />);
  return { user };
}

describe('map elevation coupling deleted (009-010 T01)', () => {
  it('exposes no map-pick control', async () => {
    const withReference: SegmentListRow = {
      ...SEGMENT,
      activity_reference_id: 42,
      reference_start_point: 0,
      reference_end_point: 100,
    };
    getRoute.mockResolvedValue({
      data: {
        path_coords: [[0, 0], [0, 0.001]],
        elevations: [10, 12],
        elapsed_s: [0, 300],
        distance_m: 200,
      },
    });
    getCandidates.mockResolvedValue(candidatesResponse([]));
    getMapConfig.mockResolvedValue({ mapboxToken: null } as never);
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');
    expect(screen.queryByRole('button', { name: 'simulate-map-pick' })).toBeNull();
  });
});

describe('plot pick → both activities marked on the map (009-010 T05, FR-2)', () => {
  const withReference: SegmentListRow = {
    ...SEGMENT,
    activity_reference_id: 42,
    reference_start_point: 0,
    reference_end_point: 100,
  };
  /**
   * Segment route: due north, ~222 m long, elapsed 0/200/400 s. Candidate route:
   * a parallel line at lon 1, ~334 m long, elapsed 0/60/120 s — distinct
   * coordinates, lengths and times, so each series' marker and legend entry is
   * identifiable and the two series' extents differ.
   */
  const routeResponse = {
    data: {
      path_coords: [
        [0, 0],
        [0, 0.001],
        [0, 0.002],
      ],
      elevations: [10, 12, 14],
      elapsed_s: [0, 200, 400],
      distance_m: 300,
    },
  };
  const candidateRouteResponse = {
    data: {
      path_coords: [
        [1, 0],
        [1, 0.001],
        [1, 0.003],
      ],
      elevations: [11, 13, 15],
      elapsed_s: [0, 60, 120],
      distance_m: 400,
    },
  };

  const markers = () => {
    const attr = screen.getByTestId('segment-map').getAttribute('data-pick-markers');
    return attr ? JSON.parse(attr) : [];
  };

  async function pickOnPlot(targetM: number) {
    plotPick.targetM = targetM;
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    const view = render(
      <MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />,
    );
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await user.click(await screen.findByRole('button', { name: 'simulate-plot-pick' }));
    return { user, view };
  }

  beforeEach(() => {
    getRoute.mockResolvedValueOnce(routeResponse).mockResolvedValue(candidateRouteResponse);
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50)]));
    bulkReject.mockResolvedValue({ rejected: 1 });
  });

  it('marks both series at the same fraction of their own lengths and names the fraction', async () => {
    // Click at 200 m of the 334 m plotted span ≈ 60%: each series marks its
    // own 60% — snapped to the nearest plotted point of its 3-point fixture
    // (route 60%×222≈133→111 m at [0, 0.001]; candidate 60%×334≈200→111 m at
    // [1, 0.001]). The SAME fraction of different tracks, never one distance.
    await pickOnPlot(200);

    expect(markers()).toEqual([
      { series: 'route', point: [0, 0.001], label: 'Test Loop · 111 m' },
      { series: 'route-overlay', point: [1, 0.001], label: 'Candidate 11 · 111 m' },
    ]);

    // Legend carries the meaning: fraction of the span, then per-series
    // fraction, picked-distance point and elapsed time (never colour alone).
    expect(await screen.findByText(/Picked 200 m on the elevation plot/)).toHaveTextContent(
      'Picked 200 m on the elevation plot (60% of the 334 m span) — Test Loop: 60% of its length · 111 m (0.07 mi), elapsed 3:20 · Candidate 11: 60% of its length · 111 m (0.07 mi), elapsed 1:00.',
    );
    // The plot indicator sits at the picked x itself, not at a series point.
    expect(screen.getByTestId('elevation')).toHaveAttribute('data-pick-distance', '200');
    // No map-point readout exists anymore (009-010 T01 deleted that path).
    expect(screen.queryByText(/Picked point:/)).toBeNull();
  });

  it('at the span end the shorter series is beyond its extent, so only the longer marks (AC-4 boundary)', async () => {
    // 334 m IS the span (the candidate's extent): the route's 222 m extent is
    // absolutely beyond it, so only the candidate marks — at its own 100%.
    await pickOnPlot(334);

    expect(markers()).toEqual([
      { series: 'route-overlay', point: [1, 0.003], label: 'Candidate 11 · 334 m' },
    ]);
    expect(await screen.findByText(/Picked 334 m on the elevation plot/)).toHaveTextContent(
      'Picked 334 m on the elevation plot (100% of the 334 m span) — Test Loop: beyond its 222 m extent, so no marker · Candidate 11: 100% of its length · 334 m (0.21 mi), elapsed 2:00.',
    );
  });

  it('marks only the series that reaches the distance and names the missing side', async () => {
    // 300 m is past the segment route's 222 m extent but inside the candidate's 334 m.
    await pickOnPlot(300);

    expect(markers()).toEqual([
      { series: 'route-overlay', point: [1, 0.003], label: 'Candidate 11 · 334 m' },
    ]);
    expect(await screen.findByText(/Picked 300 m on the elevation plot/)).toHaveTextContent(
      'Picked 300 m on the elevation plot (90% of the 334 m span) — Test Loop: beyond its 222 m extent, so no marker · Candidate 11: 90% of its length · 334 m (0.21 mi), elapsed 2:00.',
    );
  });

  it('names an unselected comparison instead of drawing a second marker', async () => {
    // No candidate selected: the span is the route's own 222 m, so 111 m is
    // 50% of it — the route marks its own 50% and the legend says so.
    plotPick.targetM = 111;
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');
    await user.click(await screen.findByRole('button', { name: 'simulate-plot-pick' }));

    expect(markers()).toEqual([{ series: 'route', point: [0, 0.001], label: 'Test Loop · 111 m' }]);
    expect(await screen.findByText(/Picked 111 m on the elevation plot/)).toHaveTextContent(
      'Picked 111 m on the elevation plot (50% of the 222 m span) — Test Loop: 50% of its length · 111 m (0.07 mi), elapsed 3:20 · Candidate: no comparison activity is selected, so no marker.',
    );
  });

  it('clears the markers with the explicit clear control', async () => {
    const { user } = await pickOnPlot(200);
    expect(markers()).toHaveLength(2);

    // Touch users cannot hover a pick away, so the pick has its own control.
    await user.click(screen.getByRole('button', { name: 'Clear picked point' }));
    await waitFor(() =>
      expect(screen.getByTestId('segment-map')).toHaveAttribute('data-pick-markers', ''),
    );
    expect(screen.queryByText(/on the elevation plot/)).toBeNull();
    expect(screen.getByTestId('elevation')).toHaveAttribute('data-pick-distance', '');
  });

  it('clears the markers when the segment changes', async () => {
    const { view } = await pickOnPlot(200);
    expect(markers()).toHaveLength(2);

    view.rerender(
      <MatchSession
        segment={{ ...withReference, segment_id: 10 }}
        onChooseDifferentSegment={() => {}}
      />,
    );

    await waitFor(() =>
      expect(screen.getByTestId('segment-map')).toHaveAttribute('data-pick-markers', ''),
    );
    expect(screen.queryByText(/on the elevation plot/)).toBeNull();
  });

  it('clears the markers when the comparison is deselected (reject-all)', async () => {
    const { user } = await pickOnPlot(200);
    expect(markers()).toHaveLength(2);

    // Deselecting drops the pick entirely — both markers and the legend go.
    await user.click(screen.getByRole('button', { name: 'Reject all remaining' }));
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));

    await waitFor(() =>
      expect(screen.getByTestId('segment-map')).toHaveAttribute('data-pick-markers', ''),
    );
    expect(screen.queryByText(/on the elevation plot/)).toBeNull();
  });
});
/**
 * Candidate-table render scope (009-003 T42, fixes Bug 009-003-41-1): a pick
 * moves session state per input event and the candidate list can hold hundreds
 * of rows (five metrics and three buttons each), so the table is `memo(...)`
 * and `MatchSession` hands it stable row actions. A pick must therefore not
 * re-render it — while a real prop change (selection, refreshed candidate set)
 * still must.
 */
describe('candidate-table render scope (T42, Bug 009-003-41-1)', () => {
  const withReference: SegmentListRow = {
    ...SEGMENT,
    activity_reference_id: 42,
    reference_start_point: 0,
    reference_end_point: 100,
  };
  /** Segment route: due north, ~222 m, elapsed 0/200/400 s. */
  const routeResponse = {
    data: {
      path_coords: [
        [0, 0],
        [0, 0.001],
        [0, 0.002],
      ],
      elevations: [10, 12, 14],
      elapsed_s: [0, 200, 400],
      distance_m: 300,
    },
  };
  /** Candidate route: a parallel line at lon 1, so the selection has its own route. */
  const candidateRouteResponse = {
    data: {
      path_coords: [
        [1, 0],
        [1, 0.003],
      ],
      elevations: [11, 13],
      elapsed_s: [0, 60],
      distance_m: 334,
    },
  };

  beforeEach(() => {
    getRoute.mockResolvedValueOnce(routeResponse).mockResolvedValue(candidateRouteResponse);
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50)]));
    findMatchesStep.mockResolvedValue({ message: 'ok' });
  });

  it('is untouched by a pick but re-rendered by a selection change', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('candidate-table');
    await waitFor(() => expect(tableProps.renders).toBeGreaterThan(0));

    // A selection change is a real prop change: selectedActivityId moves and the
    // finalize-derived row actions take new identities.
    const beforeSelect = tableProps.renders;
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() =>
      expect(screen.getByTestId('elevation')).toHaveAttribute('data-comparison', 'Candidate 11:2:2'),
    );
    expect(tableProps.renders).toBeGreaterThan(beforeSelect);

    // A pick only moves state the table does not consume, so `memo` skips it —
    // the plot pick after the map → elevation deletion (009-010 T01).
    const afterSelect = tableProps.renders;
    const stable = tableProps.last;
    expect(stable).not.toBeNull();

    plotPick.targetM = 150;
    await user.click(screen.getByRole('button', { name: 'simulate-plot-pick' }));
    await screen.findByText(/Picked 150 m on the elevation plot/);
    expect(tableProps.renders).toBe(afterSelect);

    // The props the memo compares never changed identity either (the three row
    // actions are stable adapters, not per-render inline arrows).
    expect(tableProps.last?.rows).toBe(stable?.rows);
    expect(tableProps.last?.selectedActivityId).toBe(stable?.selectedActivityId);
    expect(tableProps.last?.onSelect).toBe(stable?.onSelect);
    expect(tableProps.last?.onConfirm).toBe(stable?.onConfirm);
    expect(tableProps.last?.onReject).toBe(stable?.onReject);
  });

  it('re-renders when the candidate set is refreshed', async () => {
    // The mount read returns one candidate and the post-find re-read returns
    // two, so the rows prop genuinely changes identity here.
    getCandidates
      .mockReset()
      .mockResolvedValueOnce(candidatesResponse([candidate(11, 50)]))
      .mockResolvedValue(candidatesResponse([candidate(11, 50), candidate(12, 45)]));

    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('candidate-table');
    expect(screen.getByTestId('candidate-table')).toHaveTextContent('11');

    const beforeFind = tableProps.renders;
    await user.click(screen.getByRole('button', { name: 'Find matches' }));
    await waitFor(() =>
      expect(screen.getByTestId('candidate-table')).toHaveTextContent('11,12'),
    );
    expect(tableProps.renders).toBeGreaterThan(beforeFind);
  });

  /**
   * The stub above only mirrors the real component; this guards the real thing,
   * so removing `memo` from `CandidateTable` fails here instead of leaving the
   * render-scope test passing on a stub that is memoised by itself.
   */
  it('keeps the real CandidateTable memo-wrapped', async () => {
    const actual = await vi.importActual<{
      default: { $$typeof?: symbol; type?: unknown };
    }>('../CandidateTable');
    expect(actual.default.$$typeof).toBe(Symbol.for('react.memo'));
    expect(typeof actual.default.type).toBe('function');
  });
});

describe('directional arrows (009-010 T04, FR-3)', () => {
  const withReference: SegmentListRow = {
    ...SEGMENT,
    activity_reference_id: 42,
    reference_start_point: 0,
    reference_end_point: 100,
  };
  /**
   * Segment route: due north, ~222 m → glyphs at 25/75/125/175 m, all bearing 0.
   * Candidate route: due east, ~334 m → glyphs at 25…325 m, all bearing 90. The
   * two series differ in length AND orientation, so a glyph can be traced back
   * to the series that produced it.
   */
  const routeResponse = {
    data: {
      path_coords: [
        [0, 0],
        [0, 0.001],
        [0, 0.002],
      ],
      elevations: [10, 12, 14],
      elapsed_s: [0, 200, 400],
      distance_m: 300,
    },
  };
  const candidateRouteResponse = {
    data: {
      path_coords: [
        [1, 0],
        [1.001, 0],
        [1.003, 0],
      ],
      elevations: [11, 13, 15],
      elapsed_s: [0, 60, 120],
      distance_m: 400,
    },
  };

  const arrows = () => {
    const attr = screen.getByTestId('segment-map').getAttribute('data-arrows');
    return attr
      ? (JSON.parse(attr) as { series: string; lngLat: number[]; bearing: number }[])
      : [];
  };

  beforeEach(() => {
    getRoute.mockResolvedValueOnce(routeResponse).mockResolvedValue(candidateRouteResponse);
    getCandidates.mockResolvedValue(candidatesResponse([candidate(11, 50)]));
    bulkReject.mockResolvedValue({ rejected: 1 });
  });

  it('samples each route at the nominal spacing, in that series’ own direction', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');

    // Route only: 222 m at 50 m spacing → four glyphs, all pointing north.
    const routeOnly = arrows();
    expect(routeOnly).toHaveLength(4);
    expect(new Set(routeOnly.map((a) => a.series))).toEqual(new Set(['route']));
    for (const a of routeOnly) {
      expect(a.bearing).toBeCloseTo(0, 0);
      expect(a.lngLat[0]).toBeCloseTo(0, 5);
    }

    // Selecting a candidate adds its own glyph series — same spacing, its own
    // eastbound heading — while the route's glyphs are untouched.
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() => expect(arrows().some((a) => a.series === 'route-overlay')).toBe(true));

    const overlay = arrows().filter((a) => a.series === 'route-overlay');
    expect(overlay).toHaveLength(7);
    for (const a of overlay) {
      expect(a.bearing).toBeCloseTo(90, 0);
      expect(a.lngLat[1]).toBeCloseTo(0, 5);
    }
    expect(arrows().filter((a) => a.series === 'route')).toEqual(routeOnly);
  });

  it('names the arrows and both series in the map legend and the aria text', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');

    // Before a candidate is selected the legend names the route's arrows only.
    const ariaBefore = screen.getByTestId('segment-map').getAttribute('data-aria-label') ?? '';
    expect(ariaBefore).toContain('filled triangle arrows on the route');
    expect(ariaBefore).not.toContain('open chevron');
    expect(
      screen.getByText(/filled triangle arrows pointing the way it was travelled/),
    ).toBeInTheDocument();
    expect(screen.queryByText(/open chevron arrows/)).toBeNull();

    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() =>
      expect(screen.getByText(/candidate 11 effort, with open chevron arrows/)).toBeInTheDocument(),
    );
    const ariaAfter = screen.getByTestId('segment-map').getAttribute('data-aria-label') ?? '';
    expect(ariaAfter).toContain('filled triangle arrows on the route');
    expect(ariaAfter).toContain('open chevron arrows on the candidate effort');
  });

  it('runs one pass per route and none for state the routes do not carry', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() => expect(arrows().some((a) => a.series === 'route-overlay')).toBe(true));

    // Exactly one pass per loaded route (the two memos above), not one per render.
    expect(vi.mocked(directionArrows)).toHaveBeenCalledTimes(2);

    // A pick moves only pick state: the routes are untouched, so neither pass
    // may run again.
    plotPick.targetM = 150;
    await user.click(screen.getByRole('button', { name: 'simulate-plot-pick' }));
    await screen.findByText(/Picked 150 m on the elevation plot/);
    expect(vi.mocked(directionArrows)).toHaveBeenCalledTimes(2);
  });

  it('collapses cross-series glyphs that land within ~4 m, one per series', async () => {
    // A candidate that covered the very same track: every overlay glyph collides
    // with a route glyph of the same path distance, so the route's glyphs (listed
    // first) survive and the overlay keeps its first glyph instead of vanishing
    // (AC-6b collapse + AC-6 "no series is ever arrow-less").
    getRoute.mockReset().mockResolvedValueOnce(routeResponse).mockResolvedValue(routeResponse);

    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() => expect(arrows().some((a) => a.series === 'route-overlay')).toBe(true));

    const routeGlyphs = arrows().filter((a) => a.series === 'route');
    const overlayGlyphs = arrows().filter((a) => a.series === 'route-overlay');
    expect(routeGlyphs).toHaveLength(4);
    expect(overlayGlyphs).toHaveLength(1);
    expect(overlayGlyphs[0].lngLat).toEqual(routeGlyphs[0].lngLat);
  });

  it('says in the legend that both lines are solid and collided glyphs are dropped', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    const user = userEvent.setup();
    render(<MatchSession segment={withReference} onChooseDifferentSegment={() => {}} />);
    await screen.findByTestId('segment-map');
    await user.click(await screen.findByRole('button', { name: 'select-candidate' }));
    await waitFor(() =>
      expect(screen.getByText(/candidate 11 effort, with open chevron arrows/)).toBeInTheDocument(),
    );

    // OQ-5 retired the dash convention, so the legend must name colour and shape
    // instead — and must never call a line dashed again.
    const legend = screen.getByText(/both lines are solid/);
    expect(legend.textContent).toMatch(/blue = the (course|segment) route/);
    expect(legend.textContent).toMatch(/orange = candidate 11 effort/);
    expect(legend.textContent).toMatch(/filled triangle arrows/);
    expect(legend.textContent).toMatch(/open chevron arrows/);
    expect(legend.textContent).toMatch(/within 4 m/);
    expect(legend.textContent).not.toMatch(/dashed/i);
  });
});



