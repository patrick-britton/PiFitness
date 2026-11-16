/**
 * Recent Activity page behaviour tests (004-006 T07, AC-6/AC-7).
 *
 * Pins the `{n} segment{s}:` count header and the empty-state block whose two
 * links switch the Activities module tab via the UI store. The chart module
 * (MapLibre + Chart.js at import time) is stubbed — its rendering is covered
 * by the T05 static harness, not jsdom.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import RecentActivityPage from '../page';
import { API } from '@/lib/api-client';
import { useUIStore } from '@/stores/uiStore';
import type { ActivityReport } from '@/lib/types/activity-report';

vi.mock('@/lib/api-client', () => ({
  API: { activities: { getReport: vi.fn() } },
}));

vi.mock('../components/ActivityReportCharts', () => ({
  CoursePathChart: () => null,
  ElevationChart: () => null,
  HeartrateChart: () => null,
}));

/** Stubbed host child: the real table pulls in Chart.js/MapLibre at import. */
vi.mock('@/app/activities/leaderboards/components/LeaderboardTable', () => ({
  default: ({
    segmentId,
    embedded,
  }: {
    segmentId: number;
    segmentName: string;
    embedded?: boolean;
  }) => (
    <div
      data-testid="leaderboard-table"
      data-segment-id={segmentId}
      data-embedded={String(embedded ?? false)}
    />
  ),
}));

const getReport = vi.mocked(API.activities.getReport);

const HEADER: ActivityReport['header'] = {
  start_utc: new Date(2026, 8, 18, 7, 5).toISOString(),
  distance_mi: 1,
  distance_display: '1.0 mi',
  elevation_m_gain: 10,
  total_time_s: 600,
  total_time_text: '10:00.0',
  total_time_display: '10:00.0',
  pace_text: '10:00.0/mi',
  pace_display: '10:00.0/mi',
  hr_median: 140,
  hr_max: 160,
  activity_name: 'Morning Run',
  activity_type_name: 'running',
  distance_m: 1609.344,
};

const SEGMENT: ActivityReport['segments'][number] = {
  segment_id: 42,
  name: 'Canal Straight',
  is_course: false,
  all_time_rank: 3,
  total_attempts: 10,
  prior_delta_s: 0.58,
  best_delta_s: -1297,
};

function makeReport(overrides: Partial<ActivityReport>): ActivityReport {
  return {
    activity_id: 24663109355,
    nav: { prev_activity_id: 24652871872, next_activity_id: null },
    header: HEADER,
    course: null,
    segments: [],
    has_segments: false,
    efficiency: null,
    charts: { elevation: [], heartrate: [] },
    course_path: [],
    ...overrides,
  };
}

const setActiveSubPage = vi.fn<(subPageId: string | null) => void>();

beforeEach(() => {
  getReport.mockReset();
  setActiveSubPage.mockReset();
  useUIStore.setState({ activeSubPage: null, setActiveSubPage });
});

describe('RecentActivityPage empty state (AC-6)', () => {
  // VERIFIED: Behavior — empty segment state displays navigation buttons that switch active tabs
  it('shows "No Segments Found." with links that switch tabs', async () => {
    getReport.mockResolvedValue(makeReport({}));
    render(<RecentActivityPage />);

    expect(await screen.findByText('No Segments Found.')).toBeInTheDocument();

    await userEvent.click(
      screen.getByRole('button', { name: 'Go to Segment Management' }),
    );
    expect(setActiveSubPage).toHaveBeenCalledWith('segment-management');

    await userEvent.click(
      screen.getByRole('button', { name: 'Go to Activity Processing' }),
    );
    expect(setActiveSubPage).toHaveBeenCalledWith('activity-processing');
  });
});

describe('RecentActivityPage segment list (AC-7)', () => {
  // VERIFIED: Component — renders segment count header and pace deltas for segments
  it('renders the plural count header and AC-7 card deltas', async () => {
    getReport.mockResolvedValue(
      makeReport({ segments: [SEGMENT], has_segments: true }),
    );
    render(<RecentActivityPage />);

    expect(await screen.findByText('1 segment:')).toBeInTheDocument();
    expect(screen.getByText('Canal Straight')).toBeInTheDocument();
    expect(screen.getByText(/0\.58s faster/)).toBeInTheDocument();
    expect(screen.getByText(/21:37 slower/)).toBeInTheDocument();
    expect(screen.queryByText('No Segments Found.')).not.toBeInTheDocument();
  });

  // VERIFIED: Component — treats course as segment in total count header
  it('uses the singular header for a lone course', async () => {
    getReport.mockResolvedValue(
      makeReport({
        course: { ...SEGMENT, segment_id: 7, name: 'Towpath Loop', is_course: true },
        has_segments: true,
      }),
    );
    render(<RecentActivityPage />);

    await waitFor(() =>
      expect(screen.getByText('1 segment:')).toBeInTheDocument(),
    );
  });
});

describe('RecentActivityPage leaderboard screen (AC-8)', () => {
  // VERIFIED: Behavior — clicking segment card opens embedded leaderboard replacing report view
  it('opens the embedded leaderboard for the tapped card, replacing the report', async () => {
    getReport.mockResolvedValue(
      makeReport({ segments: [SEGMENT], has_segments: true }),
    );
    render(<RecentActivityPage />);

    await userEvent.click(
      await screen.findByRole('button', { name: /Canal Straight/ }),
    );

    const table = await screen.findByTestId('leaderboard-table');
    expect(table).toHaveAttribute('data-segment-id', '42');
    expect(table).toHaveAttribute('data-embedded', 'true');
    // Host header shows the segment name; the report view is hidden.
    expect(screen.getByText('Canal Straight')).toBeInTheDocument();
    expect(screen.queryByText('1 segment:')).not.toBeInTheDocument();
  });

  // VERIFIED: Behavior — closing embedded leaderboard returns to activity report view
  it('X returns to the report view', async () => {
    getReport.mockResolvedValue(
      makeReport({ segments: [SEGMENT], has_segments: true }),
    );
    render(<RecentActivityPage />);

    await userEvent.click(
      await screen.findByRole('button', { name: /Canal Straight/ }),
    );
    await screen.findByTestId('leaderboard-table');

    await userEvent.click(
      screen.getByRole('button', {
        name: 'Close Canal Straight leaderboard and return to the report',
      }),
    );

    expect(screen.queryByTestId('leaderboard-table')).not.toBeInTheDocument();
    expect(await screen.findByText('1 segment:')).toBeInTheDocument();
  });
});

/** Course row fixture (Bug 004-006-T07-1: the course is a card too). */
const COURSE: NonNullable<ActivityReport['course']> = {
  segment_id: 7,
  name: 'Towpath Loop',
  is_course: true,
  all_time_rank: 1,
  total_attempts: 12,
  prior_delta_s: -5,
  best_delta_s: 10,
};

describe('RecentActivityPage course card (Bug 004-006-T07-1)', () => {
  // VERIFIED: Behavior — clicking course card opens embedded leaderboard for that course
  it('opens the embedded leaderboard for the course card (course segment_id)', async () => {
    getReport.mockResolvedValue(
      makeReport({ course: COURSE, segments: [], has_segments: true }),
    );
    render(<RecentActivityPage />);

    await userEvent.click(
      await screen.findByRole('button', { name: /Towpath Loop/ }),
    );
    const table = await screen.findByTestId('leaderboard-table');
    expect(table).toHaveAttribute('data-segment-id', '7');
    expect(table).toHaveAttribute('data-embedded', 'true');
  });
});
