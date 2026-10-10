/**
 * LeaderboardTable layout-order tests (004-006 T08, AC-8).
 *
 * Pins the shared element's screen order — Effort Distribution → Generate
 * Race → range/type filters → rankings table — which BOTH hosts (Leaderboards
 * focus view and the recent-activity embedded screen) inherit, plus the
 * `embedded` header suppression. The lollipop/visuals children are stubbed
 * (Chart.js/MapLibre import-time deps, same pattern as MatchSession tests);
 * jsdom has no layout engine, so visual appearance stays a device check.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';

import LeaderboardTable from '../LeaderboardTable';
import { API } from '@/lib/api-client';
import type { Leaderboard, LeaderboardEffort } from '@/lib/types/leaderboards';

vi.mock('@/lib/api-client', () => ({
  API: { activities: { getLeaderboard: vi.fn() } },
}));

vi.mock('../EffortLollipop', () => ({
  default: () => <div data-testid="effort-distribution" />,
}));

vi.mock('../LeaderboardVisuals', () => ({
  default: () => <div data-testid="leaderboard-visuals" />,
}));

const getLeaderboard = vi.mocked(API.activities.getLeaderboard);

const EFFORT: LeaderboardEffort = {
  activity_id: 24663109355,
  activity_start_point: 0,
  activity_end_point: 1000,
  rank: 1,
  all_time_rank: 1,
  last_365_rank: 1,
  current_cycle_rank: 1,
  recency_rank: 1,
  cycle_name: 'All Time',
  start_time_utc: new Date(2026, 8, 18, 7, 5).toISOString(),
  elapsed_duration_s: 600,
  gap_s: 0,
  pace_str: null,
  avg_hr: null,
  max_hr: null,
  vo2_max_value: null,
  resting_hr_asleep: null,
  resting_hr_awake: null,
  training_load_acute: null,
  training_load_pct: null,
  weight_lb: null,
  fat_pct: null,
  muscle_pct: null,
  avg_cadence: null,
  avg_vert_osc: null,
  avg_vert_ratio: null,
  avg_gct: null,
  avg_stride_length: null,
  avg_temp: null,
  altitude_acclimation_m: null,
  heat_acclimation_pct: null,
  sleep_hours: null,
  sleep_score: null,
  awake_hours: null,
  avg_perf: null,
  avg_balance: null,
};

const DATA: Leaderboard = {
  segment_id: 7,
  segment_name: 'Towpath Loop',
  is_course: false,
  efforts: [EFFORT],
};

/** True when `a` comes before `b` in document order. */
function appearsBefore(a: Element, b: Element): boolean {
  return (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
}

beforeEach(() => {
  getLeaderboard.mockReset();
  getLeaderboard.mockResolvedValue(DATA);
});

describe('LeaderboardTable screen order (AC-8)', () => {
  it('orders Effort Distribution → Generate Race → filters → table', async () => {
    render(<LeaderboardTable segmentId={7} segmentName="Towpath Loop" />);

    const distribution = await screen.findByTestId('effort-distribution');
    const raceGroup = screen.getByRole('group', { name: 'Race selection mode' });
    const range = document.getElementById('lb-range')!;
    const table = screen.getByRole('table');

    expect(appearsBefore(distribution, raceGroup)).toBe(true);
    expect(appearsBefore(raceGroup, range)).toBe(true);
    expect(appearsBefore(range, table)).toBe(true);
  });

  it('renders the built-in name header when NOT embedded', async () => {
    render(<LeaderboardTable segmentId={7} segmentName="Towpath Loop" />);

    await screen.findByTestId('effort-distribution');
    expect(
      screen.getByRole('heading', { name: 'Towpath Loop' }),
    ).toBeInTheDocument();
    expect(screen.getByText('1 effort')).toBeInTheDocument();
  });

  it('suppresses the built-in name header in embedded mode (host renders it)', async () => {
    render(<LeaderboardTable segmentId={7} segmentName="Towpath Loop" embedded />);

    await screen.findByTestId('effort-distribution');
    expect(
      screen.queryByRole('heading', { name: 'Towpath Loop' }),
    ).not.toBeInTheDocument();
    // Controls + table still render for the embedded host.
    expect(document.getElementById('lb-range')).toBeInTheDocument();
    expect(screen.getByRole('table')).toBeInTheDocument();
  });
});
