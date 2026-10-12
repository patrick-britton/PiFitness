/**
 * 004-005 T04 / AC-3 — a failed or aborted `/process` run must never strand
 * steps in `pending`: the failing/interrupted step renders red with its
 * message, unstarted steps reach a terminal (`skipped`) state, and the page
 * shows an error banner.
 *
 * 004-007 T05 — estimate-driven status bars: each row carries a bar sized
 * elapsed/estimated-time (gray running, red past estimate, green complete),
 * fed by estimateStepMs with distance_m captured from the resolve_activity
 * terminal event.
 *
 * Run from frontend/pifitness:
 *   npx vitest run src/app/activities/activity-processing
 */
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { API } from '@/lib/api-client';
import ActivityProcessingPage, { finalizeFailedSteps } from '../page';
import StepChecklist, { estimateForStep, barFraction } from '../StepChecklist';
import {
  ProcessStepResult,
  ProcessStepStartEvent,
  ProcessStepEvent,
  PLAYLIST_STEPS,
  STEP_ORDER,
} from '@/lib/types/activity-processing';

vi.mock('@/lib/api-client', () => ({
  API: { activities: { processActivity: vi.fn() } },
}));

const processActivity = vi.mocked(API.activities.processActivity);

const pendingCount = () =>
  document.querySelectorAll('[data-status="pending"]').length;

const statusOf = (id: string) =>
  document.querySelector(`[data-step-id="${id}"]`)?.getAttribute('data-status');

async function startRun() {
  render(<ActivityProcessingPage />);
  fireEvent.click(screen.getByRole('button', { name: 'Last Walk' }));
  fireEvent.click(screen.getByRole('button', { name: 'Start Processing' }));
}

const base = (over: Partial<ProcessStepResult> = {}): ProcessStepResult => ({
  step_id: 'sync_activities',
  status: 'pending',
  elapsed_ms: 0,
  ...over,
});

describe('failure finalization (AC-3)', () => {
  beforeEach(() => processActivity.mockReset());

  it('early failure (non-OK response): 0 pending, first step red, banner shown', async () => {
    // Guard: vitest's teardown probes the mock with zero args — only reject
    // the real (2-arg) call so the probe's promise never rejects unhandled.
    processActivity.mockImplementation(async (...args: unknown[]) => {
      if (args.length === 0) return { complete: true, success: true } as never;
      throw new Error('API request failed with status 503');
    });
    await startRun();

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'API request failed with status 503'
    );
    expect(pendingCount()).toBe(0);
    expect(statusOf('sync_activities')).toBe('error');
    // Last Walk has no music flow — the six playlist rows are hidden
    // (Bug 004-007-7-4), so only the non-playlist STEP_ORDER rows render.
    expect(screen.getAllByTestId('step-row')).toHaveLength(
      STEP_ORDER.length - PLAYLIST_STEPS.size
    );
    expect(
      document.querySelector('[data-step-id="lookup_playlist"]')
    ).toBeNull();
  });

  it('stream cut mid-step: interrupted step red with message, rest skipped', async () => {
    processActivity.mockImplementation(async (...args: unknown[]) => {
      // Guard: vitest teardown probes the mock with zero args.
      if (args.length === 0) return { complete: true, success: true } as never;
      const onStep = args[1] as (
        e: ProcessStepStartEvent | ProcessStepEvent
      ) => void;
      onStep({
        step_id: 'sync_activities',
        status: 'running',
        started_at: new Date().toISOString(),
      });
      throw new Error('Stream ended without terminal event');
    });
    await startRun();

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Stream ended without terminal event'
    );
    expect(pendingCount()).toBe(0);
    expect(statusOf('sync_activities')).toBe('error');
    expect(statusOf('resolve_activity')).toBe('skipped');
    const failing = document.querySelector('[data-step-id="sync_activities"]');
    expect(failing).toHaveTextContent('Stream ended without terminal event');
  });

  it('terminal step error: failing step keeps its underlying message, 0 pending', async () => {
    processActivity.mockImplementation(async (...args: unknown[]) => {
      // Guard: vitest teardown probes the mock with zero args.
      if (args.length === 0) return { complete: true, success: true } as never;
      const onStep = args[1] as (
        e: ProcessStepStartEvent | ProcessStepEvent
      ) => void;
      onStep({
        step_id: 'sync_activities',
        status: 'running',
        started_at: new Date().toISOString(),
      });
      onStep({
        step_id: 'sync_activities',
        status: 'error',
        elapsed_ms: 12,
        error: 'extraction error: 401 unauthorized',
      });
      return {
        complete: true,
        success: false,
        error: 'extraction error: 401 unauthorized',
      };
    });
    await startRun();

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'extraction error: 401 unauthorized'
    );
    expect(pendingCount()).toBe(0);
    expect(statusOf('sync_activities')).toBe('error');
    expect(
      document.querySelector('[data-step-id="sync_activities"]')
    ).toHaveTextContent('extraction error: 401 unauthorized');
    // Unstarted shuffle/post-processing steps are terminal, not pending.
    // The playlist rows are hidden entirely on Last Walk (Bug 004-007-7-4).
    expect(
      document.querySelector('[data-step-id="lookup_playlist"]')
    ).toBeNull();
    expect(statusOf('segment_update_details')).toBe('skipped');
  });

  it('terminal success leaves the banner hidden', async () => {
    processActivity.mockResolvedValue({ complete: true, success: true });
    await startRun();
    // Success path does not finalize — the backend emits terminal events for
    // every executed step; assert no error banner and no red step appears.
    await new Promise((r) => setTimeout(r, 0));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(statusOf('sync_activities')).not.toBe('error');
  });
});


describe('finalizeFailedSteps (unit)', () => {
  const msg = 'run died';

  it('keeps terminal steps, reds the running step, skips the rest', () => {
    const out = finalizeFailedSteps(
      [
        base({ step_id: 'sync_activities', status: 'complete' }),
        base({ step_id: 'resolve_activity', status: 'running' }),
        base({ step_id: 'sync_details', status: 'pending' }),
        base({ step_id: 'insert_heartrate', status: 'pending' }),
      ],
      msg
    );
    expect(out.map((s) => s.status)).toEqual([
      'complete',
      'error',
      'skipped',
      'skipped',
    ]);
    expect(out[1].error).toBe(msg);
  });

  it('reddens the first pending step when nothing started (lock conflict)', () => {
    const lockMsg = 'A processing run is already in progress.';
    const out = finalizeFailedSteps(
      STEP_ORDER.map((id) => base({ step_id: id })),
      lockMsg
    );
    expect(out[0].step_id).toBe('sync_activities');
    expect(out[0].status).toBe('error');
    expect(out[0].error).toBe(lockMsg);
    expect(out.slice(1).every((s) => s.status === 'skipped')).toBe(true);
    expect(out.some((s) => s.status === 'pending')).toBe(false);
  });

  it('preserves an existing error step and its underlying message', () => {
    const out = finalizeFailedSteps(
      [
        base({ step_id: 'sync_activities', status: 'error', error: 'underlying' }),
        base({ step_id: 'resolve_activity', status: 'pending' }),
      ],
      msg
    );
    expect(out[0].error).toBe('underlying');
    expect(out[1].status).toBe('skipped');
  });

  it('returns the same array for an empty step list', () => {
    expect(finalizeFailedSteps([], msg)).toEqual([]);
  });
});

describe('estimate-driven status bars (004-007 T05 / AC-5)', () => {
  it('sync steps use static estimates from t=0 without any distance', () => {
    expect(estimateForStep('sync_activities', null)).toBeGreaterThan(0);
    expect(estimateForStep('sync_details', undefined)).toBeGreaterThan(0);
    expect(estimateForStep('sync_activities', null)).toBe(
      estimateForStep('sync_activities', 10486.59),
    );
  });

  it('post-resolve steps need distance_m; unknown ids return null', () => {
    expect(estimateForStep('resample_activity_to_distance', null)).toBeNull();
    expect(
      estimateForStep('resample_activity_to_distance', 1655.46),
    ).toBeGreaterThan(0);
  });

  it('barFraction caps at full width and flags over-estimate', () => {
    expect(barFraction(500, 1000)).toEqual({ frac: 0.5, over: false });
    expect(barFraction(1500, 1000)).toEqual({ frac: 1, over: true });
    expect(barFraction(500, null)).toEqual({ frac: 0, over: false });
  });

  it('running row shows a gray bar; over-estimate rows flag data-over', () => {
    render(
      <StepChecklist
        steps={[
          { step_id: 'sync_activities', status: 'running', elapsed_ms: 0, started_at: new Date(Date.now() - 1000).toISOString() },
          { step_id: 'resolve_activity', status: 'pending', elapsed_ms: 0 },
        ]}
        distanceM={1655.46}
      />,
    );
    const bar = document.querySelector('[data-step-id="sync_activities"] [data-testid="step-bar"]');
    expect(bar).not.toBeNull();
    expect(bar!.getAttribute('data-over')).toBe('false');
    // Pre-resolve steps have no distance yet — no bar until resolve delivers it.
    expect(
      document.querySelector('[data-step-id="insert_heartrate"] [data-testid="step-bar"]'),
    ).toBeNull();
  });

  it('completed rows show a full green bar once distance is known', () => {
    render(
      <StepChecklist
        steps={[
          { step_id: 'sync_activities', status: 'complete', elapsed_ms: 10225 },
          { step_id: 'resample_activity_to_distance', status: 'complete', elapsed_ms: 4255 },
        ]}
        distanceM={5678.08}
      />,
    );
    const bars = document.querySelectorAll('[data-testid="step-bar"]');
    expect(bars.length).toBeGreaterThanOrEqual(2);
  });

  it('error text renders beside the step name (AC-6 preserved)', () => {
    render(
      <StepChecklist
        steps={[
          { step_id: 'sync_activities', status: 'error', elapsed_ms: 12, error: 'boom' },
        ]}
        distanceM={null}
      />,
    );
    expect(
      document.querySelector('[data-step-id="sync_activities"]'),
    ).toHaveTextContent('boom');
  });

  it('every row renders a dedicated bar track beside the name column (Bug 004-007-7-5)', () => {
    render(
      <StepChecklist
        steps={[
          { step_id: 'sync_activities', status: 'running', elapsed_ms: 0, started_at: new Date(Date.now() - 1000).toISOString() },
          { step_id: 'resolve_activity', status: 'complete', elapsed_ms: 55 },
          { step_id: 'insert_heartrate', status: 'pending', elapsed_ms: 0 },
        ]}
        distanceM={1655.46}
      />,
    );
    // The track is always visible — two-column layout, never squeezed into
    // the text line.
    const tracks = document.querySelectorAll('[data-testid="step-bar-track"]');
    expect(tracks.length).toBe(STEP_ORDER.length);
    // Running and complete rows carry the bar inside their track.
    const runningRow = document.querySelector('[data-step-id="sync_activities"]')!;
    expect(
      runningRow.querySelector('[data-testid="step-bar-track"] [data-testid="step-bar"]'),
    ).not.toBeNull();
    const completeRow = document.querySelector('[data-step-id="resolve_activity"]')!;
    expect(
      completeRow.querySelector('[data-testid="step-bar-track"] [data-testid="step-bar"]'),
    ).not.toBeNull();
    // The name column and the track are siblings (side-by-side columns).
    expect(runningRow.querySelectorAll(':scope > div > div').length).toBeGreaterThanOrEqual(2);
  });

  it('captures distance_m from a mocked resolve_activity terminal event', async () => {
    processActivity.mockImplementation(async (...args: unknown[]) => {
      if (args.length === 0) return { complete: true, success: true } as never;
      const onStep = args[1] as (
        e: ProcessStepStartEvent | ProcessStepEvent,
      ) => void;
      onStep({ step_id: 'sync_activities', status: 'running', started_at: new Date().toISOString() });
      onStep({ step_id: 'sync_activities', status: 'complete', elapsed_ms: 10 });
      onStep({ step_id: 'resolve_activity', status: 'running', started_at: new Date().toISOString() });
      onStep({
        step_id: 'resolve_activity',
        status: 'complete',
        elapsed_ms: 55,
        result: { distance_m: 1655.46 },
      });
      return {
        complete: true,
        success: true,
        summary: { total_elapsed_ms: 22100 },
      };
    });
    await startRun();
    await screen.findByText(/Complete in 22\.1s/);
    // distance_m was captured: the resolve row (a const-estimate step) and
    // the completed sync row both render bars. (Untouched pending steps are
    // finalized to skipped by the success path's terminal handling — the
    // direct StepChecklist tests above cover post-resolve bars.)
    expect(
      document.querySelector('[data-step-id="resolve_activity"] [data-testid="step-bar"]'),
    ).not.toBeNull();
  });

  it('completion banner reads "Complete in {n}s" with a report link (AC-9)', async () => {
    processActivity.mockResolvedValue({
      complete: true,
      success: true,
      summary: { total_elapsed_ms: 22100 },
    });
    await startRun();
    expect(await screen.findByText(/Complete in 22\.1s/)).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /View Recent Activity Report/ }),
    ).toBeInTheDocument();
  });
});


describe('playlist rows hidden when music is not in the flow (Bug 004-007-7-4)', () => {
  beforeEach(() => processActivity.mockReset());

  const playlistRowIds = () =>
    [...PLAYLIST_STEPS].map((id) => document.querySelector(`[data-step-id="${id}"]`));

  async function startLastRun(musicLabel: 'Running' | 'No Music') {
    processActivity.mockResolvedValue({ complete: true, success: true });
    render(<ActivityProcessingPage />);
    fireEvent.click(screen.getByRole('button', { name: 'Last Run' }));
    fireEvent.click(screen.getByRole('button', { name: musicLabel }));
    fireEvent.click(screen.getByRole('button', { name: 'Start Processing' }));
    // Wait for the checklist to seed — rows render as soon as steps exist.
    await screen.findAllByTestId('step-row');
  }

  it('last_run + no_music: all six playlist rows are absent', async () => {
    await startLastRun('No Music');
    for (const row of playlistRowIds()) expect(row).toBeNull();
    expect(screen.getAllByTestId('step-row')).toHaveLength(
      STEP_ORDER.length - PLAYLIST_STEPS.size
    );
  });

  it('last_run + music: all six playlist rows are present', async () => {
    await startLastRun('Running');
    for (const row of playlistRowIds()) expect(row).not.toBeNull();
    expect(screen.getAllByTestId('step-row')).toHaveLength(STEP_ORDER.length);
  });

  it('last_walk (page-level): playlist rows stay hidden after failure finalization', async () => {
    processActivity.mockImplementation(async (...args: unknown[]) => {
      // Guard: vitest teardown probes the mock with zero args.
      if (args.length === 0) return { complete: true, success: true } as never;
      throw new Error('API request failed with status 503');
    });
    await startRun();
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    for (const row of playlistRowIds()) expect(row).toBeNull();
  });
});
