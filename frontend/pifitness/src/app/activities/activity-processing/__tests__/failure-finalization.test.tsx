/**
 * 004-005 T04 / AC-3 — a failed or aborted `/process` run must never strand
 * steps in `pending`: the failing/interrupted step renders red with its
 * message, unstarted steps reach a terminal (`skipped`) state, and the page
 * shows an error banner.
 *
 * Run from frontend/pifitness:
 *   npx vitest run src/app/activities/activity-processing
 */
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { API } from '@/lib/api-client';
import ActivityProcessingPage, { finalizeFailedSteps } from '../page';
import {
  ProcessStepResult,
  ProcessStepStartEvent,
  ProcessStepEvent,
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
    expect(screen.getAllByTestId('step-row')).toHaveLength(STEP_ORDER.length);
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
    expect(statusOf('lookup_playlist')).toBe('skipped');
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
