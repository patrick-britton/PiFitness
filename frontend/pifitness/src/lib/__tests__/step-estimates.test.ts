/**
 * Step-estimate tests (004-007 T03).
 *
 * Pins the ported time_estimator.py coefficients: every STEP_ORDER id returns
 * a finite positive estimate at short/medium/long distances, playlist ids
 * return exactly 250, sync steps are distance-independent constants, and
 * fitted values track the CSV per-activity means.
 */

import { describe, expect, it } from 'vitest';
import { estimateStepMs, PLAYLIST_STEP_MS } from '../step-estimates';
import { STEP_ORDER } from '../types/activity-processing';

const SHORT_M = 1655.46;
const MEDIUM_M = 5678.08;
const LONG_M = 10486.59;

const PLAYLIST_IDS = new Set([
  'lookup_playlist',
  'insert_history',
  'query_isrc_stats',
  'send_to_spotify',
  'verify_spotify',
  'report_shuffle',
]);

describe('estimateStepMs (T03 verify cases)', () => {
  it('covers every STEP_ORDER id with a finite positive estimate', () => {
    for (const step of STEP_ORDER) {
      for (const d of [SHORT_M, MEDIUM_M, LONG_M]) {
        const v = estimateStepMs(step, d);
        expect(Number.isFinite(v), `${step}@${d}`).toBe(true);
        expect(v, `${step}@${d}`).toBeGreaterThan(0);
      }
    }
  });

  it('returns exactly 250 ms for every playlist step', () => {
    for (const step of STEP_ORDER) {
      if (!PLAYLIST_IDS.has(step)) continue;
      expect(estimateStepMs(step, SHORT_M)).toBe(PLAYLIST_STEP_MS);
      expect(estimateStepMs(step, LONG_M)).toBe(PLAYLIST_STEP_MS);
    }
  });

  it('keeps sync steps distance-independent (OQ-6 static constants)', () => {
    expect(estimateStepMs('sync_activities', SHORT_M)).toBe(
      estimateStepMs('sync_activities', LONG_M),
    );
    expect(estimateStepMs('sync_details', SHORT_M)).toBe(
      estimateStepMs('sync_details', LONG_M),
    );
  });

  it('tracks the CSV means at the three calibration distances (±25%)', () => {
    // Spot checks against time_estimator.py per-activity means (ms).
    const cases: Array<[string, number, number]> = [
      ['sync_activities', SHORT_M, 10225.4],
      ['sync_details', MEDIUM_M, 3471.5],
      ['resolve_activity', LONG_M, 54.6],
      ['resample_activity_to_distance', MEDIUM_M, 4254.8],
      ['resample_activity_to_distance', LONG_M, 6312.5],
      ['smooth_elevation_spikes_by_distance', LONG_M, 203.5 + 151.8 * 10.48659],
      ['segment_hausdorff_match', LONG_M, 1234.3],
      ['segment_frechet_match', SHORT_M, 124.3],
    ];
    for (const [step, d, mean] of cases) {
      const v = estimateStepMs(step as (typeof STEP_ORDER)[number], d);
      expect(Math.abs(v - mean) / mean, `${step}@${d}`).toBeLessThan(0.25);
    }
  });
});
