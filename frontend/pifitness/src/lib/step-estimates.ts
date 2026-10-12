/**
 * Step time estimates for the Activity Processing screen (004-007 T03 / FR-3).
 *
 * Pure, dependency-free lib module (no React, no DOM) — same pattern as
 * `report-format.ts`. Coefficients are ported from the human-fitted
 * `.features/designs_active/time_estimator.py`, calibrated from
 * `local_storage/step_timing_004_007.csv` (3 activities: 1.66 km, 5.68 km,
 * 10.49 km; 10 iterations each; all 690 rows status=complete).
 *
 * Model kinds per step:
 *   const  - time doesn't track distance (sync steps, resolve, *_time
 *            elevation steps which scale with duration/sample count, frechet…).
 *            Central values are per-activity MEANS so step estimates sum to
 *            the expected total.
 *   linear - a + b*km, used where a straight line fits the three means.
 *   interp - piecewise-linear through the three observed means, extended past
 *            the longest with a positive tail slope (flat-then-jump or curved).
 *
 * Playlist steps are exempt from measurement: static 250 ms each.
 */

import type { ProcessStepId } from './types/activity-processing';

type ConstModel = { kind: 'const'; valueMs: number };
type LinearModel = { kind: 'linear'; aMs: number; bMsPerKm: number };
type InterpModel = { kind: 'interp'; pointsKmMs: readonly (readonly [number, number])[]; tailMsPerKm: number };
type StepModel = ConstModel | LinearModel | InterpModel;

/** Static estimate for every playlist step (exempt from measurement). */
export const PLAYLIST_STEP_MS = 250;

/** Playlist steps (exempt): constant 250 ms each. */
const PLAYLIST_STEPS: readonly ProcessStepId[] = [
  'lookup_playlist',
  'insert_history',
  'query_isrc_stats',
  'send_to_spotify',
  'verify_spotify',
  'report_shuffle',
];

/**
 * Fitted per-step models (ms). Source: time_estimator.py MODELS.
 * Re-fit if the pipeline changes.
 */
const MODELS: Record<string, StepModel> = {
  sync_activities: { kind: 'const', valueMs: 10225.4 },
  sync_details: { kind: 'const', valueMs: 3471.5 },
  resolve_activity: { kind: 'const', valueMs: 54.6 },
  insert_heartrate: { kind: 'const', valueMs: 162.5 },
  assign_elevation_reference_time: { kind: 'const', valueMs: 298.5 },
  smooth_elevation_spikes_by_time: { kind: 'const', valueMs: 2030.3 },
  smooth_elevation_python_time: { kind: 'const', valueMs: 1489.8 },
  update_elevation_reference_by_time: { kind: 'const', valueMs: 191.3 },
  resample_activity_to_distance: {
    kind: 'interp',
    pointsKmMs: [[1.655, 1232.8], [5.678, 4254.8], [10.487, 6312.5]],
    tailMsPerKm: 570.5,
  },
  smooth_elevation_spikes_by_distance: { kind: 'linear', aMs: 203.5, bMsPerKm: 151.8 },
  smooth_elevation_python_distance: { kind: 'linear', aMs: 285.3, bMsPerKm: 96.9 },
  smooth_elevation_python_reference: { kind: 'linear', aMs: 227.0, bMsPerKm: 98.7 },
  update_elevation_reference_by_distance: { kind: 'linear', aMs: 171.1, bMsPerKm: 9.1 },
  build_activity_path: { kind: 'const', valueMs: 174.4 },
  segment_match_segments: { kind: 'linear', aMs: 134.6, bMsPerKm: 8.3 },
  segment_pair_generation: {
    kind: 'interp',
    pointsKmMs: [[1.655, 272.1], [5.678, 286.2], [10.487, 840.7]],
    tailMsPerKm: 66.0,
  },
  segment_polygon_match: { kind: 'linear', aMs: 101.8, bMsPerKm: 5.8 },
  segment_mass_confirm_1: {
    kind: 'interp',
    pointsKmMs: [[1.655, 134.2], [5.678, 140.7], [10.487, 235.7]],
    tailMsPerKm: 11.8,
  },
  segment_hausdorff_match: {
    kind: 'interp',
    pointsKmMs: [[1.655, 112.0], [5.678, 128.9], [10.487, 1234.3]],
    tailMsPerKm: 130.4,
  },
  segment_mass_confirm_2: { kind: 'linear', aMs: 110.8, bMsPerKm: 8.8 },
  segment_frechet_match: { kind: 'const', valueMs: 124.3 },
  segment_mass_confirm_3: { kind: 'linear', aMs: 121.2, bMsPerKm: 3.3 },
  segment_update_details: {
    kind: 'interp',
    pointsKmMs: [[1.655, 138.5], [5.678, 144.4], [10.487, 329.2]],
    tailMsPerKm: 22.1,
  },
  // Legacy id kept in STEP_ORDER/STEP_LABELS but no longer emitted by the
  // pipeline: reuse the closest live equivalent so coverage stays total.
  match_segments: { kind: 'linear', aMs: 134.6, bMsPerKm: 8.3 },
};

function modelMs(stepId: string, km: number): number {
  const m = MODELS[stepId];
  if (m.kind === 'const') return m.valueMs;
  if (m.kind === 'linear') return Math.max(m.aMs + m.bMsPerKm * km, 0);
  const pts = m.pointsKmMs;
  if (km <= pts[0][0]) return pts[0][1];
  for (let i = 0; i < pts.length - 1; i++) {
    const [x0, y0] = pts[i];
    const [x1, y1] = pts[i + 1];
    if (km <= x1) return y0 + ((y1 - y0) * (km - x0)) / (x1 - x0);
  }
  const [xLast, yLast] = pts[pts.length - 1];
  return yLast + m.tailMsPerKm * (km - xLast);
}

/**
 * Estimated wall time for one processing step on an activity of
 * `distanceM` metres. `distanceM` must be a finite, non-negative number.
 * Playlist steps always return exactly 250.
 */
export function estimateStepMs(stepId: ProcessStepId, distanceM: number): number {
  if (PLAYLIST_STEPS.includes(stepId)) return PLAYLIST_STEP_MS;
  if (!Number.isFinite(distanceM) || distanceM < 0) {
    throw new Error('distanceM must be a finite, non-negative number of metres');
  }
  const m = MODELS[stepId];
  if (!m) throw new Error(`Unknown step_id ${JSON.stringify(stepId)}`);
  return modelMs(stepId, distanceM / 1000);
}
