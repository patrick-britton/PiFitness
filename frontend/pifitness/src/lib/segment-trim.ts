/**
 * Trim-gate math for Create Segment mode (009-003 T08, FR-2/FR-4).
 *
 * Dependency-free pure functions (no React, no DOM, no Chart.js) so the gate
 * clamping, focus windows, and the elevation x-axis derivation are unit-tested
 * rather than eyeballed on a device.
 *
 * Why the x-axis is derived here: the T03 route contract
 * (`GET /api/segments/{activity_id}/route`) returns `path_coords`, `elevations`
 * and the activity's total `distance_m` — there is no per-point distance. The
 * elevation profile therefore walks the returned coordinates and accumulates
 * haversine length, which is why the derivation lives behind a tested helper.
 */

import { metersBetween } from './geo';

/** Default half-width of the start/end gate focus windows, in meters. */
export const GATE_WINDOW_M = 150;

export interface TrimGates {
  /** Trim start in meters (inclusive), 0..maxM. */
  startM: number;
  /** Trim end in meters (inclusive), startM..maxM. */
  endM: number;
}

/** Coerce any input to a whole meter inside `0..maxM` (0 when maxM is unusable). */
export function clampGate(value: number, maxM: number): number {
  const max = Number.isFinite(maxM) && maxM > 0 ? Math.floor(maxM) : 0;
  if (!Number.isFinite(value)) return 0;
  return Math.min(Math.max(Math.round(value), 0), max);
}

/**
 * Normalize a pair of raw gate values against the activity's total distance:
 * both clamped into range, and end >= start (an inverted pair collapses to
 * start == end rather than silently swapping, so the user sees their edit).
 */
export function normalizeGates(startM: number, endM: number, maxM: number): TrimGates {
  const start = clampGate(startM, maxM);
  const end = clampGate(endM, maxM);
  return { startM: start, endM: Math.max(start, end) };
}

/** Length of the trimmed range in meters (never negative). */
export function trimmedMeters(gates: TrimGates): number {
  return Math.max(0, gates.endM - gates.startM);
}

/**
 * Upper gate bound in meters (009-003 T08).
 *
 * The activity's REPORTED distance wins when present: the creation procedure
 * validates `end_m` against `activities.activities.distance_m` and rejects a
 * range that exceeds it, so the gates must never be bounded by a larger derived
 * figure. Only when the reported distance is missing/unusable does the GPS path
 * length become the bound (Bug 009-003-08-1).
 */
export function gateBound(reportedM: number | null | undefined, pathM: number): number {
  if (reportedM != null && Number.isFinite(reportedM) && reportedM > 0) return Math.round(reportedM);
  return Number.isFinite(pathM) && pathM > 0 ? Math.round(pathM) : 0;
}

/**
 * Distance window for a gate focus view: the first/last `windowM` meters of the
 * trimmed range, widened to the whole range when it is shorter than the window
 * (mirrors the legacy Streamlit focus maps, which sliced the first/last 150 m).
 */
export function gateFocusWindow(gates: TrimGates, which: 'start' | 'end', windowM = GATE_WINDOW_M): [number, number] {
  const length = trimmedMeters(gates);
  const span = Math.min(windowM, length);
  if (which === 'start') return [gates.startM, gates.startM + span];
  return [gates.endM - span, gates.endM];
}

/**
 * Cumulative distance (meters) at each coordinate of a `[lon, lat]` path,
 * starting at 0. Output length always equals the input length, so elevations
 * (which are 1:1 aligned with path points) can be plotted against it.
 */
export function cumulativeMeters(coords: number[][]): number[] {
  if (coords.length === 0) return [];
  const out: number[] = [0];
  let total = 0;
  for (let i = 1; i < coords.length; i += 1) {
    total += metersBetween(coords[i - 1], coords[i]);
    out.push(total);
  }
  return out;
}

/**
 * Index range (inclusive bounds) of the points inside a distance window, so a
 * focus map can draw only the path near its gate. Empty when no point falls in
 * the window — the caller then renders the whole path rather than nothing.
 */
export function indexWindow(coords: number[][], fromM: number, toM: number): [number, number] | null {
  const dists = cumulativeMeters(coords);
  let first = -1;
  let last = -1;
  for (let i = 0; i < dists.length; i += 1) {
    if (dists[i] >= fromM && dists[i] <= toM) {
      if (first === -1) first = i;
      last = i;
    }
  }
  return first === -1 ? null : [first, last];
}

/** The plotted point nearest a picked distance (009-003 T39a, AC-26/AC-27). */
export interface NearestPoint {
  /** Index into the route's `coords` (and its cumulative-distance array). */
  index: number;
  /**
   * Cumulative distance of that point in meters, on the SAME `cumulativeMeters()`
   * basis the elevation plot draws its x axis from — never the DB's `distance_mm`.
   * Sub-meter precision; callers displaying it should round to whole meters to
   * match the plot's rounded x values (`ElevationProfile` plots `Math.round`).
   */
  distanceM: number;
}

/**
 * Index of the route point nearest `targetM` along the path, with that point's
 * cumulative distance (009-003 T39a — the shared pick maths behind the
 * map↔elevation link).
 *
 * The distance basis is `cumulativeMeters()` (the very array `ElevationProfile`
 * plots), so the x the pick maths resolves and the x the axis draws can never
 * disagree. Callers should compute the cumulative array ONCE per route and pass
 * it in (`cumulative`) so a hover/tap frame costs one linear scan and no
 * recompute on a Pi/phone; the parameter is ignored when its length does not
 * match `coords` (a stale memo would misalign index → point).
 *
 * Clamping: a target before the first point resolves to index 0 and one past
 * the last point to the final index (cumulative distance is non-decreasing).
 * Empty coords return null.
 */
export function nearestPointAtDistance(
  coords: number[][],
  targetM: number,
  cumulative?: number[],
): NearestPoint | null {
  const dists =
    cumulative && cumulative.length === coords.length ? cumulative : cumulativeMeters(coords);
  if (dists.length === 0) return null;
  const target = Number.isFinite(targetM) ? targetM : 0;
  let best = 0;
  let bestGap = Math.abs(dists[0] - target);
  for (let i = 1; i < dists.length; i += 1) {
    const gap = Math.abs(dists[i] - target);
    if (gap < bestGap) {
      best = i;
      bestGap = gap;
    }
  }
  return { index: best, distanceM: dists[best] };
}

/**
 * Index of the route point nearest a picked `[lng, lat]` (009-003 T39b,
 * AC-26) — the coordinate sibling of `nearestPointAtDistance`, which resolves
 * a distance rather than a position. The map reports where the user pointed;
 * the readout needs the route point at that spot and its elapsed time, so the
 * scan runs over the already-memoized coords, one pick at a time (the caller
 * rAF-throttles hover) — no precomputed index lives on the Pi. Empty coords
 * or a non-finite target return null.
 */
export function nearestIndexAtLngLat(coords: number[][], lngLat: [number, number]): number | null {
  if (coords.length === 0) return null;
  if (!lngLat || !lngLat.every((v) => Number.isFinite(v))) return null;
  let best = 0;
  let bestM = Infinity;
  for (let i = 0; i < coords.length; i += 1) {
    const m = metersBetween(coords[i], lngLat);
    if (m < bestM) {
      bestM = m;
      best = i;
    }
  }
  return best;
}