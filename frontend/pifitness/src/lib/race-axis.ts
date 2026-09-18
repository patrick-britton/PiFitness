/**
 * Race-view x-axis math for the Leaderboards race visualisation (009-009 T16).
 *
 * Dependency-free pure functions (no React, no DOM, no Chart.js) so the
 * per-racer marker remap can be asserted by `scripts/verify-bars.mjs` — the
 * remap is the risky half of the change and a device is not a test oracle.
 *
 * Bug 009-009-T16-1: the telemetry charts plotted `elapsed_s` on x while the
 * play markers were matched as `|x - raceClockSeconds| <= 1.5`. The axis is now
 * the racer's own DISTANCE in miles, so the marker must be matched in the same
 * unit: convert the shared race-clock time into each racer's distance at that
 * time (own series, interpolated) and compare in miles with a tolerance equal
 * to ±1.5 s of that racer's own mean pace — the same slop the time axis had.
 */

/** Meters per statute mile (exact). */
export const METERS_PER_MILE = 1609.344;

/** Race-clock slop, in seconds, that used to select the highlighted sample. */
export const MARKER_TOLERANCE_S = 1.5;

/** Series distance (meters) → x-axis units (miles). Non-finite → 0. */
export function milesOf(distanceM: number): number {
  return Number.isFinite(distanceM) ? distanceM / METERS_PER_MILE : 0;
}

/**
 * One racer's distance in MILES at shared race-clock time `t` seconds, linearly
 * interpolated between the two surrounding own-series samples (times/dists are
 * ordered by own elapsed second). Clamps outside the series, so a racer who has
 * already finished holds their finish distance and a racer not yet started
 * holds their start distance.
 */
export function distanceAtTimeMiles(times: number[], dists: number[], t: number): number {
  const n = Math.min(times.length, dists.length);
  if (n === 0) return 0;
  if (!Number.isFinite(t) || t <= times[0]) return milesOf(dists[0]);
  if (t >= times[n - 1]) return milesOf(dists[n - 1]);
  let lo = 0;
  let hi = n - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (times[mid] <= t) lo = mid;
    else hi = mid;
  }
  const span = times[hi] - times[lo];
  const frac = span > 0 ? (t - times[lo]) / span : 0;
  return milesOf(dists[lo] + (dists[hi] - dists[lo]) * frac);
}

/**
 * Marker match tolerance in MILES for one racer: `±MARKER_TOLERANCE_S` of that
 * racer's own mean pace. Scales with speed, so a fast racer's samples (spread
 * further apart on the distance axis) are matched as reliably as a slow one's.
 * Degenerate series (fewer than 2 samples / zero duration) → 0.
 */
export function markerToleranceMiles(times: number[], dists: number[]): number {
  const n = Math.min(times.length, dists.length);
  if (n < 2) return 0;
  const dur = times[n - 1] - times[0];
  if (!(dur > 0)) return 0;
  const speed = (milesOf(dists[n - 1]) - milesOf(dists[0])) / dur; // mi per own second
  return speed > 0 ? MARKER_TOLERANCE_S * speed : 0;
}
