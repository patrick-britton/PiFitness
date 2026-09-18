/**
 * Bar geometry for the Leaderboards page (009-009 T14).
 *
 * Dependency-free pure math (no React, no DOM, no CSS) shared by the page's
 * `ProgressBar`, the table's `DeltaBar`, and the `verify-bars.mjs` harness —
 * so the harness exercises production math, not a copy.
 */

/** Zero-based bar scale: the value's share of the filtered set's MAX. */
export function scale(value: number, max: number): number {
  if (!Number.isFinite(value) || !Number.isFinite(max) || max <= 0) return 100;
  const clamped = Math.min(Math.max(value, 0), max);
  return Math.round((clamped / max) * 100);
}

/** Signed delta of a row value vs the baseline value; null when unrecorded. */
export function deltaOf(value: number | null, baseline: number | null): number | null {
  if (value == null || baseline == null) return null;
  if (!Number.isFinite(value) || !Number.isFinite(baseline)) return null;
  return value - baseline;
}

export interface DeltaGeometry {
  /** Bar width as % of one half-track (0..50); 0 for baseline/unrecorded. */
  width: number;
  /** Which side of the centered 0 the bar extends toward. */
  side: 'left' | 'right';
}

/** Diverging-bar geometry: |delta| scaled against the column's max |delta|. */
export function deltaGeometry(delta: number | null, maxAbs: number): DeltaGeometry {
  const width = delta != null && maxAbs > 0 ? Math.min(Math.abs(delta) / maxAbs, 1) * 50 : 0;
  return { width, side: (delta ?? 0) >= 0 ? 'right' : 'left' };
}
