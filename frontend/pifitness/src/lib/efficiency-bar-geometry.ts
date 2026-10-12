/**
 * Efficiency bar geometry (004-007 T04).
 *
 * Pure, dependency-free geometry for one MRAV-vs-PAV row — same pattern as
 * `bar-geometry.ts`: unit-testable without a canvas. The axis is centered on
 * the most-recent value (MRAV) with bounds MRAV ± 1.5·|MRAV−PAV|; the bar
 * spans exactly MRAV→PAV with the lower value labeled at its left end and
 * the higher at its right end.
 */

export interface EfficiencyBarGeometry {
  /** Axis domain (independent per row). */
  min: number;
  max: number;
  /** Fractional span [start, end] of the MRAV→PAV bar within the axis. */
  spanStart: number;
  spanEnd: number;
  /** True when MRAV equals PAV — no bar is drawn. */
  noChange: boolean;
  /** True when higher-is-better is inverted for the metric (lower-better). */
  improved: boolean;
}

/**
 * Compute one row's geometry. `higherBetter` is true for Cadence and Stride
 * Length, false for GCT and Vert Ratio. Returns null when either value is
 * missing (caller falls back to text-only rows, AC-3).
 */
export function efficiencyBarGeometry(
  current: number | null | undefined,
  prior: number | null | undefined,
  higherBetter: boolean,
): EfficiencyBarGeometry | null {
  if (current == null || prior == null) return null;
  if (!Number.isFinite(current) || !Number.isFinite(prior)) return null;
  if (current === prior) {
    return { min: current, max: current, spanStart: 0, spanEnd: 0, noChange: true, improved: true };
  }
  const half = 1.5 * Math.abs(current - prior);
  const min = current - half;
  const max = current + half;
  const lo = Math.min(current, prior);
  const hi = Math.max(current, prior);
  const improved = higherBetter ? current > prior : current < prior;
  return {
    min,
    max,
    spanStart: (lo - min) / (max - min),
    spanEnd: (hi - min) / (max - min),
    noChange: false,
    improved,
  };
}
