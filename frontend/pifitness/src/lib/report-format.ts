/**
 * Report delta formatting for the Recent Activity page (004-006 T07 / AC-7).
 *
 * Card prior/best deltas use absolute formatting with correct faster/slower
 * words and NEVER negative seconds. Deltas arrive reference-minus-current
 * (vw_report_segment_efforts: prior/best elapsed minus this effort's elapsed),
 * so positive means this effort was FASTER:
 *   - |delta| < 1.0 s  → 2 decimals ("0.58s faster", "0.18s slower")
 *   - |delta| < 60.0 s → whole seconds ("10s slower", "59s faster")
 *   - |delta| >= 60.0 s → m:ss ("1:01 slower", "21:37 faster")
 * Null → "—". Zero → "0.00s".
 *
 * Pure and dependency-free (no React, no DOM) so the cases are unit-pinned
 * without a device — same pattern as `effort-buckets.ts`.
 */

/** Format an absolute delta in seconds as m:ss (e.g. 75 → "1:15"). */
export function formatAbsMSS(delta: number | null): string {
  if (delta == null) return '—';
  const total = Math.round(Math.abs(delta));
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return `${minutes}:${String(seconds).padStart(2, '0')}`;
}

/**
 * Format a card delta (AC-7): absolute value, thresholded precision, with a
 * faster/slower direction word. `noun` lets the info-bar variant reuse the
 * m:ss branch ("than prior" / "than best").
 */
export function formatCardDelta(delta: number | null, noun?: string): string {
  if (delta == null) return noun ? `— ${noun}` : '—';
  // A tie is neither faster nor slower — never label zero with a direction.
  if (delta === 0) return noun ? `0.00s ${noun}` : '0.00s';
  const abs = Math.abs(delta);
  if (abs >= 60) {
    const mss = formatAbsMSS(delta);
    const label = delta > 0 ? 'faster' : 'slower';
    return noun ? `${mss} ${label} ${noun}` : `${mss} ${label}`;
  }
  const label = delta > 0 ? 'faster' : 'slower';
  if (abs < 1) {
    return noun ? `${abs.toFixed(2)}s ${label} ${noun}` : `${abs.toFixed(2)}s ${label}`;
  }
  const whole = Math.round(abs);
  return noun ? `${whole}s ${label} ${noun}` : `${whole}s ${label}`;
}

/** Count header: `{n} segment{s}:` over courses + segments (AC-7). */
export function formatSegmentCount(count: number): string {
  return `${count} segment${count === 1 ? '' : 's'}:`;
}
