/**
 * Date/time display formatting shared by the Segment Management surfaces
 * (009-003 T08).
 *
 * Created because the module formats timestamps on four tables (source
 * activities, candidates, courses, segment header) and the Leaderboards module
 * already carried two private copies. Keeping one implementation means the
 * display format is changed in one place. (T14 note: the leaderboards copies
 * were deliberately NOT merged — they emit a hyphenated `d-mmm-yyyy` with
 * different invalid-input placeholders, so merging would change 009-002's
 * rendered dates on a completed, device-verified surface rather than just
 * dedupe code.)
 *
 * Dependency-free (no React, no DOM) so it is unit-testable.
 */

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** Placeholder for a missing or unparseable timestamp. */
export const NO_DATE = '-';

/** ISO-8601 timestamp → `d-mmm-yyyy`; `NO_DATE` when missing/invalid. */
export function formatIsoDate(iso: string | null | undefined): string {
  const d = toDate(iso);
  if (!d) return NO_DATE;
  return `${d.getDate()} ${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
}

/** ISO-8601 timestamp → `d-mmm-yyyy hh:mm` (local time); `NO_DATE` when invalid. */
export function formatIsoDateTime(iso: string | null | undefined): string {
  const d = toDate(iso);
  if (!d) return NO_DATE;
  const hours = String(d.getHours()).padStart(2, '0');
  const minutes = String(d.getMinutes()).padStart(2, '0');
  return `${formatIsoDate(iso)} ${hours}:${minutes}`;
}

/** Meters → miles with fixed decimals; `NO_DATE`-style dash for non-finite input. */
export function formatMiles(meters: number | null | undefined, decimals = 2): string {
  if (meters == null || !Number.isFinite(meters)) return NO_DATE;
  return (meters / 1609.344).toFixed(decimals);
}

/**
 * Seconds → elapsed-duration text (009-003 T39b, OQ-6): `h:mm:ss` at 60
 * minutes or more, otherwise `m:ss` (e.g. `9:05`, `59:59`, `1:05:03`).
 *
 * This is the ONE elapsed-time formatter — the pick readout uses it now and
 * the map marker labels (T40) reuse it, so the app can never grow a second
 * copy with a different boundary. Missing/negative/non-finite input dashes
 * like every other formatter in this module.
 */
export function formatElapsedDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return NO_DATE;
  const total = Math.floor(seconds);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const mm = String(m).padStart(2, '0');
  const ss = String(s).padStart(2, '0');
  return h > 0 ? `${h}:${mm}:${ss}` : `${m}:${ss}`;
}

function toDate(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}