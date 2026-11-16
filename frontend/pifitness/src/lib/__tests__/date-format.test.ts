/**
 * Display-format tests (009-003 T08).
 *
 * Four tables in this module render timestamps and distances, so the shared
 * formatters are asserted once here instead of per surface. Fixed local-time
 * inputs (constructed with explicit components) keep the clock out of the test.
 */

import { describe, expect, it } from 'vitest';
import { NO_DATE, formatElapsedDuration, formatIsoDate, formatIsoDateTime, formatMiles } from '../date-format';

describe('formatIsoDate', () => {
  // VERIFIED: Unit — formatIsoDate renders d-mmm-yyyy
  it('renders d-mmm-yyyy', () => {
    const iso = new Date(2026, 8, 18, 7, 5).toISOString(); // 18 Sep 2026
    expect(formatIsoDate(iso)).toBe('18 Sep 2026');
  });

  // VERIFIED: Unit — formatIsoDate dashes a missing, empty, or invalid timestamp
  it('dashes a missing, empty, or invalid timestamp', () => {
    expect(formatIsoDate(null)).toBe(NO_DATE);
    expect(formatIsoDate(undefined)).toBe(NO_DATE);
    expect(formatIsoDate('')).toBe(NO_DATE);
    expect(formatIsoDate('not-a-date')).toBe(NO_DATE);
  });
});

describe('formatIsoDateTime', () => {
  // VERIFIED: Unit — formatIsoDateTime appends zero-padded local hh:mm
  it('appends zero-padded local hh:mm', () => {
    const iso = new Date(2026, 0, 3, 9, 7).toISOString();
    expect(formatIsoDateTime(iso)).toBe('3 Jan 2026 09:07');
  });

  // VERIFIED: Unit — formatIsoDateTime dashes an invalid timestamp
  it('dashes an invalid timestamp', () => {
    expect(formatIsoDateTime('nope')).toBe(NO_DATE);
  });
});

describe('formatMiles', () => {
  // VERIFIED: Unit — formatMiles converts meters to miles with configurable decimals (default two)
  it('converts meters to miles with two decimals by default', () => {
    expect(formatMiles(1609.344)).toBe('1.00');
    expect(formatMiles(804.672, 3)).toBe('0.500');
  });

  // VERIFIED: Unit — formatMiles dashes a missing or non-finite distance
  it('dashes a missing or non-finite distance', () => {
    expect(formatMiles(null)).toBe(NO_DATE);
    expect(formatMiles(Number.NaN)).toBe(NO_DATE);
  });

  // VERIFIED: Unit — formatMiles formats zero as a real value, not a dash
  it('formats zero as a real value, not a dash', () => {
    expect(formatMiles(0)).toBe('0.00');
  });
});

describe('formatElapsedDuration', () => {
  // VERIFIED: Unit — formatElapsedDuration renders m:ss below one hour
  it('renders m:ss below one hour', () => {
    expect(formatElapsedDuration(0)).toBe('0:00');
    expect(formatElapsedDuration(65)).toBe('1:05');
  });

  // VERIFIED: Unit — formatElapsedDuration renders h:mm:ss at sixty minutes and above (hour boundary)
  it('renders h:mm:ss at sixty minutes and above', () => {
    expect(formatElapsedDuration(3599)).toBe('59:59');
    expect(formatElapsedDuration(3600)).toBe('1:00:00');
    expect(formatElapsedDuration(3903)).toBe('1:05:03');
  });

  // VERIFIED: Unit — formatElapsedDuration dashes missing, negative, or non-finite input
  it('dashes missing, negative, or non-finite input', () => {
    expect(formatElapsedDuration(null)).toBe(NO_DATE);
    expect(formatElapsedDuration(undefined)).toBe(NO_DATE);
    expect(formatElapsedDuration(-1)).toBe(NO_DATE);
    expect(formatElapsedDuration(Number.NaN)).toBe(NO_DATE);
  });
});