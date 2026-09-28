/**
 * Display-format tests (009-003 T08).
 *
 * Four tables in this module render timestamps and distances, so the shared
 * formatters are asserted once here instead of per surface. Fixed local-time
 * inputs (constructed with explicit components) keep the clock out of the test.
 */

import { describe, expect, it } from 'vitest';
import { NO_DATE, formatIsoDate, formatIsoDateTime, formatMiles } from '../date-format';

describe('formatIsoDate', () => {
  it('renders d-mmm-yyyy', () => {
    const iso = new Date(2026, 8, 18, 7, 5).toISOString(); // 18 Sep 2026
    expect(formatIsoDate(iso)).toBe('18 Sep 2026');
  });

  it('dashes a missing, empty, or invalid timestamp', () => {
    expect(formatIsoDate(null)).toBe(NO_DATE);
    expect(formatIsoDate(undefined)).toBe(NO_DATE);
    expect(formatIsoDate('')).toBe(NO_DATE);
    expect(formatIsoDate('not-a-date')).toBe(NO_DATE);
  });
});

describe('formatIsoDateTime', () => {
  it('appends zero-padded local hh:mm', () => {
    const iso = new Date(2026, 0, 3, 9, 7).toISOString();
    expect(formatIsoDateTime(iso)).toBe('3 Jan 2026 09:07');
  });

  it('dashes an invalid timestamp', () => {
    expect(formatIsoDateTime('nope')).toBe(NO_DATE);
  });
});

describe('formatMiles', () => {
  it('converts meters to miles with two decimals by default', () => {
    expect(formatMiles(1609.344)).toBe('1.00');
    expect(formatMiles(804.672, 3)).toBe('0.500');
  });

  it('dashes a missing or non-finite distance', () => {
    expect(formatMiles(null)).toBe(NO_DATE);
    expect(formatMiles(Number.NaN)).toBe(NO_DATE);
  });

  it('formats zero as a real value, not a dash', () => {
    expect(formatMiles(0)).toBe('0.00');
  });
});