/**
 * Report delta formatter tests (004-006 T07, AC-7).
 *
 * Pins the absolute-value formatting used by the Recent Activity course/segment
 * cards: <1.0 s → 2 decimals, <60.0 s → whole seconds, ≥60.0 s → m:ss, with the
 * correct faster/slower word and never a negative seconds value.
 */

import { describe, expect, it } from 'vitest';
import { formatAbsMSS, formatCardDelta, formatSegmentCount } from '../report-format';

describe('formatAbsMSS', () => {
  it('renders m:ss with zero-padded seconds', () => {
    expect(formatAbsMSS(61)).toBe('1:01');
    expect(formatAbsMSS(1297)).toBe('21:37');
    expect(formatAbsMSS(59)).toBe('0:59');
  });

  it('takes the absolute value of a negative delta', () => {
    expect(formatAbsMSS(-61)).toBe('1:01');
  });

  it('dashes a null delta', () => {
    expect(formatAbsMSS(null)).toBe('—');
  });
});

describe('formatCardDelta (AC-7 verify cases)', () => {
  it('renders sub-second deltas with two decimals', () => {
    expect(formatCardDelta(0.58)).toBe('0.58s faster');
    expect(formatCardDelta(-0.18)).toBe('0.18s slower');
  });

  it('renders whole seconds below 60.0 s', () => {
    expect(formatCardDelta(10)).toBe('10s faster');
    expect(formatCardDelta(-59)).toBe('59s slower');
  });

  it('renders m:ss at 60.0 s and above', () => {
    expect(formatCardDelta(61)).toBe('1:01 faster');
    expect(formatCardDelta(-1297)).toBe('21:37 slower');
    expect(formatCardDelta(60)).toBe('1:00 faster');
  });

  it('never shows negative seconds (absolute value only)', () => {
    for (const delta of [-0.58, -10, -61, -1297]) {
      expect(formatCardDelta(delta)).not.toMatch(/-/);
    }
  });

  it('labels a zero delta as a tie, with no direction word', () => {
    expect(formatCardDelta(0)).toBe('0.00s');
    expect(formatCardDelta(0)).not.toMatch(/faster|slower/);
  });

  it('dashes a missing delta', () => {
    expect(formatCardDelta(null)).toBe('—');
  });

  it('appends the noun form when provided', () => {
    expect(formatCardDelta(1297, 'than prior')).toBe('21:37 faster than prior');
    expect(formatCardDelta(null, 'than best')).toBe('— than best');
  });

  it('labels reference-minus-current deltas correctly (Bug 004-007-7-1)', () => {
    // View emits reference elapsed minus current elapsed: positive means
    // this effort was FASTER (reference took longer).
    expect(formatCardDelta(42.58, 'than prior')).toBe('43s faster than prior');
    expect(formatCardDelta(-42.58, 'than prior')).toBe('43s slower than prior');
  });
});

describe('formatSegmentCount', () => {
  it('pluralizes the header', () => {
    expect(formatSegmentCount(3)).toBe('3 segments:');
    expect(formatSegmentCount(0)).toBe('0 segments:');
  });

  it('keeps the singular form for one match', () => {
    expect(formatSegmentCount(1)).toBe('1 segment:');
  });
});
