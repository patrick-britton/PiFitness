/**
 * volleyballAxis unit tests (006-003 T08, AC-5).
 *
 * yMax = max(21, sr, opp); axisLength = max(21 + min(sr, opp), pointCount).
 * Worked examples from the spec: 0-0→21, 11-0→21, 11-5→26, 21-23→44,
 * y = 23 at 21-23, 25-23→48 slots.
 */

import { describe, expect, it } from 'vitest';
import { volleyballAxis } from '../VolleyballScoreChart';

describe('volleyballAxis (006-003, AC-5)', () => {
  // VERIFIED: Unit — volleyballAxis computes correct yMax and axisLength across standard and deuce scores
  it('reproduces the spec worked examples', () => {
    // [sr, opp, points, expected yMax, expected axisLength]
    const cases: [number, number, number, number, number][] = [
      [0, 0, 0, 21, 21],
      [11, 0, 11, 21, 21],
      [11, 5, 16, 21, 26],
      [21, 23, 44, 23, 44],
      [25, 23, 48, 25, 48],
    ];
    for (const [sr, opp, points, yMax, axisLength] of cases) {
      expect(volleyballAxis(sr, opp, points)).toEqual({ yMax, axisLength });
    }
  });

  // VERIFIED: Unit — volleyballAxis extends axisLength when pointCount exceeds minimum
  it('points-count guard extends the axis past the score minimum', () => {
    expect(volleyballAxis(3, 3, 30)).toEqual({ yMax: 21, axisLength: 30 });
  });

  // VERIFIED: Unit — volleyballAxis maintains minimum floor of 21 slots
  it('never shrinks below 21 slots even for a single point', () => {
    expect(volleyballAxis(1, 0, 1)).toEqual({ yMax: 21, axisLength: 21 });
  });
});
