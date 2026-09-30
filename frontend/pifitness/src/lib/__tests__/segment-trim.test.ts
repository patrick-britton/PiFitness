/**
 * Trim-gate math tests (009-003 T08).
 *
 * These functions decide what the maps and elevation profile show and what
 * meters the creation call sends, and jsdom has no map/WebGL to check them in a
 * browser — so the arithmetic is asserted directly.
 */

import { describe, expect, it } from 'vitest';
import {
  GATE_WINDOW_M,
  clampGate,
  cumulativeMeters,
  gateBound,
  gateFocusWindow,
  indexWindow,
  nearestPointAtDistance,
  normalizeGates,
  trimmedMeters,
} from '../segment-trim';

/** ~111.19 m per 0.001° of latitude at the equator. */
const STEP = 0.001;

/** A straight north-running path of `n` points, ~111 m apart. */
function straightPath(n: number): number[][] {
  return Array.from({ length: n }, (_, i) => [0, i * STEP]);
}

describe('clampGate', () => {
  it('clamps into 0..maxM and rounds to whole meters', () => {
    expect(clampGate(-50, 1000)).toBe(0);
    expect(clampGate(1500, 1000)).toBe(1000);
    expect(clampGate(12.6, 1000)).toBe(13);
  });

  it('returns 0 for non-finite values or an unusable max', () => {
    expect(clampGate(Number.NaN, 1000)).toBe(0);
    expect(clampGate(100, 0)).toBe(0);
    expect(clampGate(100, Number.NaN)).toBe(0);
  });
});

describe('normalizeGates', () => {
  it('keeps an in-range pair unchanged', () => {
    expect(normalizeGates(100, 900, 1000)).toEqual({ startM: 100, endM: 900 });
  });

  it('collapses an inverted pair to start === end rather than swapping', () => {
    expect(normalizeGates(800, 200, 1000)).toEqual({ startM: 800, endM: 800 });
  });

  it('clamps both gates to the activity distance', () => {
    expect(normalizeGates(-10, 5000, 1052)).toEqual({ startM: 0, endM: 1052 });
  });
});

describe('trimmedMeters', () => {
  it('measures the selected range', () => {
    expect(trimmedMeters({ startM: 200, endM: 1200 })).toBe(1000);
    expect(trimmedMeters({ startM: 500, endM: 500 })).toBe(0);
  });
});

describe('gateBound', () => {
  it("prefers the activity's reported distance over a longer derived path", () => {
    // The creation procedure rejects end_m above the reported distance, so a
    // longer haversine path must not raise the gate ceiling (Bug 009-003-08-1).
    expect(gateBound(1111, 1111.95)).toBe(1111);
  });

  it('uses the derived path length when no distance was reported', () => {
    expect(gateBound(null, 1111.95)).toBe(1112);
    expect(gateBound(0, 1111.95)).toBe(1112);
    expect(gateBound(Number.NaN, 500.4)).toBe(500);
  });

  it('returns 0 when neither value is usable', () => {
    expect(gateBound(null, 0)).toBe(0);
    expect(gateBound(undefined, Number.NaN)).toBe(0);
  });
});

describe('gateFocusWindow', () => {
  it('uses a 150 m window at each end for a long range', () => {
    const gates = { startM: 100, endM: 2000 };
    expect(gateFocusWindow(gates, 'start')).toEqual([100, 100 + GATE_WINDOW_M]);
    expect(gateFocusWindow(gates, 'end')).toEqual([2000 - GATE_WINDOW_M, 2000]);
  });

  it('widens to the whole range when it is shorter than the window', () => {
    const gates = { startM: 40, endM: 90 };
    expect(gateFocusWindow(gates, 'start')).toEqual([40, 90]);
    expect(gateFocusWindow(gates, 'end')).toEqual([40, 90]);
  });
});

describe('cumulativeMeters', () => {
  it('starts at 0 and is 1:1 with the coordinate list', () => {
    const coords = straightPath(5);
    const dists = cumulativeMeters(coords);
    expect(dists).toHaveLength(coords.length);
    expect(dists[0]).toBe(0);
  });

  it('accumulates roughly 111 m per 0.001 degrees of latitude', () => {
    const dists = cumulativeMeters(straightPath(3));
    expect(dists[1]).toBeGreaterThan(110);
    expect(dists[1]).toBeLessThan(112);
    expect(dists[2]).toBeCloseTo(dists[1] * 2, 0);
  });

  it('ignores a non-finite coordinate instead of producing NaN', () => {
    const dists = cumulativeMeters([[0, 0], [Number.NaN, 0], [0, STEP]]);
    expect(dists.every((d) => Number.isFinite(d))).toBe(true);
  });

  it('returns an empty list for no coordinates', () => {
    expect(cumulativeMeters([])).toEqual([]);
  });
});

describe('indexWindow', () => {
  it('returns the inclusive index range inside the distance window', () => {
    const coords = straightPath(5); // 0, ~111, ~222, ~333, ~445 m
    const window = indexWindow(coords, 100, 250);
    expect(window).toEqual([1, 2]);
  });

  it('returns null when no point falls in the window', () => {
    expect(indexWindow(straightPath(5), 5000, 6000)).toBeNull();
  });

  it('includes boundary points exactly at the window edges', () => {
    const coords = straightPath(3);
    const dists = cumulativeMeters(coords);
    expect(indexWindow(coords, dists[1], dists[1])).toEqual([1, 1]);
  });
});

describe('nearestPointAtDistance', () => {
  it('returns that index for a distance that lands exactly on a point', () => {
    const coords = straightPath(5);
    const dists = cumulativeMeters(coords);
    expect(nearestPointAtDistance(coords, dists[3])).toEqual({ index: 3, distanceM: dists[3] });
  });

  it('returns the nearer of the two points surrounding an in-between distance', () => {
    const coords = straightPath(5); // 0, ~111, ~222, ~333, ~445 m
    // 150 m is 39 m past point 1 (~111) but 72 m short of point 2 (~222).
    expect(nearestPointAtDistance(coords, 150)).toEqual({
      index: 1,
      distanceM: cumulativeMeters(coords)[1],
    });
    // 190 m flips it: 32 m short of point 2 (~222) vs 79 m past point 1.
    expect(nearestPointAtDistance(coords, 190)?.index).toBe(2);
  });

  it('clamps a target before the first point to index 0 and past the last to the final index', () => {
    const coords = straightPath(4); // last point ~333 m
    expect(nearestPointAtDistance(coords, -100)).toEqual({ index: 0, distanceM: 0 });
    expect(nearestPointAtDistance(coords, 5000)).toEqual({
      index: 3,
      distanceM: cumulativeMeters(coords)[3],
    });
  });

  it('returns null for empty coords', () => {
    expect(nearestPointAtDistance([], 100)).toBeNull();
  });

  it('resolves a 3,228 m fixture to the distance ElevationProfile ends its axis at (AC-18 convention)', () => {
    // Same fixture builder as ElevationProfile.test.tsx: a route whose last
    // plotted point rounds to exactly 3,228 m, so the axis max is 3,228.
    const mPerStep = cumulativeMeters([[0, 0], [0, 0.001]])[1];
    const coords = [[0, 0], [0, (3228 / mPerStep) * 0.001]];
    const pick = nearestPointAtDistance(coords, 3228);
    expect(pick).not.toBeNull();
    expect(pick!.index).toBe(1);
    // The plot draws x as Math.round(cumulative) and sets scales.x.max to the
    // last such value — the pick must land on that same distance.
    expect(Math.round(pick!.distanceM)).toBe(3228);
  });

  it('uses a caller-supplied cumulative array and ignores one whose length disagrees with coords', () => {
    const coords = straightPath(5);
    const dists = cumulativeMeters(coords);
    // Same answer as recomputing internally…
    expect(nearestPointAtDistance(coords, 150, dists)).toEqual(
      nearestPointAtDistance(coords, 150),
    );
    // …and a stale memo (length mismatch) must not misalign index -> point.
    expect(nearestPointAtDistance(coords, 150, dists.slice(0, 2))).toEqual(
      nearestPointAtDistance(coords, 150),
    );
  });
});