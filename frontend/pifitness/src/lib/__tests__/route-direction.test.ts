/**
 * Route-direction helper tests (009-010 T02, FR-3).
 *
 * Pure math + pixel masks — asserted directly; jsdom has no map to check
 * glyph orientation on. `STEP` reuses the segment-trim convention (~111 m
 * per 0.001° latitude at the equator).
 */

import { describe, expect, it } from 'vitest';
import {
  arrowIconMask,
  cssColorToRgba,
  DEFAULT_ARROW_MIN_SEPARATION_M,
  dedupeArrows,
  directionArrows,
  headingBetween,
} from '../route-direction';

/** ~111.19 m per 0.001° of latitude at the equator. */
const STEP = 0.001;

/** A straight north-running path of `n` points, ~111 m apart. */
function northPath(n: number): number[][] {
  return Array.from({ length: n }, (_, i) => [0, i * STEP]);
}

describe('headingBetween', () => {
  it('reports cardinal headings clockwise from north', () => {
    expect(headingBetween([0, 0], [0, 1])).toBeCloseTo(0, 5);
    expect(headingBetween([0, 0], [1, 0])).toBeCloseTo(90, 5);
    expect(headingBetween([0, 0], [0, -1])).toBeCloseTo(180, 5);
    expect(headingBetween([0, 0], [-1, 0])).toBeCloseTo(270, 5);
  });

  it('wraps a just-west-of-north heading near 360, not near 0', () => {
    const b = headingBetween([0, 0], [-0.0001, 1]);
    expect(b).toBeGreaterThan(359);
    expect(b).toBeLessThanOrEqual(360);
  });

  it('returns 0 for degenerate input instead of NaN', () => {
    expect(headingBetween([0, 0], [0, 0])).toBe(0);
    expect(headingBetween([0, 0], [Number.NaN, 0])).toBe(0);
    expect(headingBetween([], [0, 0])).toBe(0);
  });
});

describe('directionArrows', () => {
  it('honours spacing within tolerance on a straight path', () => {
    // 6 points ≈ 556 m → arrows at 25/75/…/525 m: 11 glyphs.
    const arrows = directionArrows(northPath(6), 50);
    expect(arrows).toHaveLength(11);
    for (const a of arrows) {
      expect(a.bearing).toBeCloseTo(0, 0);
      expect(a.lngLat[0]).toBeCloseTo(0, 6);
    }
    // Positions advance monotonically north.
    for (let i = 1; i < arrows.length; i += 1) {
      expect(arrows[i].lngLat[1]).toBeGreaterThan(arrows[i - 1].lngLat[1]);
    }
  });

  it('tracks a right-angle turn: north bearings then east bearings', () => {
    const coords = [
      [0, 0],
      [0, STEP],
      [0, 2 * STEP],
      [STEP, 2 * STEP],
      [2 * STEP, 2 * STEP],
    ];
    const arrows = directionArrows(coords, 50, { windowM: 5 });
    expect(arrows.length).toBeGreaterThan(2);
    expect(arrows[0].bearing).toBeCloseTo(0, 0);
    expect(arrows[arrows.length - 1].bearing).toBeCloseTo(90, 0);
  });

  it('yields nothing for degenerate input', () => {
    expect(directionArrows([], 50)).toEqual([]);
    expect(directionArrows([[0, 0]], 50)).toEqual([]);
    expect(directionArrows(northPath(3), 0)).toEqual([]);
    expect(directionArrows(northPath(3), -10)).toEqual([]);
    expect(directionArrows(northPath(3), Number.NaN)).toEqual([]);
    expect(
      directionArrows([[0, 0], [0, 0], [0, 0]], 50),
    ).toEqual([]);
    expect(
      directionArrows([[0, 0], [Number.NaN, 0]], 50),
    ).toEqual([]);
  });
});

describe('arrowIconMask', () => {
  it('builds symmetric masks with an opaque centre and transparent corners', () => {
    for (const shape of ['triangle', 'chevron'] as const) {
      const mask = arrowIconMask(24, { r: 0, g: 114, b: 178, a: 255 }, shape);
      expect(mask.width).toBe(24);
      expect(mask.height).toBe(24);
      expect(mask.data).toHaveLength(24 * 24 * 4);
      const alpha = (x: number, y: number): number => mask.data[(y * 24 + x) * 4 + 3];
      // Left-right symmetry on every row.
      for (let y = 0; y < 24; y += 1) {
        for (let x = 0; x < 12; x += 1) {
          expect(alpha(x, y)).toBe(alpha(23 - x, y));
        }
      }
      // Corners transparent; triangle centre opaque, chevron centre hollow.
      expect(alpha(0, 0)).toBe(0);
      expect(alpha(23, 23)).toBe(0);
      if (shape === 'triangle') {
        expect(alpha(12, 12)).toBe(255);
      } else {
        expect(alpha(12, 12)).toBe(0);
      }
    }
  });

  it('honours the requested RGBA channels', () => {
    const mask = arrowIconMask(16, { r: 213, g: 94, b: 0, a: 200 }, 'triangle');
    // Centre pixel carries the colour with full-coverage alpha.
    const idx = (8 * 16 + 8) * 4;
    expect(mask.data[idx]).toBe(213);
    expect(mask.data[idx + 1]).toBe(94);
    expect(mask.data[idx + 2]).toBe(0);
    expect(mask.data[idx + 3]).toBe(200);
  });

  it('differs by shape: the chevron is stroked open, the triangle filled', () => {
    const tri = arrowIconMask(24, { r: 0, g: 0, b: 0, a: 255 }, 'triangle');
    const chev = arrowIconMask(24, { r: 0, g: 0, b: 0, a: 255 }, 'chevron');
    const alphaAt = (m: { data: Uint8ClampedArray }, x: number, y: number): number =>
      m.data[(y * 24 + x) * 4 + 3];
    // Apex pixel is painted in both; the interior midpoint is filled only
    // in the triangle (the chevron's open middle stays hollow).
    expect(alphaAt(tri, 12, 3)).toBeGreaterThan(0);
    expect(alphaAt(chev, 12, 3)).toBeGreaterThan(0);
    expect(alphaAt(tri, 12, 14)).toBe(255);
    expect(alphaAt(chev, 12, 14)).toBe(0);
  });
});
describe('cssColorToRgba', () => {
  it('parses space-separated token channels and comma rgb', () => {
    expect(cssColorToRgba('rgb(0 114 178)')).toMatchObject({ r: 0, g: 114, b: 178 });
    expect(cssColorToRgba('rgb(0, 114, 178)')).toMatchObject({ r: 0, g: 114, b: 178 });
  });

  it('parses hex colours', () => {
    expect(cssColorToRgba('#0072b2')).toMatchObject({ r: 0, g: 114, b: 178 });
    expect(cssColorToRgba('#07b')).toMatchObject({ r: 0, g: 119, b: 187 });
  });

  it('falls back rather than yielding NaN channels', () => {
    expect(cssColorToRgba('not-a-color')).toMatchObject({ r: 0, g: 0, b: 0 });
    expect(cssColorToRgba('')).toMatchObject({ r: 0, g: 0, b: 0 });
  });
});

describe('arrowIconMask halo (009-010 T11, AC-6b)', () => {
  type Mask = { width: number; height: number; data: Uint8ClampedArray };
  const GLYPH = { r: 0, g: 114, b: 178, a: 255 };
  const HALO = { r: 15, g: 23, b: 42, a: 255 };

  const alphaAt = (m: Mask, x: number, y: number): number => m.data[(y * m.width + x) * 4 + 3];
  const rgbAt = (m: Mask, x: number, y: number): number[] =>
    Array.from(m.data.slice((y * m.width + x) * 4, (y * m.width + x) * 4 + 3));
  const painted = (m: Mask): number => {
    let n = 0;
    for (let i = 3; i < m.data.length; i += 4) if (m.data[i] > 0) n += 1;
    return n;
  };
  const paintsChannel = (m: Mask, rgb: number[]): boolean => {
    for (let i = 0; i < m.data.length; i += 4) {
      if (
        m.data[i + 3] > 0 &&
        m.data[i] === rgb[0] &&
        m.data[i + 1] === rgb[1] &&
        m.data[i + 2] === rgb[2]
      ) {
        return true;
      }
    }
    return false;
  };

  it('wraps the glyph in a halo of its own colour without recolouring the glyph', () => {
    for (const shape of ['triangle', 'chevron'] as const) {
      const bare = arrowIconMask(24, GLYPH, shape);
      const haloed = arrowIconMask(24, GLYPH, shape, HALO);

      // The halo adds painted pixels, and they are the halo's colour — the
      // un-haloed mask never carries it.
      expect(painted(haloed)).toBeGreaterThan(painted(bare));
      expect(paintsChannel(haloed, [HALO.r, HALO.g, HALO.b])).toBe(true);
      expect(paintsChannel(bare, [HALO.r, HALO.g, HALO.b])).toBe(false);

      // The glyph keeps its own colour where it is the thicker mark on the pixel.
      expect(paintsChannel(haloed, [GLYPH.r, GLYPH.g, GLYPH.b])).toBe(true);
      if (shape === 'triangle') {
        expect(alphaAt(haloed, 12, 12)).toBe(255);
        expect(rgbAt(haloed, 12, 12)).toEqual([GLYPH.r, GLYPH.g, GLYPH.b]);
      } else {
        // The chevron's interior stays hollow even with the halo on (checked
        // where the V is at its widest, well clear of both arms), and the apex
        // stroke is thick enough to carry the glyph's own colour.
        expect(alphaAt(haloed, 12, 19)).toBe(0);
        expect(rgbAt(haloed, 12, 3)).toEqual([GLYPH.r, GLYPH.g, GLYPH.b]);
      }

      // Corners stay transparent and the mask stays symmetric.
      expect(alphaAt(haloed, 0, 0)).toBe(0);
      expect(alphaAt(haloed, 23, 23)).toBe(0);
      for (let y = 0; y < 24; y += 1) {
        for (let x = 0; x < 12; x += 1) {
          expect(alphaAt(haloed, x, y)).toBe(alphaAt(haloed, 23 - x, y));
        }
      }
    }
  });

  it('keeps the chevron legible — real painted area, still hollow in the middle', () => {
    const chev = arrowIconMask(24, GLYPH, 'chevron', HALO);
    // ≥ ~100 of 576 pixels: far above the hairline a 24 px icon can lose.
    expect(painted(chev)).toBeGreaterThanOrEqual(100);
    expect(alphaAt(chev, 12, 19)).toBe(0); // open middle, never a blob
    expect(alphaAt(chev, 12, 3)).toBeGreaterThan(0); // apex stroke painted
  });
});

describe('dedupeArrows (009-010 T11, AC-6b)', () => {
  const glyph = (series: 'route' | 'route-overlay', lngLat: [number, number], bearing: number) => ({
    series,
    lngLat,
    bearing,
  });
  const seriesOf = (a: { series: string }): string => a.series;
  const SEP = DEFAULT_ARROW_MIN_SEPARATION_M;

  it('collapses any two glyphs closer than the separation, across series', () => {
    // ~2.2 m apart at the equator — one glyph survives, the first listed.
    const route = glyph('route', [0, 0], 0);
    const overlay = glyph('route-overlay', [0.00002, 0], 180);
    expect(dedupeArrows([route, overlay], SEP, seriesOf)).toEqual([route]);

    // Reverse the order and the survivor flips: whichever is listed first wins,
    // and a series with a single colliding glyph is not resurrected.
    expect(dedupeArrows([overlay, route], SEP, seriesOf)).toEqual([overlay]);
  });

  it('keeps glyphs at the nominal spacing, in order, untouched', () => {
    const list = [
      glyph('route', [0, 0], 0),
      glyph('route', [0, 0.00045], 0), // ~50 m north
      glyph('route', [0, 0.0009], 0), // ~100 m north
    ];
    expect(dedupeArrows(list, SEP, seriesOf)).toEqual(list);
  });

  it('measures separation in meters, not degrees', () => {
    // At latitude 60 a degree of longitude is half as long: ~2.8 m here.
    const a = glyph('route', [0, 60], 0);
    const b = glyph('route-overlay', [0.00005, 60], 90);
    expect(dedupeArrows([a, b], SEP, seriesOf)).toHaveLength(1);
  });

  it('leaves a fully coincident series its first glyph', () => {
    const route = [glyph('route', [0, 0], 0), glyph('route', [0, 0.0009], 0)];
    const overlay = [
      glyph('route-overlay', [0, 0], 180),
      glyph('route-overlay', [0, 0.0009], 180),
    ];
    const out = dedupeArrows([...route, ...overlay], SEP, seriesOf);
    expect(out.filter((a) => a.series === 'route')).toEqual(route);
    expect(out.filter((a) => a.series === 'route-overlay')).toEqual([overlay[0]]);
  });

  it('honours a custom separation and drops unusable coordinates', () => {
    const a = glyph('route', [0, 0], 0);
    const b = glyph('route-overlay', [0.00002, 0], 180);
    expect(dedupeArrows([a, b], 1, seriesOf)).toHaveLength(2); // 2.2 m apart, 1 m tolerance
    expect(dedupeArrows([glyph('route', [Number.NaN, 0], 0), a], SEP, seriesOf)).toEqual([a]);
  });

  it('returns an empty list for no glyphs and a copy for a single glyph', () => {
    expect(dedupeArrows([], SEP, seriesOf)).toEqual([]);
    const one = [glyph('route', [0, 0], 0)];
    const out = dedupeArrows(one, SEP, seriesOf);
    expect(out).toEqual(one);
    expect(out).not.toBe(one);
  });
});
