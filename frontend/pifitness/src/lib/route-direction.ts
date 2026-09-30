/**
 * Route-direction helpers for Segment Management (009-010 T02, FR-3).
 *
 * Dependency-free pure functions (no React, no DOM, no MapLibre) so arrow
 * placement, heading math and icon-mask generation are unit-tested rather
 * than eyeballed on a device. The map layer (T03) only consumes the output.
 *
 * Coordinate convention: `[lon, lat]` pairs, same as `geo.ts`/`segment-trim.ts`.
 * Bearings are MapLibre `icon-rotate` degrees clockwise from north, matching
 * the initial-bearing convention the symbol layer expects with
 * `icon-rotation-alignment: 'map'`.
 */

import { metersBetween } from './geo';
import { cumulativeMeters } from './segment-trim';

/** One directional glyph placement on a route. */
export interface DirectionArrow {
  /** Arrow position as `[lon, lat]` (interpolated between route points). */
  lngLat: [number, number];
  /** Ground-referenced heading in degrees clockwise from north. */
  bearing: number;
}

/** Arrow icon silhouette. `triangle` = filled triangle, `chevron` = open chevron. */
export type ArrowShape = 'triangle' | 'chevron';

/** RGBA channels for an arrow icon mask. */
export interface ArrowRgba {
  r: number;
  g: number;
  b: number;
  /** 0..255. */
  a: number;
}

/** RGBA pixel mask consumable via `map.addImage(id, { width, height, data })`. */
export interface ArrowIconMask {
  width: number;
  height: number;
  data: Uint8ClampedArray;
}

/** Default along-track spacing between arrows, in meters. */
export const DEFAULT_ARROW_SPACING_M = 50;

/**
 * Default minimum separation between any two drawn glyphs, in meters
 * (009-010 T11, AC-6b). Arrows closer than this render as one blob on the
 * basemap — the later glyph is dropped instead, whichever series it belongs to.
 */
export const DEFAULT_ARROW_MIN_SEPARATION_M = 4;

/** Chevron stroke half-width in unit space, baked at the icon's pixel size. */
const CHEVRON_STROKE_HALF = 0.09;

/**
 * Halo ring thickness in unit space — ~1.2 CSS pixels at the 24 px icon (3–4
 * device pixels on a phone), thin enough on purpose to leave the chevron's open
 * interior hollow so the two glyph shapes stay distinguishable (AC-6).
 */
const OUTLINE_HALF = 0.05;

/** Meters per degree of latitude (mean); longitude cells scale by cos(lat). */
const METERS_PER_DEG_LAT = 111320;

/** Default heading window: bearings are taken over ±15 m of path. */
export const DEFAULT_HEADING_WINDOW_M = 15;

/** Minimum icon size in pixels (mask generation clamps below this). */
export const MIN_ARROW_ICON_SIZE = 8;

const toRad = (deg: number): number => (deg * Math.PI) / 180;
const toDeg = (rad: number): number => (rad * 180) / Math.PI;

/**
 * Initial bearing from `a` to `b` in degrees clockwise from north (0..360).
 * Returns 0 for degenerate input (identical or non-finite points) rather than
 * NaN so a dirty coordinate cannot poison a layer layout property.
 */
export function headingBetween(a: number[], b: number[]): number {
  if (!a || !b) return 0;
  const [lon1, lat1] = a;
  const [lon2, lat2] = b;
  if (![lon1, lat1, lon2, lat2].every((v) => typeof v === 'number' && Number.isFinite(v))) return 0;
  const phi1 = toRad(lat1);
  const phi2 = toRad(lat2);
  const dLon = toRad(lon2 - lon1);
  const y = Math.sin(dLon) * Math.cos(phi2);
  const x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(dLon);
  if (x === 0 && y === 0) return 0;
  return (toDeg(Math.atan2(y, x)) + 360) % 360;
}

/** Valid `[lon, lat]` pair (finite numbers, at least two elements). */
function isValidCoord(c: number[]): boolean {
  return (
    Array.isArray(c) &&
    c.length >= 2 &&
    typeof c[0] === 'number' &&
    typeof c[1] === 'number' &&
    Number.isFinite(c[0]) &&
    Number.isFinite(c[1])
  );
}

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

/**
 * Interpolate the route position at an exact cumulative distance.
 * Returns null when the distance is outside the path extent.
 */
function pointAtDistance(
  coords: number[][],
  cumulative: number[],
  targetM: number,
): [number, number] | null {
  if (coords.length === 0 || cumulative.length !== coords.length) return null;
  if (!Number.isFinite(targetM) || targetM < 0 || targetM > cumulative[cumulative.length - 1]) {
    return null;
  }
  let lo = 0;
  let hi = cumulative.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cumulative[mid] < targetM) lo = mid + 1;
    else hi = mid;
  }
  if (lo === 0) return [coords[0][0], coords[0][1]];
  const prev = cumulative[lo - 1];
  const curr = cumulative[lo];
  const span = curr - prev;
  const t = span > 0 ? (targetM - prev) / span : 0;
  return [lerp(coords[lo - 1][0], coords[lo][0], t), lerp(coords[lo - 1][1], coords[lo][1], t)];
}

export interface DirectionArrowOptions {
  /** Along-track spacing in meters (default 50). */
  spacingM?: number;
  /** Heading look-back/look-ahead window in meters (default 15). */
  windowM?: number;
}
/**
 * Sample directional glyph placements along a route — ONE pass over the
 * memoized cumulative array, emitting one `{ lngLat, bearing }` every
 * `spacingM` meters of path length. Each bearing is taken over a short
 * look-back/look-ahead window (`windowM`, default ±15 m) so GPS jitter cannot
 * flip a single arrow; at the path ends the window clamps to the extent.
 *
 * Yields nothing for degenerate input (fewer than two valid points, zero
 * path length, non-finite coordinates, spacing ≤ 0).
 */
export function directionArrows(
  coords: number[][],
  spacingM = DEFAULT_ARROW_SPACING_M,
  opts: DirectionArrowOptions = {},
): DirectionArrow[] {
  if (!Array.isArray(coords) || coords.length < 2) return [];
  const spacing = opts.spacingM ?? spacingM;
  const windowM = opts.windowM ?? DEFAULT_HEADING_WINDOW_M;
  if (!Number.isFinite(spacing) || spacing <= 0) return [];
  if (!Number.isFinite(windowM) || windowM < 0) return [];
  if (!coords.every(isValidCoord)) return [];

  const cumulative = cumulativeMeters(coords);
  const total = cumulative[cumulative.length - 1];
  if (!Number.isFinite(total) || total <= 0) return [];

  const arrows: DirectionArrow[] = [];
  // First arrow sits half a spacing into the path so glyphs never stack on
  // the start marker; subsequent arrows every full spacing.
  for (let target = spacing / 2; target < total; target += spacing) {
    const at = pointAtDistance(coords, cumulative, target);
    if (!at) continue;
    const back = pointAtDistance(coords, cumulative, Math.max(0, target - windowM));
    const ahead = pointAtDistance(coords, cumulative, Math.min(total, target + windowM));
    if (!back || !ahead) continue;
    arrows.push({ lngLat: at, bearing: headingBetween(back, ahead) });
  }
  return arrows;
}

/**
 * Collapse glyphs that would render as one blob (009-010 T11, AC-6b).
 *
 * ONE pass over the already-memoized glyph lists, bucketed into a
 * `minSeparationM`-sized lat/lon grid so the cost stays linear in the glyph
 * count (a Pi-5 map must never pay an all-pairs scan per selection). Input
 * order is the survive order — callers list the reference route's glyphs first,
 * so where two series coincide the reference's glyph is the one drawn — and any
 * two glyphs closer than the separation collapse to one, whichever series they
 * belong to. Invalid coordinates are dropped rather than handed to MapLibre
 * (a non-finite position cannot be rendered). Returns a new array; the input is
 * left untouched.
 */
export function dedupeArrows<T extends { lngLat: [number, number] }>(
  arrows: T[],
  minSeparationM = DEFAULT_ARROW_MIN_SEPARATION_M,
  seriesOf?: (arrow: T) => string,
): T[] {
  if (!Array.isArray(arrows) || arrows.length === 0) return [];
  const minM =
    Number.isFinite(minSeparationM) && minSeparationM > 0
      ? minSeparationM
      : DEFAULT_ARROW_MIN_SEPARATION_M;
  const cellDeg = minM / METERS_PER_DEG_LAT;
  const keyOf = (latIdx: number, lngIdx: number): string => `${latIdx}:${lngIdx}`;
  const cellOf = (lngLat: [number, number]): [number, number] => [
    Math.floor(lngLat[1] / cellDeg),
    Math.floor((lngLat[0] * Math.max(0.01, Math.cos(toRad(lngLat[1])))) / cellDeg),
  ];
  const seriesKey = seriesOf ?? ((): string => '');
  const grid = new Map<string, T[]>();
  const kept: T[] = [];
  const keptPerSeries = new Map<string, number>();
  const contributedPerSeries = new Map<string, number>();

  for (const arrow of arrows) {
    if (!arrow || !isValidCoord(arrow.lngLat)) continue;
    const series = seriesKey(arrow);
    contributedPerSeries.set(series, (contributedPerSeries.get(series) ?? 0) + 1);
    const [latIdx, lngIdx] = cellOf(arrow.lngLat);
    let collides = false;
    for (let dy = -1; dy <= 1 && !collides; dy += 1) {
      for (let dx = -1; dx <= 1 && !collides; dx += 1) {
        const bucket = grid.get(keyOf(latIdx + dy, lngIdx + dx));
        if (!bucket) continue;
        for (const other of bucket) {
          if (metersBetween(other.lngLat, arrow.lngLat) < minM) {
            collides = true;
            break;
          }
        }
      }
    }
    if (collides) continue;
    kept.push(arrow);
    keptPerSeries.set(series, (keptPerSeries.get(series) ?? 0) + 1);
    const cell = keyOf(latIdx, lngIdx);
    const bucket = grid.get(cell);
    if (bucket) bucket.push(arrow);
    else grid.set(cell, [arrow]);
  }

  // Floor: a series that contributed more than one glyph and lost every one of
  // them keeps its first glyph, so two activities covering exactly the same
  // track cannot leave one series arrow-less while the other is covered in them
  // (AC-6). A lone colliding pair still collapses to ONE glyph — that is what
  // AC-6b asks for — because the series has nothing else to show anyway.
  for (const [series, contributed] of contributedPerSeries) {
    if (contributed < 2 || (keptPerSeries.get(series) ?? 0) > 0) continue;
    const first = arrows.find(
      (a) => a && isValidCoord(a.lngLat) && seriesKey(a) === series,
    );
    if (first) kept.push(first);
  }
  return kept;
}

/**
 * Parse a CSS color to RGBA channels. Accepts the two shapes `tokenRgb(...)`
 * produces: space-separated channels wrapped in `rgb(...)` (e.g.
 * `rgb(0 114 178)` / `rgb(0, 114, 178)`), and `#rrggbb` / `#rgb` hex.
 * Unknown shapes yield opaque black rather than NaN channels.
 */
export function cssColorToRgba(color: string, alpha = 255): ArrowRgba {
  const fallback: ArrowRgba = { r: 0, g: 0, b: 0, a: alpha };
  if (typeof color !== 'string') return fallback;
  const v = color.trim();
  if (!v) return fallback;
  if (v.startsWith('#')) {
    const hex = v.slice(1);
    const full =
      hex.length === 3
        ? hex
            .split('')
            .map((c) => c + c)
            .join('')
        : hex;
    if (!/^[0-9a-fA-F]{6}$/.test(full)) return fallback;
    return {
      r: parseInt(full.slice(0, 2), 16),
      g: parseInt(full.slice(2, 4), 16),
      b: parseInt(full.slice(4, 6), 16),
      a: alpha,
    };
  }
  const m = v.match(/^rgba?\(\s*(.+?)\s*\)$/i);
  if (m) {
    const parts = m[1].split(/[\s,/]+/).filter((p) => p.length > 0);
    if (parts.length >= 3) {
      const nums = parts.slice(0, 3).map((p) => Number(p));
      if (nums.every((n) => Number.isFinite(n))) {
        return {
          r: Math.min(255, Math.max(0, Math.round(nums[0]))),
          g: Math.min(255, Math.max(0, Math.round(nums[1]))),
          b: Math.min(255, Math.max(0, Math.round(nums[2]))),
          a: alpha,
        };
      }
    }
  }
  return fallback;
}
/**
 * Build an RGBA arrow icon mask of `size × size` pixels, pointing north
 * (bearing 0 — MapLibre rotates it per feature via data-driven `icon-rotate`).
 *
 * `triangle` is a filled triangle (apex top centre, base near the bottom);
 * `chevron` is an open chevron stroked along the same outline, so the two
 * series are told apart by shape as well as colour. Coverage-based alpha
 * feathers edges; RGB channels are constant across each region.
 *
 * `outline` (009-010 T11, AC-6b) bakes a contrasting halo around the glyph: a
 * ring of `OUTLINE_HALF` unit-space thickness painted in that colour wherever
 * the silhouette is not, so two glyphs from different series can never blend
 * into one blob over each other's line. The halo lives in the same icon image —
 * no extra layer, no DOM marker, no animation — and a caller passing none gets
 * a byte-identical mask to what it got before. Pixel-built with no canvas and
 * no `ImageData` dependency.
 */
export function arrowIconMask(
  size: number,
  rgba: ArrowRgba,
  shape: ArrowShape = 'triangle',
  outline?: ArrowRgba | null,
): ArrowIconMask {
  const dim = Number.isFinite(size) ? Math.max(MIN_ARROW_ICON_SIZE, Math.floor(size)) : MIN_ARROW_ICON_SIZE;
  const channel = (v: number | undefined, fallback: number): number =>
    Math.min(255, Math.max(0, Math.round(v ?? fallback)));
  const glyph: ArrowRgba = {
    r: channel(rgba?.r, 0),
    g: channel(rgba?.g, 0),
    b: channel(rgba?.b, 0),
    a: channel(rgba?.a, 255),
  };
  // Halo channels — read only when an outline is asked for.
  const halo: ArrowRgba = {
    r: channel(outline?.r, 0),
    g: channel(outline?.g, 0),
    b: channel(outline?.b, 0),
    a: channel(outline?.a, 255),
  };
  const data = new Uint8ClampedArray(dim * dim * 4);

  // Glyph geometry in unit space (0..1, y down): apex top-centre, base spread.
  const apex = { x: 0.5, y: 0.12 };
  const baseL = { x: 0.18, y: 0.86 };
  const baseR = { x: 0.82, y: 0.86 };
  // Point-in-triangle test (sign method).
  const sign = (px: number, py: number, ax: number, ay: number, bx: number, by: number): number =>
    (px - bx) * (ay - by) - (ax - bx) * (py - by);
  const insideTriangle = (px: number, py: number): boolean => {
    const d1 = sign(px, py, apex.x, apex.y, baseL.x, baseL.y);
    const d2 = sign(px, py, baseL.x, baseL.y, baseR.x, baseR.y);
    const d3 = sign(px, py, baseR.x, baseR.y, apex.x, apex.y);
    const hasNeg = d1 < 0 || d2 < 0 || d3 < 0;
    const hasPos = d1 > 0 || d2 > 0 || d3 > 0;
    return !(hasNeg && hasPos);
  };
  // Distance from a point to a segment (unit space) — chevron stroke.
  const distToSegment = (
    px: number, py: number, ax: number, ay: number, bx: number, by: number,
  ): number => {
    const dx = bx - ax;
    const dy = by - ay;
    const lenSq = dx * dx + dy * dy;
    if (lenSq === 0) return Math.hypot(px - ax, py - ay);
    const t = Math.min(1, Math.max(0, ((px - ax) * dx + (py - ay) * dy) / lenSq));
    return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
  };

  /**
   * Is a unit-space point inside the glyph silhouette, grown by `grow` units?
   * `grow = 0` is the glyph itself; a positive value is the halo ring, which is
   * never counted where the glyph is, so the halo cannot eat into the glyph.
   */
  const insideGlyph = (px: number, py: number, grow: number): boolean => {
    if (shape === 'triangle') {
      if (insideTriangle(px, py)) return true;
      if (grow <= 0) return false;
      return (
        distToSegment(px, py, apex.x, apex.y, baseL.x, baseL.y) <= grow ||
        distToSegment(px, py, apex.x, apex.y, baseR.x, baseR.y) <= grow ||
        distToSegment(px, py, baseL.x, baseL.y, baseR.x, baseR.y) <= grow
      );
    }
    // Chevron: the stroked V is the glyph; `grow` widens it into the halo.
    return (
      Math.min(
        distToSegment(px, py, apex.x, apex.y, baseL.x, baseL.y),
        distToSegment(px, py, apex.x, apex.y, baseR.x, baseR.y),
      ) <=
      CHEVRON_STROKE_HALF + grow
    );
  };

  for (let y = 0; y < dim; y += 1) {
    for (let x = 0; x < dim; x += 1) {
      // Supersample 2x2 within the pixel for coverage-based alpha.
      let core = 0;
      let ring = 0;
      for (let sy = 0; sy < 2; sy += 1) {
        for (let sx = 0; sx < 2; sx += 1) {
          const px = (x + (sx + 0.5) / 2) / dim;
          const py = (y + (sy + 0.5) / 2) / dim;
          if (insideGlyph(px, py, 0)) core += 1;
          else if (outline && insideGlyph(px, py, OUTLINE_HALF)) ring += 1;
        }
      }
      // A pixel takes the colour of whichever region covers more of it. With no
      // outline the ring is always 0, which makes this exactly the previous
      // behaviour: glyph colour, alpha = coverage of the glyph.
      const glyphWins = core >= ring;
      const paint = glyphWins ? glyph : halo;
      const coverage = glyphWins ? core : ring;
      const idx = (y * dim + x) * 4;
      data[idx] = paint.r;
      data[idx + 1] = paint.g;
      data[idx + 2] = paint.b;
      data[idx + 3] = Math.round((paint.a * coverage) / 4);
    }
  }
  return { width: dim, height: dim, data };
}
