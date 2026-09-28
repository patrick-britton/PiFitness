/**
 * Geographic helpers for Segment Management (009-003 T08).
 *
 * Dependency-free (no React, no DOM). `metersBetween` is the haversine length
 * of one segment of a GPS track, used to derive the elevation profile's
 * distance axis from `[lon, lat]` coordinates (the T03 route contract carries
 * no per-point distance).
 */

/** Mean Earth radius in meters (same constant family as the legacy Python helper). */
const EARTH_RADIUS_M = 6371008.8;

const toRad = (deg: number) => (deg * Math.PI) / 180;

/**
 * Great-circle distance in meters between two `[lon, lat]` coordinate pairs.
 * Returns 0 for non-finite input rather than NaN so a dirty coordinate cannot
 * poison a cumulative axis.
 */
export function metersBetween(a: number[], b: number[]): number {
  if (!a || !b) return 0;
  const [lon1, lat1] = a;
  const [lon2, lat2] = b;
  if (![lon1, lat1, lon2, lat2].every((v) => Number.isFinite(v))) return 0;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1, Math.sqrt(h)));
}

/** Bounding box `[[west, south], [east, north]]` of a path; null when empty. */
export function boundsOf(coords: number[][]): [[number, number], [number, number]] | null {
  const valid = coords.filter(
    (c) => Array.isArray(c) && c.length >= 2 && Number.isFinite(c[0]) && Number.isFinite(c[1]),
  );
  if (valid.length === 0) return null;
  let west = valid[0][0];
  let east = valid[0][0];
  let south = valid[0][1];
  let north = valid[0][1];
  for (const [lon, lat] of valid) {
    west = Math.min(west, lon);
    east = Math.max(east, lon);
    south = Math.min(south, lat);
    north = Math.max(north, lat);
  }
  return [
    [west, south],
    [east, north],
  ];
}