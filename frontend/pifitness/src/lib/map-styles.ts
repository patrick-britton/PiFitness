/**
 * Basemap catalogue + raster tile URL builder for the Segment Management maps
 * (009-003 T08).
 *
 * The catalogue itself is owned by the backend (`GET /api/segments/map-styles`,
 * T04/FR-3): ids, labels, and which styles need the Mapbox token. This module
 * only turns an id into a MapLibre raster tile URL, so both Segment Management
 * modes (Create Segment, Match Activities) resolve URLs identically.
 *
 * Satellite note: Mapbox Satellite is the one style still on `v9` —
 * `satellite-v11` does not exist and every tile request 404s, which rendered
 * the basemap blank (Bug 009-009-T09-1, live-probed 2026-09-15: satellite-v11
 * → 404, satellite-v9 → 200 image/jpeg). Kept here so the fix cannot be lost.
 *
 * Dependency-free (no React, no DOM) so pure URL resolution is unit-tested.
 */

/** Mapbox style ids per Mapbox-backed style id from the T04 catalogue. */
const MAPBOX_STYLES: Record<string, { mapboxId: string; version?: string; dark: boolean }> = {
  'mb-basic': { mapboxId: 'basic', dark: false },
  'mb-streets': { mapboxId: 'streets', dark: false },
  'mb-outdoors': { mapboxId: 'outdoors', dark: false },
  'mb-light': { mapboxId: 'light', dark: false },
  'mb-dark': { mapboxId: 'dark', dark: true },
  'mb-satellite': { mapboxId: 'satellite', version: 'v9', dark: true },
  'mb-sat-streets': { mapboxId: 'satellite-streets', dark: false },
};

/** Token-free OSM/Carto raster tile URLs (always available). */
const FREE_STYLES: Record<string, { url: string; dark: boolean }> = {
  positron: { url: 'https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', dark: false },
  osm: { url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', dark: false },
  darkmatter: { url: 'https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png', dark: true },
};

/** True when a style is a token-gated Mapbox style (per the T04 catalogue). */
export function requiresMapboxToken(styleId: string): boolean {
  return styleId in MAPBOX_STYLES;
}

/** True when the basemap is dark, so overlay colors can be chosen to contrast it. */
export function isDarkBasemap(styleId: string): boolean {
  return MAPBOX_STYLES[styleId]?.dark ?? FREE_STYLES[styleId]?.dark ?? false;
}

/**
 * Raster tile URL for a catalogue style id.
 *
 * Returns null when the style cannot be drawn: the `blank` vector style (no
 * basemap by design) and any token-gated style without a configured token.
 */
export function buildStyleUrl(styleId: string, token: string | null): string | null {
  const free = FREE_STYLES[styleId];
  if (free) return free.url;
  const mb = MAPBOX_STYLES[styleId];
  if (!mb || !token) return null;
  const version = mb.version ?? 'v11';
  return (
    `https://api.mapbox.com/styles/v1/mapbox/${mb.mapboxId}-${version}/tiles/256/{z}/{x}/{y}` +
    `?access_token=${encodeURIComponent(token)}`
  );
}

/**
 * Default style for a theme when no choice exists yet: a token style in the
 * matching polarity when a token is configured, else a free one.
 */
export function defaultStyleForTheme(isDark: boolean, token: string | null): string {
  if (token) return isDark ? 'mb-dark' : 'mb-light';
  return isDark ? 'darkmatter' : 'positron';
}