/**
 * Basemap style tests (009-003 T08, FR-3).
 *
 * The tile URL decides whether a basemap draws at all — a wrong Mapbox style
 * version silently 404s every tile (Bug 009-009-T09-1) — so URL resolution and
 * token gating are asserted rather than eyeballed in the browser.
 */

import { describe, expect, it } from 'vitest';
import {
  buildStyleUrl,
  defaultStyleForTheme,
  isDarkBasemap,
  requiresMapboxToken,
} from '../map-styles';

describe('requiresMapboxToken', () => {
  it('flags Mapbox styles and leaves the free styles alone', () => {
    expect(requiresMapboxToken('mb-satellite')).toBe(true);
    expect(requiresMapboxToken('mb-dark')).toBe(true);
    expect(requiresMapboxToken('positron')).toBe(false);
    expect(requiresMapboxToken('osm')).toBe(false);
    expect(requiresMapboxToken('blank')).toBe(false);
  });
});

describe('buildStyleUrl', () => {
  it('returns token-free URLs for the OSM/Carto styles', () => {
    expect(buildStyleUrl('positron', null)).toContain('cartocdn.com/light_all');
    expect(buildStyleUrl('darkmatter', null)).toContain('cartocdn.com/dark_all');
    expect(buildStyleUrl('osm', null)).toContain('tile.openstreetmap.org');
  });

  it('returns null for a token-gated style when no token is configured', () => {
    expect(buildStyleUrl('mb-satellite', null)).toBeNull();
    expect(buildStyleUrl('mb-basic', null)).toBeNull();
  });

  it('returns null for the blank vector style (no basemap by design)', () => {
    expect(buildStyleUrl('blank', 'tok')).toBeNull();
  });

  it('builds a Mapbox raster URL and URL-encodes the token', () => {
    const url = buildStyleUrl('mb-outdoors', 'pk.a/b+c');
    expect(url).toContain('/styles/v1/mapbox/outdoors-v11/tiles/256/{z}/{x}/{y}');
    expect(url).toContain('access_token=pk.a%2Fb%2Bc');
  });

  it('pins Mapbox Satellite to v9, the only version that serves tiles', () => {
    const url = buildStyleUrl('mb-satellite', 'tok');
    expect(url).toContain('/mapbox/satellite-v9/');
    expect(url).not.toContain('satellite-v11');
  });

  it('uses v11 for the satellite-streets style', () => {
    expect(buildStyleUrl('mb-sat-streets', 'tok')).toContain('satellite-streets-v11');
  });

  it('returns null for an unknown style id', () => {
    expect(buildStyleUrl('nope', 'tok')).toBeNull();
  });
});

describe('isDarkBasemap', () => {
  it('reports the polarities the UI needs to contrast overlays', () => {
    expect(isDarkBasemap('positron')).toBe(false);
    expect(isDarkBasemap('darkmatter')).toBe(true);
    expect(isDarkBasemap('mb-dark')).toBe(true);
    expect(isDarkBasemap('mb-satellite')).toBe(true);
    expect(isDarkBasemap('mb-light')).toBe(false);
  });
});

describe('defaultStyleForTheme', () => {
  it('prefers the matching-polarity Mapbox style when a token exists', () => {
    expect(defaultStyleForTheme(false, 'tok')).toBe('mb-light');
    expect(defaultStyleForTheme(true, 'tok')).toBe('mb-dark');
  });

  it('falls back to a free style with the same polarity without a token', () => {
    expect(defaultStyleForTheme(false, null)).toBe('positron');
    expect(defaultStyleForTheme(true, null)).toBe('darkmatter');
  });
});