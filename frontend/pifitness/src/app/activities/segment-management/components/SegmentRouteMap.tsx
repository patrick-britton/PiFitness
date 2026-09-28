'use client';

import { useEffect, useRef } from 'react';
import maplibregl, { type StyleSpecification } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { buildStyleUrl } from '@/lib/map-styles';
import { boundsOf } from '@/lib/geo';
import { tokenRgb } from '@/lib/token-color';
import { isDarkTheme, useThemeVersion } from '@/lib/theme-version';

/**
 * Route map for Segment Management (009-003 T08, FR-2/FR-4).
 *
 * One MapLibre map per instance: the basemap is rebuilt only when the chosen
 * style or the Mapbox token changes, and route/gate layers are (re)built from
 * the current coordinates. Colors come from design tokens and are re-applied on
 * theme change, because MapLibre bakes paint properties in.
 *
 * Callers pass the coordinates the view should show: the full trimmed range for
 * the main map, or the first/last gate window slice for the gate views (AC-4).
 */

export interface SegmentRouteMapProps {
  /** Route to display as `[lon, lat]` pairs. */
  coords: number[][];
  /** Catalogue style id (backend-owned list). */
  styleId: string;
  /** Mapbox token, or null when only token-free basemaps are available. */
  token: string | null;
  /** Container height (reserve it so the layout does not shift). */
  height: string;
  /** Accessible description of what the map shows. */
  ariaLabel: string;
  /** Gate markers to highlight, as `[lon, lat]` pairs. */
  gates?: { start?: number[]; end?: number[] };
  /**
   * Optional comparison track (009-003 T09: the selected candidate effort),
   * drawn dashed so the two tracks are told apart by more than color (design
   * system: meaning is never carried by color alone). Bounds include it.
   */
  overlayCoords?: number[][];
  className?: string;
}

/** First/last point of a path, used as the start/end gate marker. */
export function gatePoint(coords: number[][], which: 'start' | 'end'): number[] | undefined {
  if (coords.length === 0) return undefined;
  return which === 'start' ? coords[0] : coords[coords.length - 1];
}

export default function SegmentRouteMap({
  coords,
  styleId,
  token,
  height,
  ariaLabel,
  gates,
  overlayCoords,
  className = '',
}: SegmentRouteMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const themeVersion = useThemeVersion();

  // Basemap: rebuilt on style/token change (a raster style swap keeps the
  // route layers below, which the next effect re-applies).
  useEffect(() => {
    if (typeof window === 'undefined' || !containerRef.current) return;
    const url = buildStyleUrl(styleId, token);
    const styleSpec: StyleSpecification = url
      ? {
          version: 8,
          sources: { basemap: { type: 'raster', tiles: [url], tileSize: 256 } },
          layers: [{ id: 'basemap', type: 'raster', source: 'basemap', minzoom: 0, maxzoom: 22 }],
        }
      : { version: 8, sources: {}, layers: [] };
    const map = new maplibregl.Map({ container: containerRef.current, style: styleSpec });
    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [styleId, token]);

  // Route + gate layers, re-applied on data/style/theme change.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || typeof window === 'undefined') return;
    const apply = () => {
      const isDark = isDarkTheme();
      const lineColor = tokenRgb('--chart-1', isDark);
      const route: GeoJSON.FeatureCollection = {
        type: 'FeatureCollection',
        features: coords.length
          ? [
              {
                type: 'Feature',
                properties: {},
                geometry: { type: 'LineString', coordinates: coords },
              },
            ]
          : [],
      };
      const existing = map.getSource('route') as maplibregl.GeoJSONSource | undefined;
      if (existing) {
        existing.setData(route);
      } else {
        map.addSource('route', { type: 'geojson', data: route });
        map.addLayer({
          id: 'route',
          type: 'line',
          source: 'route',
          layout: { 'line-cap': 'round', 'line-join': 'round' },
          paint: { 'line-color': lineColor, 'line-width': 3.5 },
        });
      }
      map.setPaintProperty('route', 'line-color', lineColor);

      // Optional comparison track (candidate effort), dashed on purpose.
      const overlay: GeoJSON.FeatureCollection = {
        type: 'FeatureCollection',
        features: overlayCoords?.length
          ? [
              {
                type: 'Feature',
                properties: {},
                geometry: { type: 'LineString', coordinates: overlayCoords },
              },
            ]
          : [],
      };
      const overlayColor = tokenRgb('--chart-2', isDark);
      const overlaySource = map.getSource('route-overlay') as maplibregl.GeoJSONSource | undefined;
      if (overlaySource) {
        overlaySource.setData(overlay);
      } else {
        map.addSource('route-overlay', { type: 'geojson', data: overlay });
        map.addLayer({
          id: 'route-overlay',
          type: 'line',
          source: 'route-overlay',
          layout: { 'line-cap': 'round', 'line-join': 'round' },
          paint: { 'line-color': overlayColor, 'line-width': 3, 'line-dasharray': [2, 2] },
        });
      }
      map.setPaintProperty('route-overlay', 'line-color', overlayColor);

      const gateLayers: { id: string; point?: number[]; colorToken: string }[] = [
        { id: 'gate-start', point: gates?.start, colorToken: '--ok' },
        { id: 'gate-end', point: gates?.end, colorToken: '--danger' },
      ];
      gateLayers.forEach(({ id, point, colorToken }) => {
        const fc: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: point
            ? [{ type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: point } }]
            : [],
        };
        const src = map.getSource(id) as maplibregl.GeoJSONSource | undefined;
        if (src) {
          src.setData(fc);
        } else {
          map.addSource(id, { type: 'geojson', data: fc });
          map.addLayer({
            id,
            type: 'circle',
            source: id,
            paint: {
              'circle-radius': 7,
              'circle-color': tokenRgb(colorToken, isDark),
              'circle-stroke-color': tokenRgb('--text', isDark),
              'circle-stroke-width': 2,
            },
          });
        }
        map.setPaintProperty(id, 'circle-color', tokenRgb(colorToken, isDark));
        map.setPaintProperty(id, 'circle-stroke-color', tokenRgb('--text', isDark));
      });

      const bounds = boundsOf(overlayCoords?.length ? [...coords, ...overlayCoords] : coords);
      if (!bounds) return;
      const [[west, south], [east, north]] = bounds;
      if (west === east && south === north) {
        map.setCenter([west, south]);
        map.setZoom(16);
      } else {
        map.fitBounds(bounds, { padding: 28, duration: 0 });
      }
    };

    if (map.isStyleLoaded()) {
      apply();
      return;
    }
    map.once('load', apply);
    return () => {
      map.off('load', apply);
    };
  }, [coords, gates?.start, gates?.end, styleId, token, themeVersion, overlayCoords]);

  return (
    <div
      ref={containerRef}
      className={`w-full rounded-md overflow-hidden border border-gray-200 dark:border-gray-700 ${className}`}
      style={{ height }}
      role="img"
      aria-label={ariaLabel}
    />
  );
}