'use client';

import { useEffect, useRef } from 'react';
import maplibregl, { type StyleSpecification } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { buildStyleUrl } from '@/lib/map-styles';
import { boundsOf } from '@/lib/geo';
import {
  arrowIconMask,
  cssColorToRgba,
  type ArrowShape,
} from '@/lib/route-direction';
import { tokenRgb } from '@/lib/token-color';
import { isDarkTheme, useThemeVersion } from '@/lib/theme-version';

/**
 * A picked-distance marker to draw on the map (009-003 T40, AC-27): the point
 * of one series nearest a distance picked on the elevation plot. The caller
 * resolves the coordinate; the map only draws it.
 */
export interface PickMarker {
  /** Series the marker belongs to (drives its colour/weight, AC-24). */
  series: 'route' | 'route-overlay';
  /** Marker position as `[lon, lat]`. */
  point: number[];
  /** Label carried on the feature (the caller's legend names it too). */
  label: string;
}

/**
 * A directional glyph placement on a route (009-010 T03, FR-3): the caller
 * resolves positions + ground-referenced bearings via `directionArrows`;
 * the map only draws them.
 */
export interface RouteArrow {
  /** Which series the arrow belongs to (drives its icon, AC-6). */
  series: 'route' | 'route-overlay';
  /** Arrow position as `[lon, lat]`. */
  lngLat: [number, number];
  /** Ground-referenced heading in degrees clockwise from north. */
  bearing: number;
}

/**
 * Symbol layers for the directional arrows, one per series (009-010 T03; halo
 * added by T11, AC-6b).
 *
 * Colour follows the T35/AC-24 convention (route = `--chart-1` blue,
 * comparison = `--chart-2` orange) and the icons differ by shape as well —
 * filled triangle vs open chevron — so travel direction is never carried by
 * colour alone (AC-6). Every glyph is baked with a contrasting halo
 * (`ARROW_HALO_TOKEN`) so glyphs that land close together stay separable —
 * without a second layer and without a DOM marker (Pi-5 budget).
 */
const ARROW_LAYERS: {
  id: string;
  series: RouteArrow['series'];
  colorToken: string;
  shape: ArrowShape;
}[] = [
  { id: 'route-arrows', series: 'route', colorToken: '--chart-1', shape: 'triangle' },
  { id: 'route-arrows-overlay', series: 'route-overlay', colorToken: '--chart-2', shape: 'chevron' },
];

/** Pixel size of the generated arrow icons. */
const ARROW_ICON_SIZE = 24;

/**
 * Design token for the halo baked around each glyph (009-010 T11, AC-6b).
 *
 * `--text` is already this app's marker-outline convention (the gate and pick
 * circles stroke with it) and it is theme-aware — near-black in light mode,
 * near-white in dark mode — so the halo contrasts with the basemap in both
 * themes rather than matching the arrow it is meant to separate.
 */
const ARROW_HALO_TOKEN = '--text';

/**
 * Circle layers for the picked-distance markers, one pair per series (009-003
 * T40; halo ring added by 009-010 T05, AC-3).
 *
 * Colour follows the T35/AC-24 convention (route = `--chart-1` blue, comparison
 * = `--chart-2` orange), and each marker gains its own radius/stroke weight as
 * well, so the two are told apart by more than colour alone — the caller's legend
 * carries the text labels. Each is emphasised by a translucent halo ring at
 * `haloRadius`, drawn UNDER its solid marker: at the route's default framing zoom
 * on a phone a bare 6–9 px dot is easy to lose, and the halo makes the picked
 * position findable without a DOM marker (Pi-5 budget) or an animation (AC-8).
 */
const PICK_MARKER_LAYERS: {
  id: string;
  series: PickMarker['series'];
  colorToken: string;
  radius: number;
  strokeWidth: number;
  /** Radius of the emphasis ring drawn beneath the marker. */
  haloRadius: number;
}[] = [
  { id: 'pick-route', series: 'route', colorToken: '--chart-1', radius: 9, strokeWidth: 2, haloRadius: 17 },
  { id: 'pick-overlay', series: 'route-overlay', colorToken: '--chart-2', radius: 6, strokeWidth: 3, haloRadius: 13 },
];

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
   * drawn SOLID in the comparison colour (009-010 OQ-5: the solid/dashed
   * convention is retired), so the two series are told apart by colour plus
   * their own arrow shapes (AC-6), not by a second line style. Bounds include
   * it.
   */
  overlayCoords?: number[][];
  /**
   * Framing key (T15): when provided, bounds are fitted only when this key
   * changes or the map instance is rebuilt — NOT on every coords change, so
   * gate-meter edits that slice the same route cannot resize/recenter the
   * map. Omitted = legacy behaviour (refit on every coords change).
   */
  fitKey?: unknown;
  /**
   * Gate-view centering (T16): when provided, the map centers on this point
   * (the gate) at `centerZoom` instead of fitting bounds, and keeps
   * re-centering as the gate moves.
   */
  centerOn?: number[];
  /** Fixed zoom used with `centerOn` (gate views). */
  centerZoom?: number;
  /**
   * Picked-distance markers (009-003 T40, AC-27): one circle per series at the
   * point nearest a distance picked on the elevation plot, drawn as GeoJSON
   * circle layers. Updated in place and NEVER reframed — the picking interaction
   * must not move the map — so the layers live in their own effect rather than
   * the route/gate apply pass (which refits when no `fitKey` is supplied).
   * Omitted = no pick layers at all, leaving other callers' layer sets unchanged.
   */
  pickMarkers?: PickMarker[];
  /**
   * Directional arrows (009-010 T03, FR-3): one `symbol` layer per series,
   * fed from the caller's `directionArrows` pass. Omitted/empty = no arrow
   * layers at all, leaving other callers' layer sets unchanged. Never
   * reframes the map.
   */
  arrows?: RouteArrow[];
  className?: string;
}

/** First/last point of a path, used as the start/end gate marker. */
export function gatePoint(coords: number[][], which: 'start' | 'end'): number[] | undefined {
  if (coords.length === 0) return undefined;
  return which === 'start' ? coords[0] : coords[coords.length - 1];
}

/**
 * Run `apply` as soon as the style can accept sources/layers, retrying on the
 * style events MapLibre keeps firing (Bug 009-010-11-1).
 *
 * Why not `if (map.isStyleLoaded()) apply(); else map.once('load', apply)`: the
 * `load` event fires ONCE per map instance, so an effect that re-runs after the
 * map has loaded and finds the style momentarily "not loaded" registers a
 * handler that can never fire again — its layers are then never created, with
 * no error to show for it. And `isStyleLoaded()` is momentarily false in
 * ordinary use: it delegates to `Style#loaded()`, which returns false while any
 * recently added/updated source has yet to report its first data event
 * (`maplibre-gl-dev.js`: `Style#loaded` checks `_updatedSources`, and
 * `Style#addLayer` sets `_updatedSources[source] = 'reload'` for every
 * non-raster layer). Effects run in declaration order, so the arrow pass checked
 * the style immediately after the route/gate pass had added the `route-overlay`
 * source, saw false, and deferred to a `load` that had already fired: the
 * comparison series' arrow layer never existed on the device while the
 * reference series (created during the real first `load`) was fine.
 *
 * `styledata` (style loaded or modified), `sourcedata` (a source's data
 * arrived) and `idle` (everything loaded and rendered, which implies
 * `isStyleLoaded()`) are the retry signals; the first successful call wins and
 * detaches all of them, so `apply` still runs exactly once per effect pass.
 */
function applyWhenStyleReady(map: maplibregl.Map, apply: () => void): () => void {
  let done = false;
  const attempt = () => {
    if (done || !map.isStyleLoaded()) return;
    done = true;
    apply();
  };
  map.on('styledata', attempt);
  map.on('sourcedata', attempt);
  map.on('idle', attempt);
  map.once('load', attempt);
  attempt();
  return () => {
    map.off('styledata', attempt);
    map.off('sourcedata', attempt);
    map.off('idle', attempt);
    map.off('load', attempt);
  };
}

export default function SegmentRouteMap({
  coords,
  styleId,
  token,
  height,
  ariaLabel,
  gates,
  overlayCoords,
  fitKey,
  centerOn,
  centerZoom,
  pickMarkers,
  arrows,
  className = '',
}: SegmentRouteMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const fittedRef = useRef<{ map: maplibregl.Map | null; key: unknown } | null>(null);
  /**
   * Zoom latch for the gate views (T25): the instance whose fixed `centerZoom`
   * has already been applied. Later gate moves re-center only, so a manual zoom
   * survives; a new map instance (basemap/style swap re-creates it) re-applies
   * `centerZoom`.
   */
  const zoomSetRef = useRef<maplibregl.Map | null>(null);
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

      // Optional comparison track (candidate effort): SOLID, exactly like the
      // route line — OQ-5 retired the solid/dashed convention, so colour tells
      // the two series apart and the glyph shapes carry direction (AC-6).
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
          paint: { 'line-color': overlayColor, 'line-width': 3 },
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

      // Framing (T15/T16): gate views pass `centerOn` and track the gate point
      // at a fixed zoom; everything else fits bounds. With a `fitKey`, bounds
      // are fitted only when the key changes or the map instance is new (style
      // rebuild re-creates the map) — gate-meter changes slice the same route
      // and must not reframe the map. No fitKey = legacy behaviour (refit on
      // every coords change).
      if (centerOn && centerOn.length === 2) {
        map.setCenter([centerOn[0], centerOn[1]]);
        if (centerZoom != null && zoomSetRef.current !== map) {
          // T25: apply the fixed zoom once per map instance. Re-applying it on
          // every gate move would clobber whatever zoom the user picked.
          map.setZoom(centerZoom);
        }
        zoomSetRef.current = map;
        fittedRef.current = { map, key: fitKey };
        return;
      }
      const mapIsNew = fittedRef.current?.map !== map;
      const keyChanged = fittedRef.current !== null && fittedRef.current.key !== fitKey;
      const shouldFit = fitKey === undefined || mapIsNew || keyChanged;
      fittedRef.current = { map, key: fitKey };
      if (!shouldFit) return;
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

    return applyWhenStyleReady(map, apply);
  }, [coords, gates?.start, gates?.end, styleId, token, themeVersion, overlayCoords, centerOn, centerZoom, fitKey]);

  // Picked-distance markers (T40, AC-27; halo ring added by 009-010 T05, AC-3):
  // one source and a halo + marker layer pair per series, fed from `pickMarkers`
  // (no framing, no refit — picking must not move the map). Layers are created
  // once per map instance/style and then updated in place, exactly like the gate
  // points; a caller that never passes the prop gains no layers at all. Paint is
  // re-applied on every pass so a theme change recolours halo and marker together.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || typeof window === 'undefined' || !pickMarkers) return;
    const apply = () => {
      const isDark = isDarkTheme();
      PICK_MARKER_LAYERS.forEach(({ id, series, colorToken, radius, strokeWidth, haloRadius }) => {
        const color = tokenRgb(colorToken, isDark);
        const stroke = tokenRgb('--text', isDark);
        const haloId = `${id}-halo`;
        const fc: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: pickMarkers
            .filter((m) => m.series === series)
            .map((m) => ({
              type: 'Feature',
              properties: { label: m.label },
              geometry: { type: 'Point', coordinates: m.point },
            })),
        };
        const src = map.getSource(id) as maplibregl.GeoJSONSource | undefined;
        if (src) {
          src.setData(fc);
          map.setPaintProperty(haloId, 'circle-color', color);
          map.setPaintProperty(haloId, 'circle-stroke-color', color);
          map.setPaintProperty(id, 'circle-color', color);
          map.setPaintProperty(id, 'circle-stroke-color', stroke);
          return;
        }
        map.addSource(id, { type: 'geojson', data: fc });
        // Halo first, so the solid marker always draws on top of its own ring.
        map.addLayer({
          id: haloId,
          type: 'circle',
          source: id,
          paint: {
            'circle-radius': haloRadius,
            'circle-color': color,
            'circle-opacity': 0.25,
            'circle-stroke-color': color,
            'circle-stroke-width': 2,
            'circle-stroke-opacity': 0.9,
          },
        });
        map.addLayer({
          id,
          type: 'circle',
          source: id,
          paint: {
            'circle-radius': radius,
            'circle-color': color,
            'circle-stroke-color': stroke,
            'circle-stroke-width': strokeWidth,
          },
        });
      });
    };
    return applyWhenStyleReady(map, apply);
  }, [pickMarkers, styleId, token, themeVersion]);

  // Directional arrows (009-010 T03, FR-3): one `symbol` layer per series, fed
  // from its own GeoJSON point source whose features carry `bearing`. The glyph
  // is a runtime-generated image (this style declares no `glyphs`, so a
  // text layer could not render) and `icon-rotation-alignment: 'map'` keeps it
  // ground-referenced, so an arrow stays correctly oriented when the map is
  // rotated. Its own effect, not the route/gate apply pass: that pass owns
  // framing, and arrow changes (a candidate being selected/deselected) must not
  // reframe the map or re-slice the route. Additive only — a caller passing no
  // `arrows` gains no layer, so existing callers' layer sets are unchanged —
  // and one glyph per spacing interval keeps the icon count proportional to
  // path length (AC-7). Declared after the route/gate pass so a fresh instance
  // receives these layers last. Runs on style/token/theme change too, which is
  // how a rebuilt map instance (basemap/style swap) or a theme toggle
  // re-registers the icons, since images are style-scoped and colour-baked.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || typeof window === 'undefined' || !arrows) return;
    const apply = () => {
      const isDark = isDarkTheme();
      ARROW_LAYERS.forEach(({ id, series, colorToken, shape }) => {
        const fc: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: arrows
            .filter((a) => a.series === series)
            .map((a) => ({
              type: 'Feature' as const,
              properties: { bearing: a.bearing },
              geometry: { type: 'Point' as const, coordinates: a.lngLat },
            })),
        };
        const src = map.getSource(id) as maplibregl.GeoJSONSource | undefined;
        if (src) {
          src.setData(fc);
        } else if (fc.features.length > 0) {
          map.addSource(id, { type: 'geojson', data: fc });
        } else {
          return; // no glyphs on this series = no layer at all
        }
        // (Re-)register the icon in the current theme's colour before the layer
        // that references it, replacing any previous mask rather than skipping.
        const iconId = `${id}-icon`;
        if (map.hasImage(iconId)) map.removeImage(iconId);
        map.addImage(
          iconId,
          arrowIconMask(
            ARROW_ICON_SIZE,
            cssColorToRgba(tokenRgb(colorToken, isDark)),
            shape,
            // Halo: theme-aware and baked into the same mask, so a glyph stays
            // separable from its neighbour at no extra draw cost (AC-6b).
            cssColorToRgba(tokenRgb(ARROW_HALO_TOKEN, isDark)),
          ),
        );
        if (!src) {
          map.addLayer({
            id,
            type: 'symbol',
            source: id,
            layout: {
              'icon-image': iconId,
              'icon-rotate': ['get', 'bearing'],
              'icon-rotation-alignment': 'map',
              'icon-ignore-placement': true,
              'icon-allow-overlap': true,
            },
          });
        }
      });
    };
    return applyWhenStyleReady(map, apply);
  }, [arrows, styleId, token, themeVersion]);

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