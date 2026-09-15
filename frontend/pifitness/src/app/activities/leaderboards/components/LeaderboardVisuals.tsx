'use client';

import { useEffect, useRef, useState, useMemo } from 'react';
import maplibregl, {
  type StyleSpecification,
} from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  type ChartDataset,
  type ChartOptions,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { API } from '@/lib/api-client';
import { useViewportStore } from '@/stores/viewportStore';
import {
  SegmentVisuals,
  SegmentVisualSeriesPoint,
} from '@/lib/types/leaderboards';

ChartJS.register(LinearScale, PointElement, LineElement, Tooltip, Legend);

/** Speed selector levels; slower = longer pass, faster = shorter pass. */
type Speed = 'slow' | 'normal' | 'fast' | 'turbo';
const SPEED_FACTOR: Record<Speed, number> = { slow: 0.5, normal: 1, fast: 2, turbo: 4 };
const BASE_LOOP_S = 12; // seconds for one full pass at normal speed
const SPEED_LABELS: { value: Speed; label: string }[] = [
  { value: 'slow', label: 'Slow' },
  { value: 'normal', label: 'Normal' },
  { value: 'fast', label: 'Fast' },
  { value: 'turbo', label: 'Turbo' },
];

type MapStyleId =
  | 'positron' | 'osm' | 'darkmatter'
  | 'mb-basic' | 'mb-streets' | 'mb-outdoors' | 'mb-light' | 'mb-dark' | 'mb-satellite' | 'mb-sat-streets'
  | 'blank';

interface StyleDef { id: MapStyleId; label: string; url?: string; darkBasemap: boolean; }

/**
 * Map-style dropdown hierarchy (OQ-3): free OSM/Carto tile styles are always
 * available; Mapbox styles (incl. satellite variants) are appended only when a
 * Mapbox token is configured; the blank vector style (009-007) is offered only
 * as an explicit user option.
 */
function buildStyleOptions(token: string | null): StyleDef[] {
  const free: StyleDef[] = [
    { id: 'positron', label: 'Positron', url: 'https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', darkBasemap: false },
    { id: 'osm', label: 'OpenStreetMap', url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', darkBasemap: false },
    { id: 'darkmatter', label: 'Dark Matter', url: 'https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png', darkBasemap: true },
  ];
  const mapbox: StyleDef[] = [];
  if (token) {
    const mb = (id: MapStyleId, label: string, mbid: string, dark: boolean): StyleDef => ({
      id,
      label,
      darkBasemap: dark,
      url: `https://api.mapbox.com/styles/v1/mapbox/${mbid}-v11/tiles/256/{z}/{x}/{y}?access_token=${encodeURIComponent(token)}`,
    });
    mapbox.push(
      mb('mb-basic', 'MB Basic', 'basic', false),
      mb('mb-streets', 'MB Streets', 'streets', false),
      mb('mb-outdoors', 'MB Outdoors', 'outdoors', false),
      mb('mb-light', 'MB Light', 'light', false),
      mb('mb-dark', 'MB Dark', 'dark', true),
      mb('mb-satellite', 'Satellite', 'satellite', true),
      mb('mb-sat-streets', 'Sat + Streets', 'satellite-streets', false),
    );
  }
  const blank: StyleDef[] = [{ id: 'blank', label: 'Blank (Vector)', darkBasemap: false }];
  return [...free, ...mapbox, ...blank];
}

function styleById(id: MapStyleId, token: string | null): StyleDef {
  const options = buildStyleOptions(token);
  return options.find((s) => s.id === id) ?? options[0];
}

/** SSR-safe design-token read with distinct light/dark fallbacks. */
function tokenRgb(name: string, isDark: boolean): string {
  const fallback =
    name === '--text' ? (isDark ? '241 245 249' : '15 23 42')
    : name === '--muted' ? (isDark ? '148 163 184' : '107 114 128')
    : name === '--grid' ? (isDark ? '51 65 85' : '226 232 240')
    : name === '--chart-1' ? (isDark ? '96 165 250' : '0 114 178')
    : name === '--chart-2' ? (isDark ? '251 146 60' : '213 94 0')
    : name === '--chart-3' ? (isDark ? '52 211 153' : '0 158 115')
    : name === '--chart-4' ? (isDark ? '240 171 252' : '204 121 167')
    : name === '--chart-5' ? (isDark ? '253 224 71' : '230 159 0')
    : (isDark ? '125 211 252' : '86 180 233'); // --chart-6
  if (typeof window === 'undefined') return `rgb(${fallback})`;
  const value = window.getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value ? `rgb(${value})` : `rgb(${fallback})`;
}

/** Increment a version when the `dark` class on <html> toggles (theme change). */
function useThemeVersion(): number {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const observer = new MutationObserver(() => setVersion((v) => v + 1));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);
  return version;
}

/** Keep only valid [lon, lat] pairs. */
function validCoords(path: number[][] | undefined): number[][] {
  return (path || []).filter(
    (p) => Array.isArray(p) && p.length >= 2 && isFinite(p[0]) && isFinite(p[1]),
  );
}

/** Interpolate the LineString up to fraction `p` (0..1) of its length. */
function progressPath(coords: number[][], p: number): number[][] {
  if (!coords.length) return [];
  if (coords.length === 1) return [coords[0]];
  if (p <= 0) return [coords[0]];
  if (p >= 1) return coords;
  const seg: number[] = [];
  let total = 0;
  for (let i = 1; i < coords.length; i++) {
    const dx = coords[i][0] - coords[i - 1][0];
    const dy = coords[i][1] - coords[i - 1][1];
    const d = Math.sqrt(dx * dx + dy * dy);
    seg.push(d);
    total += d;
  }
  if (total <= 0) return [coords[0]];
  const target = p * total;
  let acc = 0;
  const out: number[][] = [coords[0]];
  for (let i = 1; i < coords.length; i++) {
    const d = seg[i - 1];
    if (acc + d >= target) {
      const t = d > 0 ? (target - acc) / d : 0;
      out.push([
        coords[i - 1][0] + (coords[i][0] - coords[i - 1][0]) * t,
        coords[i - 1][1] + (coords[i][1] - coords[i - 1][1]) * t,
      ]);
      break;
    }
    acc += d;
    out.push(coords[i]);
  }
  return out;
}

/** Telemetry axes rendered as overlay line charts (pre-aggregated series). */
const AXES: { key: string; label: string; val: (p: SegmentVisualSeriesPoint) => number | null }[] = [
  { key: 'elevation_m', label: 'Elevation (m)', val: (p) => p.elevation_m },
  { key: 'heartrate_bpm', label: 'Heart Rate (bpm)', val: (p) => p.heartrate_bpm },
  { key: 'cadence_spm', label: 'Cadence (spm)', val: (p) => p.cadence_spm },
  { key: 'speed', label: 'Speed (km/h)', val: (p) => (p.speed_mmps == null ? null : p.speed_mmps * 0.0036) },
];

interface LeaderboardVisualsProps { visuals: SegmentVisuals; }

export default function LeaderboardVisuals({ visuals }: LeaderboardVisualsProps) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';
  const themeVersion = useThemeVersion();
  void themeVersion; // tie chart/map colors to the exposed theme cycle

  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const coordsRef = useRef<number[][][]>(visuals.efforts.map((e) => validCoords(e.path_coords)));
  const progRefs = useRef<(maplibregl.GeoJSONSource | null)[]>([]);
  const progressRef = useRef(0);

  const [mapStyle, setMapStyle] = useState<MapStyleId>('positron');
  const [speed, setSpeed] = useState<Speed>('normal');
  const [playing, setPlaying] = useState(false);
  const [progress, setProgress] = useState(0);
  const [mapboxToken, setMapboxToken] = useState<string | null>(null);
  const [reducedMotion, setReducedMotion] = useState(false);

  // Client-safe map config — Mapbox token presence gates Mapbox styles (AC-13).
  // Fetched once and cached; on failure it degrades to the free styles only.
  useEffect(() => {
    let active = true;
    API.config
      .getMapConfig()
      .then((c) => { if (active) setMapboxToken(c.mapboxToken || null); })
      .catch(() => { if (active) setMapboxToken(null); });
    return () => { active = false; };
  }, []);

  // Honor prefers-reduced-motion: never auto-animate; show the full routes.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    setReducedMotion(window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  }, []);

  // Rebuild the map + basemap on style change (or token resolution). Only the
  // basemap lives here; routes are (re)built by the next effect.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (!containerRef.current) return;
    const def = styleById(mapStyle, mapboxToken);
    const styleSpec: StyleSpecification = def.url
      ? {
          version: 8,
          sources: { basemap: { type: 'raster', tiles: [def.url] } },
          layers: [{ id: 'basemap', type: 'raster', source: 'basemap', minzoom: 0, maxzoom: 22 }],
        }
      : { version: 8, sources: {}, layers: [] };
    const map = new maplibregl.Map({ container: containerRef.current, style: styleSpec });
    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
      progRefs.current = [];
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapStyle, mapboxToken]);

// (Re)build route layers + colors on style / theme / visuals changes. On the
  // style-change path the map above was freshly created, so this adds sources;
  // on the theme-change path the same map persists and only paints are updated.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || typeof window === 'undefined') return;
    const isDark = document.documentElement.classList.contains('dark');
    const apply = () => {
      const allCoords: number[][] = [];
      visuals.efforts.forEach((eff, i) => {
        const coords = coordsRef.current[i] ?? [];
        allCoords.push(...coords);
        const bright = tokenRgb(`--chart-${(i % 6) + 1}`, isDark);
        const dim = tokenRgb('--muted', isDark);

        const fcFull: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: coords.length
            ? [{ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: coords } }]
            : [],
        };
        const srcFull = map.getSource(`full-${i}`);
        if (srcFull) {
          (srcFull as maplibregl.GeoJSONSource).setData(fcFull);
        } else {
          map.addSource(`full-${i}`, { type: 'geojson', data: fcFull });
          map.addLayer({
            id: `full-${i}`,
            type: 'line',
            source: `full-${i}`,
            paint: { 'line-color': dim, 'line-width': 2.5, 'line-opacity': 0.5 },
          });
        }
        map.setPaintProperty(`full-${i}`, 'line-color', dim);
        map.setPaintProperty(`full-${i}`, 'line-opacity', 0.5);

        const fcProg: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: coords.length
            ? [{ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: [coords[0]] } }]
            : [],
        };
        let srcProg = map.getSource(`prog-${i}`) as maplibregl.GeoJSONSource | undefined;
        if (srcProg) {
          srcProg.setData(fcProg);
        } else {
          map.addSource(`prog-${i}`, { type: 'geojson', data: fcProg });
          map.addLayer({
            id: `prog-${i}`,
            type: 'line',
            source: `prog-${i}`,
            paint: {
              'line-color': bright,
              'line-width': 4,
            },
          });
          srcProg = map.getSource(`prog-${i}`) as maplibregl.GeoJSONSource;
        }
        map.setPaintProperty(`prog-${i}`, 'line-color', bright);
        progRefs.current[i] = srcProg;
      });

      if (allCoords.length > 1) {
        const bounds = allCoords.reduce(
          (b, c) => b.extend(c as [number, number]),
          new maplibregl.LngLatBounds(allCoords[0] as [number, number], allCoords[0] as [number, number]),
        );
        map.fitBounds(bounds, { padding: 16, animate: false });
      }
      map.resize();
    };
    if (map.loaded()) apply();
    else map.once('load', apply);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapStyle, mapboxToken, themeVersion, visuals]);

// Animation loop: advances progress by elapsed time scaled by the speed
  // selector and pushes each effort's interpolated route prefix to the map.
  useEffect(() => {
    if (!playing || reducedMotion) return;
    if (typeof window === 'undefined') return;
    const factor = SPEED_FACTOR[speed];
    let raf = 0;
    let last = performance.now();
    const step = (now: number) => {
      const dt = Math.min(now - last, 100);
      last = now;
      let p = progressRef.current + (dt / 1000) / BASE_LOOP_S * factor;
      if (p >= 1) p -= Math.floor(p);
      progressRef.current = p;
      setProgress(p);
      coordsRef.current.forEach((coords, i) => {
        const src = progRefs.current[i];
        if (src && coords.length && p > 0) {
          src.setData({
            type: 'FeatureCollection',
            features: [
              {
                type: 'Feature',
                properties: {},
                geometry: { type: 'LineString', coordinates: progressPath(coords, p) },
              },
            ],
          } as GeoJSON.FeatureCollection);
        }
      });
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [playing, speed, reducedMotion]);

  const isDark =
    typeof window !== 'undefined' && document.documentElement.classList.contains('dark');

  const lineOptions = (label: string): ChartOptions<'line'> => ({
    responsive: true,
    animation: false,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true,
        labels: { color: tokenRgb('--text', isDark), boxWidth: 12, font: { size: 11 } },
      },
    },
    scales: {
      x: {
        type: 'linear',
        title: { display: true, text: 'Elapsed (s)', color: tokenRgb('--muted', isDark) },
        grid: { color: tokenRgb('--grid', isDark) },
        ticks: { color: tokenRgb('--muted', isDark) },
      },
      y: {
        title: { display: true, text: label, color: tokenRgb('--muted', isDark) },
        grid: { color: tokenRgb('--grid', isDark) },
        ticks: { color: tokenRgb('--muted', isDark) },
      },
    },
  });

  // Memoized on [visuals, isDark]: the animation loop re-renders every frame
  // (progress bar), and we must NOT rebuild chart datasets/options each frame.
  // Reusing the same objects means react-chartjs-2 won't churn the charts.
  const chartCards = useMemo(
    () =>
      AXES.map((axis) => {
        const datasets: ChartDataset<'line', { x: number; y: number }[]>[] = visuals.efforts.map((eff, i) => {
          const color = tokenRgb(`--chart-${(i % 6) + 1}`, isDark);
          const pts = eff.series
            .map((p) => ({ x: p.elapsed_s, y: axis.val(p) }))
            .filter((pt): pt is { x: number; y: number } => pt.y != null);
          return {
            label: eff.id_val,
            data: pts,
            borderColor: color,
            backgroundColor: color,
            borderWidth: 1.5,
            pointRadius: 0,
            pointHoverRadius: 3,
            fill: false,
            tension: 0.15,
            borderDash: i % 2 === 1 ? [6, 3] : undefined,
          };
        });
        return { key: axis.key, label: axis.label, datasets, options: lineOptions(axis.label) };
      }),
    [visuals, isDark],
  );

  const mapHeight = isDesktop ? 380 : isLandscape ? 230 : 300;

return (
    <div className="space-y-4">
      {/* Controls: style dropdown + speed + play/pause + progress */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
        <div className="flex flex-wrap items-center gap-3">
          <div>
            <label htmlFor="lv-style" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Map Style</label>
            <select
              id="lv-style"
              value={mapStyle}
              onChange={(e) => { setMapStyle(e.target.value as MapStyleId); setProgress(0); progressRef.current = 0; setPlaying(false); }}
              className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {buildStyleOptions(mapboxToken).map((s) => (<option key={s.id} value={s.id}>{s.label}</option>))}
            </select>
          </div>
          <div>
            <span className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Speed</span>
            <div className="flex rounded-md bg-gray-100 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 overflow-hidden" role="group" aria-label="Replay speed">
              {SPEED_LABELS.map((s) => (
                <button
                  key={s.value}
                  type="button"
                  aria-pressed={speed === s.value}
                  onClick={() => setSpeed(s.value)}
                  className={`px-2.5 py-1.5 text-xs font-medium ${speed === s.value ? 'bg-blue-600 text-white' : 'text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>
          <div>
            <span className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">&nbsp;</span>
            <button
              type="button"
              onClick={() => setPlaying(!playing)}
              disabled={reducedMotion}
              aria-label={playing ? 'Pause replay' : 'Play replay'}
              className="px-3 py-1.5 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {playing ? 'Pause' : 'Play'}
            </button>
          </div>
          <div className="flex-1 min-w-[8rem]">
            <div
              className="h-2 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden"
              role="progressbar"
              aria-valuenow={Math.round(progress * 100)}
              aria-valuemin={0}
              aria-valuemax={100}
              aria-label="Replay progress"
            >
              <div className="h-full bg-blue-600" style={{ width: `${Math.round(progress * 100)}%` }} />
            </div>
          </div>
        </div>
        {reducedMotion && (
          <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
            Reduced motion is enabled — showing the full routes without auto-animation.
          </p>
        )}
      </div>

      {/* Map / replay */}
      <div
        ref={containerRef}
        className="w-full rounded-md overflow-hidden border border-gray-200 dark:border-gray-700"
        style={{ height: mapHeight }}
        role="img"
        aria-label="Effort replay map"
      />

      {/* Telemetry charts */}
      {chartCards.length > 0 && (
        <div className={`grid gap-4 ${isDesktop || isLandscape ? 'md:grid-cols-2' : ''}`}>
          {chartCards.map(({ options, key, label, datasets }) => (
            <div key={key} className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
              <h4 className="text-sm font-semibold text-gray-900 dark:text-white">{label}</h4>
              <div className="w-full" style={{ height: 170 }}>
                <Line data={{ datasets }} options={options} />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}