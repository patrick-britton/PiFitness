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
  SegmentVisualEffort,
  SegmentVisuals,
  SegmentVisualSeriesPoint,
} from '@/lib/types/leaderboards';
import {
  distanceAtTimeMiles,
  markerToleranceMiles,
  milesOf,
} from '@/lib/race-axis';
import { tokenRgb } from '@/lib/token-color';

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
    // Mapbox Satellite is the one default style still on v9 — `satellite-v11`
    // does not exist and every tile request 404s ("Style not found"), which
    // rendered the Satellite basemap blank (Bug 009-009-T09-1, live-probed
    // 2026-09-15: satellite-v11 → 404, satellite-v9 → 200 image/jpeg).
    const mb = (id: MapStyleId, label: string, mbid: string, dark: boolean, version = 'v11'): StyleDef => ({
      id,
      label,
      darkBasemap: dark,
      url: `https://api.mapbox.com/styles/v1/mapbox/${mbid}-${version}/tiles/256/{z}/{x}/{y}?access_token=${encodeURIComponent(token)}`,
    });
    mapbox.push(
      mb('mb-basic', 'MB Basic', 'basic', false),
      mb('mb-streets', 'MB Streets', 'streets', false),
      mb('mb-outdoors', 'MB Outdoors', 'outdoors', false),
      mb('mb-light', 'MB Light', 'light', false),
      mb('mb-dark', 'MB Dark', 'dark', true),
      mb('mb-satellite', 'Satellite', 'satellite', true, 'v9'),
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

/** Per-racer race model built from the aggregated per-second series (T08). */
interface RacerModel {
  /** Per-second [lon, lat] — path_coords[i] pairs with series[i]. */
  path: number[][];
  /** Per-second own elapsed seconds (series.elapsed_s — the racer's OWN time). */
  times: number[];
  /** Per-second normalized distance in meters (series.distance_m). */
  dists: number[];
  /** Racer's finish time (own seconds). */
  maxT: number;
  /**
   * Marker match tolerance in MILES for this racer's own series (±1.5 s of own
   * mean pace — T16: the charts' x axis is distance in miles, so the play
   * markers are matched in miles rather than seconds).
   */
  tolMi: number;
  label: string;
}

/** Build one racer's race model from a selected effort (path + own series). */
function buildRacer(eff: SegmentVisualEffort): RacerModel {
  const path = validCoords(eff.path_coords);
  const times = eff.series.map((p) => p.elapsed_s);
  const dists = eff.series.map((p) => p.distance_m);
  return {
    path,
    times,
    dists,
    maxT: times.length ? times[times.length - 1] : 0,
    tolMi: markerToleranceMiles(times, dists),
    label: eff.id_val,
  };
}

/**
 * Last series index whose own elapsed time is <= race-clock t (binary search;
 * series are ordered by elapsed second).
 */
function raceIndex(times: number[], t: number): number {
  let lo = 0;
  let hi = times.length - 1;
  let ans = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (times[mid] <= t) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

/**
 * The racer's own time at (normalized) distance D — null when the racer never
 * covers D (e.g. a slightly shorter resampled tail after the leader finished).
 * dists are monotonically non-decreasing.
 */
function timeAtDistance(dists: number[], times: number[], d: number): number | null {
  let lo = 0;
  let hi = dists.length - 1;
  let ans: number | null = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (dists[mid] <= d) {
      ans = times[mid];
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function lineFC(coords: number[][]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: coords.length
      ? [{ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: coords } }]
      : [],
  };
}

function pointFC(coord: number[] | undefined): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: coord
      ? [{ type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: coord } }]
      : [],
  };
}

/** Minimal HTML escaping for popup content built from backend labels. */
function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c] ?? c));
}

/** Telemetry axes rendered as overlay line charts (pre-aggregated series).
 *  009-009 T09/AC-15: the Speed (km/h) axis is replaced by Pace (min/mile) —
 *  mm/s → min/mile = 1609.344·1000 / (60·mm/s) = 26822.4 / speed_mmps.
 *  009-009 T16/Bug T16-1: every chart's x axis is the racer's own DISTANCE in
 *  miles (`series.distance_m` → mi, title `Distance (mi)`), and the play
 *  markers are matched in the same unit (see `@/lib/race-axis`). */
const AXES: { key: string; label: string; val: (p: SegmentVisualSeriesPoint) => number | null }[] = [
  { key: 'elevation_m', label: 'Elevation (m)', val: (p) => p.elevation_m },
  { key: 'heartrate_bpm', label: 'Heart Rate (bpm)', val: (p) => p.heartrate_bpm },
  { key: 'cadence_spm', label: 'Cadence (spm)', val: (p) => p.cadence_spm },
  { key: 'pace', label: 'Pace (min/mi)', val: (p) => (p.speed_mmps && p.speed_mmps > 0 ? 26822.4 / p.speed_mmps : null) },
];

/** Format minutes (float) as m:ss for the Pace axis ticks. */
function fmtPace(v: number | string): string {
  const n = typeof v === 'number' ? v : Number(v);
  if (!isFinite(n) || n <= 0) return String(v);
  const m = Math.floor(n);
  const s = Math.round((n - m) * 60);
  return s >= 60 ? `${m + 1}:00` : `${m}:${String(s).padStart(2, '0')}`;
}

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
  const markerRefs = useRef<(maplibregl.GeoJSONSource | null)[]>([]);
  const progressRef = useRef(0);
  /** Per-racer race model, mirrored to a ref so the animation loop reads it
   *  without re-subscribing (009-009 T08: race-clock animation). */
  // Built once per `visuals` — the animation loop re-renders every frame, so
  // constructing the models inline here would allocate two arrays per racer per
  // frame (Pi render budget).
  const racerModels = useMemo(() => visuals.efforts.map(buildRacer), [visuals]);
  const racersRef = useRef<RacerModel[]>(racerModels);

  // Keep the map-side race model in sync with new visuals (also re-derives the
  // per-second series when a fresh race is generated).
  useEffect(() => {
    coordsRef.current = visuals.efforts.map((e) => validCoords(e.path_coords));
    racersRef.current = racerModels;
  }, [visuals, racerModels]);

  const [mapStyle, setMapStyle] = useState<MapStyleId>('positron');
  /** Set once the user explicitly picks a style — the theme/token default is
   *  only applied while the choice is still untouched (AC-11). */
  const userChoseStyleRef = useRef(false);
  const [speed, setSpeed] = useState<Speed>('normal');
  const [playing, setPlaying] = useState(false);
  const [progress, setProgress] = useState(0);
  const [mapboxToken, setMapboxToken] = useState<string | null>(null);
  const [reducedMotion, setReducedMotion] = useState(false);

  // Client-safe map config — Mapbox token presence gates Mapbox styles (AC-13).
  // Fetched once and cached; on failure it degrades to the free styles only.
  // On resolution (or absence) the theme-equivalent DEFAULT style is applied
  // once (MB Light/MB Dark with a token, Positron/Dark Matter without — AC-11)
  // unless the user has already picked a style in the meantime.
  useEffect(() => {
    let active = true;
    const applyDefault = (token: string | null) => {
      if (!active || userChoseStyleRef.current) return;
      const dark =
        typeof window !== 'undefined' &&
        document.documentElement.classList.contains('dark');
      setMapStyle(
        token ? (dark ? 'mb-dark' : 'mb-light') : dark ? 'darkmatter' : 'positron',
      );
    };
    API.config
      .getMapConfig()
      .then((c) => { setMapboxToken(c.mapboxToken || null); applyDefault(c.mapboxToken || null); })
      .catch(() => { setMapboxToken(null); applyDefault(null); });
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

        // Forward-most marker (AC-14): a circle at each racer's head that the
        // animation loop moves via this GeoJSON source.
        let srcMarker = map.getSource(`marker-${i}`) as maplibregl.GeoJSONSource | undefined;
        if (!srcMarker) {
          map.addSource(`marker-${i}`, { type: 'geojson', data: pointFC(coords[coords.length - 1]) });
          map.addLayer({
            id: `marker-${i}`,
            type: 'circle',
            source: `marker-${i}`,
            paint: {
              'circle-radius': 6,
              'circle-color': bright,
              'circle-stroke-color': tokenRgb('--text', isDark),
              'circle-stroke-width': 1.5,
            },
          });
          srcMarker = map.getSource(`marker-${i}`) as maplibregl.GeoJSONSource;
        }
        markerRefs.current[i] = srcMarker;
      });

      // Click popups (AC-14): distance + time ahead/behind every other racer at
      // the CURRENT race clock (handlers read live refs). Registered once per
      // map instance; the map persists across theme-change applies.
      type MapWithRegistry = maplibregl.Map & { __lbHandlers?: Set<string> };
      const reg = map as MapWithRegistry;
      const registry = reg.__lbHandlers ?? new Set<string>();
      reg.__lbHandlers = registry;
      visuals.efforts.forEach((_eff, i) => {
        const id = `marker-${i}`;
        if (registry.has(id)) return;
        registry.add(id);
        map.on('click', id, (ev) => {
          ev.preventDefault();
          const t = progressRef.current * Math.max(1, ...racersRef.current.map((r) => r.maxT));
          const me = racersRef.current[i];
          if (!me.path.length) return;
          const myD = me.dists[raceIndex(me.times, t)];
          const mins = Math.floor(t / 60);
          const secs = Math.round(t % 60);
          const rowsHtml = racersRef.current
            .map((r, j) => {
              if (j === i) return `<li><b>${escapeHtml(r.label)}</b> — this marker</li>`;
              const dDiff = myD - r.dists[raceIndex(r.times, t)];
              const tJ = timeAtDistance(r.dists, r.times, myD);
              const tDiff = tJ == null ? null : tJ - t; // + → I reached this distance first
              const dTxt = `${dDiff >= 0 ? '+' : '−'}${Math.abs(dDiff).toFixed(0)}m`;
              const tTxt = tDiff == null ? 'n/a' : `${tDiff >= 0 ? '+' : '−'}${Math.abs(tDiff).toFixed(0)}s`;
              return `<li>${escapeHtml(r.label)}: ${dTxt} / ${tTxt}</li>`;
            })
            .join('');
          new maplibregl.Popup({ closeButton: false, maxWidth: '260px' })
            .setLngLat(ev.lngLat)
            .setHTML(
              `<div style="font-size:12px"><b>${escapeHtml(me.label)}</b><br/>Race clock: ${mins}m ${secs}s` +
              `<ul style="margin:4px 0 0;padding-left:14px">${rowsHtml}</ul>` +
              `<div style="margin-top:4px;opacity:.7">+ ahead / − behind vs each racer</div></div>`,
            )
            .addTo(map);
        });
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
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

// Animation loop (009-009 T08, AC-13; Bug 009-009-T08-1): advances a shared
// RACE CLOCK (0 → slowest finish) scaled by the speed selector and positions
// each racer from their OWN per-second series — faster racers finish earlier
// and hold at the line, so real time gaps are visible. Invariant: race
// position is a function of race time, never of path-length fraction.
  useEffect(() => {
    if (!playing || reducedMotion) return;
    if (typeof window === 'undefined') return;
    const factor = SPEED_FACTOR[speed];
    const T = Math.max(1, ...racersRef.current.map((r) => r.maxT));
    let raf = 0;
    let last = performance.now();
    let lastChart = 0; // chart-marker updates throttled to ~10 Hz (Pi perf)
    const step = (now: number) => {
      const dt = Math.min(now - last, 100);
      last = now;
      let p = progressRef.current + (dt / 1000) / BASE_LOOP_S * factor;
      if (p >= 1) p -= Math.floor(p);
      progressRef.current = p;
      setProgress(p);
      const t = p * T;
      if (now - lastChart > 100) {
        lastChart = now;
        updateChartMarkers(t);
      }
      racersRef.current.forEach((r, i) => {
        if (!r.path.length) return;
        const idx = raceIndex(r.times, t);
        const prefix = r.path.slice(0, idx + 1);
        const src = progRefs.current[i];
        if (src) src.setData(lineFC(prefix));
        const msrc = markerRefs.current[i];
        if (msrc) msrc.setData(pointFC(prefix[prefix.length - 1]));
      });
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => {
      cancelAnimationFrame(raf);
      // Pause leaves the markers parked at the last race-clock position.
      updateChartMarkers(progressRef.current * T);
    };
  }, [playing, speed, reducedMotion]);

  const isDark =
    typeof window !== 'undefined' && document.documentElement.classList.contains('dark');

  // 009-009 T09/AC-15 + T16: per-chart circle markers driven by the shared race
  // clock. The marker position lives in a ref so the memoized datasets/options
  // are NOT rebuilt per frame — chart.update('none') re-resolves the scriptable
  // pointRadius below, which reads that ref. Updates are throttled (~10 Hz) to
  // protect the Pi's render budget.
  const chartRefs = useRef<(ChartJS<'line'> | null)[]>([]);
  /** Per-racer marker position in MILES at the current race-clock time. */
  const markerDistRef = useRef<number[]>([]);
  const updateChartMarkers = (t: number) => {
    // Bug 009-009-T16-1: the x axis is distance, so the shared race clock is
    // remapped into each racer's own distance at time t (one O(log n) lookup
    // per racer, not per point) before the scriptable reads it.
    markerDistRef.current = racersRef.current.map((r) =>
      distanceAtTimeMiles(r.times, r.dists, t),
    );
    chartRefs.current.forEach((c) => { if (c) c.update('none'); });
  };

  const lineOptions = (axisKey: string, label: string): ChartOptions<'line'> => ({
    responsive: true,
    animation: false,
    maintainAspectRatio: false,
    // One shared legend is rendered outside the chart cards (AC-15).
    plugins: {
      legend: {
        display: false,
        labels: { color: tokenRgb('--text', isDark), boxWidth: 12, font: { size: 11 } },
      },
    },
    scales: {
      x: {
        type: 'linear',
        title: { display: true, text: 'Distance (mi)', color: tokenRgb('--muted', isDark) },
        grid: { color: tokenRgb('--grid', isDark) },
        ticks: { color: tokenRgb('--muted', isDark) },
      },
      y: {
        title: { display: true, text: label, color: tokenRgb('--muted', isDark) },
        grid: { color: tokenRgb('--grid', isDark) },
        ticks: {
          color: tokenRgb('--muted', isDark),
          ...(axisKey === 'pace' ? { callback: fmtPace } : {}),
        },
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
            .map((p) => ({ x: milesOf(p.distance_m), y: axis.val(p) }))
            .filter((pt): pt is { x: number; y: number } => pt.y != null);
          return {
            label: eff.id_val,
            data: pts,
            borderColor: color,
            backgroundColor: color,
            borderWidth: 1.5,
            pointRadius: (ctx) => {
              const x = (ctx.parsed as { x: number } | undefined)?.x;
              if (x == null) return 0;
              // Bug 009-009-T16-1: the axis is miles, so match THIS racer's
              // distance at the current race time, with a ±1.5 s-of-own-pace
              // tolerance expressed in miles.
              const target = markerDistRef.current[ctx.datasetIndex];
              const tol = racersRef.current[ctx.datasetIndex]?.tolMi ?? 0;
              return target != null && tol > 0 && Math.abs(x - target) <= tol ? 4 : 0;
            },
            pointHoverRadius: 4,
            fill: false,
            tension: 0.15,
            borderDash: i % 2 === 1 ? [6, 3] : undefined,
          };
        });
        return { key: axis.key, label: axis.label, datasets, options: lineOptions(axis.key, axis.label) };
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [visuals, isDark],
  );

  // Keep the play markers in step with the current progress whenever the charts
  // (re)mount or re-theme — so a paused replay keeps its markers visible.
  useEffect(() => {
    const T = Math.max(1, ...racersRef.current.map((r) => r.maxT));
    updateChartMarkers(progressRef.current * T);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chartCards]);

  // Shared legend items (AC-15): one legend above the chart grid, derived from
  // the first chart's datasets (all four share the same per-effort styling).
  const legendItems = chartCards[0]
    ? chartCards[0].datasets.map((d) => ({
        label: d.label,
        color: d.borderColor as string,
        dashed: Array.isArray(d.borderDash),
      }))
    : [];

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
              onChange={(e) => { userChoseStyleRef.current = true; setMapStyle(e.target.value as MapStyleId); setProgress(0); progressRef.current = 0; setPlaying(false); }}
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

      {/* Telemetry charts — one shared legend rendered OUTSIDE the cards (AC-15) */}
      {chartCards.length > 0 && legendItems.length > 0 && (
        <div
          className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3 flex flex-wrap gap-x-6 gap-y-2"
          role="list"
          aria-label="Effort legend"
        >
          {legendItems.map((it) => (
            <span
              key={it.label}
              role="listitem"
              className="flex items-center gap-2 text-xs text-gray-900 dark:text-white"
            >
              <svg width="26" height="8" aria-hidden="true" focusable="false">
                <line
                  x1="0" y1="4" x2="26" y2="4"
                  stroke={it.color}
                  strokeWidth="2.5"
                  strokeDasharray={it.dashed ? '6 3' : undefined}
                />
              </svg>
              {it.label}
            </span>
          ))}
        </div>
      )}
      {chartCards.length > 0 && (
        <div className={`grid gap-4 ${isDesktop || isLandscape ? 'md:grid-cols-2' : ''}`}>
          {chartCards.map(({ options, key, label, datasets }, i) => (
            <div key={key} className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
              <h4 className="text-sm font-semibold text-gray-900 dark:text-white">{label}</h4>
              <div className="w-full" style={{ height: 170 }}>
                <Line
                  data={{ datasets }}
                  options={options}
                  ref={(el) => { chartRefs.current[i] = el ?? null; }}
                />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}