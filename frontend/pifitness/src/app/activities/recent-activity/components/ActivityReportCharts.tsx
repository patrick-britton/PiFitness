/**
 * Activity Report Charts (009-006, T05).
 *
 * Chart.js (react-chartjs-2) plots fed by the pre-aggregated per-minute series:
 *   - Elevation: bare area chart, no labels/gridlines/titles/axis lines.
 *   - Heart rate: red line with thin axes, y = min−10 .. max+10 bpm.
 *   - Pace: green line, same style, y = numeric seconds/mile.
 *
 * Colors come from design-system tokens via getComputedStyle (light/dark) and are
 * rebuilt on theme change (MutationObserver), matching the TriTipChart/DbSizeView
 * pattern. The frontend consumes pre-aggregated data only (no pandas).
 */
'use client';

import { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
  type ChartDataset,
  type ChartOptions,
  type Plugin,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import {
  ActivityElevationPoint,
  ActivityHeartratePoint,
  ActivityPacePoint,
} from '@/lib/types/activity-report';

ChartJS.register(LinearScale, PointElement, LineElement, Filler, Tooltip, Legend);

type XY = { x: number; y: number };

/** SSR-safe token read with distinct light/dark fallbacks (pattern from TriTipChart). */
function tokenRgb(name: string, isDark: boolean): string {
  const fallback =
    name === '--text'
      ? isDark ? '241 245 249' : '15 23 42'
      : name === '--danger'
        ? isDark ? '248 113 113' : '220 38 38'
        : name === '--ok'
          ? isDark ? '74 222 128' : '22 163 74'
          : name === '--chart-1'
            ? isDark ? '96 165 250' : '0 114 178'
            : name === '--grid'
              ? isDark ? '51 65 85' : '226 232 240'
              : isDark ? '148 163 184' : '107 114 128'; // --muted
  if (typeof window === 'undefined') return `rgb(${fallback})`;
  const value = window
    .getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
  return value ? `rgb(${value})` : `rgb(${fallback})`;
}

/** Increment a version when the `dark` class on <html> toggles (theme change). */
function useThemeVersion(): number {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const observer = new MutationObserver(() => setVersion((v) => v + 1));
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });
    return () => observer.disconnect();
  }, []);
  return version;
}

/** [min, max] over a 1-D point array, or null when empty. */
function range(points: XY[]): [number, number] | null {
  if (!points.length) return null;
  let mn = Infinity;
  let mx = -Infinity;
  for (const p of points) {
    if (p.y < mn) mn = p.y;
    if (p.y > mx) mx = p.y;
  }
  return mn === Infinity ? null : [mn, mx];
}

/** Median of a numeric array (no pandas). */
function median(vals: number[]): number {
  if (!vals.length) return 0;
  const s = [...vals].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/**
 * Inline Chart.js plugin: faint horizontal reference lines with far-left labels.
 * Replaces chartjs-plugin-annotation (not installed). Each entry:
 *   { value, label, color } → dashed line across the chart area + label text.
 */
function referenceLinesPlugin(lines: { value: number; label: string; color: string }[]): Plugin<'line'> {
  return {
    id: 'referenceLines',
    afterDatasetsDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      if (!chartArea) return;
      ctx.save();
      ctx.font = '11px sans-serif';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      for (const ln of lines) {
        const y = scales.y.getPixelForValue(ln.value);
        if (y < chartArea.top - 2 || y > chartArea.bottom + 2) continue;
        ctx.beginPath();
        ctx.setLineDash([4, 4]);
        ctx.strokeStyle = ln.color;
        ctx.lineWidth = 1;
        ctx.globalAlpha = 0.35;
        ctx.moveTo(chartArea.left, y);
        ctx.lineTo(chartArea.right, y);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.globalAlpha = 0.7;
        ctx.fillStyle = ln.color;
        ctx.fillText(ln.label, chartArea.left + 4, y - 7);
      }
      ctx.restore();
    },
  };
}

/**
 * Elevation area chart: uncolored, no labels/gridlines/titles/axis lines.
 * x = elapsed minute 0→max; y = min−10 → max+10 m. Fixed height/width.
 */
export function ElevationChart({ points }: { points: ActivityElevationPoint[] }) {
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  const data = points.map((p) => ({ x: p.minute, y: p.elevation_m }));
  const [mn, mx] = range(data) ?? [0, 0];
  const lineColor = tokenRgb('--muted', isDark);
  const fillColor = isDark ? 'rgba(148,163,184,0.25)' : 'rgba(107,114,128,0.25)';
  void themeVersion; // participate so colors rebuild on toggle

  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    scales: {
      x: { type: 'linear', display: false, min: 0, ticks: { display: false }, grid: { display: false } },
      y: { type: 'linear', display: false, min: mn - 10, max: mx + 10, ticks: { display: false }, grid: { display: false } },
    },
  };
  const dataset: ChartDataset<'line', XY[]> = {
    label: 'Elevation',
    data,
    borderColor: lineColor,
    backgroundColor: fillColor,
    fill: 'origin',
    borderWidth: 1,
    pointRadius: 0,
    pointHoverRadius: 0,
    tension: 0.2,
  };

  return (
    <div
      className="h-full w-full"
      aria-label="Elevation profile"
      role="img"
    >
      <Line data={{ datasets: [dataset] }} options={options} />
    </div>
  );
}
/** Filter consecutive near-duplicate points (GPS jitter reduction). */
function filterNearDuplicates(
  data: XY[],
  tol = 1e-6,
): XY[] {
  if (data.length < 2) return data;
  const out: XY[] = [data[0]];
  for (let i = 1; i < data.length; i++) {
    const prev = out[out.length - 1];
    if (
      Math.abs(data[i].x - prev.x) > tol ||
      Math.abs(data[i].y - prev.y) > tol
    ) {
      out.push(data[i]);
    }
  }
  return out;
}

/** Shared line-chart options builder: no gridlines, no ticks, no axis labels. */
function lineOptions(
  yRange: [number, number] | null,
): ChartOptions<'line'> {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    scales: {
      x: {
        type: 'linear',
        min: 0,
        ticks: { display: false },
        grid: { display: false },
      },
      y: {
        type: 'linear',
        min: yRange ? yRange[0] - 10 : undefined,
        max: yRange ? yRange[1] + 10 : undefined,
        ticks: { display: false },
        grid: { display: false },
      },
    },
  };
}

/** Heart-rate line chart: red, no gridlines/ticks, median/max reference lines. */
export function HeartrateChart({ points }: { points: ActivityHeartratePoint[] }) {
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  const data = points
    .filter((p) => p.heartrate_bpm != null)
    .map((p) => ({ x: p.minute, y: p.heartrate_bpm as number }));
  void themeVersion;
  const lineColor = tokenRgb('--danger', isDark);

  const values = data.map((d) => d.y);
  const med = median(values);
  const mx = values.length ? Math.max(...values) : 0;
  const refLines = [
    { value: med, label: `Median ${Math.round(med)}`, color: lineColor },
    { value: mx, label: `Max ${Math.round(mx)}`, color: lineColor },
  ];

  const dataset: ChartDataset<'line', XY[]> = {
    label: 'Heart rate',
    data,
    borderColor: lineColor,
    backgroundColor: 'transparent',
    borderWidth: 2,
    pointRadius: 0,
    pointHoverRadius: 3,
    tension: 0.2,
  };
  return (
    <div className="flex flex-col" aria-label="Heart rate over time" role="img">
      <span className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Heart Rate</span>
      <div className="w-full" style={{ height: 180 }}>
        <Line
          data={{ datasets: [dataset] }}
          options={lineOptions(range(data))}
          plugins={[referenceLinesPlugin(refLines)]}
        />
      </div>
    </div>
  );
}

/** Pace line chart: green, no gridlines/ticks, 8:00 and 9:00 reference lines. */
export function PaceChart({ points }: { points: ActivityPacePoint[] }) {
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  const data = points
    .filter((p) => p.pace_sec_per_mi != null)
    .map((p) => ({ x: p.minute, y: p.pace_sec_per_mi as number }));
  void themeVersion;
  const lineColor = tokenRgb('--ok', isDark);

  const refLines = [
    { value: 480, label: '8:00', color: lineColor },
    { value: 540, label: '9:00', color: lineColor },
  ];

  const dataset: ChartDataset<'line', XY[]> = {
    label: 'Pace',
    data,
    borderColor: lineColor,
    backgroundColor: 'transparent',
    borderWidth: 2,
    pointRadius: 0,
    pointHoverRadius: 3,
    tension: 0.2,
  };
  return (
    <div className="flex flex-col" aria-label="Pace over time" role="img">
      <span className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Pace</span>
      <div className="w-full" style={{ height: 180 }}>
        <Line
          data={{ datasets: [dataset] }}
          options={lineOptions(range(data))}
          plugins={[referenceLinesPlugin(refLines)]}
        />
      </div>
    </div>
  );
}
/**
 * Course shape (009-007, T05 / OQ-2 Option B): MapLibre GL line layer on a
 * blank style (no tile dependency — Pi/offline friendly). Renders the
 * `course_path` [lon,lat][] GeoJSON line natively (no equirectangular axis
 * scaling, no Chart.js smoothing). Fills its fixed-height column container.
 * Lazy-inits on mount; rebuilds the data + paint on path/theme change.
 */
export function CoursePathChart({ path }: { path: number[][] }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  void themeVersion; // recompute color on theme toggle

  const lineColor = tokenRgb('--chart-1', isDark);

  // Init once (lazy: component only mounts when path.length > 1).
  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (mapRef.current || !containerRef.current) return;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: { version: 8, sources: {}, layers: [] },
      interactive: false,
      attributionControl: false,
    });
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // Push path data + paint on path/theme change.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const coords = (path || []).filter(
      (p) => Array.isArray(p) && p.length >= 2 && isFinite(p[0]) && isFinite(p[1]),
    );
    const apply = () => {
      const src = map.getSource('course') as maplibregl.GeoJSONSource | undefined;
      const geojson: GeoJSON.FeatureCollection = {
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
      if (src) {
        src.setData(geojson);
      } else {
        map.addSource('course', { type: 'geojson', data: geojson });
        map.addLayer({
          id: 'course-line',
          type: 'line',
          source: 'course',
          paint: { 'line-color': lineColor, 'line-width': 2 },
        });
      }
      map.setPaintProperty('course-line', 'line-color', lineColor);
      if (coords.length > 1) {
        const bounds = coords.reduce(
          (b, c) => b.extend(c as [number, number]),
          new maplibregl.LngLatBounds(coords[0] as [number, number], coords[0] as [number, number]),
        );
        map.fitBounds(bounds, { padding: 8, animate: false });
      }
      map.resize();
    };
    if (map.loaded()) apply();
    else map.once('load', apply);
  }, [path, lineColor]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full"
      aria-label="Course path"
      role="img"
    />
  );
}