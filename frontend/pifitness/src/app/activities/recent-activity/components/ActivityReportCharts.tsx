/**
 * Activity Report Charts (009-006, reworked 004-006 T05).
 *
 * Chart.js (react-chartjs-2) plots fed by the pre-aggregated distance-keyed series:
 *   - Elevation: bare area chart, no labels/gridlines/titles/axis lines,
 *     x = distance (m) 0 → activity length.
 *   - Heart rate: red line, no gridlines/ticks, median/max reference lines,
 *     x = distance (m) 0 → activity length. Stacked directly above elevation
 *     at 0 px gap so the pair reads as a single chart.
 *   - Course shape: MapLibre GL line layer (no tiles — Pi/offline friendly).
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
} from '@/lib/types/activity-report';
import { tokenRgb } from '@/lib/token-color';

ChartJS.register(LinearScale, PointElement, LineElement, Filler, Tooltip, Legend);

type XY = { x: number; y: number };

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
 * Elevation area chart (004-006 T05): uncolored, no labels/gridlines/titles/
 * axis lines. x = distance (m), fixed 0 → xMax (activity length); y = min−10
 * → max+10 m. Zero padding/margin — the parent stacks it flush beneath HR.
 */
export function ElevationChart({
  points,
  xMax,
}: {
  points: ActivityElevationPoint[];
  xMax: number;
}) {
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  const data = points.map((p) => ({ x: p.distance_m, y: p.elevation_m }));
  const [mn, mx] = range(data) ?? [0, 0];
  const lineColor = tokenRgb('--muted', isDark);
  const fillColor = isDark ? 'rgba(148,163,184,0.25)' : 'rgba(107,114,128,0.25)';
  void themeVersion; // participate so colors rebuild on toggle

  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    layout: { padding: 0 },
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    scales: {
      x: { type: 'linear', display: false, min: 0, max: xMax, ticks: { display: false }, grid: { display: false }, border: { display: false } },
      y: { type: 'linear', display: false, min: mn - 10, max: mx + 10, ticks: { display: false }, grid: { display: false }, border: { display: false } },
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
/** Shared line-chart options builder (004-006 T05): x = distance 0 → xMax,
 *  no gridlines, no ticks, no axis labels/lines. */
function lineOptions(
  yRange: [number, number] | null,
  xMax: number,
): ChartOptions<'line'> {
  return {
    responsive: true,
    maintainAspectRatio: false,
    layout: { padding: 0 },
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    scales: {
      x: {
        type: 'linear',
        min: 0,
        max: xMax,
        ticks: { display: false },
        grid: { display: false },
        border: { display: false },
      },
      y: {
        type: 'linear',
        min: yRange ? yRange[0] - 10 : undefined,
        max: yRange ? yRange[1] + 10 : undefined,
        ticks: { display: false },
        grid: { display: false },
        border: { display: false },
      },
    },
  };
}

/** Heart-rate line chart (004-006 T05): red, median/max reference lines.
 *  Full width; x = distance (m) 0 → xMax. Zero bottom padding/margin so the
 *  elevation area below reads as one continuous chart (0 px seam). */
export function HeartrateChart({
  points,
  xMax,
}: {
  points: ActivityHeartratePoint[];
  xMax: number;
}) {
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  const data = points
    .filter((p) => p.heartrate_bpm != null)
    .map((p) => ({ x: p.distance_m, y: p.heartrate_bpm as number }));
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
    <div className="flex flex-col p-0 m-0" aria-label="Heart rate over distance" role="img">
      <span className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Heart Rate</span>
      <div className="w-full p-0 m-0" style={{ height: 180 }}>
        <Line
          data={{ datasets: [dataset] }}
          options={lineOptions(range(data), xMax)}
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