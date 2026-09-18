'use client';

import { useMemo, useState } from 'react';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  type ChartDataset,
  type ChartOptions,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import {
  Leaderboard,
  LeaderboardEffort,
} from '@/lib/types/leaderboards';
import { tokenRgb, useThemeVersion } from './chart-theme';
import { bucketOf, type EffortBucket } from '@/lib/effort-buckets';

ChartJS.register(LinearScale, PointElement, LineElement, Tooltip);

/** Chart.js renders one color per dataset, so each recency bucket is its own
 *  dataset; per-point tooltips still read from the underlying effort. */
interface Bucket {
  key: EffortBucket;
  label: string;
  token: string;
  efforts: { x: number; y: number; effort: LeaderboardEffort }[];
}

/** Y-field selector options (009-009 FR-4). */
const Y_FIELDS = [
  { key: 'avg_hr', label: 'Avg HR', unit: 'bpm' },
  { key: 'weight_lb', label: 'Weight', unit: 'lb' },
  { key: 'avg_temp', label: 'Temp', unit: '°F' },
  { key: 'training_load_acute', label: 'Acute Load', unit: '' },
  { key: 'avg_cadence', label: 'Cadence', unit: 'spm' },
  { key: 'avg_vert_osc', label: 'Vert Osc', unit: 'cm' },
  { key: 'vo2_max_value', label: 'VO2', unit: '' },
  { key: 'fat_pct', label: 'Fat %', unit: '%' },
] as const;

type YFieldKey = (typeof Y_FIELDS)[number]['key'];

const fmt = (v: number | null | undefined, digits = 1): string =>
  v == null || !Number.isFinite(v) ? '—' : String(Number(v.toFixed(digits)));

const dateLabel = (iso: string): string => {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${d.getDate()}-${months[d.getMonth()]}-${d.getFullYear()}`;
};

/**
 * Lollipop distribution of a segment's efforts (009-009 T04, AC-6; OQ-2:
 * ALWAYS all efforts, independent of the leaderboard's Range selector).
 * X = total time, fastest → slowest, 1-minute ticks; Y = the selected field,
 * scaled to the recorded min/max (null values plot at the recorded minimum
 * without driving the axis to 0). Thin stem from the baseline to a circle
 * marker at the value. Bucket colors: most recent = bright red, training
 * cycle = dark blue, last 365 = light blue, all time = gray; bucket names
 * ride along in tooltips and the legend (never color alone).
 */
export default function EffortLollipop({ data }: { data: Leaderboard }) {
  const themeVersion = useThemeVersion();
  void themeVersion; // re-render on theme change so Chart.js re-reads tokens
  const [field, setField] = useState<YFieldKey>('avg_hr');
  const fieldDef = Y_FIELDS.find((f) => f.key === field) ?? Y_FIELDS[0];

  const buckets = useMemo<Bucket[]>(() => {
    const out: Bucket[] = [
      { key: 'recent', label: 'Most Recent', token: '--lb-recent', efforts: [] },
      { key: 'cycle', label: 'Training Cycle', token: '--lb-cycle', efforts: [] },
      { key: '365', label: 'Last 365', token: '--lb-365', efforts: [] },
      { key: 'alltime', label: 'All Time', token: '--lb-alltime', efforts: [] },
    ];
    const byKey = Object.fromEntries(out.map((b) => [b.key, b]));
    for (const e of data.efforts) {
      // Bug 009-009-T17-1: membership comes from the view's `cycle_name`
      // (OQ-4), never from a rank being non-null — every rank column is
      // populated for every row.
      const bucket = bucketOf(e);
      (byKey[bucket] as Bucket).efforts.push({ x: e.elapsed_duration_s, y: 0, effort: e });
    }
    const recorded = data.efforts
      .map((e) => e[field])
      .filter((v): v is number => v != null && Number.isFinite(v));
    const min = recorded.length ? Math.min(...recorded) : 0;
    // Null/unrecorded values plot at the recorded minimum — never 0 (AC-6).
    for (const b of out) for (const p of b.efforts) p.y = safeVal(p.effort, field, min);
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, field]);

  function safeVal(e: LeaderboardEffort, key: YFieldKey, fallback: number): number {
    const v = e[key];
    return v != null && Number.isFinite(v) ? v : fallback;
  }

  // Read once per render; `themeVersion` triggers the re-render on theme
  // toggle, so memos keyed on isDark rebuild their token colors.
  const isDark = typeof window !== 'undefined' && document.documentElement.classList.contains('dark');

  const yExtents = useMemo(() => {
    const recorded = data.efforts
      .map((e) => e[field])
      .filter((v): v is number => v != null && Number.isFinite(v));
    if (!recorded.length) return null;
    return { min: Math.min(...recorded), max: Math.max(...recorded) };
  }, [data, field]);

  const xExtents = useMemo(() => {
    const times = data.efforts.map((e) => e.elapsed_duration_s).filter((t) => Number.isFinite(t));
    if (!times.length) return null;
    return { min: Math.min(...times), max: Math.max(...times) };
  }, [data]);

  // Lollipop geometry (Bug 009-009-T12-1): one marker dataset per bucket
  // (showLine:false — isolated circles, never joined) plus one 2-vertex stem
  // dataset per effort running vertically from the axis floor to the value.
  const datasets = useMemo<ChartDataset<'line', { x: number; y: number }[]>[]>(() => {
    const pad = yExtents && yExtents.max > yExtents.min ? (yExtents.max - yExtents.min) * 0.05 : 1;
    const floor = yExtents ? yExtents.min - pad : 0;
    const out: ChartDataset<'line', { x: number; y: number }[]>[] = [];
    for (const b of buckets) {
      if (!b.efforts.length) continue;
      const color = tokenRgb(b.token, isDark);
      out.push({
        label: b.label,
        data: b.efforts,
        // Markers: isolated circles at each value (no connecting line).
        showLine: false,
        pointRadius: 4,
        pointHoverRadius: 6,
        pointBackgroundColor: color,
        pointBorderColor: color,
        fill: false,
      } satisfies ChartDataset<'line', { x: number; y: number }[]>);
      for (const p of b.efforts) {
        out.push({
          // Stem: thin vertical line from the axis floor to the marker.
          data: [
            { x: p.x, y: floor },
            { x: p.x, y: p.y },
          ],
          showLine: true,
          borderWidth: 1,
          borderColor: color,
          pointRadius: 0,
          pointHoverRadius: 0,
          fill: false,
          tension: 0,
        } satisfies ChartDataset<'line', { x: number; y: number }[]>);
      }
    }
    return out;
  }, [buckets, yExtents, isDark]);

  const options = useMemo<ChartOptions<'line'>>(() => {
    const pad = yExtents && yExtents.max > yExtents.min ? (yExtents.max - yExtents.min) * 0.05 : 1;
    return {
      responsive: true,
      animation: false,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          // Stems are hover-silent (pointRadius 0); still, filter tooltips
          // to marker datasets only so each effort shows exactly one entry.
          filter: (item) => (item.dataset.pointRadius as number) > 0,
          callbacks: {
            title: (items) => {
              const raw = items[0]?.raw as { x: number; y: number; effort: LeaderboardEffort } | undefined;
              return raw ? dateLabel(raw.effort.start_time_utc) : '';
            },
            label: (item) => {
              const raw = item.raw as { x: number; y: number; effort: LeaderboardEffort };
              const mins = Math.floor(raw.x / 60);
              const secs = Math.round(raw.x % 60);
              return [
                `Time: ${mins}:${String(secs).padStart(2, '0')}`,
                `${fieldDef.label}: ${fmt(raw.effort[field])}${fieldDef.unit ? ` ${fieldDef.unit}` : ''}`,
                `Bucket: ${item.dataset.label}`,
              ];
            },
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          min: xExtents ? xExtents.min : undefined,
          max: xExtents ? xExtents.max : undefined,
          title: { display: true, text: 'Total Time (fastest → slowest)', color: tokenRgb('--muted', isDark) },
          ticks: {
            color: tokenRgb('--muted', isDark),
            // 1-minute tick marks (AC-6).
            stepSize: 60,
            callback: (v) => `${Math.floor(Number(v) / 60)}m`,
          },
          grid: { color: tokenRgb('--grid', isDark) },
        },
        y: {
          type: 'linear',
          title: {
            display: true,
            text: `${fieldDef.label}${fieldDef.unit ? ` (${fieldDef.unit})` : ''}`,
            color: tokenRgb('--muted', isDark),
          },
          min: yExtents ? yExtents.min - pad : undefined,
          max: yExtents ? yExtents.max + pad : undefined,
          ticks: { color: tokenRgb('--muted', isDark) },
          grid: { color: tokenRgb('--grid', isDark) },
        },
      },
    };
  }, [field, fieldDef, yExtents, xExtents, isDark]);

  if (!data.efforts.length) return null;

  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-3">
        <h3 className="text-sm font-semibold text-gray-900 dark:text-white">Effort Distribution</h3>
        <div className="flex items-center gap-2">
          <label htmlFor="lolli-field" className="text-xs font-medium text-gray-500 dark:text-gray-400">
            Field
          </label>
          <select
            id="lolli-field"
            value={field}
            onChange={(e) => setField(e.target.value as YFieldKey)}
            className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {Y_FIELDS.map((f) => (
              <option key={f.key} value={f.key}>
                {f.label}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="h-56 md:h-64">
        <Line data={{ datasets }} options={options} />
      </div>
      {/* Bucket names ride along in the legend — meaning is never color alone. */}
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500 dark:text-gray-400">
        {buckets
          .filter((b) => b.efforts.length > 0)
          .map((b) => (
            <span key={b.key} className="inline-flex items-center gap-1.5">
              <span
                aria-hidden="true"
                className="inline-block w-2 h-2 rounded-full"
                style={{ backgroundColor: tokenRgb(b.token, isDark) }}
              />
              {b.label} ({b.efforts.length})
            </span>
          ))}
      </div>
    </div>
  );
}
