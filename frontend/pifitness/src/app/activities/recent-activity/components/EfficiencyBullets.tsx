/**
 * Efficiency Bullets (004-006 T06).
 *
 * Four horizontal bullet charts (current-value bar + reference line at
 * `prior10_avg`) for Cadence, Stride, GCT, and Vert Ratio. Each row is a
 * side-by-side grid: short label LEFT, Chart.js horizontal bar immediately to
 * its right, values beneath the bar. Rendered only when `efficiency != null`
 * — the backend gates that to run/trail-run efforts on a course.
 *
 * Chart.js (react-chartjs-2, the house charting library) draws the bars on
 * canvas in device pixels: no viewBox stretching (which warped the old SVG
 * pills' rounded ends) and a pixel-space inline plugin for the reference
 * line. Colors resolve via `tokenRgb` with theme-aware fallbacks — never a
 * bare `rgb(var(--…))` reference to an undeclared token, which paints nothing
 * (Bug 004-006-T06-1, second recheck). Values are text-labelled so meaning
 * never rides on color alone (design-system.md).
 */
'use client';

import { useEffect, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  type ChartData,
  type ChartOptions,
  type Plugin,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';
import type { ActivityEfficiencyMetric } from '@/lib/types/activity-report';
import { tokenRgb } from '@/lib/token-color';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

export interface EfficiencyBulletDatum {
  label: string;
  metric: ActivityEfficiencyMetric;
  unit: string;
  format: (v: number) => string;
}

function fmt1(v: number): string {
  return v.toFixed(1);
}
function fmt0(v: number): string {
  return String(Math.round(v));
}

export const EFFICIENCY_BULLETS: Array<{
  key: 'avg_cadence' | 'avg_stride_length' | 'avg_gct' | 'avg_vert_ratio';
  label: string;
  unit: string;
  format: (v: number) => string;
}> = [
  { key: 'avg_cadence', label: 'Cadence', unit: 'spm', format: fmt1 },
  { key: 'avg_stride_length', label: 'Stride', unit: 'cm', format: fmt0 },
  { key: 'avg_gct', label: 'GCT', unit: 'ms', format: fmt0 },
  { key: 'avg_vert_ratio', label: 'Vert Ratio', unit: '%', format: fmt1 },
];

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

/** Scale domain for one bullet: [0, max(current, ref) × 1.1], min span 1. */
export function bulletDomain(
  current: number | null,
  ref: number | null,
): [number, number] {
  const peak = Math.max(current ?? 0, ref ?? 0, 0);
  return [0, Math.max(peak * 1.1, 1)];
}

/** Fraction (0–1) of the track a value fills within a domain. */
export function bulletFrac(value: number | null, max: number): number {
  if (value == null || max <= 0) return 0;
  return Math.min(Math.max(value / max, 0), 1);
}

/**
 * A prior-10 reference is usable only when present AND positive (Bug
 * 004-006-T06-2: -99.0 sentinel-poisoned averages arrive negative and must
 * never draw a line or print a nonsense value).
 */
export function refVisible(ref: number | null | undefined): ref is number {
  return ref != null && ref > 0;
}

/** Reference-line position (0–1) within the track; call only when refVisible. */
export function refFraction(current: number | null, ref: number): number {
  const [, hi] = bulletDomain(current, ref);
  return bulletFrac(ref, hi);
}

interface BulletPalette {
  bar: string;
  ref: string;
  refOutline: string;
  tick: string;
  grid: string;
}

function bulletPalette(isDark: boolean): BulletPalette {
  return {
    bar: tokenRgb('--chart-1', isDark),
    ref: tokenRgb('--warn', isDark),
    refOutline: tokenRgb('--text', isDark),
    tick: tokenRgb('--muted', isDark),
    grid: tokenRgb('--grid', isDark),
  };
}

export interface BulletChartSpec {
  data: ChartData<'bar', number[], string>;
  options: ChartOptions<'bar'>;
  plugins: Plugin<'bar'>[];
}

/**
 * Pure builder for one bullet row's Chart.js spec — unit-testable without a
 * canvas. Current value is the horizontal bar; the prior-10 average is a
 * vertical reference line drawn by the `bulletRefLine` inline plugin.
 */
export function buildBulletChart(
  current: number | null,
  ref: number | null,
  max: number,
  palette: BulletPalette,
): BulletChartSpec {
  const data: ChartData<'bar', number[], string> = {
    labels: [''],
    datasets: [
      {
        // Null current (e.g. watch recorded cadence only) → empty dataset so
        // Chart.js draws NO bar stub at the origin; the ref line still draws.
        data: current == null ? [] : [current],
        backgroundColor: palette.bar,
        borderWidth: 0,
        borderSkipped: false,
        borderRadius: 5,
        barThickness: 14,
      },
    ],
  };
  const options: ChartOptions<'bar'> = {
    indexAxis: 'y',
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    layout: { padding: 0 },
    scales: {
      y: { display: false, grid: { display: false } },
      x: { min: 0, max, display: false, grid: { display: false }, ticks: { display: false } },
    },
  };
  const refLine: Plugin<'bar'> = {
    id: 'bulletRefLine',
    afterDatasetsDraw(chart) {
      if (ref == null) return;
      const { ctx, chartArea, scales } = chart;
      if (!chartArea) return;
      const x = scales.x.getPixelForValue(ref);
      if (!Number.isFinite(x)) return;
      ctx.save();
      ctx.strokeStyle = palette.refOutline;
      ctx.lineWidth = 6;
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top - 2);
      ctx.lineTo(x, chartArea.bottom + 2);
      ctx.stroke();
      ctx.strokeStyle = palette.ref;
      ctx.lineWidth = 3;
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top - 2);
      ctx.lineTo(x, chartArea.bottom + 2);
      ctx.stroke();
      ctx.restore();
    },
  };
  return { data, options, plugins: ref != null ? [refLine] : [] };
}

function EfficiencyBulletRow({
  label,
  metric,
  unit,
  format,
  palette,
}: EfficiencyBulletDatum & { palette: BulletPalette }) {
  // Capture the narrowed ref so TS knows it's non-null when showRef.
  const ref = refVisible(metric.prior10_avg) ? metric.prior10_avg : null;
  const [, hi] = bulletDomain(metric.current, ref);
  const spec = buildBulletChart(metric.current, ref, hi, palette);
  const curText =
    metric.current != null ? `${format(metric.current)} ${unit}` : 'no reading';
  const refText =
    ref != null
      ? `prior-10 avg ${format(ref)} ${unit}`
      : metric.prior10_avg != null
        ? 'prior-10 avg unavailable'
        : 'no prior attempts';
  return (
    <div
      data-testid="bullet-row"
      className="grid min-w-0 grid-cols-[88px_minmax(0,1fr)] items-center gap-x-2 gap-y-0.5"
    >
      <p className="truncate text-xs font-medium text-gray-700 dark:text-gray-200">
        {label}
      </p>
      {/*
        Bug 004-006-T06-1 (Chart.js rebuild): the bar AND the reference line
        are canvas-drawn in device pixels (no viewBox stretch). The ref marker
        host carries the `bullet-ref-line` test id when the ref is usable.
      */}
      <div
        className="relative h-6 min-w-0"
        {...(ref != null ? { 'data-testid': 'bullet-ref-line' } : {})}
      >
        <Bar
          data={spec.data}
          options={spec.options}
          plugins={spec.plugins}
          aria-label={`${label}: ${curText}, ${refText}`}
          role="img"
        />
      </div>
      <p className="col-start-2 row-start-2 shrink-0 text-left text-xs tabular-nums text-gray-500 dark:text-gray-400">
        {curText} · {refText}
      </p>
    </div>
  );
}

export function EfficiencyBullets({
  data,
}: {
  data: EfficiencyBulletDatum[];
}) {
  const themeVersion = useThemeVersion();
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');
  void themeVersion; // recompute palette on theme toggle
  const palette = bulletPalette(isDark);
  return (
    <div
      className="mt-4 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4"
      role="group"
      aria-label="Running efficiency vs prior 10 course attempts"
    >
      <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">
        Running efficiency
      </h3>
      <div className="space-y-3">
        {data.map((d) => (
          <EfficiencyBulletRow key={d.label} {...d} palette={palette} />
        ))}
      </div>
    </div>
  );
}
