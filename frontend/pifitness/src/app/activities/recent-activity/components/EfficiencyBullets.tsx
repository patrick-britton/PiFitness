/**
 * Efficiency Bars (004-007 T04; replaces the 004-006 T06 bullet charts).
 *
 * One precise horizontal Chart.js bar row per metric (Cadence, Stride Length,
 * GCT, Vert Ratio): axis centered on the most-recent value (MRAV) with bounds
 * MRAV ± 1.5·|MRAV−PAV|, the bar spans exactly MRAV→PAV, the lower value is
 * labeled at the bar's left end and the higher value at its right end, square
 * corners, no gridlines/axis lines. End-value labels are drawn by a small
 * canvas plugin in the same style as the old reference-line plugin.
 *
 * Bar color is green when the current value improves on the prior-10 average
 * given the metric direction (higher-better: Cadence, Stride Length;
 * lower-better: GCT, Vert Ratio) and red when worse. When MRAV equals PAV no
 * bar is drawn — the row shows the metric value plus deemphasized
 * "No change vs prior" text (OQ-3). Missing current or prior-10 values fall
 * back to the existing text-only rows ("no reading / prior-10 avg
 * unavailable / no prior attempts", AC-3). Values are text-labelled so
 * meaning never rides on color alone (design-system.md).
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
import {
  efficiencyBarGeometry,
  type EfficiencyBarGeometry,
} from '@/lib/efficiency-bar-geometry';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

export interface EfficiencyBulletDatum {
  label: string;
  metric: ActivityEfficiencyMetric;
  unit: string;
  format: (v: number) => string;
  /** True for Cadence/Stride Length; false for GCT/Vert Ratio. */
  higherBetter: boolean;
}

export type EfficiencyBarDatum = EfficiencyBulletDatum;

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
  higherBetter: boolean;
}> = [
  { key: 'avg_cadence', label: 'Cadence', unit: 'spm', format: fmt1, higherBetter: true },
  { key: 'avg_stride_length', label: 'Stride Length', unit: 'cm', format: fmt0, higherBetter: true },
  { key: 'avg_gct', label: 'GCT', unit: 'ms', format: fmt0, higherBetter: false },
  { key: 'avg_vert_ratio', label: 'Vert Ratio', unit: '%', format: fmt1, higherBetter: false },
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

/**
 * A prior-10 reference is usable only when present AND positive (Bug
 * 004-006-T06-2: -99.0 sentinel-poisoned averages arrive negative and must
 * never draw a bar or print a nonsense value).
 */
export function refVisible(ref: number | null | undefined): ref is number {
  return ref != null && ref > 0;
}

interface BarPalette {
  good: string;
  bad: string;
  tick: string;
}

function barPalette(isDark: boolean): BarPalette {
  return {
    good: tokenRgb('--ok', isDark),
    bad: tokenRgb('--danger', isDark),
    tick: tokenRgb('--muted', isDark),
  };
}

export interface EfficiencyBarSpec {
  data: ChartData<'bar', number[][], string>;
  options: ChartOptions<'bar'>;
  plugins: Plugin<'bar'>[];
}

/**
 * Pure builder for one metric row's Chart.js spec — unit-testable without a
 * canvas. The floating bar spans exactly MRAV→PAV on an MRAV-centered axis;
 * end-value labels are drawn by the `efficiencyEndLabels` inline plugin.
 */
export function buildEfficiencyBar(
  geo: EfficiencyBarGeometry,
  current: number,
  prior: number,
  format: (v: number) => string,
  palette: BarPalette,
): EfficiencyBarSpec {
  const color = geo.improved ? palette.good : palette.bad;
  const data: ChartData<'bar', number[][], string> = {
    labels: [''],
    datasets: [
      {
        // Floating bar: exactly [min(MRAV,PAV), max(MRAV,PAV)].
        data: [[Math.min(current, prior), Math.max(current, prior)]],
        backgroundColor: color,
        borderWidth: 0,
        borderSkipped: false,
        borderRadius: 0,
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
      x: { min: geo.min, max: geo.max, display: false, grid: { display: false }, ticks: { display: false } },
    },
  };
  const endLabels: Plugin<'bar'> = {
    id: 'efficiencyEndLabels',
    afterDatasetsDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      if (!chartArea) return;
      const loX = scales.x.getPixelForValue(Math.min(current, prior));
      const hiX = scales.x.getPixelForValue(Math.max(current, prior));
      if (!Number.isFinite(loX) || !Number.isFinite(hiX)) return;
      const loText = format(Math.min(current, prior));
      const hiText = format(Math.max(current, prior));
      ctx.save();
      ctx.font = '11px sans-serif';
      ctx.textBaseline = 'middle';
      ctx.fillStyle = palette.tick;
      ctx.textAlign = 'right';
      ctx.fillText(loText, Math.max(loX - 4, chartArea.left), (chartArea.top + chartArea.bottom) / 2);
      ctx.textAlign = 'left';
      ctx.fillText(hiText, Math.min(hiX + 4, chartArea.right), (chartArea.top + chartArea.bottom) / 2);
      ctx.restore();
    },
  };
  return { data, options, plugins: [endLabels] };
}

function EfficiencyBulletRow({
  label,
  metric,
  unit,
  format,
  higherBetter,
  palette,
}: EfficiencyBulletDatum & { palette: BarPalette }) {
  const curText =
    metric.current != null ? `${format(metric.current)} ${unit}` : 'no reading';
  const priorRaw = metric.prior10_avg;
  // AC-3 null fallback: text-only row, never a degenerate axis. A usable
  // prior still prints its value (existing behavior); only a missing or
  // sentinel-poisoned prior prints the unavailable/no-attempts text.
  if (metric.current == null || !refVisible(priorRaw)) {
    const refText =
      metric.current == null && refVisible(priorRaw)
        ? `prior-10 avg ${format(priorRaw)} ${unit}`
        : priorRaw != null
          ? 'prior-10 avg unavailable'
          : 'no prior attempts';
    return (
      <div
        data-testid="efficiency-row"
        className="grid min-w-0 grid-cols-[88px_minmax(0,1fr)] items-center gap-x-2 gap-y-0.5"
      >
        <p className="truncate text-xs font-medium text-gray-700 dark:text-gray-200">
          {label}
        </p>
        <p className="col-start-2 shrink-0 text-left text-xs tabular-nums text-gray-500 dark:text-gray-400">
          {curText} · {refText}
        </p>
      </div>
    );
  }
  // OQ-3: MRAV = PAV suppresses the bar.
  const geo = efficiencyBarGeometry(metric.current, priorRaw, higherBetter);
  if (!geo || geo.noChange) {
    return (
      <div
        data-testid="efficiency-row"
        className="grid min-w-0 grid-cols-[88px_minmax(0,1fr)] items-center gap-x-2 gap-y-0.5"
      >
        <p className="truncate text-xs font-medium text-gray-700 dark:text-gray-200">
          {label}
        </p>
        <p className="col-start-2 shrink-0 text-left text-xs tabular-nums text-gray-500 dark:text-gray-400">
          {curText} · <span className="opacity-70">No change vs prior</span>
        </p>
      </div>
    );
  }
  const spec = buildEfficiencyBar(geo, metric.current, priorRaw, format, palette);
  return (
    <div
      data-testid="efficiency-row"
      className="grid min-w-0 grid-cols-[88px_minmax(0,1fr)] items-center gap-x-2 gap-y-0.5"
    >
      <p className="truncate text-xs font-medium text-gray-700 dark:text-gray-200">
        {label}
      </p>
      <div className="relative h-6 min-w-0" data-testid="efficiency-bar">
        <Bar
          data={spec.data}
          options={spec.options}
          plugins={spec.plugins}
          aria-label={`${label}: ${curText}, prior-10 avg ${format(priorRaw)} ${unit}`}
          role="img"
        />
      </div>
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
  const palette = barPalette(isDark);
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
