/**
 * Tri-tip Temperature + Prediction Chart (T08)
 *
 * Chart.js (react-chartjs-2) plot:
 *   x = grill minutes elapsed since t₀ (MIN recorded_at of each event),
 *   y = internal temperature (°F).
 * Datasets:
 *   - Active event readings: bold line + points (--chart-1).
 *   - Prediction curve: dashed, from t₀ to the 125 °F target crossing.
 *   - Target line at 125 °F (subtle).
 *   - Prior completed events (OQ-2): muted gray reference curves, each
 *     normalized to its own t₀.
 * Colors come from CSS custom-property tokens (--chart-1, --text, --grid) via
 * window.getComputedStyle, rebuilt on theme change (MutationObserver on the
 * `dark` class), matching the DbSizeView pattern.
 */

'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  type ChartDataset,
  type ChartOptions,
  type ChartData,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { useTriTipActive } from '../../../hooks/useTriTip';
import type {
  TriTipPrediction,
  TriTipReading,
  TriTipReferenceEvent,
  TriTipEvent,
} from '../../../lib/types/tri-tip';

ChartJS.register(LinearScale, PointElement, LineElement, Tooltip, Legend);

/** Minutes elapsed between t₀ ISO and a reading ISO. */
function grillMin(t0Iso: string, atIso: string): number {
  return (new Date(atIso).getTime() - new Date(t0Iso).getTime()) / 60_000;
}

function eventT0(event: TriTipEvent, readings: TriTipReading[]): string | null {
  if (event.started_at) return event.started_at;
  const times = readings.map((r) => r.recorded_at).sort();
  return times[0] ?? null;
}

/** SSR-safe token read with light/dark fallbacks (pattern from DbSizeView). */
function tokenRgb(name: string, isDark: boolean): string {
  const fallback =
    name === '--text' ? (isDark ? '241 245 249' : '15 23 42') : isDark ? '148 163 184' : '226 232 240';
  if (typeof window === 'undefined') return `rgb(${fallback})`;
  const value = window.getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value ? `rgb(${value})` : `rgb(${fallback})`;
}

interface TriTipChartProps {
  event: TriTipEvent | null;
  readings: TriTipReading[];
  prediction: TriTipPrediction | null;
  references: TriTipReferenceEvent[];
}

export default function TriTipChart({ event, readings, prediction, references }: TriTipChartProps) {
  // Rebuild chart colors when the theme class toggles.
  const [themeVersion, setThemeVersion] = useState(0);
  useEffect(() => {
    const observer = new MutationObserver(() => setThemeVersion((v) => v + 1));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);

  const isDark =
    typeof window !== 'undefined' &&
    window.document.documentElement.classList.contains('dark');

  const chartData = useMemo(() => {
    if (typeof window === 'undefined') return null;
    const textColor = tokenRgb('--text', isDark);
    const gridColor = tokenRgb('--grid', isDark);
    const accent = tokenRgb('--chart-1', isDark);
    const accentMuted = isDark ? 'rgba(96, 165, 250, 0.45)' : 'rgba(0, 114, 178, 0.45)';
    const grayRef = isDark ? 'rgba(148, 163, 184, 0.35)' : 'rgba(100, 116, 139, 0.35)';

    const t0 = event ? eventT0(event, readings) : null;

    const datasets: ChartDataset<'line', { x: number; y: number }[]>[] = [];

    // Prior completed events: muted gray reference curves, each from its own t0.
    for (const ref of references) {
      const refT0 = ref.event.started_at ?? ref.readings[0]?.recorded_at;
      if (!refT0) continue;
      datasets.push({
        label: `Past · ${ref.event.weight_lbs.toFixed(1)} lb ${ref.event.shape.replace('+', '/')}`,
        data: ref.readings.map((r) => ({ x: r.grill_min, y: r.internal_temp_f })),
        borderColor: grayRef,
        backgroundColor: 'transparent',
        borderWidth: 1.5,
        pointRadius: 2,
        pointHoverRadius: 4,
        borderDash: [4, 4],
        tension: 0.2,
      });
    }

    // Prediction curve (dashed accent), normalized to the active event's t0.
    if (prediction && prediction.curve.length > 0 && t0) {
      datasets.push({
        label: 'Predicted',
        data: prediction.curve.map((p) => ({ x: p.grill_min, y: p.internal_temp_f })),
        borderColor: accentMuted,
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 0,
        borderDash: [6, 4],
        tension: 0.2,
      });
    }

    // Active readings: bold line + points.
    if (t0 && readings.length > 0) {
      datasets.push({
        label: 'Readings',
        data: readings.map((r) => ({ x: grillMin(t0, r.recorded_at), y: r.internal_temp_f })),
        borderColor: accent,
        backgroundColor: accent,
        borderWidth: 3,
        pointRadius: 4,
        pointHoverRadius: 6,
        tension: 0.2,
      });
    }

    if (datasets.length === 0) return null;

    return {
      datasets,
      textColor,
      gridColor,
    };
    // themeVersion participates so colors recompute on theme toggle.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [event, readings, prediction, references, themeVersion]);

  const options = useMemo(() => {
    if (!chartData) return null;
    const opts: ChartOptions<'line'> = {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false },
      plugins: {
        legend: { labels: { color: chartData.textColor, boxWidth: 16, boxHeight: 2 } },
        tooltip: {
          callbacks: {
            title: (items) =>
              items.length ? `Grill minute ${items[0].parsed?.x ?? ''}` : '',
            label: (item) =>
              `${item.dataset.label}: ${Number(item.parsed?.y ?? 0).toFixed(1)}°F`,
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          title: { display: true, text: 'Grill minutes', color: chartData.textColor },
          ticks: { color: chartData.textColor },
          grid: { color: chartData.gridColor, display: true },
        },
        y: {
          type: 'linear',
          suggestedMin: 35,
          suggestedMax: 135,
          title: { display: true, text: 'Internal temp (°F)', color: chartData.textColor },
          ticks: { color: chartData.textColor },
          grid: { color: chartData.gridColor, display: true },
        },
      },
    };
    return opts;
  }, [chartData]);

  if (!chartData || !options) {
    return (
      <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
        <p className="text-gray-500 dark:text-gray-400 text-sm">
          Place the meat on the grill to begin the temperature curve.
        </p>
      </div>
    );
  }

  // Fixed-height container: no layout shift as data arrives.
  return (
    <div className="h-64 sm:h-72 w-full">
      <Line data={{ datasets: chartData.datasets }} options={options} />
    </div>
  );
}
