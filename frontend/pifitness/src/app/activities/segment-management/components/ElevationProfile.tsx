'use client';

import { useMemo } from 'react';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Decimation,
  type ChartOptions,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { cumulativeMeters } from '@/lib/segment-trim';
import { tokenRgb } from '@/lib/token-color';
import { isDarkTheme, useThemeVersion } from '@/lib/theme-version';

ChartJS.register(LinearScale, PointElement, LineElement, Tooltip, Decimation);

/**
 * Elevation profile for the trimmed route (009-003 T08, FR-2/FR-4).
 *
 * x is the cumulative distance derived from the returned coordinates (the T03
 * route contract carries no per-point distance), y is the returned smoothed
 * elevation; both are 1:1 with the path points, so the profile always matches
 * the range the gates currently select.
 *
 * Colors come from design tokens and are rebuilt on theme change (Chart.js bakes
 * colors into its config). Plotting is decimated as a backstop for long
 * activities on phones; animation is off for the same reason and to respect
 * reduced-motion users.
 */

export interface ElevationProfileProps {
  /** Trimmed route as `[lon, lat]` pairs. */
  coords: number[][];
  /** Elevations aligned 1:1 with `coords`. */
  elevations: number[];
  /** Container height (reserve it so the layout does not shift). */
  height?: string;
  /** Accessible description of the chart. */
  ariaLabel?: string;
}

export default function ElevationProfile({
  coords,
  elevations,
  height = '200px',
  ariaLabel = 'Elevation profile for the trimmed route',
}: ElevationProfileProps) {
  const themeVersion = useThemeVersion();

  const points = useMemo(() => {
    const dists = cumulativeMeters(coords);
    const n = Math.min(dists.length, elevations.length);
    const out: { x: number; y: number }[] = [];
    for (let i = 0; i < n; i += 1) out.push({ x: Math.round(dists[i]), y: elevations[i] });
    return out;
  }, [coords, elevations]);

  const isDark = isDarkTheme();
  const options: ChartOptions<'line'> = useMemo(() => {
    const text = tokenRgb('--muted', isDark);
    const grid = tokenRgb('--grid', isDark);
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      normalized: true,
      interaction: { mode: 'nearest', intersect: false },
      plugins: {
        legend: { display: false },
        decimation: { enabled: true, algorithm: 'lttb', samples: 600 },
        tooltip: {
          callbacks: {
            label: (ctx) => `${Math.round(ctx.parsed.y ?? 0)} m at ${Math.round(ctx.parsed.x ?? 0)} m`,
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          title: { display: true, text: 'Distance (m)', color: text },
          ticks: { color: text, maxTicksLimit: 8 },
          grid: { color: grid, drawTicks: false },
          border: { color: grid },
        },
        y: {
          title: { display: true, text: 'Elevation (m)', color: text },
          ticks: { color: text, maxTicksLimit: 6 },
          grid: { color: grid, drawTicks: false },
          border: { color: grid },
        },
      },
    } as ChartOptions<'line'>;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark, themeVersion]);

  const data = useMemo(
    () => ({
      datasets: [
        {
          label: 'Elevation',
          data: points,
          borderColor: tokenRgb('--chart-3', isDark),
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          tension: 0.15,
        },
      ],
    }),
    [points, isDark, themeVersion],
  );

  if (points.length < 2) {
    return (
      <div
        className="flex items-center justify-center rounded-md border border-gray-200 dark:border-gray-700 text-sm text-gray-500 dark:text-gray-400"
        style={{ height }}
      >
        No elevation data for this range
      </div>
    );
  }

  return (
    <div className="relative w-full" style={{ height }} role="img" aria-label={ariaLabel}>
      <Line options={options} data={data} />
    </div>
  );
}