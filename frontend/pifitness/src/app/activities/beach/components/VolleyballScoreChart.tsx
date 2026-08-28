/**
 * VolleyballScoreChart
 *
 * Chart.js line chart plotting cumulative score by point sequence
 * (x = 1..N, y = cumulative), one line per team. Line crossings make
 * comebacks visible (AC-7).
 *
 * Styling per human request:
 *   - Scripps Ranch line = #750530 (dark) / #05C460 (light)
 *   - Opponent line      = dark gray (#9ca3af) / light gray (#6b7280)
 *   - no legend, no chart title, no gridlines, no axis titles
 *   - marker only on the most recent point of each line
 *   - y-axis fixed to 28 points (begins at 0)
 */
'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  ChartData,
  ChartOptions,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { VolleyballGameDetail } from '../../../../lib/types/volleyball';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip);

interface VolleyballScoreChartProps {
  /** Active game detail (points in recorded order, derived score). */
  detail: VolleyballGameDetail | null;
}

export default function VolleyballScoreChart({ detail }: VolleyballScoreChartProps) {
  // Recompute colors when the theme class toggles.
  const [themeVersion, setThemeVersion] = useState(0);
  useEffect(() => {
    const observer = new MutationObserver(() => setThemeVersion((v) => v + 1));
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });
    return () => observer.disconnect();
  }, []);

  const isDark =
    typeof window !== 'undefined' &&
    window.document.documentElement.classList.contains('dark');

  const points = detail?.points ?? [];
  const opponentLabel = detail?.game.team_b_name ?? 'Opponent';

  const srColor = isDark ? '#750530' : '#05C460';
  const oppColor = isDark ? '#9ca3af' : '#6b7280';

  const data: ChartData<'line'> = useMemo(() => {
    const srScores: number[] = [];
    const oppScores: number[] = [];
    let sr = 0;
    let opponent = 0;

    points.forEach((p) => {
      if (p.scoring_team === 'SR') {
        sr += 1;
      } else {
        opponent += 1;
      }
      srScores.push(sr);
      oppScores.push(opponent);
    });

    const markerRadius = (values: number[]) =>
      values.map((_, i) => (i === values.length - 1 ? 5 : 0));
    const hoverRadius = (values: number[]) =>
      values.map((_, i) => (i === values.length - 1 ? 6 : 2));

    return {
      labels: points.map((_, i) => i + 1),
      datasets: [
        {
          label: 'Scripps Ranch',
          data: srScores,
          borderColor: srColor,
          backgroundColor: srColor,
          tension: 0.2,
          borderWidth: 2,
          pointRadius: markerRadius(srScores),
          pointHoverRadius: hoverRadius(srScores),
          pointBorderColor: srColor,
          pointBackgroundColor: srColor,
        },
        {
          label: opponentLabel,
          data: oppScores,
          borderColor: oppColor,
          backgroundColor: oppColor,
          tension: 0.2,
          borderWidth: 2,
          pointRadius: markerRadius(oppScores),
          pointHoverRadius: hoverRadius(oppScores),
          pointBorderColor: oppColor,
          pointBackgroundColor: oppColor,
        },
      ],
    };
    // themeVersion forces recompute on theme toggle
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [points, opponentLabel, srColor, oppColor, themeVersion]);

  const options: ChartOptions<'line'> = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (ctx) => 'Point ' + ctx[0].label,
            label: (ctx) => ctx.dataset.label + ': ' + ctx.parsed.y,
          },
        },
      },
      scales: {
        x: {
          title: { display: false },
          ticks: { display: false },
          grid: { display: false },
          border: { display: false },
        },
        y: {
          title: { display: false },
          ticks: {
            color: isDark ? '#9ca3af' : '#6b7280',
            stepSize: 4,
          },
          grid: { display: false },
          border: { display: false },
          beginAtZero: true,
          suggestedMax: 28,
          precision: 0,
        },
      },
      transitions: {
        active: {
          animation: {
            duration:
              typeof window !== 'undefined' &&
              window.matchMedia('(prefers-reduced-motion: reduce)').matches
                ? 0
                : 150,
          },
        },
      },
    }),
    [isDark, themeVersion],
  );

  if (!detail || !detail.points.length) {
    return (
      <p className="text-sm text-gray-500 dark:text-gray-400">
        No points scored yet. The comeback chart appears after the first point.
      </p>
    );
  }

  return (
    <div
      className="h-[180px] w-full"
      aria-label="Score progression chart"
      role="img"
    >
      <Line data={data} options={options} />
    </div>
  );
}