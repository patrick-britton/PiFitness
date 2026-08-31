/**
 * VolleyballScoreChart
 *
 * Chart.js line chart plotting cumulative score by point sequence
 * (x = 1..N, y = cumulative), one line per team. Line crossings make
 * comebacks visible (AC-7).
 *
 * Styling per human request:
 *   - Scripps Ranch line = #750530 (dark) / #050C46 (light, corrected 006-002)
 *   - Opponent line      = dark gray (#9ca3af) / light gray (#6b7280)
 *   - event-tagged points (006-002): filled 5-point star on the SR line at
 *     the tagged point index regardless of which team scored, labeled with
 *     the event's first letter (A/B/S/D) in the SR line color — above the
 *     star when SR led or tied after that point, below when SR trailed
 *     (Bug T08-5: drawn as a canvas polygon, not Chart.js' asterisk 'star')
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
  ChartDataset,
  ChartOptions,
  Plugin,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { VolleyballGameDetail } from '../../../../lib/types/volleyball';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip);

// --- Event markers (006-002) ------------------------------------------------
// SR dataset carrying its marker config (Bug T08-8): `chart.options` in
// Chart.js v4 is a cached option RESOLVER (verified in dist source) and
// react-chartjs-2's Object.assign mutation does not surface new plugin-
// option keys through it — but the DATA path (setDatasets) provably does.
interface VolleyballSrDataset extends ChartDataset<'line'> {
  eventMarkers: { index: number; letter: string; srLed: boolean }[];
}

// Filled 5-point star polygon (Bug T08-5): Chart.js' built-in 'star'
// pointStyle draws spokes through the center (an asterisk), so the marker
// is drawn by hand — dependency-free.
function drawFivePointStar(
  ctx: CanvasRenderingContext2D,
  cx: number,
  cy: number,
  outer: number
) {
  const inner = outer * 0.5;
  ctx.beginPath();
  for (let k = 0; k < 10; k += 1) {
    const r = k % 2 === 0 ? outer : inner;
    const angle = (Math.PI / 5) * k - Math.PI / 2;
    const x = cx + r * Math.cos(angle);
    const y = cy + r * Math.sin(angle);
    if (k === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  }
  ctx.closePath();
  ctx.fill();
}

// Module-level plugin with a STABLE identity (Bug T08-6): react-chartjs-2
// applies the `plugins` prop only at chart construction, so a memoized
// plugin's closure goes stale. This plugin instead reads the current
// entries and color from chart.options, which react-chartjs-2 propagates
// on every render.
const eventMarkerPlugin: Plugin<'line'> = {
  id: 'volleyballEventMarkers',
  afterDatasetsDraw(chart) {
    // Marker config rides on the SR dataset (Bug T08-8) — chart.data is
    // the path react-chartjs-2 provably propagates on every update.
    const ds = chart.data.datasets[0] as VolleyballSrDataset | undefined;
    if (!ds?.eventMarkers?.length) return;
    const color = typeof ds.borderColor === 'string' ? ds.borderColor : '#050C46';
    const meta = chart.getDatasetMeta(0); // SR dataset
    if (!meta) return;
    const { ctx } = chart;
    ctx.save();
    ctx.fillStyle = color;
    ds.eventMarkers.forEach((e) => {
      const el = meta.data[e.index] as PointElement | undefined;
      if (!el) return;
      drawFivePointStar(ctx, el.x, el.y, 9);
      ctx.font = 'bold 12px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = e.srLed ? 'bottom' : 'top';
      ctx.fillText(e.letter, el.x, e.srLed ? el.y - 12 : el.y + 12);
    });
    ctx.restore();
  },
};

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

  const srColor = isDark ? '#750530' : '#050C46';
  const oppColor = isDark ? '#9ca3af' : '#6b7280';

  // Pre-drawn horizontal axis (T10, human request 2026-08-31): the axis is 28
  // sequence slots wide from the first point and only extends past 28 when
  // more than 28 points are recorded.
  const axisLength = Math.max(28, points.length);

  const data: ChartData<'line'> = useMemo(() => {
    const srScores: number[] = [];
    const oppScores: number[] = [];
    let sr = 0;
    let opponent = 0;

    // 006-002: indices of event-tagged points. Star markers live on the SR
    // line regardless of which team actually scored the tagged point.
    const tagged = new Set<number>();
    const eventMarkers: { index: number; letter: string; srLed: boolean }[] = [];
    points.forEach((p, i) => {
      if (p.scoring_team === 'SR') {
        sr += 1;
      } else {
        opponent += 1;
      }
      srScores.push(sr);
      oppScores.push(opponent);
      if (p.event_type) {
        tagged.add(i);
        // Letter + position rule (OQ-3): above the star when SR led or tied
        // after this point, below when SR trailed. Carried ON the dataset —
        // the data path is what react-chartjs-2 provably propagates (T08-8).
        eventMarkers.push({
          index: i,
          letter: p.event_type.charAt(0),
          srLed: sr >= opponent,
        });
      }
    });

    const lastIndex = srScores.length - 1;
    // Tagged points get a plugin-drawn 5-point star (Bug T08-5), so their
    // default marker is hidden (radius 0) with an enlarged hit radius so
    // tooltips still find them; untagged points keep the current behavior
    // (circle only on the newest). The most-recent circle is overruled when
    // the newest point is tagged.
    const srPointRadius = srScores.map((_, i) =>
      tagged.has(i) ? 0 : i === lastIndex ? 5 : 0
    );
    const srPointHoverRadius = srScores.map((_, i) =>
      tagged.has(i) ? 0 : i === lastIndex ? 6 : 2
    );
    const srPointHitRadius = srScores.map((_, i) => (tagged.has(i) ? 10 : 1));
    const oppPointRadius = oppScores.map((_, i) => (i === lastIndex ? 5 : 0));
    const oppHoverRadius = oppScores.map((_, i) => (i === lastIndex ? 6 : 2));

    // Pad all per-point arrays out to the pre-drawn axis length with null/0
    // values: Chart.js draws nothing for null data, so the lines stop at the
    // last real point and the empty 28-slot tail stays blank (T10).
    const pad = <T,>(xs: T[], filler: T) => [...xs, ...Array(axisLength - xs.length).fill(filler)];
    const srPad = pad(srScores, null);
    const oppPad = pad(oppScores, null);
    const srDataset: VolleyballSrDataset = {
      label: 'Scripps Ranch',
      data: srPad,
      borderColor: srColor,
      backgroundColor: srColor,
      tension: 0.2,
      borderWidth: 2,
      pointRadius: pad(srPointRadius, 0),
      pointHoverRadius: pad(srPointHoverRadius, 0),
      pointHitRadius: pad(srPointHitRadius, 0),
      pointBorderColor: srColor,
      pointBackgroundColor: srColor,
      eventMarkers,
    };

    return {
      labels: Array.from({ length: axisLength }, (_, i) => i + 1),
      datasets: [
        srDataset,
        {
          label: opponentLabel,
          data: oppPad,
          borderColor: oppColor,
          backgroundColor: oppColor,
          tension: 0.2,
          borderWidth: 2,
          pointRadius: pad(oppPointRadius, 0),
          pointHoverRadius: pad(oppHoverRadius, 0),
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
      <Line data={data} options={options} plugins={[eventMarkerPlugin]} />
    </div>
  );
}