'use client';

import { useCallback, useMemo, useRef, useEffect } from 'react';
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
 * colors into its config). The primary line is `--chart-1` (blue) and a supplied
 * comparison line is `--chart-2` (orange), dashed (009-003 T35, AC-24). Note
 * that the dash is now a PLOT-ONLY cue: 009-010 T11 retired it on the map
 * (OQ-5 — both map lines solid, the arrow shapes and legend text carry the
 * series and their direction), so the plot keeps the dash because a Chart.js
 * legend names both curves in words and colour alone would not. Whether the
 * plot should follow the map and go solid is Open Question OQ-6 in the 009-010
 * design. Plotting is decimated as a backstop for long activities on phones;
 * animation is off for the same reason and to respect reduced-motion users.
 */

/** Token of the primary line — the blue the route map draws the route in (AC-24). */
export const PRIMARY_COLOR_TOKEN = '--chart-1';
/** Token of the comparison line — the orange the map's candidate overlay uses (AC-24). */
export const COMPARISON_COLOR_TOKEN = '--chart-2';
/**
 * Dash pattern of the comparison line (009-003 T35, AC-24), kept on the plot
 * only: Chart.js dashes are absolute px lengths, and the map's `line-dasharray`
 * was retired in 009-010 T11 (OQ-5) — see the module comment and OQ-6.
 */
const COMPARISON_DASH = [6, 4];

/** Points of one series: cumulative distance (x, rounded m) vs smoothed elevation (y). */
function toPoints(coords: number[][], elevations: number[]): { x: number; y: number }[] {
  const dists = cumulativeMeters(coords);
  const n = Math.min(dists.length, elevations.length);
  const out: { x: number; y: number }[] = [];
  for (let i = 0; i < n; i += 1) out.push({ x: Math.round(dists[i]), y: elevations[i] });
  return out;
}

/** Optional second series (009-003 T34/T35): a comparison activity's elevation. */
export interface ElevationComparison {
  /** Comparison route as `[lon, lat]` pairs (its own axis, from its own start). */
  coords: number[][];
  /** Elevations aligned 1:1 with `coords`. */
  elevations: number[];
  /** Legend/aria name of the comparison series (e.g. "Candidate 12345"). */
  label: string;
  /** Token for the comparison line (defaults to `--chart-2`). */
  colorToken?: string;
}

export interface ElevationProfileProps {
  /** Trimmed route as `[lon, lat]` pairs. */
  coords: number[][];
  /** Elevations aligned 1:1 with `coords`. */
  elevations: number[];
  /** Container height (reserve it so the layout does not shift). */
  height?: string;
  /** Accessible description of the chart. */
  ariaLabel?: string;
  /** Legend name of the primary series (defaults to "Elevation"). */
  label?: string;
  /** Token for the primary line (defaults to `--chart-1`, AC-24). */
  colorToken?: string;
  /** Optional dashed comparison series; omitted or null draws the primary line alone. */
  comparison?: ElevationComparison | null;
  /**
   * Picked distance in meters (009-003 T39b, AC-26): when set, a thin
   * vertical indicator is drawn at that x. Absent = no indicator dataset.
   */
  pickDistance?: number | null;
  /**
   * Optional plot pick report (009-003 T39b; click-only since 009-010 T05): called
   * with the CLICKED x distance (meters) derived from
   * `chart.scales.x.getValueForPixel` — never dataset element indices, which the
   * enabled `lttb` decimation invalidates. A click is the only gesture reported,
   * so the caller's markers stay put while the pointer moves away.
   */
  onPickDistance?: (distanceM: number) => void;
}

export default function ElevationProfile({
  coords,
  elevations,
  height = '200px',
  ariaLabel = 'Elevation profile for the trimmed route',
  label = 'Elevation',
  colorToken = PRIMARY_COLOR_TOKEN,
  comparison = null,
  pickDistance = null,
  onPickDistance,
}: ElevationProfileProps) {
  const themeVersion = useThemeVersion();

  /**
   * Latest plot-pick callback (009-003 T39b): kept in a ref so a new inline
   * identity never rebuilds the chart options.
   */
  const onPickDistanceRef = useRef(onPickDistance);
  useEffect(() => {
    onPickDistanceRef.current = onPickDistance;
  });

  const points = useMemo(() => toPoints(coords, elevations), [coords, elevations]);
  const comparisonPoints = useMemo(
    () => (comparison ? toPoints(comparison.coords, comparison.elevations) : []),
    [comparison],
  );
  const hasComparison = comparisonPoints.length > 1;

  /**
   * Data limit of the distance axis (009-003 T28/AC-18): without an explicit
   * `max`, Chart.js rounds the linear axis up to the next "nice" tick — a
   * 3,228 m activity draws 3,500 m of empty plot. The axis terminates exactly
   * at the last plotted distance instead.
   */
  const maxX = useMemo(() => {
    const limits = [points, comparisonPoints]
      .map((series) => (series.length > 0 ? series[series.length - 1].x : null))
      .filter((x): x is number => x != null);
    return limits.length > 0 ? Math.max(...limits) : undefined;
  }, [points, comparisonPoints]);

  const isDark = isDarkTheme();

  const options: ChartOptions<'line'> = useMemo(() => {
    const text = tokenRgb('--muted', isDark);
    const grid = tokenRgb('--grid', isDark);
    const reportClick = (
      event: { x?: number | null },
      _elements: unknown,
      chart: { scales?: { x?: { getValueForPixel?: (px: number) => number } } },
    ) => {
      const handler = onPickDistanceRef.current;
      if (!handler) return;
      // Chart.js calls onClick(event, elements, chart): the event's own x/y
      // are ALREADY chart-area pixels (CoreChartOptions.onClick), so the
      // pixel is read from the event — never from `native.offsetX`, which is
      // a DOM offset that disagrees with the chart area after layout/padding.
      const px = typeof event?.x === 'number' ? event.x : null;
      if (px == null || !Number.isFinite(px)) return;
      const value = chart?.scales?.x?.getValueForPixel?.(px);
      if (value == null || !Number.isFinite(value)) return;
      handler(value);
    };
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      normalized: true,
      interaction: { mode: 'nearest', intersect: false },
      // Click-only (009-010 T05, FR-2): no `onHover`, so the pick — and with it
      // the emphasized map markers — moves only when the user clicks a distance.
      onClick: reportClick,
      plugins: {
        // The legend names both series only when there are two; a lone profile
        // keeps the cleaner legend-free look (T35, AC-24).
        legend: {
          display: hasComparison,
          labels: {
            color: text,
            boxWidth: 12,
            boxHeight: 12,
            // The pick indicator is a guide line, not a series — never legend it.
            filter: (item) => item.text !== 'Picked point',
          },
        },
        decimation: { enabled: true, algorithm: 'lttb', samples: 600 },
        tooltip: {
          // The pick indicator is a guide line, not data — never tooltip it.
          filter: (item) => item.dataset.label !== 'Picked point',
          callbacks: {
            // Name the series so a two-activity tooltip is unambiguous.
            label: (ctx) =>
              `${ctx.dataset.label}: ${Math.round(ctx.parsed.y ?? 0)} m at ${Math.round(ctx.parsed.x ?? 0)} m`,
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          ...(maxX != null ? { max: maxX } : {}),
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
  }, [isDark, themeVersion, maxX, hasComparison]);

  const data = useMemo(() => {
    const elevValues = [
      ...points.map((p) => p.y),
      ...comparisonPoints.map((p) => p.y),
    ];
    const lo = elevValues.length > 0 ? Math.min(...elevValues) : 0;
    const hi = elevValues.length > 0 ? Math.max(...elevValues) : 1;
    return {
      datasets: [
        {
          label,
          data: points,
          borderColor: tokenRgb(colorToken, isDark),
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          tension: 0.15,
        },
        ...(hasComparison && comparison
          ? [
              {
                label: comparison.label,
                data: comparisonPoints,
                borderColor: tokenRgb(comparison.colorToken ?? COMPARISON_COLOR_TOKEN, isDark),
                borderDash: COMPARISON_DASH,
                borderWidth: 2,
                pointRadius: 0,
                fill: false,
                tension: 0.15,
              },
            ]
          : []),
        // Pick indicator (T39b): a thin vertical line at the picked distance.
        // A 2-point dataset, not a new chart type; hidden from legend/tooltip.
        ...(pickDistance != null && Number.isFinite(pickDistance)
          ? [
              {
                label: 'Picked point',
                data: [
                  { x: pickDistance, y: lo },
                  { x: pickDistance, y: hi },
                ],
                borderColor: tokenRgb('--text', isDark),
                borderWidth: 1,
                pointRadius: 0,
                fill: false,
                tension: 0,
              },
            ]
          : []),
      ],
    };
  }, [points, comparisonPoints, hasComparison, comparison, label, colorToken, isDark, themeVersion, pickDistance]);

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