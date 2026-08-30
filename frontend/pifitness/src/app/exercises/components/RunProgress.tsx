/**
 * Run Progress
 *
 * Exercises -> Timer Activation: live progress visualization during a run.
 *
 * (a) Interval ring — a circle whose perimeter is continuously retraced every
 *     interval. The stroke is advanced by `elapsedMs / interval` (fraction into
 *     the current interval), so it always moves — even for very long intervals
 *     (e.g., 300 s). One full ring = one interval. The ring's progress is data
 *     (constant-rate fill), so it remains visible under prefers-reduced-motion.
 *
 * (b) Prior-attempt bar — a bar calibrated to the last attempt for this timer:
 *     it fills one increment per elapsed interval and reaches exactly 100% when
 *     the current run's on-pace count equals the prior attempt's paced count,
 *     then continues past 100% (>100% shown). With no prior attempt the bar is
 *     hidden and only the ring runs.
 *
 * Colors come from Tailwind theme tokens (light/dark); no hardcoded hex.
 */

'use client';

import { useViewportStore } from '../../../stores/viewportStore';

interface RunProgressProps {
  /** Fraction elapsed into the CURRENT interval: elapsedMs / (interval*1000). */
  intervalFraction: number;
  /** Whole intervals completed so far (the on-pace rep count). */
  pacedCount: number;
  /** Pacing interval in seconds (for the ring's full rotation = one interval). */
  intervalSeconds: number;
  /** On-pace rep count of the prior attempt, or null when none exists. */
  priorPacedCount: number | null;
  /** Whether the run is active (counting). */
  active: boolean;
}

export default function RunProgress({
  intervalFraction,
  pacedCount,
  intervalSeconds,
  priorPacedCount,
  active,
}: RunProgressProps) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  // Ring geometry: size scales with layout; stroke-dasharray = circumference.
  const ringSize = isDesktop ? 200 : 160;
  const strokeWidth = 10;
  const radius = (ringSize - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;

  // Fraction of the CURRENT interval elapsed (0..1, wrapping each interval).
  const clampedFraction = Math.max(0, Math.min(1, intervalFraction));
  // How much of the ring's perimeter to reveal (as a dash).
  const reveal = circumference * clampedFraction;
  const remainder = circumference - reveal;

  // Prior-attempt bar: fraction = (on-pace reps) / (prior on-pace reps).
  // Reaches exactly 100% when the current paced count equals the prior count,
  // then continues past 100%. Rendered as a width % (clamped for display only;
  // the true value passes 100%).
  const barFraction =
    priorPacedCount != null && priorPacedCount > 0
      ? pacedCount / priorPacedCount
      : null; // no prior attempt -> bar hidden
  const barWidthPct = barFraction != null ? Math.min(barFraction * 100, 100) : 0;
  const overTarget = barFraction != null && barFraction > 1;

  const ringLabel = `${pacedCount} reps`;

  return (
    <div className={`flex flex-col items-center gap-5 ${isDesktop ? 'w-full' : ''}`}>
      {/* (a) Interval ring */}
      <div className="relative" style={{ width: ringSize, height: ringSize }}>
        <svg width={ringSize} height={ringSize} className="-rotate-90" role="img" aria-label={`Interval progress: ${Math.round(clampedFraction * 100)}% of this interval`}>
          {/* track */}
          <circle
            cx={ringSize / 2}
            cy={ringSize / 2}
            r={radius}
            fill="none"
            className="stroke-gray-200 dark:stroke-gray-700"
            strokeWidth={strokeWidth}
          />
          {/* progress arc — the perimeter retraced as the current interval elapses */}
          <circle
            cx={ringSize / 2}
            cy={ringSize / 2}
            r={radius}
            fill="none"
            className="stroke-blue-500 dark:stroke-blue-400 transition-[stroke-dashoffset] duration-300 ease-linear"
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={`${reveal} ${remainder}`}
            strokeDashoffset={0}
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-2xl font-semibold tabular-nums text-gray-900 dark:text-white">
            {active ? ringLabel : '—'}
          </span>
          <span className="sr-only">{intervalSeconds} sec per rep</span>
        </div>
      </div>

      {/* (b) Prior-attempt bar (only when a prior attempt exists) */}
      {barFraction != null && (
        <div className="w-full max-w-sm">
          <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400 mb-1">
            <span>vs last attempt ({priorPacedCount ?? 0} on-pace) </span>
            <span className={overTarget ? 'font-semibold text-blue-600 dark:text-blue-400' : ''}> 
               {Math.round(barFraction * 100)}%
            </span>
          </div>
          <div className="h-2.5 w-full rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
            <div
              className={`h-full rounded-full transition-[width] duration-300 ease-linear ${
                overTarget
                  ? 'bg-blue-500 dark:bg-blue-400'
                  : 'bg-gray-400 dark:bg-gray-500'
              }`}
              style={{ width: `${barWidthPct}%` }}
            />
          </div>
          {overTarget && (
            <p className="mt-1 text-xs font-medium text-blue-600 dark:text-blue-400">
              Past last attempt — keep going!
            </p>
          )}
        </div>
      )}
    </div>
  );
}