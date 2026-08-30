/**
 * Celebration
 *
 * CSS confetti-style celebration shown after an attempt is saved.
 *
 * - standard: fires when the new attempt beats the prior attempt on either
 *   paced or total count.
 * - big: a larger, more prominent animation when total_count exceeds the
 *   all-time-highest total (OQ-2).
 *
 * prefers-reduced-motion: the animation is suppressed (the message still shows).
 */

'use client';

interface CelebrationProps {
  /** Paced count from the saved attempt (vs last attempt). */
  pacedCount: number;
  /** Total count from the saved attempt (vs last attempt + all-time high). */
  totalCount: number;
  /** Prior attempt's paced count, or null. */
  priorPacedCount: number | null;
  /** Prior attempt's total count, or null. */
  priorTotalCount: number | null;
  /** All-time-highest total count (OQ-2), or null. */
  highestScore: number | null;
}

const PIECES = 24;

export default function Celebration({
  pacedCount,
  totalCount,
  priorPacedCount,
  priorTotalCount,
  highestScore,
}: CelebrationProps) {
  // Beat the last attempt on either metric -> standard celebration.
  const beatLast =
    (priorPacedCount != null && pacedCount > priorPacedCount) ||
    (priorTotalCount != null && totalCount > priorTotalCount);
  // Beat the all-time-high TOTAL (OQ-2) -> bigger celebration.
  const beatAllTime = highestScore != null && totalCount > highestScore;

  if (!beatLast && !beatAllTime) return null;

  const isBig = beatAllTime;
  const headline = isBig
    ? 'New all-time best! 🎉'
    : beatLast
      ? 'You beat your last attempt! 🎉'
      : '';

  return (
    <div
      className="relative overflow-hidden rounded-lg border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/30 p-6 text-center"
      role="status"
      aria-live="polite"
      data-celebration
    >
      <h3 className="text-xl font-bold text-blue-900 dark:text-blue-100">{headline}</h3>
      <p className="mt-1 text-sm text-blue-700 dark:text-blue-300">
        {isBig
          ? `Best total ever: ${totalCount} reps (previous best ${highestScore ?? 0}).`
          : `${totalCount} total reps · ${pacedCount} on-pace reps.`}
      </p>

      {/* CSS confetti — suppressed under prefers-reduced-motion */}
      <div className="pointer-events-none absolute inset-0" aria-hidden="true">
        {Array.from({ length: PIECES }).map((_, i) => {
          const left = `${(i * 37) % 100}%`;
          const delay = `${(i % 10) * 0.08}s`;
          const hue = [200, 260, 330, 40][i % 4];
          const size = isBig ? 8 : 6;
          return (
            <span
              key={i}
              className="absolute inline-block rounded-sm"
              style={{
                left,
                top: '-8%',
                width: size,
                height: size,
                backgroundColor: `hsl(${hue} 85% 55%)`,
                animation: `celebration-fall ${isBig ? 2.4 : 1.6}s ${delay} ease-in infinite`,
              }}
            />
          );
        })}
      </div>

      {/* Bigger variant: more pieces + a pulse ring */}
      {isBig && (
        <span className="pointer-events-none absolute left-1/2 top-4 h-20 w-20 -translate-x-1/2 rounded-full border-4 border-yellow-400 animate-ping" aria-hidden="true" />
      )}

      {/* Global keyframes are injected by the app stylesheet (see global.css).
          Defined here for self-containment via a <style> tag scoped to this
          component (not ideal, but the app has no CSS-in-JS). */}
      {/* eslint-disable-next-line react/no-unknown-property */}
      <style>{`
        @keyframes celebration-fall {
          0%   { transform: translateY(-10px) rotate(0deg); opacity: 1; }
          100% { transform: translateY(${isBig ? 260 : 200}px) rotate(540deg); opacity: 0; }
        }
        @media (prefers-reduced-motion: reduce) {
          [data-celebration] * { animation: none !important; }
        }
      `}</style>
    </div>
  );
}